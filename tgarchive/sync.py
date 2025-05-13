import asyncio
from datetime import datetime, timezone
from io import BytesIO
from sys import exit
from typing import Dict, Any
import json
import logging
import os
import tempfile
import shutil
import time
import ssl

from PIL import Image
import pytz
from telethon import TelegramClient, errors, sync, utils
from telethon.tl.functions.channels import GetFullChannelRequest
import telethon.tl.types

from .db import ArchivedChatInfo, User, Message, Media, ForwardedMessageMetadata


class Sync:
    """
    Sync iterates and receives messages from the Telegram group to the
    local SQLite DB.
    """

    config = {}
    db = None
    fetched_user_ids = set()

    def __init__(self, config, session_file, db):
        self.config = config
        self.db = db

        self.client = self.new_client(session_file, config)

        if not os.path.exists(self.config["media_dir"]):
            os.mkdir(self.config["media_dir"])

    def sync(self, ids=None, from_id=None):
        """
        Sync syncs messages from Telegram from the last synced message
        into the local SQLite DB.
        """

        if ids:
            last_id, last_date = (ids, None)
            logging.info("fetching message id={}".format(ids))
        elif from_id:
            last_id, last_date = (from_id, None)
            logging.info("fetching from last message id={}".format(last_id))
        else:
            last_id, last_date = self.db.get_last_message_id()
            logging.info(
                "fetching from last message id={} ({})".format(last_id, last_date)
            )

        group_id = self._get_group_id(self.config["group"])
        old_archived_chat_info = self.db.get_last_archived_chat_info()
        new_archived_chat_info = self._get_chat_info(group_id)

        # Check if group match
        if (
            old_archived_chat_info
            and old_archived_chat_info.peer_id != new_archived_chat_info.peer_id
        ):
            logging.critical(
                "Group in configuration doesn't match the archived chat ID. Please check your group configuration or create a new site."
            )
            exit(1)

        # Check if chat info needs to be updated
        need_update = (
            not old_archived_chat_info
            or old_archived_chat_info.title != new_archived_chat_info.title
            or old_archived_chat_info.peername != new_archived_chat_info.peername
            or old_archived_chat_info.desc != new_archived_chat_info.desc
            or old_archived_chat_info.avatar != new_archived_chat_info.avatar
        )

        if need_update:
            self.db.insert_archived_chat_info(new_archived_chat_info)
            logging.info(
                "Updated chat info for group: %s", new_archived_chat_info.title
            )
        else:
            updated_date_info = ArchivedChatInfo(
                id=old_archived_chat_info.id,
                peer_id=old_archived_chat_info.peer_id,
                peername=old_archived_chat_info.peername,
                title=old_archived_chat_info.title,
                desc=old_archived_chat_info.desc,
                archive_date=datetime.now().astimezone(),
                avatar=old_archived_chat_info.avatar,
            )
            self.db.update_archived_chat_info(updated_date_info)

        n = 0
        while True:
            has = False
            for m in self._get_messages(
                group_id, offset_id=last_id if last_id else 0, ids=ids
            ):
                if not m:
                    continue

                has = True

                # Insert the records into DB.
                if m.user.id not in self.fetched_user_ids:
                    self.db.insert_user(m.user)
                    self.fetched_user_ids.add(m.user.id)

                if m.media:
                    self.db.insert_media(m.media)

                if m.forwarded_message_metadata:
                    self.db.insert_forwarded_message_metadata(
                        m.forwarded_message_metadata
                    )

                self.db.insert_message(m)

                last_date = m.date
                n += 1
                if n % 300 == 0:
                    logging.info("fetched {} messages".format(n))
                    self.db.commit()

                if 0 < self.config["fetch_limit"] <= n or ids:
                    has = False
                    break

            self.db.commit()
            if has:
                last_id = m.id
                logging.info(
                    "fetched {} messages. sleeping for {} seconds".format(
                        n, self.config["fetch_wait"]
                    )
                )
                time.sleep(self.config["fetch_wait"])
            else:
                break

        self.db.commit()
        if self.config.get("use_takeout", False):
            self.finish_takeout()
        logging.info(
            "finished. fetched {} messages. last message = {}".format(n, last_date)
        )

    def new_client(self, session, config):
        if "proxy" in config and config["proxy"].get("enable"):
            proxy = config["proxy"]
            client = TelegramClient(
                session,
                config["api_id"],
                config["api_hash"],
                proxy=(proxy["protocol"], proxy["addr"], proxy["port"]),
            )
        else:
            client = TelegramClient(session, config["api_id"], config["api_hash"])
        # hide log messages
        # upstream issue https://github.com/LonamiWebs/Telethon/issues/3840
        client_logger = client._log["telethon.client.downloads"]
        client_logger._info = client_logger.info

        def patched_info(*args, **kwargs):
            if (
                args[0] == "File lives in another DC"
                or args[0]
                == "Starting direct file download in chunks of %d at %d, stride %d"
            ):
                return client_logger.debug(*args, **kwargs)
            client_logger._info(*args, **kwargs)

        client_logger.info = patched_info

        client.start()
        if config.get("use_takeout", False):
            for retry in range(3):
                try:
                    takeout_client = client.takeout(finalize=True).__enter__()
                    # check if the takeout session gets invalidated
                    takeout_client.get_messages("me")
                    return takeout_client
                except errors.TakeoutInitDelayError as e:
                    logging.info(
                        "please allow the data export request received from Telegram on your device. "
                        "you can also wait for {} seconds.".format(e.seconds)
                    )
                    logging.info(
                        "press Enter key after allowing the data export request to continue.."
                    )
                    input()
                    logging.info("trying again.. ({})".format(retry + 2))
                except errors.TakeoutInvalidError:
                    logging.info(
                        "takeout invalidated. delete the session.session file and try again."
                    )
            else:
                logging.info("could not initiate takeout.")
                raise (Exception("could not initiate takeout."))
        else:
            return client

    def finish_takeout(self):
        self.client.__exit__(None, None, None)

    def _get_messages(self, group, offset_id, ids=None) -> Message:
        messages = self._fetch_messages(group, offset_id, ids)
        # https://docs.telethon.dev/en/latest/quick-references/objects-reference.html#message
        for m in messages:
            if not m:
                continue

            # Media.
            sticker = None
            med = None
            if m.media:
                # If it's a sticker, get the alt value (unicode emoji).
                if (
                    isinstance(m.media, telethon.tl.types.MessageMediaDocument)
                    and hasattr(m.media, "document")
                    and m.media.document.mime_type == "application/x-tgsticker"
                ):
                    alt = [
                        a.alt
                        for a in m.media.document.attributes
                        if isinstance(a, telethon.tl.types.DocumentAttributeSticker)
                    ]
                    if len(alt) > 0:
                        sticker = alt[0]
                elif isinstance(m.media, telethon.tl.types.MessageMediaPoll):
                    med = self._make_poll(m)
                else:
                    med = self._get_media(m)

            # Message.
            typ = "message"

            # Action message
            action_content = None
            if m.action:
                typ = "action"
                # Process action receiver
                receiver = None
                receiver_label = None
                if m.action_entities and len(m.action_entities) > 0:
                    receiver = m.action_entities[0]
                    if receiver.id != m.sender_id:
                        receiver_label = self._generate_user_label(receiver)

                if isinstance(m.action, telethon.tl.types.MessageActionChatAddUser):
                    if receiver_label:
                        action_content = 'Added user: ' + receiver_label
                    else:
                        action_content = 'Joined'

                elif isinstance(
                    m.action, telethon.tl.types.MessageActionChatJoinedByLink
                ):
                    action_content = 'Joined by link'
                elif isinstance(
                    m.action, telethon.tl.types.MessageActionChatDeleteUser
                ):
                    if receiver_label:
                        action_content = 'Removed user: ' + receiver_label
                    else:
                        action_content = 'Left the group'
                elif isinstance(m.action, telethon.tl.types.MessageActionChannelCreate):
                    action_content = 'Created the group «' + m.action.title + '»'

            yield Message(
                type=typ,
                id=m.id,
                date=m.date,
                edit_date=m.edit_date,
                content=sticker or m.text or action_content,
                reply_to=(
                    m.reply_to_msg_id
                    if m.reply_to and m.reply_to.reply_to_msg_id
                    else None
                ),
                post_author=m.post_author,
                user=self._get_user(m.sender, m.chat),
                forwarded_message_metadata=self._get_forwarded_message_metadata(m),
                media=med,
            )

    def _fetch_messages(self, group, offset_id, ids=None) -> Message:
        try:
            if self.config.get("use_takeout", False):
                wait_time = 0
            else:
                wait_time = None
            messages = self.client.get_messages(
                group,
                offset_id=offset_id,
                limit=self.config["fetch_batch_size"],
                wait_time=wait_time,
                ids=ids,
                reverse=True,
            )
            return messages
        except errors.FloodWaitError as e:
            logging.info("flood waited: have to wait {} seconds".format(e.seconds))

    def _get_user(self, u, chat) -> User:
        """Get or create a User object from Telegram user/chat entity.

        Args:
            u: Telegram user entity
            chat: Telegram chat entity

        Returns:
            User: User object with the entity information
        """
        def create_empty_user(user_id: int) -> User:
            """Creates an empty User object with just the ID."""
            return User(
                id=user_id,
                username=None, 
                first_name=None,
                last_name=None,
                tags=None,
                avatar=None,
                usertype=None
            )

        # Handle chat messages (when u is None but chat exists)
        if u is None:
            if not (chat and chat.title):
                return None

            real_chat_id = self._get_real_user_id(chat)
            if real_chat_id in self.fetched_user_ids:
                return create_empty_user(real_chat_id)
            
            usertype = "chat"
            if isinstance(chat, telethon.tl.types.Channel):
                usertype="channel/mega_group"
            return User(
                id=real_chat_id,
                username=chat.username,
                first_name=chat.title,
                last_name=None,
                tags=["group_self"],
                avatar=self._downloadAvatarForUserOrChat(chat),
                usertype=usertype,
            )

        # Handle normal user messages
        real_user_id = self._get_real_user_id(u)
        if real_user_id in self.fetched_user_ids:
            return create_empty_user(real_user_id)

            # Handle inaccessible channels
        if isinstance(u, telethon.tl.types.ChannelForbidden):
            return User(
                id=real_user_id,
                username=None,
                first_name=u.title,
                last_name=None,
                tags=[],
                avatar=None,
                usertype="channel",
            )
        # Generate tags
        tags = []
        is_normal_user = isinstance(u, telethon.tl.types.User)
        if is_normal_user and u.bot:
            tags.append("bot")
        if hasattr(u, "scam") and u.scam:
            tags.append("scam")
        if hasattr(u, "fake") and u.fake:
            tags.append("fake")
        if is_normal_user and u.deleted:
            tags.append("deleted")
            u.first_name = 'Deleted'
            u.last_name = 'Account'
        # Handle channel
        if isinstance(u, telethon.tl.types.Channel):
            return User(
                id=real_user_id,
                username=u.username,
                first_name='Channel: ',
                last_name=u.title,
                tags=tags,
                avatar=self._downloadAvatarForUserOrChat(u),
                usertype="channel",
            )
        # Handle normal users
        return User(
            id=real_user_id,
            username=u.username,
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=self._downloadAvatarForUserOrChat(u),
            usertype="user",
        )

    def _get_forwarded_message_metadata(self, msg):
        if not msg.forward:
            return None

        # Get forwarded message metadata
        originator_label = None
        source_url = None
        if msg.forward.sender:
            originator_label = self._generate_user_label(msg.forward.sender)
            source_url = f"https://t.me/{msg.forward.sender.username}"
        if msg.forward.chat:
            originator_label = self._generate_user_label(msg.forward.chat)
            source_url = (
                f"https://t.me/c/{msg.forward.chat.id}/{msg.forward.channel_post}"
            )

        if msg.forward.from_name:
            originator_label = msg.forward.from_name

        return ForwardedMessageMetadata(
            id=msg.id,
            originator_label=originator_label,
            source_url=source_url,
            date=msg.forward.date,
            views=msg.views,
            forwarded_count=msg.forwards,
        )

    def _make_poll(self, msg):
        if not msg.media.results or not msg.media.results.results:
            return None

        options = [
            {"label": a.text.text, "count": 0, "correct": False}
            for a in msg.media.poll.answers
        ]

        total = msg.media.results.total_voters
        if msg.media.results.results:
            for i, r in enumerate(msg.media.results.results):
                options[i]["count"] = r.voters
                options[i]["percent"] = r.voters / total * 100 if total > 0 else 0
                options[i]["correct"] = r.correct

        return Media(
            id=msg.id,
            type="poll",
            url=None,
            title=msg.media.poll.question.text,
            description=json.dumps(options),
            thumb=None,
        )

    def _get_media(self, msg):
        if isinstance(
            msg.media, telethon.tl.types.MessageMediaWebPage
        ) and not isinstance(msg.media.webpage, telethon.tl.types.WebPageEmpty):
            return Media(
                id=msg.id,
                type="webpage",
                url=msg.media.webpage.url,
                title=msg.media.webpage.title,
                description=(
                    msg.media.webpage.description
                    if msg.media.webpage.description
                    else None
                ),
                thumb=None,
            )
        elif (
            isinstance(msg.media, telethon.tl.types.MessageMediaPhoto)
            or isinstance(msg.media, telethon.tl.types.MessageMediaDocument)
            or isinstance(msg.media, telethon.tl.types.MessageMediaContact)
        ):
            if self.config["download_media"]:
                # Filter by extensions?
                if len(self.config["media_mime_types"]) > 0:
                    if (
                        hasattr(msg, "file")
                        and hasattr(msg.file, "mime_type")
                        and msg.file.mime_type
                    ):
                        if msg.file.mime_type not in self.config["media_mime_types"]:
                            logging.info(
                                "skipping media #{} / {}".format(
                                    msg.file.name, msg.file.mime_type
                                )
                            )
                            return

                logging.info("downloading media #{}".format(msg.id))
                try:
                    basename, fname, thumb = self._download_media(msg)
                    return Media(
                        id=msg.id,
                        type="photo",
                        url=fname,
                        title=basename,
                        description=None,
                        thumb=thumb,
                    )
                except Exception as e:
                    logging.error("error downloading media: #{}: {}".format(msg.id, e))

    def _fast_download_document(self, msg) -> str:
        """
        Use FastTelethon to download media files in messages and return the saved path.

        Args:
            msg: Message object containing media

        Returns:
            str: Save path of the downloaded file
        """
        from .FastTelethon import download_file

        if isinstance(msg, telethon.tl.types.Message):
            date = msg.date
            document = msg.media
        else:
            date = datetime.now()
            document = msg

        if isinstance(
            document,
            (telethon.tl.types.MessageMediaDocument, telethon.tl.types.Document),
        ):
            if isinstance(document, telethon.tl.types.MessageMediaDocument):
                document = document.document
            kind, possible_names = self.client._get_kind_and_names(document.attributes)
            file = self.client._get_proper_filename(
                os.path.join(self.config["media_dir"]),
                kind,
                utils.get_extension(document),
                date=date,
                possible_names=possible_names,
            )

        try:
            # Use FastTelethon to download
            with open(file, "wb") as out:
                asyncio.get_event_loop().run_until_complete(
                    download_file(
                        client=self.client,
                        location=document,
                        out=out,
                        progress_callback=None,
                    )
                )
            return file

        except Exception as e:
            logging.error(f"Error downloading media: {e}")
            # Clean up temporary files
            if os.path.exists(file):
                os.unlink(file)
            raise

    def _download_media(self, msg) -> [str, str, str]:
        """
        Download a media / file attached to a message and return its original
        filename, sanitized name on disk, and the thumbnail (if any).
        """
        try:
            use_fast = self.config["fast_download"]
            # Choose download method based on configuration
            if use_fast and isinstance(
                msg.media,
                (telethon.tl.types.MessageMediaDocument, telethon.tl.types.Document),
            ):
                fpath = self._fast_download_document(msg)
            else:
                # Use original download method for other media types
                fpath = self.client.download_media(msg, file=tempfile.gettempdir())

            basename = os.path.basename(fpath)
            newname = "{}.{}".format(msg.id, self._get_file_ext(basename))
            shutil.move(fpath, os.path.join(self.config["media_dir"], newname))

            # If it's a photo, download thumbnail
            tname = None
            if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
                tpath = self.client.download_media(
                    msg, file=tempfile.gettempdir(), thumb=1
                )
                tname = "thumb_{}.{}".format(
                    msg.id, self._get_file_ext(os.path.basename(tpath))
                )
                shutil.move(tpath, os.path.join(self.config["media_dir"], tname))

            return basename, newname, tname

        except Exception as e:
            logging.error(f"Error in _download_media: {e}")
            raise

    def _get_file_ext(self, f) -> str:
        if "." in f:
            e = f.split(".")[-1]
            if len(e) < 6:
                return e

        return ".file"

    def _download_avatar(self, user):
        fname = "avatar_{}.jpg".format(user.id)
        fpath = os.path.join(self.config["media_dir"], fname)

        if os.path.exists(fpath):
            return fname

        logging.info("downloading avatar #{}".format(user.id))

        # Download the file into a container, resize it, and then write to disk.
        b = BytesIO()
        profile_photo = self.client.download_profile_photo(user, file=b)
        if profile_photo is None:
            logging.info("user has no avatar #{}".format(user.id))
            return None

        im = Image.open(b)
        im.thumbnail(self.config["avatar_size"], Image.LANCZOS)
        im.save(fpath, "JPEG")

        return fname

    def _get_group_id(self, group):
        """
        which can be a str/int for group ID, group name, or a group username.
        Syncs the Entity cache and returns the Entity ID for the specified group,

        The authorized user must be a part of the group.
        """
        # Get all dialogs for the authorized user, which also
        # syncs the entity cache to get latest entities
        # ref: https://docs.telethon.dev/en/latest/concepts/entities.html#getting-entities
        _ = self.client.get_dialogs()

        try:
            # If the passed group is a group ID, extract it.
            group = int(group)
        except ValueError:
            # Not a group ID, we have either a group name or
            # a group username: @group-username
            pass

        try:
            entity = self.client.get_entity(group)
        except ValueError:
            logging.critical(
                "the group: {} does not exist,"
                " or the authorized user is not a participant!".format(group)
            )
            # This is a critical error, so exit with code: 1
            exit(1)

        return entity.id

    def _get_chat_info(self, group):
        # Get full information based on entity type
        entity = self.client.get_entity(group)
        title = ''
        description = None
        if hasattr(entity, 'megagroup') or hasattr(entity, 'channel'):
            # If it's a supergroup or channel
            full_entity = self.client(GetFullChannelRequest(channel=entity))
            # Get description (about)
            description = full_entity.full_chat.about
        if hasattr(entity, 'first_name') and entity.first_name:
            title += entity.first_name + ' '
        if hasattr(entity, 'last_name') and entity.last_name:
            title += entity.last_name
        if hasattr(entity, 'title') and entity.title:
            title = entity.title
        return ArchivedChatInfo(
            peer_id=self._get_real_user_id(entity),
            peername=entity.username,
            title=title,
            desc=description,
            archive_date=datetime.now().astimezone(timezone.utc),
            avatar=self._downloadAvatarForUserOrChat(entity),
        )

    def _downloadAvatarForUserOrChat(self, entity):
        avatar = None
        if self.config["download_avatars"]:
            try:
                fname = self._download_avatar(entity)
                avatar = fname
            except Exception as e:
                logging.error("error downloading avatar: #{}: {}".format(entity.id, e))
        return avatar

    def _generate_user_label(self, user):
        if hasattr(user, 'deleted') and user.deleted:
            user.first_name = 'Deleted'
            user.last_name = 'Account'
        if hasattr(user, 'title') and user.title:
            user.first_name = 'Channel: '
            user.last_name = user.title

        label = f"{user.first_name or ''} {user.last_name or ''}".strip()
        if getattr(user, "username", None):
            label += f"(@{user.username})"
        if user.id:
            label += f" id: {user.id}"
        return label

    def _get_real_user_id(self, user):
        if isinstance(user, telethon.tl.types.Chat):
            return -user.id
        if isinstance(user, telethon.tl.types.Channel):
            return -user.id - 1000000000000
        if isinstance(user, User):
            if user.usertype == 'chat':
                return -user.id
            if user.usertype == 'channel/mega_group':
                return -user.id - 1000000000000
        return user.id
