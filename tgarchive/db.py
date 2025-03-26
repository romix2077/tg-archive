import json
import math
import os
import sqlite3
from collections import namedtuple
from datetime import datetime, timezone
import pytz
from typing import Iterator, Optional

schema = """
CREATE table archived_chat_info (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    peer_id INTEGER,
    title TEXT,
    peername TEXT,
    desc TEXT,
    archive_date TIMESTAMP,
    avatar TEXT
);
##
CREATE table messages (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    edit_date TIMESTAMP,
    content TEXT,
    reply_to INTEGER,
    post_author TEXT,
    user_id INTEGER,
    forwarded_message_metadata_id INTEGER,
    media_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(forwarded_message_metadata_id) REFERENCES forwarded_message_metadata(id),
    FOREIGN KEY(media_id) REFERENCES media(id)
);
##
CREATE table users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    tags TEXT,
    avatar TEXT,
    usertype TEXT
);
##
CREATE table media (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    thumb TEXT
);
##
CREATE table forwarded_message_metadata (
    id INTEGER PRIMARY KEY, -- Forwarded message metadata ID
    originator_label TEXT, -- Original author label
    source_url TEXT,       -- Source URL of forwarded message
    date TIMESTAMP,        -- Original date of forwarded message
    views INTEGER,         -- View count
    forwarded_count INTEGER -- Forward count
);
"""

ArchivedChatInfo = namedtuple(
    "ArchivedChatInfo",
    [
        "id",
        "peer_id",
        "title",
        "peername",
        "desc",
        "archive_date",
        "avatar",
    ],
    defaults=[None, None, None, None, None, None, None],
)

User = namedtuple(
    "User", ["id", "username", "first_name", "last_name", "tags", "avatar", "usertype"]
)

ForwardedMessageMetadata = namedtuple(
    "ForwardedMessageMetadata",
    ["id", "originator_label", "source_url", "date", "views", "forwarded_count"],
)


Message = namedtuple(
    "Message",
    [
        "id",
        "type",
        "date",
        "edit_date",
        "content",
        "reply_to",
        "post_author",
        "user",
        "forwarded_message_metadata",
        "media",
    ],
)

Media = namedtuple("Media", ["id", "type", "url", "title", "description", "thumb"])

Month = namedtuple("Month", ["date", "slug", "label", "count"])

Day = namedtuple("Day", ["date", "slug", "label", "count", "page"])


def _page(n, multiple):
    return math.ceil(n / multiple)


class DB:
    conn = None
    tz = None

    def __init__(self, dbfile, tz='UTC'):
        # Initialize the SQLite DB. If it's new, create the table schema.
        is_new = not os.path.isfile(dbfile)

        self.conn = sqlite3.Connection(
            dbfile, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )

        # Add the custom PAGE() function to get the page number of a row
        # by its row number and a limit multiple.
        self.conn.create_function("PAGE", 2, _page)

        self.tz = pytz.timezone(tz)

        if is_new:
            for s in schema.split("##"):
                self.conn.cursor().execute(s)
                self.conn.commit()

    def _parse_date(self, d) -> str:
        return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S%z")

    def get_last_message_id(self) -> [int, datetime]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, strftime('%Y-%m-%d %H:%M:%S', date) as "[timestamp]" FROM messages
            ORDER BY id DESC LIMIT 1
        """
        )
        res = cur.fetchone()
        if not res:
            return 0, None

        id, date = res
        return id, date

    def get_timeline(self) -> Iterator[Month]:
        """
        Get the list of all unique yyyy-mm month groups and
        the corresponding message counts per period in chronological order.
        """
        offset = self.tz.utcoffset(datetime.utcnow()).total_seconds() / 3600
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT strftime('%Y-%m-%d %H:%M:%S', datetime(date, ? || ' hours')) as "[timestamp]",
            COUNT(*) FROM messages AS count
            GROUP BY strftime('%Y-%m', datetime(date, ? || ' hours')) ORDER BY date
        """,
            (offset, offset),
        )

        for r in cur.fetchall():
            date = pytz.utc.localize(r[0])

            yield Month(
                date=date,
                slug=date.strftime("%Y-%m"),
                label=date.strftime("%b %Y"),
                count=r[1],
            )

    def get_dayline(self, year, month, limit=500) -> Iterator[Day]:
        """
        Get the list of all unique yyyy-mm-dd days corresponding
        message counts and the page number of the first occurrence of
        the date in the pool of messages for the whole month.
        """
        offset = self.tz.utcoffset(datetime.utcnow()).total_seconds() / 3600

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT strftime("%Y-%m-%d 00:00:00", datetime(date, ? || ' hours')) AS "[timestamp]",
            COUNT(*), PAGE(rank, ?) FROM (
                SELECT ROW_NUMBER() OVER() as rank, date FROM messages
                WHERE strftime('%Y%m', datetime(date, ? || ' hours')) = ? ORDER BY id
            )
            GROUP BY "[timestamp]";
        """,
            (offset, limit, offset, "{}{:02d}".format(year, month)),
        )

        for r in cur.fetchall():
            date = pytz.utc.localize(r[0])

            yield Day(
                date=date,
                slug=date.strftime("%Y-%m-%d"),
                label=date.strftime("%d %b %Y"),
                count=r[1],
                page=r[2],
            )

    def get_messages(
        self, year=None, month=None, last_id=0, limit=500
    ) -> Iterator[Message]:
        """Get messages

        Args:
            year: Year, get all messages if None
            month: Month, get all messages if None
            last_id: Start message ID
            limit: Number of messages to fetch per batch
        """

        query = """
            SELECT messages.id, messages.type, messages.date, messages.edit_date,
            messages.content, messages.reply_to, messages.post_author, messages.user_id, 
            users.username, users.first_name, users.last_name, users.tags, users.avatar,
            users.usertype, messages.forwarded_message_metadata_id,
            fmd.originator_label, fmd.source_url, fmd.date, fmd.views, fmd.forwarded_count,
            media.id, media.type, media.url, media.title, media.description, media.thumb
            FROM messages
            LEFT JOIN users ON (users.id = messages.user_id)
            LEFT JOIN forwarded_message_metadata AS fmd ON (fmd.id = messages.forwarded_message_metadata_id)
            LEFT JOIN media ON (media.id = messages.media_id)
            WHERE messages.id > ?
        """

        params = [last_id]

        # Add time filter condition if year and month are specified
        if year is not None and month is not None:
            query += " AND strftime('%Y%m', messages.date) = ?"
            params.append("{}{:02d}".format(year, month))

        # Add sorting and limits
        if limit:
            query += " ORDER by messages.id LIMIT ?"
            params.append(limit)

        cur = self.conn.cursor()
        cur.execute(query, params)

        for r in cur.fetchall():
            yield self._make_message(r)

    def get_message_count(self, year, month) -> int:
        date = "{}{:02d}".format(year, month)

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM messages WHERE strftime('%Y%m', date) = ?
            """,
            (date,),
        )

        (total,) = cur.fetchone()
        return total

    def get_last_archived_chat_info(self) -> Optional[ArchivedChatInfo]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, peer_id, title, peername, desc, archive_date, avatar
            FROM archived_chat_info 
            ORDER BY id DESC LIMIT 1
            """
        )

        r = cur.fetchone()
        if not r:
            return None

        id, peer_id, title, peername, desc, archive_date, avatar = r
        date_obj = pytz.utc.localize(archive_date) if archive_date else None
        if date_obj:
            date_obj = date_obj.astimezone(self.tz)

        return ArchivedChatInfo(
            id=id,
            peer_id=peer_id,
            title=title,
            peername=peername,
            desc=desc,
            archive_date=date_obj,
            avatar=avatar,
        )

    def insert_archived_chat_info(self, c: ArchivedChatInfo):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO archived_chat_info 
            (peer_id, title, peername, desc, archive_date, avatar)
            VALUES(?, ?, ?, ?, ?, ?) 
            """,
            (
                c.peer_id,
                c.title,
                c.peername,
                c.desc,
                (
                    c.archive_date
                    if c.archive_date
                    else datetime.now().astimezone(timezone.utc)
                ),
                c.avatar,
            ),
        )

    def update_archived_chat_info(self, chat_info: ArchivedChatInfo):
        cur = self.conn.cursor()
        cur.execute(
            """UPDATE archived_chat_info 
               SET peer_id = ?,
                   title = ?,
                   peername = ?,
                   desc = ?,
                   archive_date = ?,
                   avatar = ?
               WHERE id = ?""",
            (
                chat_info.peer_id,
                chat_info.title,
                chat_info.peername,
                chat_info.desc,
                chat_info.archive_date,
                chat_info.avatar,
                chat_info.id,
            ),
        )
        self.conn.commit()

    def insert_user(self, u: User):
        """Insert a user and if they exist, update the fields."""
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO users (id, username, first_name, last_name, tags, avatar, usertype)
            VALUES(?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id)
            DO UPDATE SET username=excluded.username, first_name=excluded.first_name,
                last_name=excluded.last_name, tags=excluded.tags, avatar=excluded.avatar,
                usertype=excluded.usertype
            """,
            (
                u.id,
                u.username,
                u.first_name,
                u.last_name,
                " ".join(u.tags),
                u.avatar,
                u.usertype,
            ),
        )

    def insert_media(self, m: Media):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO media
            (id, type, url, title, description, thumb)
            VALUES(?, ?, ?, ?, ?, ?)""",
            (m.id, m.type, m.url, m.title, m.description, m.thumb),
        )

    def insert_message(self, m: Message):
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO messages
            (id, type, date, edit_date, content, reply_to,post_author,  user_id, forwarded_message_metadata_id, media_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                m.id,
                m.type,
                m.date.strftime("%Y-%m-%d %H:%M:%S"),
                m.edit_date.strftime("%Y-%m-%d %H:%M:%S") if m.edit_date else None,
                m.content,
                m.reply_to,
                m.post_author,
                m.user.id,
                (
                    m.forwarded_message_metadata.id
                    if m.forwarded_message_metadata
                    else None
                ),
                m.media.id if m.media else None,
            ),
        )

    def insert_forwarded_message_metadata(self, fmd: ForwardedMessageMetadata):
        # Insert forwarded message metadata
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO forwarded_message_metadata
            (id, originator_label, source_url, date, views, forwarded_count)
            VALUES(?, ?, ?, ?, ?, ?)""",
            (
                fmd.id,
                fmd.originator_label,
                fmd.source_url,
                fmd.date.strftime("%Y-%m-%d %H:%M:%S") if fmd.date else None,
                fmd.views,
                fmd.forwarded_count,
            ),
        )

    def commit(self):
        """Commit pending writes to the DB."""
        self.conn.commit()

    def _make_message(self, m) -> Message:
        """Create Message() object from SQL result tuple."""
        (
            id,
            typ,
            date,
            edit_date,
            content,
            reply_to,
            post_author,
            user_id,
            username,
            first_name,
            last_name,
            tags,
            avatar,
            usertype,
            forwarded_metadata_id,
            fwd_originator_label,
            fwd_source_url,
            fwd_date,
            fwd_views,
            fwd_count,
            media_id,
            media_type,
            media_url,
            media_title,
            media_description,
            media_thumb,
        ) = m

        # Process media data
        md = None
        if media_id:
            desc = media_description
            if media_type == "poll":
                desc = json.loads(media_description)

            md = Media(
                id=media_id,
                type=media_type,
                url=media_url,
                title=media_title,
                description=desc,
                thumb=media_thumb,
            )

        # Process forwarded message metadata
        fwd_md = None
        if forwarded_metadata_id:
            fwd_date_obj = pytz.utc.localize(fwd_date) if fwd_date else None
            if fwd_date_obj:
                fwd_date_obj = fwd_date_obj.astimezone(self.tz)

            fwd_md = ForwardedMessageMetadata(
                id=forwarded_metadata_id,
                originator_label=fwd_originator_label,
                source_url=fwd_source_url,
                date=fwd_date_obj,
                views=fwd_views,
                forwarded_count=fwd_count,
            )

        # Process dates
        date = pytz.utc.localize(date) if date else None
        edit_date = pytz.utc.localize(edit_date) if edit_date else None

        date = date.astimezone(self.tz) if date else None
        edit_date = edit_date.astimezone(self.tz) if edit_date else None

        return Message(
            id=id,
            type=typ,
            date=date,
            edit_date=edit_date,
            content=content,
            reply_to=reply_to,
            post_author=post_author,
            user=User(
                id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                tags=tags.split(" ") if tags else None,
                avatar=avatar,
                usertype=usertype,
            ),
            forwarded_message_metadata=fwd_md,
            media=md,
        )
