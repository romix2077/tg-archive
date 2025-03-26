import json
import logging
import os
from datetime import datetime
from collections import defaultdict

import pytz


def get_full_name(user_dict):
    """Build user's full name, preserving spaces in names"""
    if not user_dict:
        return None

    first = user_dict.get('first_name') or ''
    last = user_dict.get('last_name') or ''

    # Handle first_name and last_name separately, preserving internal spaces
    if first or last:
        if first and last:
            return f"{first} {last}"
        return first or last
    return None


def namedtuple_to_dict(obj):
    """Convert namedtuple objects to dictionaries and remove None values"""
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S %z")
    if hasattr(obj, '_asdict'):
        result = {k: namedtuple_to_dict(v) for k, v in obj._asdict().items()}

        # Special handling for user dictionary
        if hasattr(obj, 'username'):  # Check if this is a User object
            filtered = {k: v for k, v in result.items() if v is not None}
            if 'id' in filtered:
                new_dict = {'id': filtered['id']}
                full_name = get_full_name(filtered)
                if full_name:
                    new_dict['full_name'] = full_name
                new_dict.update(
                    {
                        k: v
                        for k, v in filtered.items()
                        if k not in ['id', 'first_name', 'last_name']
                    }
                )
                return new_dict

        return {k: v for k, v in result.items() if v is not None}
    elif isinstance(obj, (list, tuple)):
        return [namedtuple_to_dict(item) for item in obj if item is not None]
    elif isinstance(obj, dict):
        return {
            k: v
            for k, v in {k: namedtuple_to_dict(v) for k, v in obj.items()}.items()
            if v is not None
        }
    return obj


def export_all_to_json(db, config):
    """Export all messages to a single JSON file"""
    # Ensure output directory exists
    os.makedirs(config["publish_dir"], exist_ok=True)

    # Build complete output path
    output_path = os.path.join(config["publish_dir"], config["json_output"])

    # Get all messages
    messages = []
    timeline = defaultdict(list)

    messages = [namedtuple_to_dict(msg) for msg in db.get_messages(limit=None)]

    # Get group information
    chat_info = namedtuple_to_dict(db.get_last_archived_chat_info())
    del chat_info["id"]

    # Use system timezone or configured timezone
    timezone = pytz.timezone(config.get('timezone', 'UTC'))
    current_time = datetime.now(timezone)

    # Build complete archive data
    archive_data = {
        "chat_info": chat_info,
        "export_date": current_time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "total_messages": len(messages),
        "messages": messages,
    }

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(archive_data, f, ensure_ascii=False, indent=2)

    logging.info(f"Exported {len(messages)} messages to {output_path}")
