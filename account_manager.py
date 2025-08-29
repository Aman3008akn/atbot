# account_manager.py

from telethon import TelegramClient
from telethon.sessions import StringSession

async def get_client(session_string: str, api_id: int, api_hash: str) -> TelegramClient:
    """
    Initializes and returns a TelegramClient instance from a session string.
    """
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.connect()
    return client
