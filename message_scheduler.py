# message_scheduler.py
import asyncio
import logging
from telethon.errors.rpcerrorlist import (
    PeerFloodError, UserPrivacyRestrictedError, ChatWriteForbiddenError,
    SlowModeWaitError, FloodWaitError
)

class MessageScheduler:
    def __init__(self, user_id, client, delay, bot):
        self.user_id = user_id
        self.client = client
        self.delay = delay
        self.bot = bot # Bot instance for sending logs to the user
        self.stop_event = asyncio.Event()

    async def get_all_groups(self):
        """Fetches all groups, now with better error checking."""
        groups = []
        try:
            # Step 1: Ensure client is connected
            if not self.client.is_connected():
                await self.client.connect()
            
            # Step 2: NEW - Check if the client is actually authorized
            if not await self.client.is_user_authorized():
                logging.error(f"User {self.user_id}: Client is not authorized. Session might be revoked.")
                # Inform the user that they need to re-login
                await self.bot.send_message(self.user_id, "❌ **Account session has expired or is invalid.**\n\nPlease go to 'Add/Remove Accounts' to remove and add your account again.")
                return [] # Return empty list to stop the process

            # Step 3: If authorized, get the groups
            async for dialog in self.client.iter_dialogs():
                if dialog.is_group:
                    groups.append(dialog.entity)
        except Exception as e:
            logging.error(f"User {self.user_id}: Could not fetch groups: {e}")
        return groups

    async def start_forwarding(self):
        """The core task that forwards the latest saved message."""
        logging.info(f"User {self.user_id}: Starting forwarding task.")
        try:
            groups = await self.get_all_groups()
            # If get_all_groups returned an empty list (e.g., due to auth error), stop here.
            if not groups:
                logging.warning(f"User {self.user_id}: No groups found or client not authorized. Stopping task.")
                # The user has already been notified by get_all_groups if there was an auth error.
                if get_user_data(self.user_id).get('adbot_status'):
                     await self.bot.send_message(self.user_id, "⚠️ No groups were detected, so the bot has stopped. Please check your account.")
                return

            while not self.stop_event.is_set():
                try:
                    latest_messages = await self.client.get_messages('me', limit=1)
                    if not latest_messages:
                        logging.warning(f"User {self.user_id}: 'Saved Messages' is empty. Skipping cycle.")
                        await self.bot.send_message(self.user_id, "⚠️ Your 'Saved Messages' is empty. Please add a message to forward.")
                        await asyncio.sleep(60)
                        continue
                    
                    message_to_forward = latest_messages[0]
                    
                    for group in groups:
                        if self.stop_event.is_set(): break
                        
                        try:
                            await self.client.forward_messages(entity=group, messages=message_to_forward)
                            logging.info(f"User {self.user_id}: Forwarded message to '{group.title}'.")
                            await self.bot.send_message(self.user_id, f"✅ Message sent to: **{group.title}**", parse_mode='md')
                        
                        except (PeerFloodError, UserPrivacyRestrictedError, ChatWriteForbiddenError) as e:
                            logging.error(f"User {self.user_id}: Could not forward to '{group.title}': {e}")
                            await self.bot.send_message(self.user_id, f"❌ Failed to send to: **{group.title}**\n*Reason: {e.__class__.__name__}*", parse_mode='md')
                        
                        except (SlowModeWaitError, FloodWaitError) as e:
                            wait_time = e.seconds + 2
                            logging.warning(f"User {self.user_id}: Waiting for {wait_time}s in '{group.title}'.")
                            await self.bot.send_message(self.user_id, f"⏳ Waiting for {wait_time}s in **{group.title}** (Slow Mode/Flood Wait).", parse_mode='md')
                            await asyncio.sleep(wait_time)
                        
                        except Exception as e:
                            logging.error(f"User {self.user_id}: An unexpected error with group '{group.title}': {e}")
                            await self.bot.send_message(self.user_id, f"❌ An unexpected error occurred with **{group.title}**.", parse_mode='md')
                        
                        await asyncio.sleep(self.delay)
                    
                    if self.stop_event.is_set(): break

                    logging.info(f"User {self.user_id}: Forwarding cycle complete.")
                    await self.bot.send_message(self.user_id, "Cycle complete. Waiting before starting the next round.")
                    await asyncio.sleep(30)

                except Exception as e:
                    logging.error(f"User {self.user_id}: Critical error in forwarding loop: {e}")
                    await asyncio.sleep(60)

        finally:
            logging.info(f"User {self.user_id}: Forwarding task has been shut down.")

    async def stop_forwarding(self):
        """Signals the forwarding task to stop."""
        self.stop_event.set()

