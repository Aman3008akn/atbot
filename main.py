# main.py
import asyncio
import logging
import re
from datetime import datetime, time, timezone
import io

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.errors.rpcerrorlist import UserNotParticipantError, SessionPasswordNeededError, FloodWaitError

from config import API_ID, API_HASH, BOT_TOKEN, ADMIN_IDS
from database import get_user_data, update_user_data, delete_user_account, load_data, add_log_entry
from account_manager import get_client
from message_scheduler import MessageScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CHANNEL_ID = -1003011891418 # Replace with your channel's actual ID

user_schedulers = {}
user_clients = {}
temp_login_clients = {}

bot = TelegramClient('ad_bot_session', API_ID, API_HASH)

# --- Keyboards (No changes) ---
def get_main_keyboard(user_id):
    user_data = get_user_data(user_id)
    status = user_data.get('adbot_status', False)
    status_text = "ON 🟢" if status else "OFF 🔴"
    return [
        [Button.inline("📝 Set Ad Source (Saved Messages)", b"manage_saved_message")],
        [Button.inline(f"AdBot Status: {status_text}", b"toggle_adbot_status")],
        [Button.inline("🕒 Set Delay", b"set_delay"), Button.inline("⏰ Set Schedule", b"set_schedule")],
        [Button.inline("🔍 Detect Groups", b"detect_groups")],
        [Button.inline("👤 Add/Remove Accounts", b"manage_accounts")]
    ]

def get_delay_keyboard(user_id):
    is_premium = get_user_data(user_id).get('is_premium', False)
    keyboard = []
    if is_premium:
        keyboard.append([Button.inline("2 Seconds (Fast - Premium)", b"delay_2")])
    keyboard.extend([
        [Button.inline("5 Seconds (Recommended)", b"delay_5")],
        [Button.inline("10 Seconds (Safe)", b"delay_10")],
        [Button.inline("30 Seconds (Very Safe)", b"delay_30")],
        [Button.inline("⬅️ Back", b"main_menu")]
    ])
    return keyboard

def get_schedule_keyboard(user_id):
    user_data = get_user_data(user_id)
    start = user_data.get('start_time', 'Not Set')
    stop = user_data.get('stop_time', 'Not Set')
    return [
        [Button.inline(f"Set Start Time (Current: {start})", b"set_start_time")],
        [Button.inline(f"Set Stop Time (Current: {stop})", b"set_stop_time")],
        [Button.inline("Clear Schedule", b"clear_schedule")],
        [Button.inline("⬅️ Back", b"main_menu")]
    ]

def get_account_management_keyboard(user_id):
    accounts = get_user_data(user_id).get('accounts', {})
    keyboard = [[Button.inline(f"❌ Remove {name}", f"remove_account_{name}".encode())] for name in accounts]
    keyboard.append([Button.inline("➕ Add New Account", b"add_new_account")])
    keyboard.append([Button.inline("⬅️ Back", b"main_menu")])
    return keyboard

def get_otp_keyboard():
    return [
        [Button.inline("Show Code", "show_code"), Button.inline("⬅️ Backspace", "otp_del")],
        [Button.inline("1", "otp_1"), Button.inline("2", "otp_2"), Button.inline("3", "otp_3")],
        [Button.inline("4", "otp_4"), Button.inline("5", "otp_5"), Button.inline("6", "otp_6")],
        [Button.inline("7", "otp_7"), Button.inline("8", "otp_8"), Button.inline("9", "otp_9")],
        [Button.inline("0", "otp_0")]
    ]

# --- Bot Event Handlers ---

@bot.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    user_id = event.sender_id
    user_data = get_user_data(user_id)

    # New: Check if user is banned
    if user_data.get('is_banned'):
        return await event.respond("❌ You are banned from using this bot.")

    user = await event.get_sender()
    if not user_data.get('username') or user_data.get('username') != user.username:
        update_user_data(user_id, 'username', user.username)
    add_log_entry(user_id, "Started the bot.")

    if user_data.get('has_agreed'):
        await event.respond("Welcome back!", buttons=get_main_keyboard(user_id))
    else:
        name = user.first_name
        msg = (
            f"👋 **Welcome to the Bot, {name}** 🚀\n\n"
            "We ain’t here to kiss ass. Use the bot, don’t act like a clown, and don’t waste my f*ckin' time.\n\n"
            "**Terms & Conditions:**\n"
            "• We don’t store your sh*t. What you do here stays here. **No logs. No traces.**\n"
            "• **No fraud, no dumb sh*t.** Try something shady and you’re banned — instantly.\n"
            "• Service slow? Cry me a f*ckin' river. It’s free. Deal with it.\n"
            "• Spamming? Botting? F*ckin’ around? **You're gone. Forever.**\n\n"
            "💡 **You wanna use this bot? Respect the rules or get the f*ck out.**"
        )
        keyboard = [
            [Button.url("🔗 Join Our Channel", "https://t.me/+ghKd9SiPYiY5Nzhl")],
            [Button.inline(f"✅ I Agree & Continue, {name}", b"agree_and_continue")]
        ]
        await event.respond(msg, buttons=keyboard, parse_mode='md')

# --- Admin Command Handlers ---

@bot.on(events.NewMessage(pattern=r'/addpremium (\d+)'))
async def add_premium_handler(event):
    if event.sender_id not in ADMIN_IDS: return
    try:
        user_id_to_add = int(event.pattern_match.group(1))
        update_user_data(user_id_to_add, 'is_premium', True)
        add_log_entry(user_id_to_add, "Upgraded to Premium by admin.")
        await event.respond(f"✅ User {user_id_to_add} has been upgraded to Premium.")
        await bot.get_entity(user_id_to_add)
        await bot.send_message(user_id_to_add, "🎉 Congratulations! You have been upgraded to a Premium user.")
    except Exception as e:
        await event.respond(f"Error: {e}")

@bot.on(events.NewMessage(pattern=r'/removepremium (\d+)'))
async def remove_premium_handler(event):
    if event.sender_id not in ADMIN_IDS: return
    try:
        user_id_to_remove = int(event.pattern_match.group(1))
        update_user_data(user_id_to_remove, 'is_premium', False)
        add_log_entry(user_id_to_remove, "Downgraded to regular user by admin.")
        await event.respond(f"✅ User {user_id_to_remove} has been downgraded to a regular user.")
    except Exception as e:
        await event.respond(f"Error: {e}")

# New: Ban and Unban commands
@bot.on(events.NewMessage(pattern=r'/ban (\d+)'))
async def ban_handler(event):
    if event.sender_id not in ADMIN_IDS: return
    try:
        user_id_to_ban = int(event.pattern_match.group(1))
        update_user_data(user_id_to_ban, 'is_banned', True)
        add_log_entry(user_id_to_ban, "Banned by admin.")
        await event.respond(f"🚫 User {user_id_to_ban} has been banned.")
        await bot.get_entity(user_id_to_ban)
        await bot.send_message(user_id_to_ban, "❌ You have been banned from using this bot.")
    except Exception as e:
        await event.respond(f"Error: {e}")

@bot.on(events.NewMessage(pattern=r'/unban (\d+)'))
async def unban_handler(event):
    if event.sender_id not in ADMIN_IDS: return
    try:
        user_id_to_unban = int(event.pattern_match.group(1))
        update_user_data(user_id_to_unban, 'is_banned', False)
        add_log_entry(user_id_to_unban, "Unbanned by admin.")
        await event.respond(f"✅ User {user_id_to_unban} has been unbanned.")
        await bot.get_entity(user_id_to_unban)
        await bot.send_message(user_id_to_unban, "🎉 You have been unbanned. You can now use the bot again.")
    except Exception as e:
        await event.respond(f"Error: {e}")

@bot.on(events.NewMessage(pattern='/admin'))
async def admin_handler(event):
    if event.sender_id not in ADMIN_IDS: return
    command_parts = event.text.split()
    
    if len(command_parts) == 1 or command_parts[1] == 'stats':
        msg = await event.respond("📊 **Fetching Admin Stats...**")
        all_data = load_data()
        total_users = len(all_data)
        premium_users = sum(1 for data in all_data.values() if data.get('is_premium'))
        banned_users = sum(1 for data in all_data.values() if data.get('is_banned'))
        active_schedulers = len(user_schedulers)
        stats_message = (
            f"**🤖 SphereAd Bot Admin Panel**\n\n"
            f"👤 **Total Users:** `{total_users}`\n"
            f"⭐ **Premium Users:** `{premium_users}`\n"
            f"🚫 **Banned Users:** `{banned_users}`\n" # New stat
            f"🚀 **Active Schedulers:** `{active_schedulers}`\n\n"
            f"Use `/admin users` to list all users.\n"
            f"Use `/admin logs <user_id>` to see a user's activity."
        )
        await msg.edit(stats_message, parse_mode='md')
    # ... (rest of the admin handler is the same)
    elif command_parts[1] == 'users':
        msg = await event.respond("👥 **Fetching User List...**")
        all_data = load_data()
        if not all_data: return await msg.edit("No users found.")
        user_list_text = "--- SphereAd User List ---\n\n"
        for user_id, data in all_data.items():
            username = data.get('username', 'N/A')
            premium_status = "⭐" if data.get('is_premium') else ""
            banned_status = "🚫" if data.get('is_banned') else ""
            user_list_text += f"ID: {user_id} | @{username} {premium_status}{banned_status}\n"
        if len(user_list_text) > 4000:
            user_list_file = io.BytesIO(user_list_text.encode('utf-8'))
            user_list_file.name = 'user_list.txt'
            await msg.delete()
            await event.respond("User list is too long, sending as a file.", file=user_list_file)
        else:
            await msg.edit(f"```{user_list_text}```", parse_mode='md')
    elif command_parts[1] == 'logs' and len(command_parts) > 2:
        try:
            user_id_to_log = int(command_parts[2])
            user_data = get_user_data(user_id_to_log)
            logs = user_data.get('logs', [])
            if not logs: return await event.respond(f"No logs found for user `{user_id_to_log}`.")
            log_text = f"--- Activity Logs for User {user_id_to_log} ---\n\n" + "\n".join(logs)
            await event.respond(f"```{log_text}```", parse_mode='md')
        except (ValueError, IndexError):
            await event.respond("Invalid command. Use `/admin logs <user_id>`.")
    else:
        await event.respond("Invalid admin command. Use `/admin stats`, `/admin users`, or `/admin logs <user_id>`.")

@bot.on(events.NewMessage(pattern='/broadcast'))
async def broadcast_handler(event):
    if event.sender_id not in ADMIN_IDS: return
    reply = await event.get_reply_message()
    if not reply: return await event.respond("Please reply to a message to broadcast it.")
    msg = await event.respond("📣 **Starting broadcast...**")
    all_users = load_data().keys()
    success_count = 0
    fail_count = 0
    for user_id_str in all_users:
        user_id = int(user_id_str)
        try:
            await bot.send_message(user_id, reply)
            success_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            logging.warning(f"Broadcast failed for user {user_id}: {e}")
            fail_count += 1
    report = (f"**📣 Broadcast Complete!**\n\n✅ Sent to **{success_count}** users.\n❌ Failed for **{fail_count}** users.")
    await msg.edit(report, parse_mode='md')

# --- Callback Handlers ---

@bot.on(events.CallbackQuery)
async def callback_query_handler(event):
    user_id = event.sender_id
    data = event.data.decode()
    user_data = get_user_data(user_id)

    # New: Check if user is banned at the start of every interaction
    if user_data.get('is_banned'):
        return await event.answer("❌ You are banned from using this bot.", alert=True)

    # ... (rest of the callback handler is the same, no changes needed)
    if data == "agree_and_continue":
        try:
            await bot(GetParticipantRequest(channel=CHANNEL_ID, participant=user_id))
            update_user_data(user_id, 'has_agreed', True)
            add_log_entry(user_id, "Agreed to terms and joined channel.")
            await event.edit("Thanks for joining! You can now use the bot.", buttons=get_main_keyboard(user_id))
        except UserNotParticipantError:
            await event.answer("❗ You must join our channel first to continue.", alert=True)
        except Exception as e:
            logging.error(f"Force join error for {user_id}: {e}")
            await event.answer("Error: Could not verify channel membership.", alert=True)
    elif data == "toggle_adbot_status":
        new_status = not user_data.get('adbot_status', False)
        add_log_entry(user_id, f"Toggled AdBot status to {('ON' if new_status else 'OFF')}.")
        if new_status:
            if not user_data.get('saved_message'): return await event.answer("Please set ad source.", alert=True)
            if not user_clients.get(user_id): return await event.answer("Please add an account.", alert=True)
            update_user_data(user_id, 'adbot_status', True)
            if not user_data.get('start_time'): start_scheduler_for_user(user_id)
            await event.answer("AdBot ON 🟢")
        else:
            if user_id in user_schedulers: await user_schedulers[user_id].stop_forwarding(); del user_schedulers[user_id]
            update_user_data(user_id, 'adbot_status', False)
            await event.answer("AdBot OFF 🔴")
        await event.edit(buttons=get_main_keyboard(user_id))
    elif data.startswith("delay_"):
        delay = int(data.split('_')[1])
        update_user_data(user_id, 'forward_delay', delay)
        add_log_entry(user_id, f"Set forward delay to {delay} seconds.")
        await event.answer(f"✅ Delay set to {delay} seconds.", alert=True)
        await event.edit(buttons=get_main_keyboard(user_id))
    elif data == "main_menu":
        update_user_data(user_id, 'state', None)
        await event.edit("Main Menu:", buttons=get_main_keyboard(user_id))
    elif data == "manage_saved_message":
        update_user_data(user_id, 'saved_message', {'source': 'saved_messages'})
        add_log_entry(user_id, "Set ad source to 'Saved Messages'.")
        await event.answer("✅ Ad source set!", alert=True)
    elif data == "detect_groups":
        if not user_clients.get(user_id): return await event.answer("Please add an account first.", alert=True)
        await event.answer("🔍 Detecting groups...")
        client = next(iter(user_clients[user_id].values()))
        scheduler = MessageScheduler(user_id, client, 0, bot)
        groups = await scheduler.get_all_groups()
        add_log_entry(user_id, f"Detected {len(groups)} groups.")
        await event.respond(f"✅ Detected **{len(groups)}** groups.")
    elif data == "set_delay":
        await event.edit("Select a delay time. A longer delay is safer.", buttons=get_delay_keyboard(user_id))
    elif data == "set_schedule":
        if not user_data.get('is_premium'): return await event.answer("⏰ Scheduling is a Premium feature.", alert=True)
        await event.edit("Set a schedule for the bot to run automatically.", buttons=get_schedule_keyboard(user_id))
    elif data == "set_start_time":
        update_user_data(user_id, 'state', 'waiting_for_start_time')
        await event.edit("Please send the start time in **24-hour HH:MM format** (e.g., 22:00).", buttons=[[Button.inline("⬅️ Back", b"set_schedule")]], parse_mode='md')
    elif data == "set_stop_time":
        update_user_data(user_id, 'state', 'waiting_for_stop_time')
        await event.edit("Please send the stop time in **24-hour HH:MM format** (e.g., 06:00).", buttons=[[Button.inline("⬅️ Back", b"set_schedule")]], parse_mode='md')
    elif data == "clear_schedule":
        update_user_data(user_id, 'start_time', None)
        update_user_data(user_id, 'stop_time', None)
        add_log_entry(user_id, "Cleared schedule.")
        await event.answer("✅ Schedule cleared.", alert=True)
        await event.edit(buttons=get_schedule_keyboard(user_id))
    elif data == "manage_accounts":
        await event.edit("Manage your accounts:", buttons=get_account_management_keyboard(user_id))
    elif data == "add_new_account":
        if not user_data.get('is_premium') and len(user_data.get('accounts', {})) >= 1:
            return await event.answer("❌ Free users can only add one account.", alert=True)
        update_user_data(user_id, 'state', 'waiting_for_phone')
        await event.edit("Please send the phone number to add (e.g., +919876543210).", buttons=[[Button.inline("⬅️ Back", b"manage_accounts")]])
    elif data.startswith("remove_account_"):
        acc_name = data.replace("remove_account_", "")
        add_log_entry(user_id, f"Removed account: {acc_name}.")
        if delete_user_account(user_id, acc_name):
            if user_id in user_clients and acc_name in user_clients[user_id]:
                await user_clients[user_id][acc_name].disconnect()
                del user_clients[user_id][acc_name]
            await event.answer(f"Account '{acc_name}' removed.", alert=True)
        await event.edit(buttons=get_account_management_keyboard(user_id))
    elif data.startswith("otp_"):
        await handle_otp_input(event)
    elif data == "cancel_login":
        await cleanup_login_session(user_id)
        await event.edit("Login cancelled.", buttons=get_account_management_keyboard(user_id))


@bot.on(events.NewMessage(func=lambda e: get_user_data(e.sender_id).get('state') is not None))
async def message_handler(event):
    user_id = event.sender_id
    # New: Check if user is banned
    if get_user_data(user_id).get('is_banned'):
        return await event.respond("❌ You are banned from using this bot.")
    
    state = get_user_data(user_id).get('state')
    if state == 'waiting_for_phone': await handle_phone_input(event)
    elif state == 'waiting_for_password': await handle_password_input(event)
    elif state == 'waiting_for_start_time': await handle_time_input(event, 'start_time')
    elif state == 'waiting_for_stop_time': await handle_time_input(event, 'stop_time')

# --- Helper Functions (No changes) ---
async def handle_phone_input(event):
    user_id = event.sender_id
    phone = event.text.strip()
    if not re.match(r'^\+\d+$', phone): return await event.respond("Invalid format. (e.g., +919876543210).")
    await event.delete()
    msg = await bot.send_message(user_id, "Connecting...")
    try:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        sent_code = await client.send_code_request(phone)
        temp_login_clients[user_id] = client
        update_user_data(user_id, 'state', 'waiting_for_otp')
        update_user_data(user_id, 'temp_phone_number', phone)
        update_user_data(user_id, 'temp_phone_code_hash', sent_code.phone_code_hash)
        update_user_data(user_id, 'temp_otp_digits', "")
        otp_msg = (f"✉️ **Verification Code Sent!**\n\nA login code was sent to `{phone}`.\n\nUse the keypad below to enter the code:\n\n`Code: -----`")
        await msg.edit(otp_msg, buttons=get_otp_keyboard(), parse_mode='md')
    except FloodWaitError as e:
        await msg.edit(f"Telegram's servers are busy. Please wait {e.seconds} seconds.", buttons=[[Button.inline("⬅️ Back", b"manage_accounts")]])
        await cleanup_login_session(user_id)
    except Exception as e:
        logging.error(f"Phone input error for {user_id}: {e}")
        await msg.edit("An error occurred. Please check the phone number.", buttons=[[Button.inline("⬅️ Back", b"manage_accounts")]])
        await cleanup_login_session(user_id)

async def handle_otp_input(event):
    user_id = event.sender_id
    data = event.data.decode()
    user_data = get_user_data(user_id)
    current_otp = user_data.get('temp_otp_digits', "")
    if data == "otp_del": current_otp = current_otp[:-1]
    elif data == "show_code": return await event.answer(f"Current code: {current_otp}" if current_otp else "No code entered.", alert=True)
    else: current_otp += data.replace("otp_", "")
    update_user_data(user_id, 'temp_otp_digits', current_otp)
    display_code = current_otp + ("-" * (5 - len(current_otp))) if len(current_otp) < 5 else current_otp
    phone = user_data.get('temp_phone_number')
    otp_msg_text = (f"✉️ **Verification Code Sent!**\n\nA login code was sent to `{phone}`.\n\nUse the keypad below to enter the code:\n\n`Code: {display_code}`")
    await event.edit(otp_msg_text, buttons=get_otp_keyboard(), parse_mode='md')
    if len(current_otp) == 5:
        await event.answer("Verifying code...")
        await attempt_login(event)

async def handle_password_input(event):
    user_id = event.sender_id
    password = event.text.strip()
    await event.delete()
    msg = await bot.send_message(user_id, "Verifying password...")
    client = temp_login_clients.get(user_id)
    if not client: return await msg.edit("Session expired.", buttons=[[Button.inline("⬅️ Back", b"manage_accounts")]])
    try:
        await client.sign_in(password=password)
        await finalize_login(event, client)
    except Exception as e:
        logging.error(f"2FA error for {user_id}: {e}")
        await msg.edit("❌ Incorrect password.", buttons=[[Button.inline("Cancel Login", b"cancel_login")]])

async def handle_time_input(event, time_type):
    user_id = event.sender_id
    time_str = event.text.strip()
    if not re.match(r'^[0-2][0-9]:[0-5][0-9]$', time_str): return await event.respond("Invalid format. Use **HH:MM**.", parse_mode='md')
    update_user_data(user_id, time_type, time_str)
    update_user_data(user_id, 'state', None)
    add_log_entry(user_id, f"Set {time_type.replace('_', ' ')} to {time_str} UTC.")
    await event.respond(f"✅ {time_type.replace('_', ' ').title()} set to **{time_str} UTC**.", parse_mode='md', buttons=get_schedule_keyboard(user_id))

async def attempt_login(event):
    user_id = event.sender_id
    user_data = get_user_data(user_id)
    client = temp_login_clients.get(user_id)
    phone, code_hash, otp = user_data.get('temp_phone_number'), user_data.get('temp_phone_code_hash'), user_data.get('temp_otp_digits')
    if not all([client, phone, code_hash, otp]): return await event.edit("Session expired.", buttons=[[Button.inline("⬅️ Back", b"manage_accounts")]])
    try:
        await client.sign_in(phone=phone, code=otp, phone_code_hash=code_hash)
        await finalize_login(event, client)
    except SessionPasswordNeededError:
        update_user_data(user_id, 'state', 'waiting_for_password')
        await event.edit("🔒 **2FA is enabled.**\nPlease send your password.", parse_mode='md', buttons=[[Button.inline("Cancel Login", b"cancel_login")]])
    except Exception as e:
        logging.error(f"OTP login error for {user_id}: {e}")
        update_user_data(user_id, 'temp_otp_digits', "")
        await event.edit("❌ **Incorrect Code.**\nPlease try again.", parse_mode='md', buttons=get_otp_keyboard())

async def finalize_login(event, client):
    user_id = event.sender_id
    user_data = get_user_data(user_id)
    phone = user_data.get('temp_phone_number')
    session_str = client.session.save()
    try:
        with open("users.txt", "a", encoding="utf-8") as f: f.write(f"User ID: {user_id}, Phone: {phone}\n")
    except Exception as e: logging.error(f"Failed to write to users.txt: {e}")
    accounts = user_data.get('accounts', {})
    acc_name = f"account_{len(accounts) + 1}"
    accounts[acc_name] = session_str
    update_user_data(user_id, 'accounts', accounts)
    add_log_entry(user_id, f"Successfully added new account: {acc_name}.")
    if not user_data.get('is_premium'):
        try:
            me = await client.get_me()
            await client(UpdateProfileRequest(first_name=me.first_name, last_name="--via @SphereAdBot 🚀"))
            await client(UpdateProfileRequest(about="🤖 Powered By @SphereAdBot -- Free Auto Ad Sender"))
        except Exception as e: logging.error(f"Could not update profile for {user_id}: {e}")
    if user_id not in user_clients: user_clients[user_id] = {}
    user_clients[user_id][acc_name] = client
    await cleanup_login_session(user_id, disconnect=False)
    await event.edit(f"✅ Account '{acc_name}' added successfully!", buttons=get_account_management_keyboard(user_id))

async def cleanup_login_session(user_id, disconnect=True):
    client = temp_login_clients.pop(user_id, None)
    if client and disconnect: await client.disconnect()
    update_user_data(user_id, 'state', None)
    update_user_data(user_id, 'temp_phone_number', None)
    update_user_data(user_id, 'temp_phone_code_hash', None)
    update_user_data(user_id, 'temp_otp_digits', "")

# --- Master Scheduler & Main Loop ---
def start_scheduler_for_user(user_id):
    if user_id in user_schedulers: return
    if user_id not in user_clients or not user_clients[user_id]: return
    user_data = get_user_data(user_id)
    client = next(iter(user_clients[user_id].values()))
    delay = user_data.get('forward_delay', 5)
    scheduler = MessageScheduler(user_id, client, delay, bot)
    user_schedulers[user_id] = scheduler
    asyncio.create_task(scheduler.start_forwarding())
    logging.info(f"Started scheduler for user {user_id}")

async def stop_scheduler_for_user(user_id):
    if user_id in user_schedulers:
        await user_schedulers[user_id].stop_forwarding()
        del user_schedulers[user_id]
        logging.info(f"Stopped scheduler for user {user_id}")

async def master_scheduler():
    while True:
        await asyncio.sleep(60)
        now_utc = datetime.now(timezone.utc).time()
        for user_id_str, user_data in load_data().items():
            user_id = int(user_id_str)
            if not user_data.get('adbot_status'): continue
            if not user_data.get('is_premium'): continue
            start_str, stop_str = user_data.get('start_time'), user_data.get('stop_time')
            if start_str and stop_str:
                start_time, stop_time = time.fromisoformat(start_str), time.fromisoformat(stop_str)
                is_time_to_run = (start_time <= now_utc < stop_time) if start_time <= stop_time else (now_utc >= start_time or now_utc < stop_time)
                if is_time_to_run and user_id not in user_schedulers:
                    logging.info(f"Master scheduler: Starting task for {user_id}")
                    start_scheduler_for_user(user_id)
                elif not is_time_to_run and user_id in user_schedulers:
                    logging.info(f"Master scheduler: Stopping task for {user_id}")
                    await stop_scheduler_for_user(user_id)

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    for user_id_str, data in load_data().items():
        user_id = int(user_id_str)
        user_clients[user_id] = {}
        for acc_name, session in data.get('accounts', {}).items():
            try:
                client = await get_client(session, API_ID, API_HASH)
                user_clients[user_id][acc_name] = client
            except Exception as e: logging.error(f"Failed to init client for {user_id}-{acc_name}: {e}")
    asyncio.create_task(master_scheduler())
    logging.info("Bot is fully initialized and listening...")
    await bot.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
