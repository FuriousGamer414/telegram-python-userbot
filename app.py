import os
import re
import time
import json
import asyncio
import signal
import shutil
import hashlib
import subprocess
from io import BytesIO
from typing import Dict
from datetime import datetime
from dateutil.parser import parse

import aiohttp
import yt_dlp
from telethon import TelegramClient, events
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.types import ChatBannedRights, DocumentAttributeAudio, DocumentAttributeVideo, DocumentAttributeFilename

from hachoir.parser import createParser
from hachoir.metadata import extractMetadata

## ----------------------------------------------------------------------------------------------------------------
## --- CONFIGURATION ---
## ----------------------------------------------------------------------------------------------------------------

API_ID = 1234567  # Replace with your API ID
API_HASH = 'YOUR_API_HASH'  # Replace with your API HASH
SESSION_NAME = 'my_user_bot'
SUDO_USER = 1546129837
AUTH_FILE = "auth_users.txt"
AFK_FILE = "afk_status.json"
API_BASE_URL = "http://35.221.9.111:9200/download"
CACHE_DIRECTORY = "downloads"
WORKERS_DIRECTORY = "workers"
WORKERS_FILE = "workers.json"

YOUTUBE_ID_REGEX = r"(?:https?:\/\/)?(?:www\.|m\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/)|youtu\.be\/)([\w-]{11})(?:\S+)?"
FACEBOOK_REGEX = r"(?:https?:\/\/)?(?:www\.|m\.|web\.)?(facebook\.com|fb\.watch)\/(?:video\.php\?v=\d+|\S+\/videos\/\d+|\S+\/reel\/\d+|watch\/\?v=\d+|reel\/\d+|\d{15,})\/?"
TIKTOK_REGEX = r"(?:https?:\/\/)?(?:www\.|vm\.|vt\.)?tiktok\.com\/.+"

## ----------------------------------------------------------------------------------------------------------------
## --- GLOBALS & STATE MANAGEMENT ---
## ----------------------------------------------------------------------------------------------------------------

AUTH_USERS = set()
AFK_STATE = {"is_afk": False, "reason": "", "since": 0}
WORKERS_STATE: Dict[str, dict] = {}
PENDING_SHELL_COMMANDS: Dict[int, str] = {}
ACTIVE_DOWNLOADS = set()
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
START_TIME = time.monotonic()
STOP_EVENT = asyncio.Event()

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

## ----------------------------------------------------------------------------------------------------------------
## --- HELPER FUNCTIONS ---
## ----------------------------------------------------------------------------------------------------------------
def is_process_running(pid: int) -> bool:
    if pid is None: return False
    try: os.kill(pid, 0)
    except OSError: return False
    else: return True
def load_persistent_data():
    global AFK_STATE, AUTH_USERS, WORKERS_STATE
    AUTH_USERS.clear(); AUTH_USERS.add(SUDO_USER)
    if os.path.exists(AUTH_FILE):
        with open(AUTH_FILE, 'r') as f:
            for line in f:
                try: AUTH_USERS.add(int(line.strip()))
                except ValueError: print(f"⚠️ Invalid line in {AUTH_FILE}: {line}")
    print(f"✅ Loaded {len(AUTH_USERS)} authorized users.")
    if os.path.exists(AFK_FILE):
        try:
            with open(AFK_FILE, 'r') as f: AFK_STATE = json.load(f)
        except json.JSONDecodeError: print(f"⚠️ Could not decode AFK state from {AFK_FILE}.")
    print("✅ AFK status loaded.")
    if os.path.exists(WORKERS_FILE):
        try:
            with open(WORKERS_FILE, 'r') as f: WORKERS_STATE = json.load(f)
            for name, data in WORKERS_STATE.items():
                if data.get('status') == 'running' and not is_process_running(data.get('pid')):
                    print(f"🛠️ Worker '{name}' was marked as running but PID {data['pid']} is gone. Setting to stopped.")
                    WORKERS_STATE[name]['status'] = 'stopped'; WORKERS_STATE[name]['pid'] = None
            save_workers_state()
        except json.JSONDecodeError: print(f"⚠️ Could not decode worker state from {WORKERS_FILE}.")
    print(f"✅ Loaded {len(WORKERS_STATE)} workers.")
def save_workers_state():
    with open(WORKERS_FILE, 'w') as f: json.dump(WORKERS_STATE, f, indent=2)
def save_auth_users():
    with open(AUTH_FILE, 'w') as f:
        for user_id in AUTH_USERS:
            if user_id != SUDO_USER: f.write(f"{user_id}\n")
def save_afk_state():
    with open(AFK_FILE, 'w') as f: json.dump(AFK_STATE, f)
def get_readable_time(seconds: int) -> str:
    periods = [('day', 86400), ('hour', 3600), ('minute', 60), ('second', 1)]
    result = []
    for name, secs in periods:
        if seconds >= secs:
            value, seconds = divmod(seconds, secs)
            unit = name if value == 1 else name + 's'
            result.append(f"{int(value)} {unit}")
    return ", ".join(result) or "a moment"
async def download_file(url: str, file_path: str) -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(file_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(4096)
                            if not chunk: break
                            f.write(chunk)
                    return file_path
    except Exception as e: print(f"Download error: {e}")
    return None
async def run_sync_in_executor(func):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func)
def human_readable_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0: return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"
def get_media_metadata(file_path: str) -> dict:
    metadata = {}
    try:
        parser = createParser(file_path)
        if not parser: return metadata
        with parser: data = extractMetadata(parser)
        if data:
            if data.has('duration'): metadata['duration'] = data.get('duration').total_seconds()
            if data.has('width'): metadata['width'] = data.get('width')
            if data.has('height'): metadata['height'] = data.get('height')
    except Exception as e:
        print(f"⚠️ Could not get metadata for {file_path}: {e}")
    return metadata
def run_shell_command(cmd: str, timeout: int = 60) -> str:
    try:
        process = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True, timeout=timeout)
        return process.stdout.strip()
    except subprocess.CalledProcessError as e: return f"Error (code {e.returncode}):\n{e.stderr.strip()}"
    except subprocess.TimeoutExpired: return f"Command timed out after {timeout} seconds."
    except Exception as e: return f"Execution error: {str(e)}"

## ----------------------------------------------------------------------------------------------------------------
## --- UTILITY & MENU COMMANDS ---
## ----------------------------------------------------------------------------------------------------------------
@client.on(events.NewMessage(pattern=r'^/ping(?:\s|$)'))
async def ping_handler(event):
    if event.sender_id not in AUTH_USERS: return
    start_time = time.monotonic(); msg = await event.edit("..."); end_time = time.monotonic()
    await msg.edit(f"**Pong!**\n`{end_time - start_time:.3f}` seconds")
@client.on(events.NewMessage(pattern=r'^/uptime(?:\s|$)'))
async def uptime_handler(event):
    if event.sender_id not in AUTH_USERS: return
    uptime_seconds = time.monotonic() - START_TIME
    await event.edit(f"**Bot Uptime:** `{get_readable_time(uptime_seconds)}`")
@client.on(events.NewMessage(pattern=r'^/info(?:\s|$)'))
async def info_handler(event):
    if event.sender_id not in AUTH_USERS: return
    target = event.sender;
    if event.is_reply:
        target = await (await event.get_reply_message()).get_sender()
    info_msg = (f"**User Info:**\n"
                f"**ID:** `{target.id}`\n"
                f"**First Name:** `{target.first_name}`\n"
                f"**Last Name:** `{target.last_name or 'N/A'}`\n"
                f"**Username:** `@{target.username}`\n" if target.username else ""
                f"**Profile Link:** [Click here](tg://user?id={target.id})\n"
                f"**Is Bot:** `{target.bot}`")
    await event.edit(info_msg)
@client.on(events.NewMessage(pattern=r'^/pp(?:\s|$)'))
async def pp_handler(event):
    if event.sender_id not in AUTH_USERS: return
    target = event.sender
    if event.is_reply:
        target = await (await event.get_reply_message()).get_sender()
    photos = await client.get_profile_photos(target)
    if not photos: return await event.edit("This user has no profile pictures.")
    await event.delete()
    await client.send_file(event.chat_id, photos[0], caption=f"Profile picture of `{target.first_name}`.")
@client.on(events.NewMessage(pattern=r'^/menu(?:\s|$)'))
async def menu_handler(event):
    if event.sender_id not in AUTH_USERS: return
    menu_text = """
**🤖 User Bot Menu**
---
**🎵 Media Commands**
• `/play <query>`: Searches and sends a song.
• `/ytmp3 <url>`: Sends YouTube audio.
• `/ytmp4 <url>`: Sends YouTube video.
• `/fbmp4 <url>`: Sends a Facebook video.
• `/ttmp4 <url>`: Sends a TikTok video.
---
**⚙️ Utility Commands**
• `/ping`: Checks bot latency.
• `/uptime`: Shows the bot's uptime.
• `/info <reply>`: Gets info about a user.
• `/pp <reply>`: Gets a user's profile picture.
• `/menu`: Shows this help menu.

**👑 Sudo users can use `/menuadmin` for a full list of commands.**
"""
    await event.reply(menu_text, link_preview=False)
@client.on(events.NewMessage(pattern=r'^/menuadmin(?:\s|$)', from_users=SUDO_USER))
async def menu_admin_handler(event):
    menu_text = """
**🤖 User Bot Admin Menu**
---
**🛠️ Worker System `(Sudo)`**
• `/worker new <name>`
• `/worker delete <name>`
• `/worker list`
• `/worker <name> start <file.py>`
• `/worker <name> stop | restart | status`
• `/worker <name> pip install <pkg>`
• `/worker <name> ls [path]`
• `/worker <name> rm <path>`
• `/worker <name> upload` `(reply to file)`
---
**🛡️ Moderation `(Sudo)`**
• `/ban|unban|mute|unmute <reply>`
• `/kick|promote|demote <reply>`
• `/pin <reply>` | `/unpin <reply|none>`
• `/del <reply>` | `/tagall [message]`
---
**😴 AFK Commands `(Sudo)`**
• `/afk set <reason>`
• `/afk on|off`
---
**👑 User Admin `(Sudo)`**
• `/adduser|deluser <id|reply>`
• `/listusers`
• `/shell <command>`
---
**This menu also includes all commands from the regular `/menu`.**
"""
    await event.reply(menu_text, link_preview=False)


## ----------------------------------------------------------------------------------------------------------------
## --- SUDO-ONLY COMMANDS (MODERATION, AFK, ADMIN, WORKERS, SHELL) ---
## ----------------------------------------------------------------------------------------------------------------
MUTE_RIGHTS = ChatBannedRights(until_date=None, send_messages=True)
UNMUTE_RIGHTS = ChatBannedRights(until_date=None, send_messages=False)
@client.on(events.NewMessage(pattern=r'^/(ban|unban|mute|unmute|kick|promote|demote)(?:\s|$)', from_users=SUDO_USER))
async def moderation_handler(event):
    command = event.pattern_match.group(1)
    if not event.is_group: return await event.edit("❌ This command only works in groups.")
    if not event.is_reply: return await event.edit(f"⚠️ Please reply to a user's message to `{command}` them.")
    reply_msg = await event.get_reply_message()
    target_user = await client.get_entity(reply_msg.sender_id)
    chat = await event.get_chat()
    try:
        if command == "ban":
            await client(EditBannedRequest(chat, target_user, ChatBannedRights(until_date=None, view_messages=True))); await event.edit(f"**Banned** `{target_user.first_name}`.")
        elif command == "unban":
            await client(EditBannedRequest(chat, target_user, ChatBannedRights(until_date=None, view_messages=False))); await event.edit(f"**Unbanned** `{target_user.first_name}`.")
        elif command == "mute":
            await client(EditBannedRequest(chat, target_user, MUTE_RIGHTS)); await event.edit(f"**Muted** `{target_user.first_name}`.")
        elif command == "unmute":
            await client(EditBannedRequest(chat, target_user, UNMUTE_RIGHTS)); await event.edit(f"**Unmuted** `{target_user.first_name}`.")
        elif command == "kick":
            await client.kick_participant(event.chat_id, target_user.id); await event.edit(f"**Kicked** `{target_user.first_name}`.")
        elif command == "promote":
            await client.edit_admin(event.chat_id, target_user, is_admin=True, title="Admin"); await event.edit(f"**Promoted** `{target_user.first_name}`.")
        elif command == "demote":
            await client.edit_admin(event.chat_id, target_user, is_admin=False); await event.edit(f"**Demoted** `{target_user.first_name}`.")
    except Exception as e: await event.edit(f"🚫 **Error:** {e}\n\nDo I have admin rights here?")
@client.on(events.NewMessage(pattern=r'^/(pin|unpin)(?:\s|$)', from_users=SUDO_USER))
async def pin_handler(event):
    command = event.pattern_match.group(1)
    try:
        if command == "pin":
            if not event.is_reply: return await event.edit("Reply to a message to pin it.")
            reply_msg = await event.get_reply_message()
            await client.pin_message(event.chat_id, reply_msg.id, notify=True); await event.delete()
        elif command == "unpin":
            if event.is_reply:
                reply_msg = await event.get_reply_message()
                await client.unpin_message(event.chat_id, reply_msg.id); await event.delete()
            else:
                await client.unpin_message(event.chat_id); await event.edit("**Unpinned** the latest message.")
    except Exception as e: await event.edit(f"🚫 **Error:** {e}\n\nDo I have permission to pin messages?")
@client.on(events.NewMessage(pattern=r'^/del(?:\s|$)', from_users=SUDO_USER))
async def delete_handler(event):
    if not event.is_reply: return await event.edit("Reply to a message to delete it.")
    reply_msg = await event.get_reply_message()
    try: await reply_msg.delete(); await event.delete()
    except Exception as e: await event.edit(f"🚫 **Error:** {e}")
@client.on(events.NewMessage(pattern=r'^/tagall(?:\s|$)', from_users=SUDO_USER))
async def tag_all_handler(event):
    if not event.is_group: return await event.edit("This command can only be used in groups.")
    try: _, message = event.text.split(' ', 1)
    except (ValueError, IndexError): message = "Hey everyone!"
    chat = await event.get_input_chat(); tagged_users = []
    await event.edit("`Mentioning all users...`")
    async for user in client.iter_participants(chat):
        if not user.bot: tagged_users.append(f"• [{user.first_name}](tg://user?id={user.id})")
    chunk_size = 100
    for i in range(0, len(tagged_users), chunk_size):
        chunk = tagged_users[i:i + chunk_size]
        await client.send_message(event.chat_id, f"{message}\n\n" + "\n".join(chunk))
    await event.delete()
@client.on(events.NewMessage(pattern=r'^/afk(?:\s|$)', from_users=SUDO_USER))
async def afk_handler(event):
    global AFK_STATE
    parts = event.text.split(' ', 2); command = parts[1] if len(parts) > 1 else "on"
    if command == "set":
        if len(parts) < 3 or not parts[2].strip(): return await event.edit("📋 Usage: `/afk set <reason>`")
        AFK_STATE.update({"is_afk": True, "reason": parts[2], "since": int(time.time())})
        save_afk_state(); await event.edit(f"**AFK mode is now ON.**\nReason: `{AFK_STATE['reason']}`")
    elif command == "on":
        if not AFK_STATE.get("reason"): return await event.edit("🚫 Set a reason first with `/afk set <reason>`.")
        AFK_STATE.update({"is_afk": True, "since": int(time.time())}); save_afk_state()
        await event.edit("**AFK mode is ON.**")
    elif command == "off":
        if AFK_STATE.get("is_afk"):
            duration = get_readable_time(int(time.time()) - AFK_STATE.get("since", 0))
            await event.edit(f"**Welcome back!** You were AFK for {duration}.")
            AFK_STATE["is_afk"] = False; save_afk_state()
        else: await event.edit("You weren't AFK.")
@client.on(events.NewMessage(incoming=True, func=lambda e: not e.out))
async def afk_trigger(event):
    if not AFK_STATE.get("is_afk") or event.sender_id == SUDO_USER: return
    is_reply = False
    if event.reply_to and event.reply_to.reply_to_peer_id and event.reply_to.reply_to_peer_id.user_id == SUDO_USER: is_reply = True
    if event.is_private or event.mentioned or is_reply:
        duration = get_readable_time(int(time.time()) - AFK_STATE.get("since", 0))
        await event.reply(f"**I'm currently AFK** (for {duration})\nReason: `{AFK_STATE['reason']}`")
@client.on(events.NewMessage(outgoing=True, from_users=SUDO_USER))
async def auto_disable_afk(event):
    global AFK_STATE
    if AFK_STATE.get("is_afk") and not event.text.lower().startswith(('/afk', '/del', '/unpin', '/pin', '/worker', '/shell')):
        duration = get_readable_time(int(time.time()) - AFK_STATE.get("since", 0))
        AFK_STATE["is_afk"] = False; save_afk_state()
        await client.send_message('me', f"**AFK mode disabled.** You were away for {duration}.")
@client.on(events.NewMessage(pattern=r'^/adduser(?:\s|$)', from_users=SUDO_USER))
async def add_user(event):
    try:
        if event.reply_to_msg_id: user_to_add = (await event.get_reply_message()).sender_id
        else: _, user_input = event.text.split(' ', 1); user_to_add = int(user_input)
        if user_to_add in AUTH_USERS: return await event.reply(f"✔️ User `{user_to_add}` is already authorized.")
        AUTH_USERS.add(user_to_add); save_auth_users()
        await event.reply(f"👍 User `{user_to_add}` has been authorized.")
    except (ValueError, IndexError): await event.reply("📋 Usage: Reply to a user or use `/adduser <user_id>`.")
    except Exception as e: await event.reply(f"🚫 Error: {e}")
@client.on(events.NewMessage(pattern=r'^/deluser(?:\s|$)', from_users=SUDO_USER))
async def del_user(event):
    try:
        if event.reply_to_msg_id: user_to_del = (await event.get_reply_message()).sender_id
        else: _, user_input = event.text.split(' ', 1); user_to_del = int(user_input)
        if user_to_del == SUDO_USER: return await event.reply("🚫 You cannot remove the sudo user.")
        if user_to_del in AUTH_USERS:
            AUTH_USERS.remove(user_to_del); save_auth_users()
            await event.reply(f"🗑️ User `{user_to_del}` has been removed from the authorized list.")
        else: await event.reply(f"🤔 User `{user_to_del}` was not in the authorized list.")
    except (ValueError, IndexError): await event.reply("📋 Usage: Reply to a user or use `/deluser <user_id>`.")
    except Exception as e: await event.reply(f"🚫 Error: {e}")
@client.on(events.NewMessage(pattern=r'^/listusers(?:\s|$)', from_users=SUDO_USER))
async def list_users(event):
    msg = "**👥 Authorized Users:**\n\n"
    for uid in sorted(list(AUTH_USERS)):
        msg += f"🔹 [{uid}](tg://user?id={uid})"; msg += " `(Sudo)`" if uid == SUDO_USER else ""; msg += "\n"
    await event.reply(msg)
@client.on(events.NewMessage(pattern=r'^/worker(?:\s|$)', from_users=SUDO_USER))
async def worker_handler(event):
    cmd_parts = event.text.split()
    if len(cmd_parts) < 2: return await event.edit("📋 **Usage:** `/worker <new|delete|list|name> ...` See /menu for details.")
    command = cmd_parts[1]
    if command == "new":
        if len(cmd_parts) != 3: return await event.edit("📋 **Usage:** `/worker new <worker_name>`")
        name = cmd_parts[2]
        if name in WORKERS_STATE: return await event.edit(f"🚫 Worker `{name}` already exists.")
        worker_path = os.path.join(WORKERS_DIRECTORY, name)
        os.makedirs(worker_path, exist_ok=True)
        WORKERS_STATE[name] = {"path": worker_path, "pid": None, "status": "stopped", "main_script": None}
        save_workers_state(); await event.edit(f"✅ Worker `{name}` created successfully.")
    elif command == "list":
        if not WORKERS_STATE: return await event.edit("ℹ️ No workers have been created yet.")
        msg = "**🛠️ Workers Status:**\n\n"
        for name, data in WORKERS_STATE.items():
            status = data.get('status', 'unknown'); pid = data.get('pid')
            if status == 'running' and not is_process_running(pid):
                status = 'stopped (crashed)'; WORKERS_STATE[name]['status'] = 'stopped'; WORKERS_STATE[name]['pid'] = None
            emoji = "🟢" if status == 'running' else "🔴"
            msg += f"{emoji} `{name}` (Status: **{status.capitalize()}**"
            if pid and status == 'running': msg += f", PID: `{pid}`"
            msg += ")\n"
        save_workers_state(); await event.edit(msg)
    elif command == "delete":
        if len(cmd_parts) != 3: return await event.edit("📋 **Usage:** `/worker delete <worker_name>`")
        name = cmd_parts[2]
        if name not in WORKERS_STATE: return await event.edit(f"🚫 Worker `{name}` not found.")
        if WORKERS_STATE[name].get('status') == 'running' and WORKERS_STATE[name].get('pid'):
            try: os.kill(WORKERS_STATE[name]['pid'], signal.SIGTERM)
            except ProcessLookupError: pass
        shutil.rmtree(WORKERS_STATE[name]['path']); del WORKERS_STATE[name]; save_workers_state()
        await event.edit(f"🗑️ Worker `{name}` and all its files have been deleted.")
    else:
        worker_name = command
        if worker_name not in WORKERS_STATE: return await event.edit(f"🚫 Worker `{worker_name}` not found.")
        if len(cmd_parts) < 3: return await event.edit(f"📋 **Usage:** `/worker {worker_name} <action>` (e.g., start, upload)")
        action = cmd_parts[2]; worker_data = WORKERS_STATE[worker_name]; worker_path = worker_data['path']
        if action == "upload":
            if not event.is_reply: return await event.edit("🚫 To upload, you must reply to a file.")
            reply_msg = await event.get_reply_message()
            if not reply_msg.file: return await event.edit("🚫 The replied-to message is not a file.")
            status_msg = await event.edit(f"📥 Uploading file to worker `{worker_name}`...")
            downloaded_path = await client.download_media(reply_msg, file=worker_path)
            filename = os.path.basename(downloaded_path)
            await status_msg.edit(f"✅ File `{filename}` uploaded to worker `{worker_name}` successfully.")
        elif action == "start":
            if len(cmd_parts) != 4: return await event.edit(f"📋 **Usage:** `/worker {worker_name} start <script.py>`")
            if worker_data.get('status') == 'running' and is_process_running(worker_data.get('pid')): return await event.edit(f"🚫 Worker `{worker_name}` is already running with PID `{worker_data['pid']}`.")
            script_name = cmd_parts[3]; script_path = os.path.join(worker_path, script_name)
            if not os.path.exists(script_path): return await event.edit(f"🚫 Script `{script_name}` not found.")
            process = subprocess.Popen(["python3", "-u", script_path], cwd=worker_path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            worker_data.update({"pid": process.pid, "status": "running", "main_script": script_name}); save_workers_state()
            await event.edit(f"🚀 Worker `{worker_name}` started with script `{script_name}`. PID: `{process.pid}`.")
        elif action == "stop":
            if not (worker_data.get('status') == 'running' and is_process_running(worker_data.get('pid'))): return await event.edit(f"ℹ️ Worker `{worker_name}` is not running.")
            try:
                os.kill(worker_data['pid'], signal.SIGTERM); await event.edit(f"🛑 Worker `{worker_name}` (PID: `{worker_data['pid']}`) stopped.")
            except ProcessLookupError: await event.edit(f"ℹ️ Worker `{worker_name}` (PID: `{worker_data['pid']}`) was already stopped.")
            worker_data.update({"pid": None, "status": "stopped"}); save_workers_state()
        elif action == "restart":
            script = worker_data.get('main_script')
            if not script: return await event.edit("🚫 No main script set for this worker. Use `start` first.")
            if worker_data.get('status') == 'running' and is_process_running(worker_data.get('pid')):
                try: os.kill(worker_data['pid'], signal.SIGTERM)
                except ProcessLookupError: pass
            await asyncio.sleep(1)
            script_path = os.path.join(worker_path, script)
            process = subprocess.Popen(["python3", "-u", script_path], cwd=worker_path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            worker_data.update({"pid": process.pid, "status": "running", "main_script": script}); save_workers_state()
            await event.edit(f"🔄 Worker `{worker_name}` restarted with script `{script}`. New PID: `{process.pid}`.")
        elif action == "pip":
            if len(cmd_parts) < 5 or cmd_parts[3] != 'install': return await event.edit(f"📋 **Usage:** `/worker {worker_name} pip install <package>`")
            package = " ".join(cmd_parts[4:])
            await event.edit(f"📦 Installing `{package}` for worker `{worker_name}`...")
            cmd = f"python3 -m pip install {package}"
            output = await run_sync_in_executor(lambda: run_shell_command(cmd, timeout=300))
            await event.edit(f"**Pip Install Output for `{worker_name}`:**\n```\n{output}\n```")
        elif action == "rm":
            if len(cmd_parts) < 4: return await event.edit(f"📋 **Usage:** `/worker {worker_name} rm <file_or_folder_path>`")
            path_to_remove = " ".join(cmd_parts[3:])
            safe_path = os.path.normpath(os.path.join(worker_path, path_to_remove))
            if not os.path.abspath(safe_path).startswith(os.path.abspath(worker_path)): return await event.edit("🚫 Path traversal detected. Operation cancelled.")
            if not os.path.exists(safe_path): return await event.edit(f"🚫 Path `{path_to_remove}` not found.")
            try:
                if os.path.isfile(safe_path): os.remove(safe_path)
                elif os.path.isdir(safe_path): shutil.rmtree(safe_path)
                await event.edit(f"🗑️ Successfully removed `{path_to_remove}`.")
            except Exception as e: await event.edit(f"🚫 Could not remove: {e}")
        elif action == "ls":
            path_to_list = " ".join(cmd_parts[3:]) if len(cmd_parts) > 3 else "."
            safe_path = os.path.normpath(os.path.join(worker_path, path_to_list))
            if not os.path.abspath(safe_path).startswith(os.path.abspath(worker_path)): return await event.edit("🚫 Path traversal detected.")
            if not os.path.isdir(safe_path): return await event.edit(f"🚫 Directory `{path_to_list}` not found.")
            files = os.listdir(safe_path)
            output = f"**📂 Content of `{worker_name}/{path_to_list}`:**\n\n" + "\n".join([f"`{f}`" for f in files])
            await event.edit(output or "Directory is empty.")
        elif action == "status":
            status = worker_data.get('status', 'unknown'); pid = worker_data.get('pid'); script = worker_data.get('main_script', 'Not set')
            if status == 'running' and not is_process_running(pid):
                status = 'stopped (crashed)'; pid = None; WORKERS_STATE[worker_name].update({'status': 'stopped', 'pid': None}); save_workers_state()
            emoji = "🟢" if status == 'running' else "🔴"
            msg = f"{emoji} **Status for worker `{worker_name}`:**\n\n**Status:** `{status.capitalize()}`\n**PID:** `{pid or 'N/A'}`\n**Main Script:** `{script}`\n**Path:** `{worker_path}`"
            await event.edit(msg)
        else: await event.edit(f"🚫 Unknown action `{action}` for worker `{worker_name}`.")

## ----------------------------------------------------------------------------------------------------------------
## --- MEDIA DOWNLOAD SYSTEM ---
## ----------------------------------------------------------------------------------------------------------------

@client.on(events.NewMessage(pattern=r'^/(ytmp3|ytmp4|play|fbmp4|ttmp4)(?:\s|$)'))
async def media_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if event.sender_id in ACTIVE_DOWNLOADS: return await event.reply("⚠️ Please wait for your previous request to complete.")
    ACTIVE_DOWNLOADS.add(event.sender_id)
    try:
        command_name = f"/{event.pattern_match.group(1)}"
        try: _, query = event.text.split(' ', 1); query = query.strip()
        except (ValueError, IndexError): return await event.reply(f"📋 Usage: `{command_name} <url_or_search_query>`")
        status_msg = await event.reply("⏳ Processing...")
        if command_name == '/play':
            await handle_play_command(event, query, status_msg)
        else:
            source, file_type, url_regex, error_msg = None, None, None, None
            if command_name in ['/ytmp3', '/ytmp4']:
                source, file_type, url_regex, error_msg = "youtube", "mp3" if command_name == '/ytmp3' else "mp4", YOUTUBE_ID_REGEX, "🚫 Invalid YouTube URL."
            elif command_name == '/fbmp4':
                source, file_type, url_regex, error_msg = "facebook", "mp4", FACEBOOK_REGEX, "🚫 Invalid Facebook URL."
            elif command_name == '/ttmp4':
                source, file_type, url_regex, error_msg = "tiktok", "mp4", TIKTOK_REGEX, "🚫 Invalid TikTok URL."
            
            if not re.match(url_regex, query): return await status_msg.edit(error_msg)
            await status_msg.edit("⏳ **Processing URL...**")
            await handle_download_request(event, query, file_type, status_msg, source)
    finally: ACTIVE_DOWNLOADS.remove(event.sender_id)

async def handle_play_command(event, query, status_msg):
    try:
        await status_msg.edit(f"🔎 **Searching for:** `{query}`")
        ydl_opts = {'quiet': True, 'skip_download': True, 'extract_flat': 'in_playlist', 'default_search': 'ytsearch1'}
        with yt_dlp.YoutubeDL(ydl_opts) as ydl: info = await run_sync_in_executor(lambda: ydl.extract_info(query, download=False))
        if not info.get("entries"): return await status_msg.edit("🚫 No search results found.")
        first_result = info['entries'][0]
        video_url = first_result.get('url') or f"https://www.youtube.com/watch?v={first_result['id']}"
        title = first_result.get('title', 'Unknown Title')
        await status_msg.edit(f"✅ **Found:** `{title}`\n\nNow processing...")
        await handle_download_request(event, video_url, "mp3", status_msg, "youtube")
    except Exception as e:
        await status_msg.edit(f"🚫 Search Error: {e}"); print(f"Error in /play command search: {e}")

async def handle_download_request(event, url: str, file_type: str, status_msg, source: str):
    try:
        cache_key = hashlib.md5(url.encode()).hexdigest(); ext = f".{file_type}"
        cached_file_path = os.path.join(CACHE_DIRECTORY, f"{cache_key}{ext}")
        is_cached = os.path.exists(cached_file_path)

        api_endpoint_map = {"youtube": "youtube/videofhd" if file_type == "mp4" else "youtube/audio", "facebook": "facebook/video", "tiktok": "tiktok/video"}
        api_endpoint = api_endpoint_map.get(source)
        if not api_endpoint: return await status_msg.edit("🚫 Unknown download source.")
        api_url = f"{API_BASE_URL}/{api_endpoint}?url={url}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status != 200: return await status_msg.edit(f"🚫 API Error: Server responded with status `{response.status}`.")
                data = await response.json()
        
        if not data.get("success"): return await status_msg.edit("🚫 API Error: Could not process the URL.")
        
        result = data.get("result", {}); title = result.get("title", "media"); quality = result.get("quality", "Unknown")
        download_url = result.get("download_url"); thumb_url = result.get("thumbnail")

        if not is_cached:
            if not download_url: return await status_msg.edit("🚫 API Error: Could not find a download URL.")
            await status_msg.edit(f"📥 Downloading `{title}`...")
            if not await download_file(download_url, cached_file_path): return await status_msg.edit("🚫 Download failed.")
        else: await status_msg.edit("✅ Using cached file. Preparing to upload...")

        file_size = os.path.getsize(cached_file_path)
        if file_size > MAX_FILE_SIZE:
            os.remove(cached_file_path)
            return await status_msg.edit(f"🚫 File too large: {human_readable_size(file_size)} "
                                       f"(limit: {human_readable_size(MAX_FILE_SIZE)}).\n"
                                       f"🗑️ Removed from cache.")
        
        base_caption = f"**Title:** \n**Quality:** `{quality}`"; available_space = 1024 - len(base_caption) - 4
        if len(title) > available_space: title = title[:available_space - 3] + "..."
        caption_text = f"**Title:** `{title}`\n**Quality:** `{quality}`"

        upload_start_time = time.monotonic(); last_edit_time = 0
        async def progress_callback(current, total):
            nonlocal last_edit_time
            current_time = time.monotonic()
            if current_time - last_edit_time < 2: return # Update every 2 seconds
            last_edit_time = current_time
            percent = (current / total) * 100
            elapsed_time = current_time - upload_start_time
            speed = current / elapsed_time if elapsed_time > 0 else 0
            progress_bar = "".join(["▰" if i < percent / 10 else "▱" for i in range(10)])
            try:
                await status_msg.edit(
                    f"📤 **Uploading:** `{title}`\n"
                    f"`[{progress_bar}] {percent:.1f}%`\n"
                    f"`{human_readable_size(current)} / {human_readable_size(total)}`\n"
                    f"**Speed:** `{human_readable_size(speed)}/s`")
            except Exception: pass
        
        await status_msg.edit(f"📤 Uploading `{title}`...")
        media_meta = get_media_metadata(cached_file_path)
        duration = int(media_meta.get('duration', 0)); width = media_meta.get('width', 0); height = media_meta.get('height', 0)
        thumb_path = None
        if thumb_url: thumb_path = await download_file(thumb_url, os.path.join(CACHE_DIRECTORY, f"thumb_{cache_key}.jpg"))
        
        attrs = [DocumentAttributeFilename(file_name=f"{title}{ext}")]
        if file_type == "mp3": attrs.append(DocumentAttributeAudio(duration=duration, title=title, performer=source.capitalize()))
        else: attrs.append(DocumentAttributeVideo(duration=duration, w=width, h=height, supports_streaming=True))
        
        await client.send_file(event.chat_id, cached_file_path, caption=caption_text, thumb=thumb_path, attributes=attrs, progress_callback=progress_callback)
        await status_msg.edit("✅ Done!"); await asyncio.sleep(1); await status_msg.delete()
        if thumb_path and os.path.exists(thumb_path): os.remove(thumb_path)
        
    except Exception as e:
        await status_msg.edit(f"🚫 An unexpected error occurred: {e}"); print(f"Error in handle_download_request: {e}")

@client.on(events.NewMessage(pattern=r'^/shell (.+)', from_users=SUDO_USER))
async def shell_prepare(event):
    command = event.pattern_match.group(1); PENDING_SHELL_COMMANDS[event.chat_id] = command
    await event.reply(f"⚠️ **Confirm Execution?**\n\n💻 `{command}`\n\nReply with `/confirm` or `/cancel` to this message.")

@client.on(events.NewMessage(pattern=r'^/confirm$', from_users=SUDO_USER, func=lambda e: e.is_reply))
async def shell_confirm(event):
    if event.chat_id not in PENDING_SHELL_COMMANDS: return await event.reply("ℹ️ No pending command in this chat to confirm.")
    command = PENDING_SHELL_COMMANDS.pop(event.chat_id); await event.reply(f"🚀 Executing `{command}`...")
    output = run_shell_command(command); full_output = f"$ {command}\n\n{output or 'No output.'}"
    if len(full_output) > 4000:
        with BytesIO(full_output.encode()) as f:
            f.name = "shell_output.txt"
            await event.client.send_file(event.chat_id, f, caption="💻 Shell Output (too long)")
    else: await event.reply(f"```\n{full_output.strip()}\n```")

@client.on(events.NewMessage(pattern=r'^/cancel$', from_users=SUDO_USER, func=lambda e: e.is_reply))
async def shell_cancel(event):
    if event.chat_id in PENDING_SHELL_COMMANDS:
        command = PENDING_SHELL_COMMANDS.pop(event.chat_id)
        await event.reply(f"❌ Cancelled execution of `{command}`.")
    else: await event.reply("ℹ️ No pending command to cancel in this chat.")

async def main():
    """Initializes and runs the user bot, handling graceful shutdown."""
    global MAX_FILE_SIZE
    for directory in [CACHE_DIRECTORY, WORKERS_DIRECTORY]:
        if not os.path.isdir(directory): os.makedirs(directory)
    load_persistent_data()
    print("🚀 Bot is starting...")
    await client.start()
    me = await client.get_me()
    if getattr(me, "premium", False):
        MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024; print("🌟 Premium account detected. Max file size set to 4GB.")
    else: print("📦 Standard account. Max file size set to 2GB.")
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, STOP_EVENT.set)
    print(f"✅ Bot has started successfully. Sudo user is {SUDO_USER}.")
    print("👂 Listening for all commands and events... Press Ctrl+C to stop.")
    try: await STOP_EVENT.wait()
    finally:
        print("\n🛑 Shutdown signal received.")
        for name, data in WORKERS_STATE.items():
            if data.get('status') == 'running' and is_process_running(data.get('pid')):
                print(f"Stopping worker '{name}' (PID: {data['pid']})...")
                os.kill(data['pid'], signal.SIGTERM)
        if client.is_connected():
            print("🔌 Disconnecting client and shutting down gracefully...")
            await client.disconnect()
        print("👋 Goodbye!")

if __name__ == "__main__":
    asyncio.run(main())
