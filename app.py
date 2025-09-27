import os
import re
import time
import json
import asyncio
import signal
import hashlib
import subprocess
from io import BytesIO
from typing import Dict
from datetime import datetime
from dateutil.parser import parse

# Third-party libraries
from dotenv import load_dotenv
import aiohttp
import yt_dlp
from PIL import Image
from gtts import gTTS
from telethon import TelegramClient, events, Button
from telethon.tl.functions.channels import EditBannedRequest
from telethon.tl.functions.contacts import BlockRequest, UnblockRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.types import ChatBannedRights, DocumentAttributeAudio, DocumentAttributeVideo, DocumentAttributeFilename
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata

# Load environment variables from .env file at the very beginning
load_dotenv()

## ----------------------------------------------------------------------------------------------------------------
## --- CONFIGURATION ---
## ----------------------------------------------------------------------------------------------------------------

# Configurations are now loaded from the .env file with defaults
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SUDO_USER = os.getenv("SUDO_USER")
BOT_TOKEN = os.getenv("BOT_TOKEN") or None
GPT_API_KEY = os.getenv("GPT_API_KEY") or None

SESSION_NAME = os.getenv("SESSION_NAME", "my_bot_session")
AUTH_FILE = os.getenv("AUTH_FILE", "auth_users.txt")
AFK_FILE = os.getenv("AFK_FILE", "afk_status.json")

# Non-sensitive configurations
API_BASE_URL = "http://35.221.9.111:9200/download"
CACHE_DIRECTORY = "downloads"
YOUTUBE_ID_REGEX = r"(?:https?:\/\/)?(?:www\.|m\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/)|youtu\.be\/)([\w-]{11})(?:\S+)?"
FACEBOOK_REGEX = r"(?:https?:\/\/)?(?:www\.|m\.|web\.)?(facebook\.com|fb\.watch)\/(?:video\.php\?v=\d+|\S+\/videos\/\d+|\S+\/reel\/\d+|watch\/\?v=\d+|reel\/\d+|\d{15,})\/?"
TIKTOK_REGEX = r"(?:https?:\/\/)?(?:www\.|vm\.|vt\.)?tiktok\.com\/.+"
INSTAGRAM_REGEX = r"(?:https?:\/\/)?(?:www\.)?instagram\.com\/(?:p|reel|tv)\/[\w\-]+"

## ----------------------------------------------------------------------------------------------------------------
## --- GLOBALS & STATE MANAGEMENT ---
## ----------------------------------------------------------------------------------------------------------------

AUTH_USERS = set()
AFK_STATE = {"is_afk": False, "reason": "", "since": 0}
PENDING_SHELL_COMMANDS: Dict[int, str] = {}
ACTIVE_DOWNLOADS = set()
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
START_TIME = time.monotonic()
STOP_EVENT = asyncio.Event()

# Startup check for essential configurations
try:
    API_ID = int(API_ID)
    SUDO_USER = int(SUDO_USER)
    if not API_HASH: raise ValueError("API_HASH is not set in .env file.")
except (ValueError, TypeError):
    print("🚫 FATAL ERROR: API_ID, API_HASH, and SUDO_USER must be set in your .env file.")
    exit(1)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

## ----------------------------------------------------------------------------------------------------------------
## --- HELPER FUNCTIONS ---
## ----------------------------------------------------------------------------------------------------------------
def load_persistent_data():
    """Loads all persistent data (auth users, AFK status) from files on startup."""
    global AFK_STATE, AUTH_USERS
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
## --- DUAL MENU SYSTEM ---
## ----------------------------------------------------------------------------------------------------------------

STATIC_MENU_USER = """
**🤖 Bot Menu**
---
**🎵 Media Commands**
• `/play <query>`: Searches and sends a song.
• `/ytmp3 <url>`: Sends YouTube audio.
• `/ytmp4 <url>`: Sends YouTube video.
• `/fbmp4 <url>`: Sends a Facebook video.
• `/igmp4 <url>`: Sends an Instagram video.
• `/ttmp4 <url>`: Sends a TikTok video.
---
**🛠️ Tools**
• `/sticker <reply>`: Converts replied-to image to a sticker.
• `/toimage <reply>`: Converts replied-to sticker to an image.
• `/tovnote <reply>`: Converts replied-to text to a voice note.
• `/vv <reply>`: Reveals view-once media.
• `/gpt <prompt>`: Chat with an AI assistant.
---
**⚙️ Utility Commands**
• `/ping`: Checks bot latency.
• `/uptime`: Shows the bot's uptime.
• `/info <reply>`: Gets info about a user.
• `/pp <reply>`: Gets a user's profile picture.
• `/menu`: Shows this help menu.

*👑 Sudo users can use `/menuadmin` for a full list of commands.*
"""

STATIC_MENU_ADMIN = """
**🤖 Bot Admin Menu**
---
**🛡️ Moderation `(Sudo)`**
• `/ban|unban|mute|unmute <reply>`
• `/kick|promote|demote <reply>`
• `/pin <reply>` | `/unpin <reply|none>`
• `/del <reply>` | `/tagall [message]`
• `/block|unblock <reply>`
• `/linkgc`
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
*This menu also includes all commands from the regular `/menu`.*
"""

if BOT_TOKEN:
    MENU_MAIN_TEXT = "**🤖 Bot Menu**\n\nSelect a category to view commands."
    MENU_ADMIN_TEXT = "**👑 Bot Admin Menu**\n\nSelect a category to view Sudo commands."
    MENU_TEXTS = {
        "media": STATIC_MENU_USER.split('---')[1].strip(),
        "tools": STATIC_MENU_USER.split('---')[2].strip(),
        "utility": STATIC_MENU_USER.split('---')[3].strip().split('*👑 Sudo users*')[0].strip(),
        "moderation": STATIC_MENU_ADMIN.split('---')[1].strip(),
        "afk": STATIC_MENU_ADMIN.split('---')[2].strip(),
        "user_admin": STATIC_MENU_ADMIN.split('---')[3].strip(),
    }
    USER_BUTTONS = [
        [Button.inline("🎵 Media", b"menu_media"), Button.inline("🛠️ Tools", b"menu_tools")],
        [Button.inline("⚙️ Utility", b"menu_utility"), Button.inline("Close Menu", b"menu_close")]
    ]
    ADMIN_BUTTONS = [
        [Button.inline("🛡️ Moderation", b"menu_moderation"), Button.inline("🎵 Media", b"menu_media")],
        [Button.inline("😴 AFK", b"menu_afk"), Button.inline("👑 User Admin", b"menu_user_admin")],
        [Button.inline("🛠️ Tools", b"menu_tools"), Button.inline("⚙️ Utility", b"menu_utility")],
        [Button.inline("Close Menu", b"menu_close")]
    ]

    @client.on(events.CallbackQuery)
    async def menu_callback_handler(event):
        if event.sender_id not in AUTH_USERS:
            return await event.answer("You are not authorized to use this menu.", alert=True)

        query_data = event.data.decode('utf-8')
        page = query_data.split('_', 1)[1]
        if page == "close": return await event.delete()

        back_buttons = ADMIN_BUTTONS if event.sender_id == SUDO_USER else USER_BUTTONS
        back_text = MENU_ADMIN_TEXT if event.sender_id == SUDO_USER else MENU_MAIN_TEXT

        if page == "main": await event.edit(back_text, buttons=back_buttons)
        elif page in MENU_TEXTS: await event.edit(MENU_TEXTS[page], buttons=[Button.inline("« Back", b"menu_main")])
        await event.answer()

@client.on(events.NewMessage(pattern=r'^/menu(?:\s|$)'))
async def menu_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if BOT_TOKEN: await event.reply(MENU_MAIN_TEXT, buttons=USER_BUTTONS)
    else: await event.reply(STATIC_MENU_USER, link_preview=False)

@client.on(events.NewMessage(pattern=r'^/menuadmin(?:\s|$)'))
async def menu_admin_handler(event):
    if event.sender_id != SUDO_USER: return
    if BOT_TOKEN: await event.reply(MENU_ADMIN_TEXT, buttons=ADMIN_BUTTONS)
    else: await event.reply(STATIC_MENU_ADMIN, link_preview=False)

## ----------------------------------------------------------------------------------------------------------------
## --- ALL COMMAND HANDLERS ---
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
    await event.edit(f"**Bot Uptime:** `{get_readable_time(int(uptime_seconds))}`")

@client.on(events.NewMessage(pattern=r'^/info(?:\s|$)'))
async def info_handler(event):
    if event.sender_id not in AUTH_USERS: return
    target = event.sender
    if event.is_reply: target = await (await event.get_reply_message()).get_sender()
    info_msg = (f"**User Info:**\n"
                f"**ID:** `{target.id}`\n**First Name:** `{target.first_name}`\n"
                f"**Last Name:** `{target.last_name or 'N/A'}`\n"
                f"**Username:** `@{target.username}`\n" if target.username else ""
                f"**Profile Link:** [Click here](tg://user?id={target.id})\n**Is Bot:** `{target.bot}`")
    await event.edit(info_msg)
    
@client.on(events.NewMessage(pattern=r'^/pp(?:\s|$)'))
async def pp_handler(event):
    if event.sender_id not in AUTH_USERS: return
    target = event.sender
    if event.is_reply: target = await (await event.get_reply_message()).get_sender()
    photos = await client.get_profile_photos(target)
    if not photos: return await event.edit("This user has no profile pictures.")
    await event.delete()
    await client.send_file(event.chat_id, photos[0], caption=f"Profile picture of `{target.first_name}`.")

# Tool Commands
@client.on(events.NewMessage(pattern=r'^/vv(?:\s|$)'))
async def vv_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if not event.is_reply: return await event.edit("⚠️ Reply to a view-once message.")
    reply_msg = await event.get_reply_message()
    if not reply_msg or not reply_msg.media: return await event.edit("🚫 Replied message is not a media file.")
    
    status_msg = await event.edit("`Revealing...`")
    try:
        file_path = await client.download_media(reply_msg)
        await client.send_file(event.chat_id, file_path, caption="🔓 View-once media revealed.")
        os.remove(file_path)
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit(f"🚫 **Error:** {e}")

@client.on(events.NewMessage(pattern=r'^/sticker(?:\s|$)'))
async def sticker_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if not event.is_reply: return await event.edit("⚠️ Reply to an image to make a sticker.")
    reply_msg = await event.get_reply_message()
    if not reply_msg or not reply_msg.photo: return await event.edit("🚫 Replied message is not a photo.")
    
    status_msg = await event.edit("`Creating sticker...`")
    img_path = await client.download_media(reply_msg.photo)
    sticker_path = os.path.join(CACHE_DIRECTORY, "sticker.webp")
    try:
        with Image.open(img_path) as im:
            im.thumbnail((512, 512)); im.save(sticker_path, "WEBP")
        await client.send_file(event.chat_id, sticker_path)
        await status_msg.delete()
    except Exception as e: await status_msg.edit(f"🚫 **Error:** {e}")
    finally:
        if os.path.exists(img_path): os.remove(img_path)
        if os.path.exists(sticker_path): os.remove(sticker_path)

@client.on(events.NewMessage(pattern=r'^/toimage(?:\s|$)'))
async def to_image_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if not event.is_reply: return await event.edit("⚠️ Reply to a sticker.")
    reply_msg = await event.get_reply_message()
    if not reply_msg or not reply_msg.sticker: return await event.edit("🚫 Replied message is not a sticker.")
    
    status_msg = await event.edit("`Converting sticker...`")
    sticker_path = await client.download_media(reply_msg.sticker)
    img_path = os.path.join(CACHE_DIRECTORY, "image.jpg")
    try:
        if sticker_path.endswith('.webp'):
            with Image.open(sticker_path) as im: im.save(img_path, "JPEG")
            await client.send_file(event.chat_id, img_path)
            await status_msg.delete()
        else: await status_msg.edit("🚫 This bot currently only supports converting static `.webp` stickers.")
    except Exception as e: await status_msg.edit(f"🚫 **Error:** {e}")
    finally:
        if os.path.exists(sticker_path): os.remove(sticker_path)
        if os.path.exists(img_path): os.remove(img_path)

@client.on(events.NewMessage(pattern=r'^/tovnote(?:\s|$)'))
async def to_vnote_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if not event.is_reply: return await event.edit("⚠️ Reply to a text message.")
    reply_msg = await event.get_reply_message()
    if not reply_msg or not reply_msg.text: return await event.edit("🚫 Replied message has no text.")

    status_msg = await event.edit("`Converting to voice note...`")
    vnote_path = os.path.join(CACHE_DIRECTORY, "voice.ogg")
    try:
        tts = await run_sync_in_executor(lambda: gTTS(reply_msg.text))
        tts.save(vnote_path)
        await client.send_file(event.chat_id, vnote_path, voice_note=True)
        await status_msg.delete()
    except Exception as e: await status_msg.edit(f"🚫 **Error:** {e}")
    finally:
        if os.path.exists(vnote_path): os.remove(vnote_path)

@client.on(events.NewMessage(pattern=r'^/gpt(?:\s|$)'))
async def gpt_handler(event):
    if event.sender_id not in AUTH_USERS: return
    if not GPT_API_KEY: return await event.edit("🚫 **GPT Error:** `GPT_API_KEY` is not set in the `.env` file.")
    try: _, prompt = event.text.split(' ', 1)
    except (ValueError, IndexError): return await event.edit("📋 **Usage:** `/gpt <prompt>`")
    
    status_msg = await event.edit("🤖 **Thinking...**")
    try:
        url = "https://api.together.xyz/v1/chat/completions"; headers = {"Authorization": f"Bearer {GPT_API_KEY}"}
        payload = {"model": "mistralai/Mixtral-8x7B-Instruct-v0.1", "messages": [{"role": "user", "content": prompt}], "temperature": 0.7, "max_tokens": 1500}
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    reply = result["choices"][0]["message"]["content"]
                    await status_msg.edit(f"**💡 Response:**\n\n{reply}")
                else: await status_msg.edit(f"⚠️ API error `{resp.status}`:\n`{await resp.text()}`")
    except Exception as e: await status_msg.edit(f"🚫 **Error:** {e}")

# Sudo-only Commands
MUTE_RIGHTS = ChatBannedRights(until_date=None, send_messages=True)
UNMUTE_RIGHTS = ChatBannedRights(until_date=None, send_messages=False)
@client.on(events.NewMessage(pattern=r'^/(ban|unban|mute|unmute|kick|promote|demote)(?:\s|$)'))
async def moderation_handler(event):
    if event.sender_id != SUDO_USER: return
    command = event.pattern_match.group(1)
    if not event.is_group: return await event.edit("❌ This command only works in groups.")
    if not event.is_reply: return await event.edit(f"⚠️ Please reply to a user's message to `{command}` them.")
    reply_msg = await event.get_reply_message(); target_user = await client.get_entity(reply_msg.sender_id); chat = await event.get_chat()
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
@client.on(events.NewMessage(pattern=r'^/(pin|unpin)(?:\s|$)'))
async def pin_handler(event):
    if event.sender_id != SUDO_USER: return
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
@client.on(events.NewMessage(pattern=r'^/del(?:\s|$)'))
async def delete_handler(event):
    if event.sender_id != SUDO_USER: return
    if not event.is_reply: return await event.edit("Reply to a message to delete it.")
    reply_msg = await event.get_reply_message()
    try: await reply_msg.delete(); await event.delete()
    except Exception as e: await event.edit(f"🚫 **Error:** {e}")
@client.on(events.NewMessage(pattern=r'^/tagall(?:\s|$)'))
async def tag_all_handler(event):
    if event.sender_id != SUDO_USER: return
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
@client.on(events.NewMessage(pattern=r'^/afk(?:\s|$)'))
async def afk_handler(event):
    if event.sender_id != SUDO_USER: return
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
if not BOT_TOKEN:
    @client.on(events.NewMessage(outgoing=True, from_users=SUDO_USER))
    async def auto_disable_afk(event):
        global AFK_STATE
        if AFK_STATE.get("is_afk") and not event.text.lower().startswith(('/afk', '/del', '/unpin', '/pin', '/shell')):
            duration = get_readable_time(int(time.time()) - AFK_STATE.get("since", 0))
            AFK_STATE["is_afk"] = False; save_afk_state()
            await client.send_message('me', f"**AFK mode disabled.** You were away for {duration}.")
@client.on(events.NewMessage(pattern=r'^/(adduser|deluser)(?:\s|$)'))
async def user_admin_handler(event):
    if event.sender_id != SUDO_USER: return
    command = event.pattern_match.group(1)
    try:
        if event.reply_to_msg_id: user_id = (await event.get_reply_message()).sender_id
        else: _, user_input = event.text.split(' ', 1); user_id = int(user_input)
        
        if command == "adduser":
            if user_id in AUTH_USERS: return await event.edit(f"✔️ User `{user_id}` is already authorized.")
            AUTH_USERS.add(user_id); save_auth_users()
            await event.edit(f"👍 User `{user_id}` has been authorized.")
        elif command == "deluser":
            if user_id == SUDO_USER: return await event.edit("🚫 You cannot remove the sudo user.")
            if user_id in AUTH_USERS:
                AUTH_USERS.remove(user_id); save_auth_users()
                await event.edit(f"🗑️ User `{user_id}` has been removed from the authorized list.")
            else: await event.edit(f"🤔 User `{user_id}` was not in the authorized list.")
    except (ValueError, IndexError): await event.edit(f"📋 Usage: `/{command} <user_id>` or reply to a user.")
    except Exception as e: await event.edit(f"🚫 Error: {e}")
@client.on(events.NewMessage(pattern=r'^/listusers(?:\s|$)', from_users=SUDO_USER))
async def list_users(event):
    msg = "**👥 Authorized Users:**\n\n"
    for uid in sorted(list(AUTH_USERS)):
        msg += f"🔹 [{uid}](tg://user?id={uid})"; msg += " `(Sudo)`" if uid == SUDO_USER else ""; msg += "\n"
    await event.reply(msg)
@client.on(events.NewMessage(pattern=r'^/(ytmp3|ytmp4|play|fbmp4|ttmp4|igmp4)(?:\s|$)'))
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
            if command_name in ['/ytmp3', '/ytmp4']: source, file_type, url_regex, error_msg = "youtube", "mp3" if command_name == '/ytmp3' else "mp4", YOUTUBE_ID_REGEX, "🚫 Invalid YouTube URL."
            elif command_name == '/fbmp4': source, file_type, url_regex, error_msg = "facebook", "mp4", FACEBOOK_REGEX, "🚫 Invalid Facebook URL."
            elif command_name == '/ttmp4': source, file_type, url_regex, error_msg = "tiktok", "mp4", TIKTOK_REGEX, "🚫 Invalid TikTok URL."
            elif command_name == '/igmp4': source, file_type, url_regex, error_msg = "instagram", "mp4", INSTAGRAM_REGEX, "🚫 Invalid Instagram URL."
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
        video_url = first_result.get('webpage_url') or f"https://www.youtube.com/watch?v={first_result['id']}"
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
        api_endpoint_map = {"youtube": "youtube/videofhd" if file_type == "mp4" else "youtube/audio", "facebook": "facebook/video", "tiktok": "tiktok/video", "instagram": "instagram/video"}
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
            os.remove(cached_file_path); return await status_msg.edit(f"🚫 File too large: {human_readable_size(file_size)} "
                                       f"(limit: {human_readable_size(MAX_FILE_SIZE)}).\n"
                                       f"🗑️ Removed from cache.")
        base_caption = f"**Title:** \n**Quality:** `{quality}`"; available_space = 1024 - len(base_caption) - 4
        if len(title) > available_space: title = title[:available_space - 3] + "..."
        caption_text = f"**Title:** `{title}`\n**Quality:** `{quality}`"
        upload_start_time = time.monotonic(); last_edit_time = 0
        async def progress_callback(current, total):
            nonlocal last_edit_time; current_time = time.monotonic()
            if current_time - last_edit_time < 2: return
            last_edit_time = current_time; percent = (current / total) * 100
            elapsed_time = current_time - upload_start_time
            speed = current / elapsed_time if elapsed_time > 0 else 0
            progress_bar = "".join(["▰" if i < percent / 10 else "▱" for i in range(10)])
            try:
                await status_msg.edit(f"📤 **Uploading:** `{title}`\n"
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
    if not os.path.isdir(CACHE_DIRECTORY): os.makedirs(CACHE_DIRECTORY)
    load_persistent_data()
    print("🚀 Bot is starting...")
    try:
        if BOT_TOKEN:
            print("Starting in Bot Mode..."); await client.start(bot_token=BOT_TOKEN)
        else:
            print("Starting in User Bot Mode..."); await client.start()
    except Exception as e:
        print(f"🚫 Failed to start client: {e}"); return
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
        if client.is_connected():
            print("🔌 Disconnecting client and shutting down gracefully...")
            await client.disconnect()
        print("👋 Goodbye!")

if __name__ == "__main__":
    asyncio.run(main())
