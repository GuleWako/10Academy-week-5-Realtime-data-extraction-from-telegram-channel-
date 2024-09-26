from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon import events  # Import events for real-time message listening
from telethon.errors import SessionPasswordNeededError
import pandas as pd
import os
import re

# Function to initialize the Telegram client
async def create_telegram_client(api_id, api_hash, phone):
    client = TelegramClient('session_name', api_id, api_hash)
    await client.connect()
    
    # Check if client is authorized
    if not await client.is_user_authorized():
        print("Sending code request...")
        await client.send_code_request(phone)
        code = input("Enter the code you received on Telegram: ")
        try:
            await client.sign_in(phone, code)
        except SessionPasswordNeededError:
            password = input("Two-step verification enabled. Please enter your password: ")
            await client.sign_in(password=password)
    
    return client

# Function to extract Amharic text, numbers, and the + symbol
def extract_amharic_text_and_numbers(text):
    amharic_and_number_pattern = r'[\u1200-\u137F0-9\+\-_]+'
    amharic_words_and_numbers = re.findall(amharic_and_number_pattern, text)
    return ' '.join(amharic_words_and_numbers)

# Async function to scrape old messages from Telegram
async def scrape_telegram_messages(client, channel_username, limit=1000):
    entity = await client.get_entity(channel_username)
    history = await client(GetHistoryRequest(
        peer=entity,
        limit=limit,
        offset_date=None,
        offset_id=0,
        max_id=0,
        min_id=0,
        add_offset=0,
        hash=0
    ))

    messages = []
    for message in history.messages:
        if message.message:
            amharic_text_and_numbers = extract_amharic_text_and_numbers(message.message)
            if amharic_text_and_numbers:
                messages.append({
                    'message_id': message.id,
                    'date': message.date,
                    'sender_id': message.from_id.user_id if message.from_id else None,
                    'amharic_message': amharic_text_and_numbers
                })

    return pd.DataFrame(messages)

# Function to save real-time messages (text, images, documents)
async def save_real_time_message(message):
    SAVE_DIR = 'telegram_data/'
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

    if message.text:
        print(f"Text message: {message.text}")
        with open(f'{SAVE_DIR}/messages.txt', 'a', encoding='utf-8') as f:
            f.write(f'{message.date}: {message.text}\n')

    if message.photo:
        photo_path = await message.download_media(file=f'{SAVE_DIR}/photos/')
        print(f"Photo saved: {photo_path}")
    
    if message.document:
        document_path = await message.download_media(file=f'{SAVE_DIR}/documents/')
        print(f"Document saved: {document_path}")

