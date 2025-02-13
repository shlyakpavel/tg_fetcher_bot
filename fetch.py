import os
import asyncio
from datetime import datetime, timedelta, timezone
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
import boto3
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNELS = os.getenv("TELEGRAM_CHANNELS").split(',')  # Каналы через запятую
S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("S3_REGION")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

client = TelegramClient('bot_session', API_ID, API_HASH).start()

s3_client = boto3.client(
    's3',
	endpoint_url="https://fra1.digitaloceanspaces.com",
    region_name=S3_REGION,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

async def fetch_and_upload():
    async with client:
        all_messages = []
        for channel in CHANNELS:
            try:
                entity = await client.get_entity(channel)

                await client(JoinChannelRequest(entity))

                yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                async for message in client.iter_messages(entity, limit=100):
                    if message.date >= yesterday:
                        m = f"[{message.date}] {message.sender_id}: {message.text}\n"
                        all_messages.append(m)
            except Exception as e:
                print(f"Ошибка с каналом {channel}: {e}")
                continue

        if all_messages:
            filename = f"messages_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')}.txt"
            print(filename)
            with open(filename, "w", encoding="utf-8") as file:
                file.writelines(all_messages)

            s3_client.upload_file(filename, S3_BUCKET, filename)
            print(f"Файл {filename} загружен в {S3_BUCKET}")
            os.remove(filename)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_and_upload())
