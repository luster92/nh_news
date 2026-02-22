
import feedparser
import requests
import os
import datetime
import logging
import argparse
import time
from logging.handlers import TimedRotatingFileHandler
from dateutil import parser
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables
load_dotenv()

# Logger configuration
LOG_FILE = "nh_news.log"
logger = logging.getLogger("NH_News_Logger")
logger.setLevel(logging.INFO)

# Create a handler that rotates logs every day at midnight, keeping 7 days of logs
handler = TimedRotatingFileHandler(LOG_FILE, when="midnight", interval=1, backupCount=7, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Add a stream handler to also print to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Configuration
RSS_URL = "https://news.google.com/rss/search?q=%EB%86%8D%ED%98%91%EC%9D%80%ED%96%89+OR+%EB%86%8D%ED%98%91%EC%A4%91%EC%95%99%ED%9A%8C&hl=ko&gl=KR&ceid=KR:ko"

TARGET_MEDIA = [
    # 주요 일간지 (전국종합지)
    "조선일보", "중앙일보", "동아일보", "한겨레", "경향신문",
    "한국일보", "서울신문", "국민일보", "세계일보", "문화일보",
    
    # 지상파 및 종합편성/보도전문 채널
    "KBS", "MBC", "SBS", "JTBC", "YTN",
    "연합뉴스TV", "MBN", "TV조선", "채널A",
    
    # 뉴스통신사
    "연합뉴스", "뉴시스", "뉴스1",
    
    # 경제 전문지
    "매일경제", "한국경제", "서울경제", "머니투데이", "파이낸셜뉴스", "이데일리",
    
    # 신뢰도/비평/기타
    "노컷뉴스", "시사IN"
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash')
else:
    logger.warning("GEMINI_API_KEY not found. Deduplication will be disabled.")
    model = None

def get_sent_history():
    """Parses the log file to get titles of articles sent TODAY."""
    sent_titles = []
    if not os.path.exists(LOG_FILE):
        return sent_titles
    
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith(today_str) and "Sending message for:" in line:
                    parts = line.split("Sending message for: ")
                    if len(parts) > 1:
                        title = parts[1].strip()
                        sent_titles.append(title)
    except Exception as e:
        logger.error(f"Error reading log history: {e}")
    
    return sent_titles

def is_duplicate(new_title, history_titles):
    """Checks if the new title is a duplicate of any title in history using Gemini."""
    if not model or not history_titles:
        return False

    if new_title in history_titles:
        return True

    recent_history = history_titles[-50:]
    
    prompt = f"""
    Here is a new news title: '{new_title}'
    
    Here is a list of news titles that have already been sent:
    {recent_history}
    
    Does the new news title report the EXACT SAME event/story as any of the sent titles?
    If it's just a similar topic but a different specific report or angle, it's NOT a duplicate.
    If it's covering the same breaking news or press release, it IS a duplicate.
    
    Answer only YES or NO.
    """
    
    try:
        response = model.generate_content(prompt)
        answer = response.text.strip().upper()
        if "YES" in answer:
            return True
    except Exception as e:
        logger.error(f"Gemini API error: {e}")
        
    return False

def fetch_news():
    """Fetches news from Google RSS and filters by media and time."""
    feed = feedparser.parse(RSS_URL)
    filtered_articles = []
    
    current_time = datetime.datetime.now(datetime.timezone.utc)
    time_limit = datetime.timedelta(hours=3)

    logger.info(f"Fetching news from: {RSS_URL}")
    logger.info(f"Found {len(feed.entries)} entries.")

    for entry in feed.entries:
        title = entry.title
        link = entry.link
        published = entry.published
        source = entry.source.title if 'source' in entry else ""
        
        try:
            published_dt = parser.parse(published)
        except Exception as e:
            logger.error(f"Error parsing date {published}: {e}")
            continue

        time_diff = current_time - published_dt
        if time_diff > time_limit:
            continue

        is_target_media = False
        for media in TARGET_MEDIA:
             if media in source:
                 is_target_media = True
                 break
        
        if not is_target_media:
            logger.info(f"Skipped (Source mismatch): [{source}] {title}")
            continue

        filtered_articles.append({
            'title': title,
            'link': link,
            'published': published_dt,
            'source': source
        })

    return filtered_articles

def format_message(article):
    """Formats the article into a Telegram message."""
    title = article['title']
    date_str = article['published'].strftime("%y-%m-%d %H:%M")
    link = article['link']
    message = (
        f"📢 **{title}**\n"
        f"🕒 {date_str}\n\n"
        f"🔗 [기사원문 읽기]({link})"
    )
    return message

def send_telegram_message(message):
    """Sends a message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Error: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not found in .env")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Message sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending message: {e}")

def run_cycle():
    logger.info("Starting news fetch cycle.")
    logger.info(f"Current time (UTC): {datetime.datetime.now(datetime.timezone.utc)}")
    
    sent_history = get_sent_history()
    logger.info(f"Loaded {len(sent_history)} sent articles from history.")
    
    articles = fetch_news()
    logger.info(f"Found {len(articles)} candidate articles after basic filtering.")
    
    match_count = 0
    for article in articles:
        if is_duplicate(article['title'], sent_history):
            logger.info(f"Skipped (Duplicate): {article['title']}")
            continue
            
        message = format_message(article)
        logger.info(f"Sending message for: {article['title']}")
        send_telegram_message(message)
        
        sent_history.append(article['title'])
        match_count += 1
    
    logger.info(f"Cycle finished. Sent {match_count} new articles.")

def main():
    parser = argparse.ArgumentParser(description="NH News Crawler Daemon")
    parser.add_argument("--loop", action="store_true", help="Run in a continuous loop")
    parser.add_argument("--interval", type=int, default=10, help="Wait interval in minutes (default: 10)")
    args = parser.parse_args()

    if args.loop:
        logger.info(f"Starting in LOOP mode. Interval: {args.interval} minutes.")
        while True:
            try:
                run_cycle()
            except Exception as e:
                logger.error(f"Unexpected error in loop: {e}")
            
            logger.info(f"Sleeping for {args.interval} minutes...")
            time.sleep(args.interval * 60)
    else:
        run_cycle()

if __name__ == "__main__":
    main()
