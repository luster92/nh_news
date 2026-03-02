
import feedparser
import requests
import os
import json
import datetime
import logging
import argparse
import threading
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

# Create a handler that rotates logs every day at midnight, keeping 3 days of logs
handler = TimedRotatingFileHandler(LOG_FILE, when="midnight", interval=1, backupCount=3, encoding='utf-8')
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
    "매일경제", "한국경제", "서울경제", "머니투데이", "파이낸셜뉴스", "이데일리", "헤럴드경제",

    # 신뢰도/비평/기타
    "노컷뉴스", "시사IN"
]

# 중요도 분류 키워드 (제목 기반 룰)
HIGH_IMPORTANCE_KEYWORDS = [
    "회장", "행장", "이사회", "국정감사", "감사", "검찰", "금감원", "금융위원회", "한국은행",
    "정책", "규제", "제재", "징계", "리스크", "부실", "횡령", "배임", "수사", "압수수색",
    "실적", "분기", "결산", "자본", "건전성", "연체", "충당금", "구조조정", "매각", "인수",
    "전략", "MOU", "신사업", "디지털전환", "노사", "파업"
]

MEDIUM_IMPORTANCE_KEYWORDS = [
    "출시", "확대", "협약", "업무협약", "캠페인", "지원", "대출", "예금", "적금", "플랫폼",
    "서비스", "이벤트", "프로모션", "교육", "봉사", "사회공헌"
]

LOW_CATEGORY_RULES = {
    "사회공헌": ["봉사", "기부", "사회공헌", "상생", "나눔", "헌혈"],
    "상품/서비스": ["출시", "서비스", "플랫폼", "앱", "대출", "예금", "적금"],
    "마케팅/행사": ["이벤트", "캠페인", "프로모션", "행사", "페스티벌", "홍보"],
    "조직/교육": ["교육", "연수", "워크숍", "채용", "인사", "발대식"]
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SUBSCRIBERS_FILE = "subscribers.json"
TELEGRAM_STATE_FILE = "telegram_state.json"
LOW_QUEUE_FILE = "low_importance_queue.json"
LAST_BROADCAST_FILE = "last_broadcast_message.json"
KST = datetime.timezone(datetime.timedelta(hours=9))
DIGEST_LOCK = threading.Lock()

# Freshness guards
HIGH_NEWS_MAX_AGE_HOURS = 2
DEFAULT_NEWS_MAX_AGE_HOURS = 24
QUEUE_MAX_AGE_HOURS = 24
FUTURE_TOLERANCE_MINUTES = 10

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    # Using gemini-3-flash-preview as requested
    model = genai.GenerativeModel('gemini-3-flash-preview')
else:
    logger.warning("GEMINI_API_KEY not found. Deduplication will be limited to exact matches.")
    model = None


def load_subscribers():
    """Load subscriber chat IDs from disk."""
    subscribers = set()

    # Seed with primary chat id from env for backward compatibility
    if TELEGRAM_CHAT_ID:
        subscribers.add(str(TELEGRAM_CHAT_ID))

    if not os.path.exists(SUBSCRIBERS_FILE):
        return subscribers

    try:
        with open(SUBSCRIBERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            for cid in data.get("chat_ids", []):
                subscribers.add(str(cid))
    except Exception as e:
        logger.error(f"Failed to load subscribers: {e}")

    return subscribers


def save_subscribers(subscribers):
    """Persist subscriber chat IDs to disk."""
    try:
        with open(SUBSCRIBERS_FILE, "w", encoding="utf-8") as f:
            json.dump({"chat_ids": sorted(list(subscribers))}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save subscribers: {e}")


def load_telegram_state():
    if not os.path.exists(TELEGRAM_STATE_FILE):
        return {"last_update_id": 0}
    try:
        with open(TELEGRAM_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load telegram state: {e}")
        return {"last_update_id": 0}


def save_telegram_state(state):
    try:
        with open(TELEGRAM_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save telegram state: {e}")


def send_telegram_reply(chat_id, message):
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Error: TELEGRAM_BOT_TOKEN not found in .env")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}

    try:
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending command reply to {chat_id}: {e}")


def load_last_broadcast_message():
    if not os.path.exists(LAST_BROADCAST_FILE):
        return None
    try:
        with open(LAST_BROADCAST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not data.get("message"):
                return None
            return data
    except Exception as e:
        logger.error(f"Failed to load last broadcast message: {e}")
        return None


def save_last_broadcast_message(message):
    try:
        payload = {
            "message": message,
            "saved_at": datetime.datetime.now(KST).isoformat()
        }
        with open(LAST_BROADCAST_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save last broadcast message: {e}")


def load_low_queue():
    if not os.path.exists(LOW_QUEUE_FILE):
        return {"items": [], "last_digest_date": None}
    try:
        with open(LOW_QUEUE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {
                "items": data.get("items", []),
                "last_digest_date": data.get("last_digest_date")
            }
    except Exception as e:
        logger.error(f"Failed to load low queue: {e}")
        return {"items": [], "last_digest_date": None}


def save_low_queue(queue_data):
    try:
        with open(LOW_QUEUE_FILE, "w", encoding="utf-8") as f:
            json.dump(queue_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save low queue: {e}")


def _parse_as_utc(dt_like):
    try:
        parsed = parser.parse(dt_like) if isinstance(dt_like, str) else dt_like
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def prune_stale_queue_items(queue_data):
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    max_age = datetime.timedelta(hours=QUEUE_MAX_AGE_HOURS)
    future_tolerance = datetime.timedelta(minutes=FUTURE_TOLERANCE_MINUTES)

    kept = []
    dropped = 0
    for item in queue_data.get("items", []):
        published_dt = _parse_as_utc(item.get("published"))
        if not published_dt:
            dropped += 1
            continue

        age = now_utc - published_dt
        if age > max_age or age < -future_tolerance:
            dropped += 1
            continue

        item["published"] = published_dt.isoformat()
        kept.append(item)

    if dropped:
        logger.info(f"Pruned {dropped} stale/invalid queued digest items.")

    queue_data["items"] = kept
    return queue_data


def enqueue_low_articles(articles):
    if not articles:
        return

    queue_data = prune_stale_queue_items(load_low_queue())
    existing_links = {item.get("link") for item in queue_data.get("items", [])}

    for article in articles:
        if article.get("link") in existing_links:
            continue

        published = article['published']
        if published.tzinfo is None:
            published = published.replace(tzinfo=datetime.timezone.utc)

        queue_data["items"].append({
            "title": article.get("title"),
            "link": article.get("link"),
            "published": published.isoformat(),
            "importance": article.get("importance", "LOW")
        })
        existing_links.add(article.get("link"))

    save_low_queue(queue_data)


def flush_low_digest_if_due():
    # news loop / command loop 동시 호출 시 중복 발송 방지
    with DIGEST_LOCK:
        now_kst = datetime.datetime.now(KST)
        today = now_kst.strftime("%Y-%m-%d")

        queue_data = prune_stale_queue_items(load_low_queue())
        save_low_queue(queue_data)
        items = queue_data.get("items", [])
        last_digest_date = queue_data.get("last_digest_date")

        if not items:
            return

        # 하루 1회, KST 18시 이후 발송
        if now_kst.hour < 18 or last_digest_date == today:
            return

        digest_articles = []
        for item in items:
            try:
                published_dt = parser.parse(item.get("published"))
            except Exception:
                published_dt = datetime.datetime.now(datetime.timezone.utc)

            digest_articles.append({
                "title": item.get("title", "(제목 없음)"),
                "link": item.get("link", ""),
                "published": published_dt,
                "importance": item.get("importance", "LOW")
            })

        digest_message = format_low_digest(digest_articles)
        if digest_message:
            logger.info(f"Sending daily [MEDIUM+LOW-DIGEST] for {len(digest_articles)} articles at 18:00 KST window.")
            send_telegram_message(digest_message)

        queue_data["items"] = []
        queue_data["last_digest_date"] = today
        save_low_queue(queue_data)


def process_subscriber_commands():
    """Process /start, /help, /subscribe, /unsubscribe commands via Telegram getUpdates."""
    if not TELEGRAM_BOT_TOKEN:
        return

    state = load_telegram_state()
    last_update_id = int(state.get("last_update_id", 0))
    subscribers = load_subscribers()

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"offset": last_update_id + 1, "timeout": 1, "allowed_updates": ["message"]}

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error polling Telegram updates: {e}")
        return

    if not payload.get("ok"):
        logger.error(f"Telegram getUpdates returned not ok: {payload}")
        return

    updates = payload.get("result", [])
    if not updates:
        return

    for update in updates:
        update_id = update.get("update_id", 0)
        if update_id > last_update_id:
            last_update_id = update_id

        msg = update.get("message", {})
        chat = msg.get("chat", {})
        chat_id = chat.get("id")
        text = (msg.get("text") or "").strip().lower()

        if not chat_id or not text.startswith("/"):
            continue

        chat_id_str = str(chat_id)

        if text.startswith("/start") or text.startswith("/subscribe"):
            subscribers.add(chat_id_str)
            save_subscribers(subscribers)
            send_telegram_reply(
                chat_id,
                "✅ NH_news 구독이 시작되었습니다.\n이제 농협은행 관련 뉴스를 받아보실 수 있습니다.\n중단하려면 /unsubscribe 를 입력하세요."
            )

            # 새 구독자에게 최근 발송 뉴스 샘플 1건 제공
            last_sample = load_last_broadcast_message()
            if last_sample:
                saved_at = last_sample.get("saved_at", "시간 정보 없음")
                sample_msg = (
                    "📰 최근 발송 뉴스 샘플입니다.\n"
                    f"(기준 시각: {saved_at})\n\n"
                    f"{last_sample.get('message', '')}"
                )
                send_telegram_reply(chat_id, sample_msg)
            else:
                send_telegram_reply(chat_id, "🗂 아직 발송 이력이 없어 샘플 뉴스가 없습니다. 곧 첫 알림을 보내드릴게요!")

            logger.info(f"Subscriber added: {chat_id_str}")

        elif text.startswith("/help"):
            send_telegram_reply(
                chat_id,
                "📌 NH_news 명령어 안내\n"
                "/start 또는 /subscribe : 뉴스 구독 시작\n"
                "/unsubscribe : 뉴스 구독 해제\n"
                "/help : 명령어 안내 보기\n\n"
                "알림 정책:\n"
                "- 🔴 높은 중요도 뉴스는 개별 알림 (최근 2시간 이내)\n"
                "- 🟡/⚪ 중간·낮은 중요도 뉴스는 매일 18:00(KST) 묶음 요약 발송"
            )
            logger.info(f"Help requested: {chat_id_str}")

        elif text.startswith("/unsubscribe"):
            if chat_id_str in subscribers:
                subscribers.remove(chat_id_str)
                save_subscribers(subscribers)
            send_telegram_reply(chat_id, "🛑 NH_news 구독이 해제되었습니다. 다시 시작하려면 /start")
            logger.info(f"Subscriber removed: {chat_id_str}")

    save_telegram_state({"last_update_id": last_update_id})


def get_sent_history():
    """Parses the log files to get titles of articles sent in the last 3 days."""
    sent_titles = []
    
    # 최근 3일치 날짜 문자열 생성
    date_prefixes = [
        (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(3)
    ]
    
    # 현재 로그 + 로테이션된 로그 파일 목록
    log_files = [LOG_FILE]
    for i in range(1, 4):
        rotated = f"{LOG_FILE}.{(datetime.datetime.now() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')}"
        if os.path.exists(rotated):
            log_files.append(rotated)
    
    for log_path in log_files:
        if not os.path.exists(log_path):
            continue
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if any(line.startswith(d) for d in date_prefixes) and "message for:" in line:
                        parts = line.split("message for: ")
                        if len(parts) > 1:
                            title = parts[1].strip()
                            sent_titles.append(title)
        except Exception as e:
            logger.error(f"Error reading log history from {log_path}: {e}")
    
    return list(set(sent_titles)) # Remove duplicates within history

def is_duplicate(new_title, history_titles):
    """Checks if the new title is a duplicate of any title in history."""
    if not history_titles:
        return False

    # 1. Exact match check (fast)
    if new_title in history_titles:
        return True

    # 2. Basic word-level similarity check (Jaccard-ish)
    # This acts as a robust fallback for similar titles (e.g. different source suffixes)
    # "NH농협은행, 개편 - 뉴스" vs "NH농협은행, 개편 - 서울경제"
    new_clean = new_title.split(" - ")[0].strip() # Strip source suffix if present
    new_words = set(new_clean.split())
    
    for h_title in history_titles:
        h_clean = h_title.split(" - ")[0].strip()
        h_words = set(h_clean.split())
        
        if not h_words or not new_words:
            continue
            
        intersection = new_words.intersection(h_words)
        union = new_words.union(h_words)
        similarity = len(intersection) / len(union)
        
        # If words match > 80%, consider it a duplicate regardless of AI
        if similarity > 0.8:
            logger.info(f"Deduplicated by string similarity ({similarity:.2f}): '{new_title}' matches '{h_title}'")
            return True

    if not model:
        return False

    # 3. Gemini AI check for semantic duplication
    recent_history = history_titles[-30:] # Last 30 titles for context
    
    prompt = f"""
    New news title: '{new_title}'
    
    Sent titles history:
    {recent_history}
    
    Does the 'New news title' report the EXACT SAME event or story as any title in the history?
    Common case: different media reporting the same press release or breaking news.
    If it's the same event, answer YES.
    If it's a different event or a follow-up with significant new info, answer NO.
    
    Answer ONLY 'YES' or 'NO'.
    """
    
    try:
        response = model.generate_content(prompt)
        answer = response.text.strip().upper()
        if "YES" in answer:
            logger.info(f"Deduplicated by Gemini: '{new_title}'")
            return True
    except Exception as e:
        logger.error(f"Gemini API error (model: {model.model_name}): {e}")
        
    return False

def classify_importance(title: str):
    """Classify article importance using lightweight keyword rules."""
    normalized = title.lower()

    for kw in HIGH_IMPORTANCE_KEYWORDS:
        if kw.lower() in normalized:
            return "HIGH", f"키워드:{kw}"

    for kw in MEDIUM_IMPORTANCE_KEYWORDS:
        if kw.lower() in normalized:
            return "MEDIUM", f"키워드:{kw}"

    return "LOW", "일반 동향"


def importance_rank(level: str) -> int:
    return {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(level, 2)


def fetch_news():
    """Fetches news from Google RSS and filters by media and time."""
    feed = feedparser.parse(RSS_URL)
    filtered_articles = []

    current_time = datetime.datetime.now(datetime.timezone.utc)
    default_time_limit = datetime.timedelta(hours=DEFAULT_NEWS_MAX_AGE_HOURS)
    high_time_limit = datetime.timedelta(hours=HIGH_NEWS_MAX_AGE_HOURS)
    future_tolerance = datetime.timedelta(minutes=FUTURE_TOLERANCE_MINUTES)

    logger.info(f"Fetching news from: {RSS_URL}")
    logger.info(f"Found {len(feed.entries)} entries.")

    for entry in feed.entries:
        title = entry.title
        link = entry.link
        published = entry.published
        source = entry.source.title if 'source' in entry else ""

        try:
            published_dt = parser.parse(published)
            if published_dt.tzinfo is None:
                published_dt = published_dt.replace(tzinfo=datetime.timezone.utc)
            published_dt = published_dt.astimezone(datetime.timezone.utc)
        except Exception as e:
            logger.error(f"Error parsing date {published}: {e}")
            continue

        # Skip suspicious future timestamps (clock skew / bad feed metadata)
        if (published_dt - current_time) > future_tolerance:
            continue

        is_target_media = False
        for media in TARGET_MEDIA:
             if media in source:
                 is_target_media = True
                 break

        if not is_target_media:
            # logger.info(f"Skipped (Source mismatch): [{source}] {title}")
            continue

        importance, reason = classify_importance(title)

        time_diff = current_time - published_dt
        if importance == "HIGH":
            if time_diff > high_time_limit:
                continue
        elif time_diff > default_time_limit:
            continue

        filtered_articles.append({
            'title': title,
            'link': link,
            'published': published_dt,
            'source': source,
            'importance': importance,
            'importance_reason': reason
        })

    # 중요한 뉴스 먼저 처리
    filtered_articles.sort(
        key=lambda a: (importance_rank(a.get('importance', 'LOW')), a['published']),
        reverse=False
    )

    return filtered_articles

def format_message(article):
    """Formats the article into a Telegram message."""
    title = article['title']

    # Convert to KST safely
    published = article['published']
    if published.tzinfo is None:
        published = published.replace(tzinfo=datetime.timezone.utc)
    kst_time = published.astimezone(datetime.timezone(datetime.timedelta(hours=9)))

    # Use YYYY-MM-DD format for clarity
    date_str = kst_time.strftime("%Y-%m-%d %H:%M")

    importance = article.get('importance', 'LOW')
    reason = article.get('importance_reason', '일반 동향')
    badge_map = {
        'HIGH': '🔴 중요도: 높음',
        'MEDIUM': '🟡 중요도: 중간',
        'LOW': '⚪ 중요도: 낮음'
    }
    importance_line = badge_map.get(importance, '⚪ 중요도: 낮음')

    link = article['link']
    message = (
        f"📢 **{title}**\n"
        f"{importance_line} ({reason})\n"
        f"🕒 {date_str} (KST)\n\n"
        f"🔗 [기사원문 읽기]({link})"
    )
    return message

def classify_low_category(title: str) -> str:
    normalized = title.lower()
    for category, keywords in LOW_CATEGORY_RULES.items():
        for kw in keywords:
            if kw.lower() in normalized:
                return category
    return "기타"


def format_low_digest(articles):
    """Format low-importance articles as one merged digest message grouped by category."""
    if not articles:
        return None

    grouped = {}
    for article in articles:
        category = classify_low_category(article.get("title", ""))
        grouped.setdefault(category, []).append(article)

    lines = ["🗂️ **중간/낮은 중요도 뉴스 일일 요약 (18:00 KST)**"]
    shown = 0
    max_items = 12

    for category in ["사회공헌", "상품/서비스", "마케팅/행사", "조직/교육", "기타"]:
        items = grouped.get(category, [])
        if not items:
            continue

        lines.append(f"\n• **{category}**")
        for article in items:
            if shown >= max_items:
                break
            published = article['published']
            if published.tzinfo is None:
                published = published.replace(tzinfo=datetime.timezone.utc)
            kst_time = published.astimezone(KST)
            date_str = kst_time.strftime("%m-%d %H:%M")
            imp = article.get("importance", "LOW")
            imp_badge = "🟡" if imp == "MEDIUM" else "⚪"
            lines.append(f"- {imp_badge} {article['title']} ({date_str})")
            lines.append(f"  🔗 [기사원문 읽기]({article['link']})")
            shown += 1

        if shown >= max_items:
            break

    if len(articles) > shown:
        lines.append(f"\n…외 {len(articles) - shown}건")

    return "\n".join(lines)


def send_telegram_message(message):
    """Broadcast message to all subscribers."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Error: TELEGRAM_BOT_TOKEN not found in .env")
        return

    subscribers = load_subscribers()
    if not subscribers:
        logger.warning("No subscribers found. Skipping broadcast.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    success = 0
    for chat_id in subscribers:
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            success += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending message to {chat_id}: {e}")

    logger.info(f"Message broadcast complete. Success: {success}/{len(subscribers)}")

    if success > 0:
        save_last_broadcast_message(message)

def run_news_cycle():
    logger.info("Starting news fetch cycle.")

    sent_history = get_sent_history()
    logger.info(f"Loaded {len(sent_history)} sent articles from history.")

    articles = fetch_news()
    logger.info(f"Found {len(articles)} candidate articles after basic filtering.")

    match_count = 0
    sent_stats = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    low_bucket = []

    for article in articles:
        if is_duplicate(article['title'], sent_history):
            continue

        importance = article.get('importance', 'LOW')

        if importance in ('MEDIUM', 'LOW'):
            low_bucket.append(article)
            sent_history.append(article['title'])
            match_count += 1
            sent_stats[importance] = sent_stats.get(importance, 0) + 1
            continue

        message = format_message(article)
        logger.info(
            f"Sending [{importance}] message for: {article['title']}"
        )
        send_telegram_message(message)

        sent_history.append(article['title'])
        match_count += 1
        sent_stats[importance] = sent_stats.get(importance, 0) + 1

    if low_bucket:
        enqueue_low_articles(low_bucket)
        logger.info(f"Queued {len(low_bucket)} MEDIUM/LOW articles for 18:00 KST digest.")

    flush_low_digest_if_due()

    logger.info(
        f"Cycle finished. Sent {match_count} new articles "
        f"(HIGH={sent_stats.get('HIGH', 0)}, MEDIUM={sent_stats.get('MEDIUM', 0)}, LOW={sent_stats.get('LOW', 0)})."
    )


def run_news_loop(interval_minutes: int):
    logger.info(f"News loop started. Interval: {interval_minutes} minutes.")
    while True:
        try:
            run_news_cycle()
        except Exception as e:
            logger.error(f"Unexpected error in news loop: {e}")

        logger.info(f"[News Loop] Sleeping for {interval_minutes} minutes...")
        time.sleep(interval_minutes * 60)


def run_command_loop(interval_seconds: int):
    logger.info(f"Command loop started. Poll interval: {interval_seconds} seconds.")
    while True:
        try:
            process_subscriber_commands()
            flush_low_digest_if_due()
        except Exception as e:
            logger.error(f"Unexpected error in command loop: {e}")

        time.sleep(interval_seconds)

def main():
    parser = argparse.ArgumentParser(description="NH News Crawler Daemon")
    parser.add_argument("--loop", action="store_true", help="Run in continuous loops")
    parser.add_argument("--interval", type=int, default=10, help="News loop interval in minutes (default: 10)")
    parser.add_argument("--command-interval", type=int, default=30, help="Command poll interval in seconds (default: 30)")
    args = parser.parse_args()

    if args.loop:
        logger.info(
            f"Starting split LOOP mode. news_interval={args.interval}m, "
            f"command_interval={args.command_interval}s"
        )

        command_thread = threading.Thread(
            target=run_command_loop,
            args=(args.command_interval,),
            daemon=True,
            name="telegram-command-loop"
        )
        command_thread.start()

        # Run news loop in main thread
        run_news_loop(args.interval)
    else:
        # One-shot execution for manual tests
        process_subscriber_commands()
        run_news_cycle()

if __name__ == "__main__":
    main()
