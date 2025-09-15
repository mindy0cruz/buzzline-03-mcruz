"""
json_consumer_mcruz.py

JSON messages about WNBA players

Example Kafka message format:
{
  "timestamp": "2025-09-14T23:30:00Z",
  "player": "A'ja Wilson",
  "team": "Las Vegas Aces",
  "ppg": 22.8,
  "rpg": 9.5,
  "apg": 2.1
}
"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import deque

from dotenv import load_dotenv

# Import Kafka + logging utils
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "wnba_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "wnba_consumer_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_hot_threshold() -> float:
    """Fetch hot streak threshold (ppg) or use default."""
    threshold = float(os.getenv("WNBA_HOT_THRESHOLD", 20.0))
    logger.info(f"Hot streak threshold: {threshold} PPG")
    return threshold


def get_rolling_window_size() -> int:
    """Fetch rolling window size or use default."""
    window_size = int(os.getenv("WNBA_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Detect a "hot streak"
#####################################

def detect_hot_streak(rolling_window: deque) -> bool:
    """
    Detect a hot streak if average PPG in the rolling window
    stays above the hot streak threshold.
    """
    if not rolling_window:
        return False

    avg_ppg = sum(rolling_window) / len(rolling_window)
    is_hot = avg_ppg >= get_hot_threshold()
    logger.debug(f"Avg PPG in window: {avg_ppg:.2f}. Hot streak: {is_hot}")
    return is_hot


#####################################
# Process a single message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a WNBA JSON message and check for hot streaks.
    """
    try:
        logger.debug(f"Raw message: {message}")
        data: dict = json.loads(message)

        player = data.get("player")
        team = data.get("team")
        ppg = data.get("ppg")
        timestamp = data.get("timestamp")

        if player is None or ppg is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        logger.info(f"Received {player} ({team}) - {ppg} PPG at {timestamp}")

        # Append points per game to rolling window
        rolling_window.append(ppg)

        # If window is full, check for streaks
        if len(rolling_window) == window_size:
            if detect_hot_streak(rolling_window):
                logger.info(
                    f"HOT STREAK! {player} averaging {sum(rolling_window)/window_size:.1f} PPG "
                    f"over last {window_size} games."
                )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main
#####################################

def main() -> None:
    """Main entry point for the consumer."""
    logger.info("START WNBA consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()

    rolling_window = deque(maxlen=window_size)
    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Run if main
#####################################

if __name__ == "__main__":
    main()
