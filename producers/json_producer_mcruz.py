"""
json_producer_mcruz.py

JSON messages with WNBA player stats 
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
import time
import random
from datetime import datetime

# Import external packages
import pandas as pd
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import create_kafka_producer
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


#####################################
# Create WNBA DataFrame
#####################################

def create_wnba_dataframe() -> pd.DataFrame:
    """Create a sample DataFrame of top WNBA players and their stats."""
    data = {
        "player": [
            "A'ja Wilson", "Breanna Stewart", "Nneka Ogwumike", "Jewell Loyd",
            "Sabrina Ionescu", "Kelsey Plum", "Arike Ogunbowale", "Elena Delle Donne"
        ],
        "team": [
            "Las Vegas Aces", "New York Liberty", "Seattle Storm", "Seattle Storm",
            "New York Liberty", "Las Vegas Aces", "Dallas Wings", "Washington Mystics"
        ],
        "ppg": [22.8, 23.1, 19.3, 24.7, 17.0, 18.7, 21.2, 16.5],  # Points per game
        "rpg": [9.5, 9.2, 8.7, 4.5, 5.8, 2.3, 3.9, 6.8],         # Rebounds per game
        "apg": [2.1, 3.8, 2.5, 3.2, 6.3, 4.0, 3.7, 2.1],         # Assists per game
    }
    df = pd.DataFrame(data)
    logger.info("WNBA DataFrame created.")
    return df


#####################################
# Message Generator
#####################################

def generate_messages(df: pd.DataFrame):
    """Generator that yields JSON messages for Kafka."""
    while True:
        player_row = df.sample(1).iloc[0]  # Pick a random player
        message = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "player": player_row["player"],
            "team": player_row["team"],
            "ppg": float(player_row["ppg"]),
            "rpg": float(player_row["rpg"]),
            "apg": float(player_row["apg"]),
        }
        yield message
        time.sleep(random.uniform(1, 3))  # Simulate real-time streaming


#####################################
# Main Function
#####################################

def main() -> None:
    """
    Main entry point for the producer.
    - Creates a Kafka producer
    - Streams WNBA player JSON messages
    """
    logger.info("START producer.")

    topic = get_kafka_topic()
    producer = create_kafka_producer()

    df = create_wnba_dataframe()

    try:
        for message in generate_messages(df):
            message_str = json.dumps(message)
            producer.send(topic, value=message_str)
            logger.info(f"Produced message: {message_str}")
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while producing messages: {e}")
    finally:
        producer.close()
        logger.info(f"Kafka producer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
