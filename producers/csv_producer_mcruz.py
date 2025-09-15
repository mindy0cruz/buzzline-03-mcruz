"""
csv_producer_mcruz.py

Stream numeric data to a Kafka topic.
Added City and Continent 
 
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time  # control message intervals
import pathlib  # work with file paths
import csv  # handle CSV data
import json  # work with JSON data
import random  # assign city/continent randomly
from datetime import datetime  # work with timestamps

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
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
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER.joinpath("smoker_temps.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# City + Continent Pool
#####################################

CITIES_AND_CONTINENTS = [
    ("Las Vegas", "North America"),
    ("New York", "North America"),
    ("London", "Europe"),
    ("Tokyo", "Asia"),
    ("Sydney", "Australia"),
    ("São Paulo", "South America"),
    ("Cape Town", "Africa"),
]

#####################################
# Message Generator
#####################################


def generate_messages(file_path: pathlib.Path):
    """
    Read from a csv file and yield records one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        dict: CSV row formatted as JSON-ready dict.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(DATA_FILE, "r") as csv_file:
                logger.info(f"Reading data from file: {DATA_FILE}")

                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    if "temperature" not in row:
                        logger.error(f"Missing 'temperature' column in row: {row}")
                        continue

                    current_timestamp = datetime.utcnow().isoformat()
                    city, continent = random.choice(CITIES_AND_CONTINENTS)

                    message = {
                        "timestamp": current_timestamp,
                        "temperature": float(row["temperature"]),
                        "city": city,
                        "continent": continent,
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Define main function for this module.
#####################################


def main():
    """Main entry point for the producer."""
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
