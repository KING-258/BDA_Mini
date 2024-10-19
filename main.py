import threading
from newsapi import NewsApiClient
from kafka_producer import NewsProducer
from kafka_consumer import NewsConsumer
from hdfs import InsecureClient
import time
import requests
import json

newsapi = NewsApiClient(api_key='62a80a08966b4e7fb999ed2c930b1a52')
gdelt_base_url = 'https://api.gdeltproject.org/api/v2/doc/doc?query='
wikipedia_api_url = 'https://en.wikipedia.org/w/api.php'
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

def fetch_news_and_produce(phrase, producer):
    hdfs_path = f'/user/hadoop/news_data/{phrase.replace(" ", "_")}.json'
    scraped_text = ""
    articles = []
    start_time = time.time()
    try:
        if hdfs_client.status(hdfs_path, strict=False):
            with hdfs_client.read(hdfs_path) as reader:
                existing_data = json.loads(reader.read())
                print(f"Data found in HDFS for phrase '{phrase}': {existing_data}")
                return
    except Exception as e:
        print(f"Error checking HDFS for phrase '{phrase}': {e}")
    try:
        print(f"Fetching news articles for '{phrase}' from NewsAPI...")
        articles_response = newsapi.get_everything(q=phrase, language='en', sort_by='relevancy', page_size=5)
        if articles_response['status'] == 'ok':
            for article in articles_response['articles']:
                title = article['title']
                description = article['description']
                url = article['url']
                scraped_text += f"{title} {description} "
                articles.append({'title': title, 'url': url})
        else:
            print(f"No articles found on NewsAPI for '{phrase}'.")
    except Exception as e:
        print(f"Error fetching from NewsAPI: {e}")
    try:
        print(f"Fetching data for '{phrase}' from GDELT...")
        gdelt_url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={phrase}&mode=artlist&format=json"
        gdelt_response = requests.get(gdelt_url)
        gdelt_data = gdelt_response.json()
        if 'articles' in gdelt_data:
            for article in gdelt_data['articles']:
                title = article['title']
                url = article['url']
                scraped_text += f"{title} "
                articles.append({'title': title, 'url': url})
        else:
            print(f"No data found on GDELT for '{phrase}'.")
    except Exception as e:
        print(f"Error fetching from GDELT: {e}")
    try:
        print(f"Fetching data for '{phrase}' from Wikipedia (within 3 minutes or 5 pages)...")
        wikipedia_url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{phrase.replace(' ', '%20')}"
        page_count = 0
        while time.time() - start_time < 180 and page_count < 5:
            wikipedia_response = requests.get(wikipedia_url, timeout=50)
            if wikipedia_response.status_code == 200:
                wikipedia_data = wikipedia_response.json()
                scraped_text += wikipedia_data.get('extract', '')
                page_count += 1
            else:
                print(f"No Wikipedia page found for '{phrase}'.")
                break
    except requests.exceptions.Timeout:
        print("Wikipedia fetching timed out.")
    except Exception as e:
        print(f"Error fetching from Wikipedia: {e}")
    if scraped_text:
        message = {
            'phrase': phrase,
            'scraped_text': scraped_text[:500],
            'articles': articles
        }
        producer.send_message('news_articles', message)
        print(f"Sent data for phrase: {phrase}")
        try:
            with hdfs_client.write(hdfs_path, overwrite=True) as writer:
                writer.write(json.dumps(message).encode('utf-8'))
            print(f"Stored data for phrase '{phrase}' in HDFS.")
        except Exception as e:
            print(f"Error writing to HDFS: {e}")
    else:
        print(f"No data found for phrase '{phrase}' across all APIs.")

def run_consumer():
    consumer = NewsConsumer('news_articles', 'news_group')
    consumer.consume_messages(time_limit=120, article_limit=100)

if __name__ == "__main__":
    producer = NewsProducer()
    consumer_thread = threading.Thread(target=run_consumer)
    consumer_thread.start()
    print("Sentiment Analysis Tool")
    print("Enter phrases to analyze, or 'quit' to exit.")
    try:
        while True:
            phrase = input("\nEnter a phrase to analyze (or 'quit' to exit): ").strip()
            if phrase.lower() == 'quit':
                break
            if phrase:
                fetch_news_and_produce(phrase, producer)
                print("Waiting for data to be processed...\n")
                consumer_thread.join(timeout=5)
            else:
                print("Please enter a non-empty phrase.")
    except KeyboardInterrupt:
        print("\nExiting gracefully...")
    finally:
        print("Thank you for using the Sentiment Analysis Tool.")
        consumer_thread.join(timeout=5)
