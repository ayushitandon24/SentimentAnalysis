from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob

es = Elasticsearch()


def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:
        dict_data = json.loads(msg.value)
        tweet = TextBlob(dict_data["text"]) if "text" in dict_data.keys() else None
        # if the object contains Tweet
        sentiment = ""
        if tweet:
            # determine if sentiment is positive, negative, or neutral
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"

            # print the predicted sentiment with the Tweets
            print(sentiment, tweet.sentiment.polarity, dict_data["text"])

        # extract the first hashtag from the object
        # transform the Hashtags into proper case
        if len(dict_data["entities"]["hashtags"]) > 0 and dict_data["entities"]["hashtags"][0]["text"] is not None:
            hashtags = dict_data["entities"]["hashtags"][0]["text"].title()
        else:
            # Elasticeach does not take None object
            hashtags = "None"
        print(tweet)
        # add text and sentiment info to elasticsearch
        es.index(index="tweet",
                 doc_type="test-type",
                  body={"author": dict_data["user"]["screen_name"],
                        "date": dict_data["created_at"],
                        "message": dict_data["text"],
                        "hashtags": hashtags,
                        "polarity": tweet.sentiment.polarity,
                        "subjectivity": tweet.sentiment.subjectivity,
                        "sentiment": sentiment
                        })
        print('\n')


if __name__ == "__main__":
    main()
