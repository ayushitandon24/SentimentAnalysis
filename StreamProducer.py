from kafka import KafkaProducer
import sys
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "oijjKsXh4BRNJ6IQNJVFlSqM5"
consumer_secret = "N69C8R9Ja6TxzuIHHVU0KlA3DI3k9A3J4Y1wKR073Vl1WmrAId"
access_token = "1251279846531088384-DcSyOXK49d6RzePLqAGBXpjUha3Nfs"
access_secret = "w4h0eqNZxymNn8qzRESxlodUt4SBTXc8mOTVD6f2pLmAm"

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


def main(hashtags):
    # Twitter Stream Listener
    class KafkaPushListener(StreamListener):
        def __init__(self):
            # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
            self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        # Get Producer that has topic name is Twitter
        # self.producer = self.client.topics[bytes("twitter")].get_producer()

        def on_data(self, data):
            # Producer produces data for consumer
            # Data comes from Twitter
            self.producer.send("twitter", data.encode('utf-8'))
            print(data)
            return True

        def on_error(self, status):
            print(status)
            return True

    # Twitter Stream Config
    twitter_stream = Stream(auth, KafkaPushListener())


    twitter_stream.filter(track=hashtags)


if __name__ == "__main__":
    tags = []
    tags.append('#' + sys.argv[1])
    print(tags)
    main(hashtags=tags)
