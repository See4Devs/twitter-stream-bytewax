import os
import re
import requests
import json
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.execution import run_main
from textblob import TextBlob
from dotenv import load_dotenv

load_dotenv()
bearer_token = os.getenv("TWITTER_BEARER_TOKEN" )

def remove_emoji(tweet):
    """
    This function takes in a tweet and strips off most of the emojis for the different platforms
    :param tweet:
    :return: tweet stripped off emojis
    """
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', tweet)


def remove_username(tweet):
    """
    Remove all the @usernames in a tweet
    :param tweet:
    :return: tweet without @username
    """
    return re.sub('@[\w]+', '', tweet)


def clean_tweet(tweet):
    """
    Removes spaces and special characters to a tweet
    :param tweet:
    :return: clean tweet
    """
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def get_tweet_sentiment(tweet):
    """
    Determines the sentiment of a tweet whether positive, negative or neutral
    :param tweet:
    :return: sentiment and the tweet
    """
    # create TextBlob object
    get_analysis = TextBlob(tweet)
    # get sentiment
    if get_analysis.sentiment.polarity > 0:
        return 'positive', tweet
    elif get_analysis.sentiment.polarity == 0:
        return 'neutral', tweet
    else:
        return 'negative', tweet

def add_bearer_oauth(req):
    """
    Method required by bearer token authentication for Twitter API
    :param req:
    :return: a request object with additional headers
    """
    req.headers["Authorization"] = f"Bearer {bearer_token}"
    req.headers["User-Agent"] = "v2FilteredStreamPython"
    return req


def set_stream_rules(search_terms):
    """
    Set the rules of the stream that we want the API to return
    :param search_terms: List of all the hashtags or keywords we want to search
    :return: Json response of the set rules
    """
    # only get original tweets in english
    default_rules = '-is:retweet lang:en'
    search_rules = []
    for search_term in search_terms:
        search_rules.append({"value": f"{search_term} {default_rules}"})
    payload = {"add": search_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=add_bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception("Cannot add rules (HTTP {}): {}".format(response.status_code, response.text))
    print(json.dumps(response.json()))

def input_builder(worker_index, worker_count, resume_state):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=add_bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    tweetnbr=0
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            json_response = json.dumps(json_response, indent=4, sort_keys=True)
            print("-" * 200)
            json_response = json.loads(json_response)
            tweet = json_response["data"]["text"]
            tweetnbr+=1
            yield tweetnbr, str(tweet)

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(remove_emoji)
flow.map(remove_username)
flow.map(clean_tweet)
flow.map(get_tweet_sentiment)
flow.capture(StdOutputConfig())
cluster_main(flow, [],0)
