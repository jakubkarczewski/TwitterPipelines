import pickle

import pandas as pd
import tweepy


class TwitterExtractor:
    """Class for handling extraction of tweets."""
    def __init__(self, api_keys_path='.api_keys.pkl',
                 access_tokens='.access_tokens.pkl'):
        """
        :param api_keys_path: path to pickled dict with api keys
        :param access_tokens: path to pickled dict with api tokens
        """
        self._access_tokens = access_tokens
        self._api_keys_path = api_keys_path

        self.keys_to_keep = ('created_at', 'id', 'text', 'favorited',
                             'retweeted', 'lang')

    def get_api_handle(self):
        """Loads API keys and tokens and then returns api handle object."""
        secrets = dict()
        for path in (self._access_tokens, self._api_keys_path):
            with open(path, 'rb') as f:
                secrets.update(pickle.load(f))
        auth = tweepy.OAuthHandler(secrets['key'], secrets['secret_key'])
        auth.set_access_token(secrets['token'], secrets['secret_token'])
        api = tweepy.API(auth)
        return api

    @staticmethod
    def get_tweets(api, keyword, count=1000):
        """
        Returns specified number of tweets filtered by keyword.
        :param api: api handle
        :param keyword: string keyword by which tweets are filtered
        :param count: number of tweets to be returned
        :return: list of SearchResults no longer than count
        """
        return api.search(keyword, count=count)

    def to_pandas(self, tweets):
        """
        Parses list of SearchResults with tweets into Pandas dataframe.
        :param tweets: list of tweets as SearchResult
        :return: Pandas dataframe with tweets
        """
        cleaned_tweets = {str(i): self._filter_tweet(tweet)
                          for i, tweet in enumerate(tweets)}
        df = pd.DataFrame.from_dict(cleaned_tweets, orient='index')
        df = df.sort_index()
        return df

    def _filter_tweet(self, tweet):
        """
        Filters tweet metadata and casts it to dictionary.
        :param tweet: single tweet as Status
        :return: filtered dictionary representing tweet
        """
        # todo: implement something more dynamic
        # todo: get dict in less hacky way
        return {tweet._json[key] for key in self.keys_to_keep}
