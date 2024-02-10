#!/usr/bin/env python3
import pandas as pd
from pymongo import MongoClient
from bson.regex import Regex


client = MongoClient('localhost', 27017)
db = client['twitterdb']
tweets = db['tweets']

pronouns = ["han", "hon", "den", "det", "denna", "denne", "hen"]


regex_pattern = r'\b(' + '|'.join(pronouns) + r')\b'


pipeline = [
    {
        "$match": {
            "text": {
                "$regex": Regex(regex_pattern, 'i')  # 不区分大小写的匹配
            }
        }
    },
    {
        "$project": {
            "words": {
                "$regexFindAll": {
                    "input": "$text",
                    "regex": Regex(regex_pattern, 'i')
                }
            }
        }
    },
    {
        "$unwind": "$words"
    },
    {
        "$group": {
            "_id": "$words.match",
            "count": {"$sum": 1}
        }
    }
]


results = list(tweets.aggregate(pipeline))


df = pd.DataFrame(results)


df.columns = ['Pronoun', 'Count']


df.to_csv('/home/ubuntu/pronoun_counts.csv', index=False)

print("end：/home/ubuntu/pronoun_counts.csv")

