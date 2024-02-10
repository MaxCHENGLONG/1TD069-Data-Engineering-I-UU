#!/usr/bin/env python3
import pandas as pd
from pymongo import MongoClient
from bson.regex import Regex

# 连接到MongoDB
client = MongoClient('localhost', 27017)
db = client['twitterdb']
tweets = db['tweets']

# 定义瑞典代词列表
pronouns = ["han", "hon", "den", "det", "denna", "denne", "hen"]

# 为了防止部分匹配，我们在每个代词周围添加了单词边界`\b`
# 这确保了我们匹配的是完整的单词
regex_pattern = r'\b(' + '|'.join(pronouns) + r')\b'

# 构建聚合管道
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

# 执行聚合查询
results = list(tweets.aggregate(pipeline))

# 将结果转换为DataFrame
df = pd.DataFrame(results)

# 将DataFrame的列名重命名以更清晰地反映数据含义
df.columns = ['Pronoun', 'Count']

# 输出DataFrame到CSV文件
df.to_csv('/home/ubuntu/pronoun_counts.csv', index=False)

print("end：/home/ubuntu/pronoun_counts.csv")

