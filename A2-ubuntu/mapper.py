#!/usr/bin/env python3
import sys
import json

#dingyi
pronouns = ["han", "hon", "den", "det", "denna", "denne", "hen"]


for line in sys.stdin:
    try:
       
        tweet = json.loads(line)
        
        text = tweet.get('text', '').lower()
        
        for pronoun in pronouns:
            if pronoun in text:
                print(f"{pronoun}\t1")
    except json.JSONDecodeError:
        continue  
