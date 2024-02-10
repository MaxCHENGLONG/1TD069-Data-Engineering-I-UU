#!/usr/bin/env python3
import sys
from collections import defaultdict


counters = defaultdict(int)

#count jisuan
for line in sys.stdin:
    pronoun, count = line.strip().split('\t', 1)
    counters[pronoun] += int(count)

#shuchu output
for pronoun, count in counters.items():
    print(f"{pronoun}\t{count}")
