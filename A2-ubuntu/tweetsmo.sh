#!/bin/bash

DATABASE=twitterdb
COLLECTION=tweets
DIRECTORY=/home/ubuntu/input

for FILE in $DIRECTORY/*.txt
do
  mongoimport --db $DATABASE --collection $COLLECTION --file "$FILE"
done

