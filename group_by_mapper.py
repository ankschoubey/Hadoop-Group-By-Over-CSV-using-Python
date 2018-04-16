#!/usr/bin/python
import sys
import csv

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words

    #print(len(line.split(',')))

    field_data =  csv.reader([line],delimiter =',')
    field_data = list(field_data)[0]

    # increase counters

    key_column, aggregate_column = sys.argv[1:]

    key_column, aggregate_column = int(key_column), int(aggregate_column)

    try:
        # if first line
        int(field_data[aggregate_column])
    except:
        continue

    print field_data[key_column], '\t',field_data[aggregate_column]
