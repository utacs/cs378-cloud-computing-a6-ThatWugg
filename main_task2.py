from __future__ import print_function
from utils import is_hexadecimal
from pyspark import SparkContext
from typing import Tuple

import sys

# returns a tuple of (str, (float, int))
# which represents (driver id, (total fare amount, trip seconds))
def to_pair (line: str):
    fourth_comma_idx: int = None # trip seconds
    fifth_comma_idx: int = None
    last_comma_idx: int = None    # total fare amount
    comma_count: int = 0
    for idx in range(len(line)):
        if line[idx] == ',':
            comma_count += 1
            if comma_count == 4:
                fourth_comma_idx = idx
            if comma_count == 5:
                fifth_comma_idx = idx
            if comma_count == 16:
                last_comma_idx = idx

    return (line[33:65], (float(line[last_comma_idx + 1:]), int(line[fourth_comma_idx + 1:fifth_comma_idx])))

# adds two (driver id, (total fare amount, trip seconds)) of which
# the driver id is already the same
def reduce (pair0: Tuple[float, int], pair1: Tuple[float, int]):
    return (pair0[0] + pair1[0], pair0[1] + pair1[1])

# calculates the money per minute from the
# total fare amount and total trip seconds
def calc_mpm(pair: Tuple[float, int]):
    #       money  /    minutes
    return pair[0] / (pair[1] / 60.0)

# same as in main_task1.py
def get_key_for_sorting (tuple: Tuple[str, int]):
    return -tuple[1]

# defintition;
# - driver md5sum is 32 length & hexadecimal only
# - trip seconds is > 0
# - total fare amount is >= 0
def is_valid_line2 (line: str):
    # check comma count
    comma_count: int = 0
    comma_idxs = [None, None, None, None, None]
    for idx in range(len(line)):
        if line[idx] == ',':
            comma_count += 1
            if comma_count == 1:   # driver id
                comma_idxs[0] = idx
            elif comma_count == 2:
                comma_idxs[1] = idx
            elif comma_count == 4:   # seconds
                comma_idxs[2] = idx
            elif comma_count == 5:
                comma_idxs[3] = idx
            elif comma_count == 16:   # total fare
                comma_idxs[4] = idx
    if comma_count != 16:
        return False

    # ensure that length of taxi driver id is correct
    # -1 to not include the comma
    if comma_idxs[1] - comma_idxs[0] - 1 != 32: return False
    # check contents of md5sum correct
    for idx in range(comma_idxs[0] + 1, comma_idxs[1]):
        if not is_hexadecimal(line[idx]):
            return False
    
    try:
        # check trip seconds is int and greater than 0
        if int(line[comma_idxs[2] + 1:comma_idxs[3]]) <= 0:
            return False
        
        # check trip cost is float and non-negative
        if float(line[comma_idxs[4] + 1:]) < 0:
            return False
    except:
        return False
    
    return True

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Args: <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Top 10 Best Drivers")

    # rdd
    lines = sc.textFile(sys.argv[1])
    filtered_lines = lines.filter(is_valid_line2)
    driver_tripstats_pairs = filtered_lines.map(to_pair)
    driver_tripstats_aggregate = driver_tripstats_pairs.reduceByKey(reduce)
    driver_money_per_minute_pair = driver_tripstats_aggregate.mapValues(calc_mpm)

    # list
    top_10_drivers = driver_money_per_minute_pair.takeOrdered(10, get_key_for_sorting)

    # output
    sc.parallelize(top_10_drivers, 1).saveAsTextFile(sys.argv[2])
    
    sc.stop()
