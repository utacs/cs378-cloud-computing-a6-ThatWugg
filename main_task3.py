from __future__ import print_function
from datetime import datetime
from pyspark import SparkContext
from typing import Tuple

import sys

# definition
# - pickup time has a valid date
# - surcharge is >= 0
# - travel distance in miles > 0
def is_valid_line3 (line: str):
    # comma checking
    comma_count: int = 0
    comma_idxs = [None, None, None, None, None, None]
    for idx in range(len(line)):
        if line[idx] == ',':
            comma_count += 1
            if comma_count == 2:      # pickup time
                comma_idxs[0] = idx
            elif comma_count == 3:
                comma_idxs[1] = idx
            elif comma_count == 5:    # trip distance
                comma_idxs[2] = idx
            elif comma_count == 6:
                comma_idxs[3] = idx
            elif comma_count == 12:   # surcharge
                comma_idxs[4] = idx
            elif comma_count == 13:
                comma_idxs[5] = idx
    if comma_count != 16: 
        return False
    
    try:
        # pickup time check
        datetime.strptime(line[comma_idxs[0] + 1: comma_idxs[1]], '%Y-%m-%d %H:%M:%S')
        # trip distance check
        if float(line[comma_idxs[2] + 1: comma_idxs[3]]) <= 0:
            return False
        
        # surcharge check
        if float(line[comma_idxs[4] + 1: comma_idxs[5]]) < 0:
            return False
    except:
        return False
    
    return True

# returns a tuple of (int, (float, float))
# of which is (hour, (surcharge, miles))
# and hours is 0-24
def to_pair (line: str):
    comma_count: int = 0
    comma_idxs = [None, None, None, None, None, None]
    for idx in range(len(line)):
        if line[idx] == ',':
            comma_count += 1
            if comma_count == 2:      # pickup time
                comma_idxs[0] = idx
            elif comma_count == 3:
                comma_idxs[1] = idx
            elif comma_count == 5:    # trip distance
                comma_idxs[2] = idx
            elif comma_count == 6:
                comma_idxs[3] = idx
            elif comma_count == 12:   # surcharge
                comma_idxs[4] = idx
            elif comma_count == 13:
                comma_idxs[5] = idx
    return (datetime.strptime(line[comma_idxs[0] + 1: comma_idxs[1]], '%Y-%m-%d %H:%M:%S').hour,
            (float(line[comma_idxs[4] + 1: comma_idxs[5]]), float(line[comma_idxs[2] + 1: comma_idxs[3]])))

def reduce (pair0: Tuple[float, float], pair1: Tuple[float, float]):
    return (pair0[0] + pair1[0], pair0[1] + pair1[1])

def calc_profitratio (surchargemiles: Tuple[float, float]):
    return surchargemiles[0] / surchargemiles[1]

# purpose same as in task 1
def get_key_for_sorting (hour_profitratio: Tuple[int, float]):
    return -hour_profitratio[1]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Args: <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top 3 Best Times")

    # rdd
    lines = sc.textFile(sys.argv[1])
    filtered_lines = lines.filter(is_valid_line3)
    hour_surchargemiles_pair = filtered_lines.map(to_pair)
    hour_surchargemiles_aggregate = hour_surchargemiles_pair.reduceByKey(reduce)
    hour_profitratio = hour_surchargemiles_aggregate.mapValues(calc_profitratio)

    # list
    top_3_hours = hour_profitratio.takeOrdered(3, get_key_for_sorting)

    # output
    sc.parallelize(top_3_hours).saveAsTextFile(sys.argv[2])
    
    sc.stop()
