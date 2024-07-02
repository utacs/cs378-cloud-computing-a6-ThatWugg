import sys

from utils import is_valid_line1
from typing import Tuple

from pyspark import SparkContext
    
# returns (taxi_id, driver_id) pair
def to_pair (line: str):
    return (line[0:32], line[33:65])

# adding to a set
def set_add (accum: set, driver: str):
    accum.add(driver)
    return accum

# combines sets
def set_combine (set0: set, set1: set):
    set0.update(set1)
    return set0

# converts a tuple (str, set) to a (set, int)
# which the int is the size of the set
def to_count (tuple: Tuple[str, set]):
    return (tuple[0], len(tuple[1]))

# returns the count associated with the taxi
# we negate it as to make sure it is sorted in
# descending order
def get_key_for_sorting (tuple: Tuple[str, int]):
    return -tuple[1]

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Args: <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="Top 10 Active Taxis")
    
    # rdd
    raw_lines = sc.textFile(sys.argv[1])
    filtered_lines = raw_lines.filter(is_valid_line1)
    taxi_driver_pairs = filtered_lines.map(to_pair)
    taxi_to_unique_drivers = taxi_driver_pairs.aggregateByKey(set(), set_add, set_combine)
    taxi_to_unique_drivers_cnt = taxi_to_unique_drivers.map(to_count)

    # list
    top_10_taxis = taxi_to_unique_drivers_cnt.takeOrdered(10, get_key_for_sorting)

    # output, it is doing all this because we want spark to handle file system interaction
    sc.parallelize(top_10_taxis, 1).saveAsTextFile(sys.argv[2])

    sc.stop()