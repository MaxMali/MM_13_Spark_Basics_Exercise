import numpy as np
from pyspark import SparkConf, SparkContext

# Environment

n = 24

# Context
conf = SparkConf().setAppName("word count").setMaster("local[1]")
sc = SparkContext(conf=conf)

# Import Data
text = sc.textFile("J:\coden\Data Science Modul\MM_13_Spark_Basics_Exercise\shakespeare_text_hand_clean.txt")

# Setup Filter Function
disabledCharacters = [".", ",", ";", "!", "?", "(", ")", "[", "]", "'", "-"]


def mapper(text):
    line = np.array(text.split(" "))
    line = line[np.logical_and(line != "", line != " ")]
    line = np.char.lower(line)

    for char in disabledCharacters:
        line = np.char.strip(line, chars=char)

    if len(line) > 0:
        return line.tolist()
    else:
        return []


# Processing Spark
lines = text.map(lambda textLine: mapper(textLine)) \
            .filter(lambda line: len(line) > 0)
result = lines.countByKey()

# Sorting
sorted = sorted(result.items(), key=lambda x: x[1], reverse=True)
num = 1
for i in sorted[:n]:

    print(str(num),". ",i[0], "  ", i[1])
    num = num+1