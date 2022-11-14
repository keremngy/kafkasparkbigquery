from kafka import KafkaProducer
from json import dumps
import json
import csv
import time
from itertools import islice

if __name__ == "__main__":
    # execute only if run as a script

    with open('kopeksaldiriuzun.csv') as file:
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        reader = csv.reader(file, delimiter=",")

        while True:
            next_n_lines = islice(reader, 10)
            if not next_n_lines:
                break
            else:
                for line in next_n_lines:
                    producer.send('kafkaprojesi', {'id': line[0], 'tarih': line[1], 'cins': line[3], 'bolge': line[7]})
                producer.flush()
                time.sleep(5)




