#!/usr/bin/env python

import csv
import argparse
import logging
from collections import defaultdict, Counter
import datetime


def main(input_file):
    data = []
    with open(input_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)

    # Sort by time. If multiple events at same time, pressure comes last
    #   We assume that such pressures are triggered by the other event at the same time.
    data.sort(key=lambda row: row['DateTime'] + '1' if row['Event'] == 'Pressure' else row['DateTime'] + '0')

    events_per_pressures = defaultdict(lambda: {'duration': datetime.timedelta(), 'event_counts':Counter()})
    current_pressure_bin = None
    current_pressure_time = None
    current_session = None

    for row in data:
        if row['Event'] == 'Pressure':
            timestamp = datetime.datetime.strptime(row['DateTime'], '%Y-%m-%dT%H:%M:%S')
            pressure_bin = bin_pressure(row['Data/Duration'])
            if row['Session'] == current_session:
                events_per_pressures[current_pressure_bin]['duration'] += timestamp - current_pressure_time
            current_pressure_bin = pressure_bin
            current_pressure_time = timestamp
            current_session = row['Session']
        elif row['Session'] == current_session:
            events_per_pressures[current_pressure_bin]['event_counts'][row['Event']] += 1
        else:
            logging.warning('Found event with no pressure: %s' % str(row))

    for key, value in events_per_pressures.iteritems():
        print key, value['duration'].total_seconds()/60., value['event_counts']


def bin_pressure(pressure_string):
    return pressure_string.split('.')[0]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='input_file')
    args = parser.parse_args()
    main(args.input_file)


