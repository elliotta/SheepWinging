#!/usr/bin/env python

import csv
import argparse
import logging
from collections import defaultdict, Counter
from decimal import Decimal
import datetime
import sys


def main(input_file, bin_size, output_file):
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
            pressure_bin = bin_pressure(row['Data/Duration'], bin_size)
            if row['Session'] == current_session:
                events_per_pressures[current_pressure_bin]['duration'] += timestamp - current_pressure_time
            current_pressure_bin = pressure_bin
            current_pressure_time = timestamp
            current_session = row['Session']
        elif row['Session'] == current_session:
            events_per_pressures[current_pressure_bin]['event_counts'][row['Event']] += 1
        else:
            logging.warning('Found event with no pressure: %s' % str(row))

    data_list = []
    field_list = ['Pressure', 'Hours', 'Obstructive', 'ClearAirway', 'Hypopnea', 'RERA', 'FlowLimit', 'PressurePulse', 'VSnore', 'VSnore2' ]
    for key, value in events_per_pressures.iteritems():
        d = {'Pressure': key, 'Hours': '%.3f' % (value['duration'].total_seconds()/3600.)}
        for event, count in value['event_counts'].iteritems():
            d[event] = count
            if not event in field_list:
                field_list.append(event)
        data_list.append(d)
    data_list.sort(key=lambda d: d['Pressure'])
    if output_file:
        f = open(output_file, 'w')
    else:
        f = sys.stdout
    dw = csv.DictWriter(f, field_list, restval='0')
    dw.writeheader()
    for row in data_list:
        dw.writerow(row)
    if output_file:
        f.close()


def bin_pressure(pressure_string, bin_size):
    pressure = Decimal(pressure_string)
    return (pressure // bin_size) * bin_size


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='From an input cpap log, find how many events of what type happened at which pressure ranges.')
    parser.add_argument(dest='input_file')
    parser.add_argument('-b', '--binsize', dest='bin_size', default='.5', help='Bin size for pressures. Bin names are lower edge of range.')
    parser.add_argument('-o', '--outputfile', dest='output_file', help='Output data to file. If not specified, output to stdout')
    args = parser.parse_args()
    main(args.input_file, Decimal(args.bin_size), args.output_file)


