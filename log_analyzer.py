#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import logging
from datetime import date
from collections import namedtuple

# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';


config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "REPORT_TEMPLATE": "report.html"
}


DateLog = namedtuple('DateLog', ('date', 'logfile'))
LogMetrics = namedtuple('LogMetrics', ('url', 'time'))


def read_argv(global_config):
    import argparse

    parser = argparse.ArgumentParser(description='Log analyzer')
    parser.add_argument(
        '--config',
        type=str,
        default='config.json',
        help='provide configuration file'
    )

    return read_config(parser.parse_args().config, global_config)


def read_config(config_file, global_config):
    try:
        with open(config_file, 'r') as c_file:
            try:
                new_config = json.load(c_file)
                global_config.update(new_config)
                return True

            except json.decoder.JSONDecodeError:
                print("error while parsing json")
                return False

    except IOError:
        print("error opening file")
        return False


def configurate_logger(logger_file_name):
    logger_config = {
        'filemode': 'w',
        'level': logging.INFO,
        'format': '[%(asctime)s] %(levelname).1s %(message)s',
        'datefmt': '%Y.%m.%d %H:%M:%S'
    }

    if logger_file_name:
        logger_config['filename'] = logger_file_name
    else:
        logger_config['stream'] = sys.stdout

    logging.basicConfig(**logger_config)


def read_paths(log_path):
    file_begin = 'nginx-access-ui.log-'
    date_pattern = r'.*.log-(\d{4})(\d{2})(\d{2}).*'
    last_date, last_log_file = max(
        [(date(*[int(num) for num in re.match(date_pattern, log).groups()]), log)
         for log in filter(lambda filename: filename.startswith(file_begin), os.listdir(log_path))]
    )

    return DateLog(last_date, last_log_file)


def check_report(report_path, report_date):
    file_to_seek = os.path.join(report_path, 'report-' + report_date.strftime('%Y.%m.%d') + '.html')

    if os.path.exists(report_path):
        if os.path.exists(file_to_seek):
            logging.info('report file {} is up-to-date'.format(file_to_seek))
            return None

    else:
        logging.info('creating a non-existent report directory: {}'.format(report_path))
        os.mkdir(report_path)

    logging.info('creating report for {} file'.format(file_to_seek))
    return file_to_seek


def analyze_log_file(filename):
    import gzip

    open_params = {
        'mode': 'rt',
        'encoding': 'utf-8'
    }

    opener = gzip.open if filename.endswith('.gz') else open

    try:
        with opener(filename, **open_params) as file:
            for line in file.readlines():
                line, request_time_str = line.rsplit(sep=' ', maxsplit=1)

                try:
                    url = re.search(r'\"[A-Z]*\s*([^\"\s]*)\s[^\"]*\"', line).group(1)
                except AttributeError:
                    logging.info('url not found in: {}'.format(line))
                    yield ()

                try:
                    request_time = float(request_time_str)
                except ValueError:
                    logging.info('invalid time: {}'.format(request_time_str))
                    yield ()

                yield LogMetrics(url, request_time)

    except IOError:
        logging.error("error opening the analysable file {}".format(filename))


def median(lst):
    sorted_lst = sorted(lst)
    lst_len = len(lst)
    index = (lst_len - 1) // 2

    if lst_len % 2:
        return sorted_lst[index]
    else:
        return (sorted_lst[index] + sorted_lst[index + 1]) / 2.


class Statistics:
    def __init__(self):
        self.requests = {}

    def update(self, metrics):
        time_list = self.requests.setdefault(metrics.url, [metrics.time])
        if time_list != [metrics.time]:
            time_list.append(metrics.time)

    def get_report(self, report_size, sort_by='time_sum'):
        if sort_by not in ['url', 'count', 'count_perc', 'time_sum', 'time_perc', 'time_avg', 'time_max', 'time_med']:
            logging.error('wrong sorting parameter')
            return None

        logging.info('calculating statistics')

        full_report = []

        total_count = sum([len(lst) for lst in self.requests.values()])
        total_requests_time = sum([sum(lst) for lst in self.requests.values()])

        for url in self.requests.keys():
            count = len(self.requests[url])
            time_sum = sum(self.requests[url])
            full_report.append({
                'url': url,
                'count': count,
                'count_perc': (count * 100) / total_count,
                'time_sum': time_sum,
                'time_perc': (time_sum * 100) / total_requests_time,
                'time_avg': time_sum / count,
                'time_max': max(self.requests[url]),
                'time_med': median(self.requests[url])
            })

        full_report.sort(key=lambda d: d[sort_by], reverse=True)
        return full_report[: report_size]


def write_report(report, report_file, report_template='report.html'):
    from string import Template

    logging.info('generating report file {}'.format(report_file))

    try:
        with open(report_template, mode='rt', encoding='utf-8') as template_file:
            template = Template(template_file.read())
            result = template.safe_substitute(table_json=json.dumps(report))

            with open(report_file, mode='wt', encoding='utf-8') as rep_file:
                rep_file.write(result)

    except IOError:
        logging.exception('error reading template file')


def main():
    if not read_argv(config):
        return

    configurate_logger(config.get('LOG_FILE'))

    try:
        log_info = read_paths(config["LOG_DIR"])

        report_file = check_report(config["REPORT_DIR"], log_info.date)

        if not report_file:
            return

        statistics = Statistics()
        total, errors = 0, 0
        for metrics in analyze_log_file(log_info.logfile):
            total += 1
            if metrics:
                statistics.update(metrics)
            else:
                errors += 1

        if errors / total > 0.5:
            logging.error('too many parsing errors. Aborting')
            return

        report = statistics.get_report(config["REPORT_SIZE"], config["REPORT_SORT"])

        if report:
            write_report(report, report_file, config['REPORT_TEMPLATE'])

    except BaseException:
        logging.exception("Unexpected exception occured with traceback:")


if __name__ == "__main__":
    main()
