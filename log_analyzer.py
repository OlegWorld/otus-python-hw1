#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import json
import logging
from datetime import date

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


def read_argv(args):
    if 2 <= len(args) <= 3:
        if args[1] == '--config':
            return read_config(args[2] if len(args) == 3 else 'config.json')

        else:
            print('unknown command line argument')
            return False

    elif len(args) > 3:
        print('too many command line arguments')
        return False


def read_config(config_file):
    try:
        with open(config_file, 'r') as c_file:
            try:
                new_config = json.load(c_file)
                config.update(new_config)
                return True

            except json.decoder.JSONDecodeError:
                print("error while parsing json")
                return False

    except IOError:
        print("error opening file")
        return False


def read_paths(log_path, report_path):
    log_files = list(filter(lambda filename: filename.startswith('nginx-access-ui.log-'), os.listdir(log_path)))
    dates = [date(*[int(num) for num in re.match('.*.log-(\d{4})(\d{2})(\d{2}).*', log).groups()]) for log in log_files]
    last_date, last_date_log = max(zip(dates, log_files))

    if check_report(report_path, last_date):
        logging.info('report file is up-to-date')
        return None
    else:
        logging.info('creating report for {} file'.format(last_date_log))
        return last_date_log, last_date


def check_report(path, report_date):
    report_files = os.listdir(path)
    return 'report-' + report_date.strftime('%Y.%m.%d') + '.html' in report_files


def analyze_log_file(filename):
    import gzip

    open_params = {
        'mode': 'rt',
        'encoding': 'utf-8'
    }

    try:
        with gzip.open(filename, **open_params) if filename.endswith('.gz') else open(filename, **open_params) as file:
            for line in file.readlines():
                line, request_time_str = line.rsplit(sep=' ', maxsplit=1)

                try:
                    url = re.search('\"[A-Z]*\s*([^\"\s]*)\s[^\"]*\"', line).group(1)
                except AttributeError:
                    logging.info('url not found in: {}'.format(line))
                    yield ()

                try:
                    request_time = float(request_time_str)
                except ValueError:
                    logging.info('invalid time: {}'.format(request_time_str))
                    yield ()

                yield url, request_time
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
        url, time = metrics
        if url in self.requests:
            self.requests[url].append(time)
        else:
            self.requests[url] = [time]

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


def write_report(report, report_date, report_template='report.html'):
    from string import Template

    report_file_name = 'report-' + report_date.strftime('%Y.%m.%d') + '.html'
    logging.info('generating report file {}'.format(report_file_name))

    try:
        with open(report_template, mode='rt', encoding='utf-8') as template_file:
            template = Template(template_file.read())
            result = template.safe_substitute(table_json=json.dumps(report))

            with open(report_file_name, mode='wt', encoding='utf-8') as rep_file:
                rep_file.write(result)

    except IOError:
        logging.exception('error reading template file')


def main():
    args = sys.argv
    if not read_argv(args):
        return

    logger_config = {
        'filemode': 'w',
        'level': logging.INFO,
        'format': '[%(asctime)s] %(levelname).1s %(message)s',
        'datefmt': '%Y.%m.%d %H:%M:%S'
    }
    if "LOG_FILE" in config:
        logger_config['filename'] = config["LOG_FILE"]
    else:
        logger_config['stream'] = sys.stdout

    logging.basicConfig(**logger_config)

    try:
        log_info = read_paths(config["LOG_DIR"], config["REPORT_DIR"])
        if log_info:
            log_file, log_date = log_info
            statistics = Statistics()
            total, errors = 0, 0
            for metrics in analyze_log_file(log_file):
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
                write_report(report, log_date, config['REPORT_TEMPLATE'])

    except BaseException:
        logging.exception("Unexpected exception occured with traceback:")


if __name__ == "__main__":
    main()
