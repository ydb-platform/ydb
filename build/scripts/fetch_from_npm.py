import os
import sys
import time
import logging
import argparse
import hashlib

import sky
import fetch_from


NPM_BASEURL = "http://npm.yandex-team.ru"


def parse_args():
    parser = argparse.ArgumentParser()
    fetch_from.add_common_arguments(parser)

    parser.add_argument("--tarball-url", required=True)
    parser.add_argument("--sky-id", required=True)
    parser.add_argument("--integrity", required=True)
    parser.add_argument("--integrity-algorithm", required=True)

    return parser.parse_args()


def fetch(tarball_url, sky_id, integrity, integrity_algorithm, file_name, tries=5):
    """
    :param name: package name
    :type name: str
    :param version: package version
    :type version: str
    :param sky_id: sky id of tarball
    :type sky_id: str
    :param integrity: tarball integrity (hex)
    :type integrity: str
    :param integrity_algorithm: integrity algorithm (known for openssl)
    :type integrity_algorithm: str
    :param tries: tries count
    :type tries: int
    :return: path to fetched file
    :rtype: str
    """
    # if sky.is_avaliable() and 'NOTS_FETCH_FROM_HTTP_ONLY' not in os.environ:
    #    fetcher = lambda: sky.fetch(sky_id, file_name)
    # else:
    # Отключаем походы через скай
    # TODO: https://st.yandex-team.ru/FBP-351
    if 'NOTS_FETCH_FROM_SKY' in os.environ and sky.is_avaliable():
        fetcher = lambda: sky.fetch(sky_id, file_name)
    else:
        fetcher = lambda: _fetch_via_http(tarball_url, integrity, integrity_algorithm, file_name)

    fetched_file = None
    exc_info = None

    for i in range(0, tries):
        try:
            fetched_file = fetcher()
            exc_info = None
            break
        except Exception as e:
            logging.exception(e)
            exc_info = exc_info or sys.exc_info()
            time.sleep(i)

    if exc_info:
        raise exc_info[0], exc_info[1], exc_info[2]

    return fetched_file


def _fetch_via_http(tarball_url, integrity, integrity_algorithm, file_name):
    is_abs_url = tarball_url.startswith("https://") or tarball_url.startswith("http://")
    url_delim = "" if tarball_url.startswith("/") else "/"
    url = tarball_url if is_abs_url else NPM_BASEURL + url_delim + tarball_url

    hashobj = hashlib.new(integrity_algorithm)
    fetched_file = fetch_from.fetch_url(url, False, file_name, tries=1, writers=[hashobj.update])

    if hashobj.hexdigest() != integrity:
        raise fetch_from.BadChecksumFetchError("Expected {}, but got {} for {}".format(
            integrity,
            hashobj.hexdigest(),
            file_name,
        ))

    return fetched_file


def main(args):
    file_name = os.path.basename(args.copy_to)
    fetched_file = fetch(args.tarball_url, args.sky_id, args.integrity, args.integrity_algorithm, file_name)
    fetch_from.process(fetched_file, file_name, args)


if __name__ == "__main__":
    args = parse_args()
    fetch_from.setup_logging(args, os.path.basename(__file__))

    try:
        main(args)
    except Exception as e:
        logging.exception(e)
        print >>sys.stderr, open(args.abs_log_path).read()
        sys.stderr.flush()

        import error
        sys.exit(error.ExitCodes.INFRASTRUCTURE_ERROR if fetch_from.is_temporary(e) else 1)
