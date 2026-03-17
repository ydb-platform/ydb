import re

import requests


def get_search_names(name):
    """
    Return a list of values to search on when we are looking for a package
    with the given name.

    This is required to search on both pyramid_debugtoolbar and
    pyramid-debugtoolbar.
    """
    parts = re.split('[-_.]', name)
    if len(parts) == 1:
        return parts

    result = set()
    for i in range(len(parts) - 1, 0, -1):
        for s1 in '-_.':
            prefix = s1.join(parts[:i])
            for s2 in '-_.':
                suffix = s2.join(parts[i:])
                for s3 in '-_.':
                    result.add(s3.join([prefix, suffix]))
    return list(result)


def get_package_information(index_url, package_name):
    index_url = index_url.rstrip('/')
    response = requests.get('%s/%s/json' % (index_url, package_name))
    if response.status_code == 200:
        return response.json()


def normalize_name(name):
    return re.sub(r'[-_.]+', '-', name).lower()
