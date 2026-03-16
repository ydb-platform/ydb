#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#



def get_resource_summary(resource, key, as_attribute=False):
    """
    Return the "summary" value as mapping for the `key` summary attribute of a
    resource.

    This is collected either from a direct Resource.summary attribute if
    `as_attribute` is True or as a Resource.extra_data summary item otherwise.
    """
    if as_attribute:
        summary = resource.summary
    else:
        summary = resource.extra_data.get('summary', {})
    summary = summary or {}
    return summary.get(key) or None


def set_resource_summary(resource, key, value, as_attribute=False):
    """
    Set `value` as the "summary" value for the `key` summary attribute of a
    resource

    This is set either in a direct Resource.summary attribute if `as_attribute`
    is True or as a Resource.extra_data summary item otherwise.
    """
    if as_attribute:
        resource.summary[key] = value
    else:
        summary = resource.extra_data.get('summary')
        if not summary:
            summary = dict([(key, value)])
            resource.extra_data['summary'] = summary
        summary[key] = value


def sorted_counter(counter):
    """
    Return a list of ordered mapping of {value:val, count:cnt} built from a
    `counter` mapping of {value: count} and sortedd by decreasing count then by
    value.
    """

    def by_count_value(value_count):
        value, count = value_count
        return -count, value or ''

    summarized = [
        dict([('value', value), ('count', count)])
        for value, count in sorted(counter.items(), key=by_count_value)]
    return summarized
