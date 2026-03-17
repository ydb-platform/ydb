#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# nsmap2 is ref https://bitbucket.org/cyrilrbt/caldav/issue/29/centos59-minifi
# This looks wrong - should think more about it at a later stage.
# -- Tobias Brox, 2014-02-1

nsmap = {
    "D": "DAV",
    "C": "urn:ietf:params:xml:ns:caldav",
    "I": "http://apple.com/ns/ical/",
}

nsmap2 = {
    "D": "DAV:",
    "C": "urn:ietf:params:xml:ns:caldav",
    "I": "http://apple.com/ns/ical/",
}


def ns(prefix, tag=None):
    name = "{%s}" % nsmap2[prefix]
    if tag is not None:
        name = "%s%s" % (name, tag)
    return name
