#!/usr/bin/env python
# -*- encoding: utf-8 -*-


import re
from aiocaldav.lib.python_utilities import to_local


def fix(event):
    fixed = re.sub('COMPLETED:(\d+)\s', 'COMPLETED:\g<1>T120000Z',
                   to_local(event))
    # The following line fixes a data bug in some Google Calendar events
    fixed = re.sub('CREATED:00001231T000000Z',
                   'CREATED:19700101T000000Z', fixed)
    fixed = re.sub(r"\\+('\")", r"\1", fixed)

    return fixed
