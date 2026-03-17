#
# Copyright 2013, Martin Owens <doctormo@gmail.com>
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.
#
"""
Access logs in known locations to find information about them.
"""

import os
import re
import codecs
import platform
from dateutil import parser as dateparse

MATCHER = r'(?P<date>\w+ +\d+ +\d\d:\d\d:\d\d) (?P<host>\w+) ' + \
        r'CRON\[(?P<pid>\d+)\]: \((?P<user>\w+)\) CMD \((?P<cmd>.*)\)'

class LogReader(object):
    """Opens a Log file, reading backwards and watching for changes"""
    def __init__(self, filename, mass=4096):
        self.filename = filename
        self.mass = mass
        self.size = -1
        self.read = -1
        self.pipe = None

    def __enter__(self):
        self.size = os.stat(self.filename)[6]
        self.pipe = codecs.open(self.filename, 'r', encoding='utf-8')
        return self

    def __exit__(self, error_type, value, traceback):
        self.pipe.close()

    def __iter__(self):
        if self.pipe is None:
            with self as reader:
                for (offset, line) in reader.readlines():
                    yield line
        else:
            for (offset, line) in self.readlines():
                yield line

    def readlines(self, until=0):
        """Iterator for reading lines from a file backwards"""
        if not self.pipe or self.pipe.closed:
            raise IOError("Can't readline, no opened file.")
        # Always seek to the end of the file, this accounts for file updates
        # that happen during our running process.
        location = self.size
        halfline = ''

        while location > until:
            location -= self.mass
            mass = self.mass
            if location < 0:
                mass = self.mass + location
                location = 0
            self.pipe.seek(location)
            line = self.pipe.read(mass) + halfline
            data = line.split('\n')
            if location != 0:
                halfline = data.pop(0)
            loc = location + mass
            data.reverse()
            for line in data:
                if line.strip() == '':
                    continue
                yield (loc, line)
                loc -= len(line)



class CronLog(LogReader):
    """Use the LogReader to make a Cron specific log reader"""
    def __init__(self, filename='/var/log/syslog', user=None):
        LogReader.__init__(self, filename)
        self.user = user

    def for_program(self, command):
        """Return log entries for this specific command name"""
        return ProgramLog(self, command)

    def __iter__(self):
        for line in super(CronLog, self).__iter__():
            match = re.match(MATCHER, str(line))
            datum = match and match.groupdict()
            if datum and (not self.user or datum['user'] == self.user):
                datum['date'] = dateparse.parse(datum['date'])
                yield datum


class ProgramLog(object):
    """Specific log control for a single command/program"""
    def __init__(self, log, command):
        self.log = log
        self.command = command

    def __iter__(self):
        for entry in self.log:
            if entry['cmd'] == str(self.command):
                yield entry

