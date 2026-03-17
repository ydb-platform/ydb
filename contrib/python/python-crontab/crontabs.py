#
# Copyright 2016, Martin Owens <doctormo@gmail.com>
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
The crontabs manager will list all available crontabs on the system.
"""

import os
import sys
import itertools

from os import stat, access, X_OK
from crontab import CronTab, WINOS

getpwuid = None
if not WINOS:
    from pwd import getpwuid

class UserSpool(list):
    """Generates all user crontabs, yields both owned and abandoned tabs"""
    def __init__(self, loc, tabs=None):
        for username in self.listdir(loc):
            tab = self.generate(loc, username)
            if tab:
                self.append(tab)
        if not self:
            tab = CronTab(user=True)
            if tab:
                self.append(tab)

    def listdir(self, loc):
        try:
            return os.listdir(loc)
        except OSError:
            return []

    def get_owner(self, path):
        """Returns user file at path"""
        if not getpwuid:
            raise OSError("This functionality is not available on Windows")
        try:
            return getpwuid(stat(path).st_uid).pw_name
        except KeyError:
            return

    def generate(self, loc, username):
        path = os.path.join(loc, username)
        if username != self.get_owner(path):
            # Abandoned crontab pool entry!
            return CronTab(tabfile=path)
        return CronTab(user=username)


class SystemTab(list):
    """Generates all system tabs"""
    def __init__(self, loc, tabs=None):
        if os.path.isdir(loc):
            for item in os.listdir(loc):
                if item[0] == '.':
                    continue
                path = os.path.join(loc, item)
                self.append(CronTab(user=False, tabfile=path))
        elif os.path.isfile(loc):
            self.append(CronTab(user=False, tabfile=loc))


class AnaCronTab(list):
    """Attempts to digest anacron entries (if possible)"""
    def __init__(self, loc, tabs=None):
        if tabs and os.path.isdir(loc):
            self.append(CronTab(user=False))
            jobs = list(tabs.all.find_command(loc))
            if jobs:
                for item in os.listdir(loc):
                    self.add(loc, item, jobs[0])
                jobs[0].delete()

    def add(self, loc, item, anajob):
        path = os.path.join(loc, item)
        if item in ['0anacron'] or item[0] == '.' or not access(path, X_OK):
            return
        job = self[0].new(command=path, user=anajob.user)
        job.set_comment('Anacron %s' % loc.split('.')[-1])
        job.setall(anajob)
        return job


# Files are direct, directories are listed (recursively)
KNOWN_LOCATIONS = [
  # Known linux locations (Debian, RedHat, etc)
  (UserSpool, '/var/spool/cron/crontabs/'),
  (SystemTab, '/etc/crontab'),
  (SystemTab, '/etc/cron.d/'),
  # Anacron digestion (we want to know more)
  (AnaCronTab, '/etc/cron.hourly'),
  (AnaCronTab, '/etc/cron.daily'),
  (AnaCronTab, '/etc/cron.weekly'),
  (AnaCronTab, '/etc/cron.monthly'),
  # Known MacOSX locations
   # None
  # Other (windows, bsd)
   # None
]

class CronTabs(list):
    """Singleton dictionary of all detectable crontabs"""
    _all = None
    _self = None

    def __new__(cls, *args, **kw):
        if not cls._self:
            cls._self = super(CronTabs, cls).__new__(cls, *args, **kw)
        return cls._self

    def __init__(self):
        if not self:
            for loc in KNOWN_LOCATIONS:
                self.add(*loc)

    def add(self, cls, *args):
        for tab in cls(*args, tabs=self):
            self.append(tab)
            self._all = None

    @property
    def all(self):
        """Return a CronTab object with all jobs (read-only)"""
        if self._all is None:
            self._all = CronTab(user=False)
            for tab in self:
                for job in tab:
                    if job.user is None:
                        job.user = tab.user or 'unknown'
                    self._all.append(job)
        return self._all

