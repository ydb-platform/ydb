# -*- coding: utf-8 -*-
import time
import json
import collections
from six.moves.urllib import request


class KikimrMonitor(object):
    def __init__(self, host, mon_port, update_interval_seconds=1.0):
        super(KikimrMonitor, self).__init__()
        self.__host = host
        self.__mon_port = mon_port
        self.__url = "http://{host}:{mon_port}/counters/json".format(host=host, mon_port=mon_port)
        self.__pdisks = set()
        self.__data = {}
        self.__next_update_time = time.time()
        self.__update_interval_seconds = update_interval_seconds
        self._by_sensor_name = collections.defaultdict(list)

    def tabletcounters(self, tablet_id):
        url = request.urlopen(
            "http://{host}:{mon_port}/viewer/json/tabletcounters?tablet_id={tablet_id}".format(
                host=self.__host, mon_port=self.__mon_port, tablet_id=tablet_id
            )
        )
        return json.load(url)

    @property
    def pdisks(self):
        return tuple(self.__pdisks)

    def fetch(self, deadline=60):
        self.__update_if_required(force_update=True, deadline=deadline)
        return self

    def __update_if_required(self, force_update=False, deadline=None):
        if not force_update and time.time() < self.__next_update_time:
            return

        try:
            url = request.urlopen(self.__url)
        except Exception:
            return False
        try:
            raw = json.load(url)
        except ValueError:
            return False

        self.__data = {}
        for sensor_dict in raw.get('sensors', ()):
            labels = sensor_dict.get('labels', {})
            sensor_name = labels.get('sensor')
            value = sensor_dict.get('value', None)
            key = self.normalize(labels)

            if labels.get('counters', None) == 'pdisks':
                if 'pdisk' in labels:
                    self.__pdisks.add(int(labels['pdisk']))

            if value is not None:
                self.__data[key] = value
                self._by_sensor_name[sensor_name].append(
                    (key, value))

        deadline = deadline if deadline is not None else self.__update_interval_seconds
        self.__next_update_time = time.time() + deadline
        return True

    def get_by_name(self, sensor):
        return self._by_sensor_name.get(sensor, [])

    @staticmethod
    def normalize(labels):
        return tuple(
            (key, value)
            for key, value in sorted(labels.items(), key=lambda x: x[0])
        )

    def has_actual_data(self):
        return self.__update_if_required(force_update=True)

    def force_update(self):
        self.__update_if_required(force_update=True)

    def sensor(self, counters, sensor, _default=None, **kwargs):
        self.__update_if_required()
        kwargs.update({'counters': counters, 'sensor': sensor})
        key = self.normalize(kwargs)
        return self.__data.get(key, _default)
