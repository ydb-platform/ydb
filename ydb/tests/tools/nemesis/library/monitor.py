# -*- coding: utf-8 -*-
import flask
import copy

from library.python.monlib.metric_registry import MetricRegistry
from library.python.monlib import encoder


CONTENT_TYPE_SPACK = 'application/x-solomon-spack'
CONTENT_TYPE_JSON = 'application/json'
app = flask.Flask(__name__)


class Monitor(object):
    def __init__(self):
        self._registry = MetricRegistry()

    @property
    def registry(self):
        return self._registry

    def int_gauge(self, sensor, labels):
        all_labels = copy.deepcopy(labels)
        all_labels.update({'sensor': sensor})
        return self._registry.int_gauge(all_labels)

    def rate(self, sensor, labels):
        all_labels = copy.deepcopy(labels)
        all_labels.update({'sensor': sensor})
        return self._registry.rate(all_labels)


_MONITOR = Monitor()


@app.route('/sensors')
def sensors():
    if flask.request.headers['accept'] == CONTENT_TYPE_SPACK:
        return flask.Response(encoder.dumps(monitor().registry), mimetype=CONTENT_TYPE_SPACK)
    return flask.Response(encoder.dumps(monitor().registry, format='json'), mimetype=CONTENT_TYPE_JSON)


def monitor():
    return _MONITOR


def setup_page(host, port):
    app.run(host, port)
