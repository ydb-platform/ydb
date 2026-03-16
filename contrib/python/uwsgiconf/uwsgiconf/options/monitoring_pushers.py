from ..base import ParametrizedValue
from ..utils import KeyValue


class Pusher(ParametrizedValue):

    args_joiner = ','


class PusherSocket(Pusher):
    """Push metrics to a UDP server.

    Uses the following format: ``<metric> <type> <value>``
    ``<type>`` - is in the numeric form of metric type.

    """
    name = 'socket'
    plugin = 'stats_pusher_socket'

    def __init__(self, address, *, prefix=None):
        """
        :param str address:

        :param str prefix: Arbitrary prefix to differentiate sender.

        """
        super().__init__(address, prefix)


class PusherRrdtool(Pusher):
    """This will store an rrd file for each metric in the specified directory.

    Each rrd file has a single data source named "metric".

    """
    name = 'rrdtool'
    plugin = 'rrdtool'

    def __init__(self, target_dir, *, library=None, push_interval=None):
        """
        :param str target_dir: Directory to store rrd files into.

        :param str library: Set the name of rrd library. Default: librrd.so.

        :param int push_interval: Set push frequency.

        """
        super().__init__(target_dir)

        self._set('rrdtool-freq', push_interval)
        self._set('rrdtool-lib', library)


class PusherStatsd(Pusher):
    """Push metrics to a statsd server."""

    name = 'statsd'
    plugin = 'stats_pusher_statsd'

    def __init__(self, address, *, prefix=None, no_workers=None, all_gauges=None):
        """
        :param str address:

        :param str prefix: Arbitrary prefix to differentiate sender.

        :param bool no_workers: Disable generation of single worker metrics.

        :param bool all_gauges: Push all metrics to statsd as gauges.
        """
        super().__init__(address, prefix)

        self._set('statsd-no-workers', no_workers, cast=bool)
        self._set('statsd-all-gauges', all_gauges, cast=bool)


class PusherCarbon(Pusher):
    """Push metrics to a Carbon server of Graphite.

    Metric node format: ``<node_root>.hostname.<node_realm>.metrics_data``.

    * http://uwsgi.readthedocs.io/en/latest/Carbon.html
    * http://uwsgi.readthedocs.io/en/latest/tutorials/GraphiteAndMetrics.html

    """
    name = 'carbon'
    plugin = 'carbon'
    opt_key = name

    def __init__(
            self, address, *, node_realm=None, node_root=None, push_interval=None, idle_avg_source=None,
            use_metrics=None, no_workers=None, timeout=None, retries=None, retries_delay=None,
            hostname_dots_replacer=None):
        """
        :param str|list[str] address: Host and port. Example: 127.0.0.1:2004

        :param str node_realm: Set carbon metrics realm node.

        :param str node_root: Set carbon metrics root node. Default: uwsgi.

        :param int push_interval: Set carbon push frequency in seconds. Default: 60.

        :param bool no_workers: Disable generation of single worker metrics.

        :param str idle_avg_source: Average values source during idle period (no requests).

            Variants:
                * last (default)
                * zero
                * none

        :param bool use_metrics: Don't compute all statistics, use metrics subsystem data
            instead.

            .. warning:: Key names of built-in stats are different from those of metrics system.

        :param int timeout: Set carbon connection timeout in seconds. Default: 3.

        :param int retries: Set maximum number of retries in case of connection errors. Default: 1.

        :param int retries_delay: Set connection retry delay in seconds. Default: 7.

        :param str hostname_dots_replacer: Set char to use as a replacement for
            dots in hostname in `<node_root>.hostname.<node_realm>.metrics_data``

            This affects Graphite aggregation mechanics.

            .. note:: Dots are not replaced by default.

        """
        super().__init__(address)

        self._set('carbon-id', node_realm)
        self._set('carbon-root', node_root)
        self._set('carbon-freq', push_interval)
        self._set('carbon-idle-avg', idle_avg_source)
        self._set('carbon-use-metrics', use_metrics, cast=bool)
        self._set('carbon-no-workers', no_workers, cast=bool)
        self._set('carbon-timeout', timeout)
        self._set('carbon-max-retry', retries)
        self._set('carbon-retry-delay', retries_delay)
        self._set('carbon-hostname-dots', hostname_dots_replacer)

        if not address.split(':')[0].replace('.', '').isdigit():
            self._set('carbon-name-resolve', True, cast=bool)


class PusherZabbix(Pusher):
    """Push metrics to a zabbix server."""

    name = 'zabbix'
    plugin = 'zabbix'

    def __init__(self, address, *, prefix=None, template=None):
        """
        :param str address:

        :param str prefix: Arbitrary prefix to differentiate sender.

        :param str template: Print (or store to a file) the zabbix template
            for the current metrics setup.

        """
        super().__init__(address, prefix)

        self._set('zabbix-template', template)


class PusherMongo(Pusher):
    """Push statistics (as JSON) the the specified MongoDB database."""

    name = 'mongodb'
    plugin = 'stats_pusher_mongodb'

    def __init__(self, *, address=None, collection=None, push_interval=None):
        """
        :param str address: Default: 127.0.0.1:27017

        :param str collection: MongoDB colection to write into. Default: uwsgi.statistics

        :param int push_interval: Write interval in seconds.
        """
        value = KeyValue(locals(), aliases={'push_interval': 'freq'})

        super().__init__(value)


class PusherFile(Pusher):
    """Stores stats JSON into a file.

    .. note:: Mainly for demonstration purposes.

    """
    name = 'file'
    plugin = 'stats_pusher_file'

    def __init__(self, fpath=None, *, separator=None, push_interval=None):
        """
        :param str fpath: File path. Default: uwsgi.stats

        :param str separator: New entry separator. Default: \n\n

        :param int push_interval: Write interval in seconds.
        """
        value = KeyValue(locals(), aliases={'fpath': 'path', 'push_interval': 'freq'})

        super().__init__(value)
