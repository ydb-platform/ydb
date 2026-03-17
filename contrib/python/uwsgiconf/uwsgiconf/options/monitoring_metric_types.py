from ..base import ParametrizedValue
from ..utils import KeyValue, filter_locals
from ..exceptions import ConfigurationError


class Metric(ParametrizedValue):

    type_str = None
    type_id = None
    name_separator = ','

    def __init__(
            self, name, *, oid=None, alias_for=None, collector=None,
            initial_value=None, collect_interval=None, reset_after_push=None):
        """

        :param str name: Metric name.

            .. note:: Only numbers, letters, underscores, dashes and dots.

        :param str alias_for: If set metric will be a simple alias for the specified one.

        :param str oid: Metric OID.

            Required for SNMP.

            * http://uwsgi-docs.readthedocs.io/en/latest/Metrics.html#oid-assigment-for-plugins

        :param Collector collector: Collector to be used. If not set it is considered that the value must
            be updated manually from applications using the metrics API.

            * http://uwsgi-docs.readthedocs.io/en/latest/Metrics.html#api

        :param int initial_value: Set the metric to a specific value on startup.

        :param int collect_interval: How ofter the metric should be gathered. In seconds.

        :param bool reset_after_push: Reset the metric to zero (or the configured initial_value)
            after it's been pushed.

        """
        self.name = name

        if alias_for:
            # Set any type to ``alias``.
            self.type_str = MetricTypeAlias.type_str
            self.type_id = MetricTypeAlias.type_id

            if isinstance(alias_for, Metric):
                alias_for = alias_for.name

        if self.type_id == MetricTypeAlias.type_id and not alias_for:
            raise ConfigurationError('Parameter ``alias_for`` is required for ``MetricTypeAlias``.')

        type_str = self.type_str

        value = KeyValue(
            filter_locals(locals(), drop=['name']),
            aliases={'collect_interval': 'freq', 'type_str': 'type', 'alias_for': 'alias'}
        )

        super().__init__(value)

    def __str__(self):
        return f'name={super().__str__()}'


class MetricTypeCounter(Metric):
    """A generally-growing up number.

        Example:
            * number of requests

    """
    type_str = 'counter'
    type_id = 0


class MetricTypeGauge(Metric):
    """A number that can increase or decrease dynamically.

        Example:
            * memory used by a worker
            * CPU load

    """
    type_str = 'gauge'
    type_id = 1


class MetricTypeAbsolute(Metric):
    """An absolute number.

        Example:
            * memory of the whole server
            * size of the hard disk.

    """
    type_str = 'absolute'
    type_id = 2


class MetricTypeAlias(Metric):
    """This is a virtual metric pointing to another one .

    You can use it to give different names to already existing metrics.

    """
    type_str = 'alias'
    type_id = 3
