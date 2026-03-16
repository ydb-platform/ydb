"""Support for OpenTSDB."""

import attr
from attr.validators import instance_of
from grafanalib.validators import is_in

# OpenTSDB aggregators
OTSDB_AGG_AVG = 'avg'
OTSDB_AGG_COUNT = 'count'
OTSDB_AGG_DEV = 'dev'
OTSDB_AGG_EP50R3 = 'ep50r3'
OTSDB_AGG_EP50R7 = 'ep50r7'
OTSDB_AGG_EP75R3 = 'ep75r3'
OTSDB_AGG_EP75R7 = 'ep75r7'
OTSDB_AGG_EP90R3 = 'ep90r3'
OTSDB_AGG_EP90R7 = 'ep90r7'
OTSDB_AGG_EP95R3 = 'ep95r3'
OTSDB_AGG_EP95R7 = 'ep95r7'
OTSDB_AGG_EP99R3 = 'ep99r3'
OTSDB_AGG_EP99R7 = 'ep99r7'
OTSDB_AGG_EP999R3 = 'ep999r3'
OTSDB_AGG_EP999R7 = 'ep999r7'
OTSDB_AGG_FIRST = 'first'
OTSDB_AGG_LAST = 'last'
OTSDB_AGG_MIMMIN = 'mimmin'
OTSDB_AGG_MIMMAX = 'mimmax'
OTSDB_AGG_MIN = 'min'
OTSDB_AGG_MAX = 'max'
OTSDB_AGG_NONE = 'none'
OTSDB_AGG_P50 = 'p50'
OTSDB_AGG_P75 = 'p75'
OTSDB_AGG_P90 = 'p90'
OTSDB_AGG_P95 = 'p95'
OTSDB_AGG_P99 = 'p99'
OTSDB_AGG_P999 = 'p999'
OTSDB_AGG_SUM = 'sum'
OTSDB_AGG_ZIMSUM = 'zimsum'

OTSDB_DOWNSAMPLING_FILL_POLICIES = ('none', 'nan', 'null', 'zero')
OTSDB_DOWNSAMPLING_FILL_POLICY_DEFAULT = 'none'

OTSDB_QUERY_FILTERS = (
    'literal_or', 'iliteral_or', 'not_literal_or',
    'not_iliteral_or', 'wildcard', 'iwildcard', 'regexp')
OTSDB_QUERY_FILTER_DEFAULT = 'literal_or'


@attr.s
class OpenTSDBFilter(object):

    value = attr.ib()
    tag = attr.ib()
    type = attr.ib(
        default=OTSDB_QUERY_FILTER_DEFAULT,
        validator=is_in(OTSDB_QUERY_FILTERS))
    groupBy = attr.ib(default=False, validator=instance_of(bool))

    def to_json_data(self):
        return {
            'filter': self.value,
            'tagk': self.tag,
            'type': self.type,
            'groupBy': self.groupBy
        }


@attr.s
class OpenTSDBTarget(object):
    """Generates OpenTSDB target JSON structure.

    Grafana docs on using OpenTSDB:
    http://docs.grafana.org/features/datasources/opentsdb/
    OpenTSDB docs on querying or reading data:
    http://opentsdb.net/docs/build/html/user_guide/query/index.html


    :param metric: OpenTSDB metric name
    :param refId: target reference id
    :param aggregator: defines metric aggregator.
        The list of opentsdb aggregators:
        http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html#available-aggregators
    :param alias: legend alias. Use patterns like $tag_tagname to replace part
        of the alias for a tag value.
    :param isCounter: defines if rate function results should
        be interpret as counter
    :param counterMax: defines rate counter max value
    :param counterResetValue: defines rate counter reset value
    :param disableDownsampling: defines if downsampling should be disabled.
        OpenTSDB docs on downsampling:
        http://opentsdb.net/docs/build/html/user_guide/query/index.html#downsampling
    :param downsampleAggregator: defines downsampling aggregator
    :param downsampleFillPolicy: defines downsampling fill policy
    :param downsampleInterval: defines downsampling interval
    :param filters: defines the list of metric query filters.
        OpenTSDB docs on filters:
        http://opentsdb.net/docs/build/html/user_guide/query/index.html#filters
    :param shouldComputeRate: defines if rate function should be used.
        OpenTSDB docs on rate function:
        http://opentsdb.net/docs/build/html/user_guide/query/index.html#rate
    :param currentFilterGroupBy: defines if grouping should be enabled for
        current filter
    :param currentFilterKey: defines current filter key
    :param currentFilterType: defines current filter type
    :param currentFilterValue: defines current filter value
    """

    metric = attr.ib()
    refId = attr.ib(default="")
    aggregator = attr.ib(default='sum')
    alias = attr.ib(default=None)
    isCounter = attr.ib(default=False, validator=instance_of(bool))
    counterMax = attr.ib(default=None)
    counterResetValue = attr.ib(default=None)
    disableDownsampling = attr.ib(default=False, validator=instance_of(bool))
    downsampleAggregator = attr.ib(default=OTSDB_AGG_SUM)
    downsampleFillPolicy = attr.ib(
        default=OTSDB_DOWNSAMPLING_FILL_POLICY_DEFAULT,
        validator=is_in(OTSDB_DOWNSAMPLING_FILL_POLICIES))
    downsampleInterval = attr.ib(default=None)
    filters = attr.ib(default=attr.Factory(list))
    shouldComputeRate = attr.ib(default=False, validator=instance_of(bool))
    currentFilterGroupBy = attr.ib(default=False, validator=instance_of(bool))
    currentFilterKey = attr.ib(default="")
    currentFilterType = attr.ib(default=OTSDB_QUERY_FILTER_DEFAULT)
    currentFilterValue = attr.ib(default="")

    def to_json_data(self):

        return {
            'aggregator': self.aggregator,
            'alias': self.alias,
            'isCounter': self.isCounter,
            'counterMax': self.counterMax,
            'counterResetValue': self.counterResetValue,
            'disableDownsampling': self.disableDownsampling,
            'downsampleAggregator': self.downsampleAggregator,
            'downsampleFillPolicy': self.downsampleFillPolicy,
            'downsampleInterval': self.downsampleInterval,
            'filters': self.filters,
            'metric': self.metric,
            'refId': self.refId,
            'shouldComputeRate': self.shouldComputeRate,
            'currentFilterGroupBy': self.currentFilterGroupBy,
            'currentFilterKey': self.currentFilterKey,
            'currentFilterType': self.currentFilterType,
            'currentFilterValue': self.currentFilterValue,
        }
