"""Helpers to create Elasticsearch-specific Grafana queries."""

import attr
import itertools
from attr.validators import in_, instance_of
from grafanalib.core import AlertCondition

DATE_HISTOGRAM_DEFAULT_FIELD = 'time_iso8601'
ORDER_ASC = 'asc'
ORDER_DESC = 'desc'


@attr.s
class CountMetricAgg(object):
    """An aggregator that counts the number of values.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html

    It's the default aggregator for elasticsearch queries.
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param inline: script to apply to the data, using '_value'
    """
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'count',
            'field': 'select field',
            'inlineScript': self.inline,
            'settings': self.settings,
        }


@attr.s
class MaxMetricAgg(object):
    """An aggregator that provides the max. value among the values.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html

    :param field: name of elasticsearch field to provide the maximum for
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param inline: script to apply to the data, using '_value'
    """
    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'max',
            'field': self.field,
            'inlineScript': self.inline,
            'settings': self.settings,
        }


@attr.s
class CardinalityMetricAgg(object):
    """An aggregator that provides the cardinality. value among the values.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html

    :param field: name of elasticsearch field to provide the cardinality for
    :param id: id of the metric
    :param hide: show/hide the metric in the final panel display
    :param inline: script to apply to the data, using '_value'
    """
    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'cardinality',
            'field': self.field,
            'inlineScript': self.inline,
            'settings': self.settings,
        }


@attr.s
class AverageMetricAgg(object):
    """An aggregator that provides the average. value among the values.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html

    :param field: name of elasticsearch metric aggregator to provide the average of
    :param id: id of the metric
    :param hide: show/hide the metric in the final panel display
    :param inline: script to apply to the data, using '_value'
    """

    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'avg',
            'field': self.field,
            'inlineScript': self.inline,
            'settings': self.settings,
            'meta': {}
        }


@attr.s
class DerivativeMetricAgg(object):
    """An aggregator that takes the derivative of another metric aggregator.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-derivative-aggregation.html

    :param field: id of elasticsearch metric aggregator to provide the derivative of
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param pipelineAgg: pipeline aggregator id
    :param unit: derivative units
    """
    field = attr.ib(default="", validator=instance_of(str))
    hide = attr.ib(default=False, validator=instance_of(bool))
    id = attr.ib(default=0, validator=instance_of(int))
    pipelineAgg = attr.ib(default=1, validator=instance_of(int))
    unit = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        settings = {}
        if self.unit != "":
            settings['unit'] = self.unit

        return {
            'id': str(self.id),
            'pipelineAgg': str(self.pipelineAgg),
            'hide': self.hide,
            'type': 'derivative',
            'field': self.field,
            'settings': settings,
        }


@attr.s
class SumMetricAgg(object):
    """An aggregator that provides the sum of the values.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html
    :param field: name of elasticsearch field to provide the sum over
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param inline: script to apply to the data, using '_value'
    """
    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'sum',
            'field': self.field,
            'inlineScript': self.inline,
            'settings': self.settings,
        }


@attr.s
class DateHistogramGroupBy(object):
    """A bucket aggregator that groups results by date.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html

    :param id: ascending unique number per GroupBy clause
    :param field: name of the elasticsearch field to group by
    :param interval: interval to group by
    :param minDocCount: min. amount of records in the timespan to return a
                        result
    """
    id = attr.ib(default=0, validator=instance_of(int))
    field = attr.ib(
        default=DATE_HISTOGRAM_DEFAULT_FIELD,
        validator=instance_of(str),
    )
    interval = attr.ib(default='auto', validator=instance_of(str))
    minDocCount = attr.ib(default=0, validator=instance_of(int))

    def to_json_data(self):
        return {
            'field': self.field,
            'id': str(self.id),
            'settings': {
                'interval': self.interval,
                'min_doc_count': self.minDocCount,
                'trimEdges': 0,
            },
            'type': 'date_histogram',
        }


@attr.s
class BucketScriptAgg(object):
    """An aggregator that applies a bucket script to the results of previous aggregations.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-bucket-script-aggregation.html

    :param fields: dictionary of field names mapped to aggregation IDs to be used in the bucket script
                   e.g. { "field1":1 }, which allows the output of aggregate ID 1 to be referenced as
                   params.field1 in the bucket script
    :param script: script to apply to the data using the variables specified in 'fields'
    :param id: id of the aggregator
    :param hide: show/hide the metric in the final panel display
    """
    fields = attr.ib(factory=dict, validator=instance_of(dict))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    script = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        pipelineVars = []
        for field in self.fields:
            pipelineVars.append({
                'name': str(field),
                'pipelineAgg': str(self.fields[field])
            })

        return {
            'field': 'select field',
            'type': 'bucket_script',
            'id': str(self.id),
            'hide': self.hide,
            'pipelineVariables': pipelineVars,
            'settings': {
                'script': self.script
            },
        }


@attr.s
class Filter(object):
    """ A Filter for a FilterGroupBy aggregator.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filter-aggregation.html

    :param label: label for the metric that is shown in the graph
    :param query: the query to filter by
    """
    label = attr.ib(default="", validator=instance_of(str))
    query = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        return {
            'label': self.label,
            'query': self.query,
        }


@attr.s
class FiltersGroupBy(object):
    """ A bucket aggregator that groups records by a filter expression.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filter-aggregation.html

    :param id: ascending unique number per GroupBy clause
    :param filters: list of Filter objects
    """
    id = attr.ib(default=0, validator=instance_of(int))
    filters = attr.ib(default=attr.Factory(list))

    def to_json_data(self):
        return {
            'id': str(self.id),
            'settings': {
                'filters': self.filters,
            },
            'type': 'filters',
        }


@attr.s
class TermsGroupBy(object):
    """ A multi-bucket aggregator based on field values.

    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html

    :param id: ascending unique number per GroupBy clause
    :param field: name of the field to group by
    :param minDocCount: min. amount of matching records to return a result
    :param order: ORDER_ASC or ORDER_DESC
    :param orderBy: term to order the bucket Term value: '_term', Doc Count: '_count'
        or to use metric function use the string value "2"
    :param size: how many buckets are returned
    """
    field = attr.ib(validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    minDocCount = attr.ib(default=1, validator=instance_of(int))
    order = attr.ib(default=ORDER_DESC, validator=instance_of(str))
    orderBy = attr.ib(default='_term', validator=instance_of(str))
    size = attr.ib(default=0, validator=instance_of(int))

    def to_json_data(self):
        return {
            'id': str(self.id),
            'type': 'terms',
            'field': self.field,
            'settings': {
                'min_doc_count': self.minDocCount,
                'order': self.order,
                'orderBy': self.orderBy,
                'size': self.size,
            },
        }


@attr.s
class ElasticsearchTarget(object):
    """Generates Elasticsearch target JSON structure.

    Grafana docs on using Elasticsearch:
    http://docs.grafana.org/features/datasources/elasticsearch/
    Elasticsearch docs on querying or reading data:
    https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html

    :param alias: legend alias
    :param bucketAggs: Bucket aggregators
    :param metricAggs: Metric Aggregators
    :param query: query
    :param refId: target reference id
    :param timeField: name of the elasticsearch time field
    :param hide: show/hide the target result in the final panel display
    """

    alias = attr.ib(default=None)
    bucketAggs = attr.ib(
        default=attr.Factory(lambda: [DateHistogramGroupBy()]),
    )
    metricAggs = attr.ib(default=attr.Factory(lambda: [CountMetricAgg()]))
    query = attr.ib(default="", validator=instance_of(str))
    refId = attr.ib(default="", validator=instance_of(str))
    timeField = attr.ib(default="@timestamp", validator=instance_of(str))
    hide = attr.ib(default=False, validator=instance_of(bool))

    def _map_bucket_aggs(self, f):
        return attr.evolve(self, bucketAggs=list(map(f, self.bucketAggs)))

    def auto_bucket_agg_ids(self):
        """Give unique IDs all bucketAggs without ID.

        Returns a new ``ElasticsearchTarget`` that is the same as this one,
        except all of the bucketAggs have their ``id`` property set. Any panels
        which had an ``id`` property set will keep that property, all others
        will have auto-generated IDs provided for them.

        If the bucketAggs don't have unique ID associated with it, the
        generated graph will be broken.
        """
        ids = set([agg.id for agg in self.bucketAggs if agg.id])
        auto_ids = (i for i in itertools.count(1) if i not in ids)

        def set_id(agg):
            if agg.id:
                return agg

            return attr.evolve(agg, id=next(auto_ids))

        return self._map_bucket_aggs(set_id)

    def to_json_data(self):
        return {
            'alias': self.alias,
            'bucketAggs': self.bucketAggs,
            'metrics': self.metricAggs,
            'query': self.query,
            'refId': self.refId,
            'timeField': self.timeField,
            'hide': self.hide,
        }


@attr.s
class ElasticsearchAlertCondition(AlertCondition):
    """
    Override alert condition to support Elasticseach target.

    See AlertCondition for more information.

    :param Target target: Metric the alert condition is based on.
    :param Evaluator evaluator: How we decide whether we should alert on the
        metric. e.g. ``GreaterThan(5)`` means the metric must be greater than 5
        to trigger the condition. See ``GreaterThan``, ``LowerThan``,
        ``WithinRange``, ``OutsideRange``, ``NoValue``.
    :param TimeRange timeRange: How long the condition must be true for before
        we alert.
    :param operator: One of ``OP_AND`` or ``OP_OR``. How this condition
        combines with other conditions.
    :param reducerType: RTYPE_*
    :param type: CTYPE_*
    """

    target = attr.ib(default=None, validator=instance_of(ElasticsearchTarget))


@attr.s
class MinMetricAgg(object):
    """An aggregator that provides the min. value among the values.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html
    :param field: name of elasticsearch field to provide the minimum for
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param inline: script to apply to the data, using '_value'
    """

    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'min',
            'field': self.field,
            'inlineScript': self.inline,
            'settings': self.settings,
        }


@attr.s
class PercentilesMetricAgg(object):
    """A multi-value metrics aggregation that calculates one or more percentiles over numeric values extracted from the aggregated documents
    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html
    :param field: name of elasticsearch field to provide the percentiles for
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param inline: script to apply to the data, using '_value'
    :param percents: list of percentiles, like [95,99]
    """

    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    inline = attr.ib(default="", validator=instance_of(str))
    percents = attr.ib(default=attr.Factory(list))
    settings = attr.ib(factory=dict)

    def to_json_data(self):
        self.settings = {}

        self.settings['percents'] = self.percents

        if self.inline:
            self.settings['script'] = {'inline': self.inline}

        return {
            'id': str(self.id),
            'hide': self.hide,
            'type': 'percentiles',
            'field': self.field,
            'inlineScript': self.inline,
            'settings': self.settings,
        }


@attr.s
class RateMetricAgg(object):
    """An aggregator that provides the rate of the values.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-rate-aggregation.html
    :param field: name of elasticsearch field to provide the sum over
    :param hide: show/hide the metric in the final panel display
    :param id: id of the metric
    :param unit: calendar interval to group by
        supported calendar intervals
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html#calendar_intervals
        "minute"
        "hour"
        "day"
        "week"
        "month"
        "quarter"
        "year"
    :param mode: sum or count the values
    :param script: script to apply to the data, using '_value'
    """

    field = attr.ib(default="", validator=instance_of(str))
    id = attr.ib(default=0, validator=instance_of(int))
    hide = attr.ib(default=False, validator=instance_of(bool))
    unit = attr.ib(default="", validator=instance_of(str))
    mode = attr.ib(default="", validator=in_(["", "value_count", "sum"]))
    script = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        self.settings = {}

        if self.unit:
            self.settings["unit"] = self.unit

        if self.mode:
            self.settings["mode"] = self.mode

        if self.script:
            self.settings["script"] = self.script

        return {
            "id": str(self.id),
            "hide": self.hide,
            "field": self.field,
            "settings": self.settings,
            "type": "rate",
        }
