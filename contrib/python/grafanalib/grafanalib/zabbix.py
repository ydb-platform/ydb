import attr
import itertools
from attr.validators import instance_of
from numbers import Number
from grafanalib.validators import is_interval, is_in, is_color_code, is_list_of
from grafanalib.core import (
    RGBA, Percent, Pixels, DashboardLink,
    DEFAULT_ROW_HEIGHT, BLANK, GREEN)

ZABBIX_TRIGGERS_TYPE = 'alexanderzobnin-zabbix-triggers-panel'

ZABBIX_QMODE_METRICS = 0
ZABBIX_QMODE_SERVICES = 1
ZABBIX_QMODE_TEXT = 2

ZABBIX_SLA_PROP_STATUS = {
    'name': 'Status',
    'property': 'status'}

ZABBIX_SLA_PROP_SLA = {
    'name': 'SLA',
    'property': 'sla'}

ZABBIX_SLA_PROP_OKTIME = {
    'name': 'OK time',
    'property': 'okTime'}

ZABBIX_SLA_PROP_PROBTIME = {
    'name': 'Problem time',
    'property': 'problemTime'}

ZABBIX_SLA_PROP_DOWNTIME = {
    'name': 'Down time',
    'property': 'downtimeTime',
}

ZABBIX_EVENT_PROBLEMS = {
    'text': 'Problems',
    'value': [1]}

ZABBIX_EVENT_OK = {
    'text': 'OK',
    'value': [0]}

ZABBIX_EVENT_ALL = {
    'text': 'All',
    'value': [0, 1]}

ZABBIX_TRIGGERS_SHOW_ALL = 'all triggers'
ZABBIX_TRIGGERS_SHOW_ACK = 'acknowledged'
ZABBIX_TRIGGERS_SHOW_NACK = 'unacknowledged'

ZABBIX_SORT_TRIGGERS_BY_CHANGE = {
    'text': 'last change',
    'value': 'lastchange',
}
ZABBIX_SORT_TRIGGERS_BY_SEVERITY = {
    'text': 'severity',
    'value': 'priority',
}

ZABBIX_SEVERITY_COLORS = (
    ('#B7DBAB', 'Not classified'),
    ('#82B5D8', 'Information'),
    ('#E5AC0E', 'Warning'),
    ('#C15C17', 'Average'),
    ('#BF1B00', 'High'),
    ('#890F02', 'Disaster'),
)


def convertZabbixSeverityColors(colors):
    priorities = itertools.count(0)
    return [ZabbixColor(color=c, priority=next(priorities), severity=s)
            for c, s in colors]


@attr.s
class ZabbixTargetOptions(object):
    showDisabledItems = attr.ib(default=False, validator=instance_of(bool))

    def to_json_data(self):
        return {
            'showDisabledItems': self.showDisabledItems
        }


@attr.s
class ZabbixTargetField(object):
    filter = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        return {
            'filter': self.filter
        }


@attr.s
class ZabbixTarget(object):
    """Generates Zabbix datasource target JSON structure.

    Grafana-Zabbix is a plugin for Grafana allowing
    to visualize monitoring data from Zabbix and create
    dashboards for analyzing metrics and realtime monitoring.

    Grafana docs on using Zabbix plugin: https://alexanderzobnin.github.io/grafana-zabbix/

    :param application: zabbix application name
    :param expr: zabbix arbitary query
    :param functions: list of zabbix aggregation functions
    :param group: zabbix host group
    :param host: hostname
    :param intervalFactor: defines interval between metric queries
    :param item: regexp that defines which metric to query
    :param itService: zabbix it service name
    :param mode: query mode type
    :param options: additional query options
    :param refId: target reference id
    :param slaProperty: zabbix it service sla property.
        Zabbix returns the following availability information about IT service
        Status - current status of the IT service
        SLA - SLA for the given time interval
        OK time - time the service was in OK state, in seconds
        Problem time - time the service was in problem state, in seconds
        Down time - time the service was in scheduled downtime, in seconds
    :param textFilter: query text filter. Use regex to extract a part of
        the returned value.
    :param useCaptureGroups: defines if capture groups should be used during
        metric query
    """

    application = attr.ib(default="", validator=instance_of(str))
    expr = attr.ib(default="")
    functions = attr.ib(default=attr.Factory(list))
    group = attr.ib(default="", validator=instance_of(str))
    host = attr.ib(default="", validator=instance_of(str))
    intervalFactor = attr.ib(default=2, validator=instance_of(int))
    item = attr.ib(default="", validator=instance_of(str))
    itService = attr.ib(default="", validator=instance_of(str))
    mode = attr.ib(default=ZABBIX_QMODE_METRICS, validator=instance_of(int))
    options = attr.ib(default=attr.Factory(ZabbixTargetOptions),
                      validator=instance_of(ZabbixTargetOptions))
    refId = attr.ib(default="")
    slaProperty = attr.ib(default=attr.Factory(dict))
    textFilter = attr.ib(default="", validator=instance_of(str))
    useCaptureGroups = attr.ib(default=False, validator=instance_of(bool))

    def to_json_data(self):
        obj = {
            'application': ZabbixTargetField(self.application),
            'expr': self.expr,
            'functions': self.functions,
            'group': ZabbixTargetField(self.group),
            'host': ZabbixTargetField(self.host),
            'intervalFactor': self.intervalFactor,
            'item': ZabbixTargetField(self.item),
            'mode': self.mode,
            'options': self.options,
            'refId': self.refId,
        }
        if self.mode == ZABBIX_QMODE_SERVICES:
            obj['slaProperty'] = self.slaProperty,
            obj['itservice'] = {'name': self.itService}
        if self.mode == ZABBIX_QMODE_TEXT:
            obj['textFilter'] = self.textFilter
            obj['useCaptureGroups'] = self.useCaptureGroups
        return obj


@attr.s
class ZabbixDeltaFunction(object):
    """ZabbixDeltaFunction

    Convert absolute values to delta, for example, bits to bits/sec
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#delta
    """
    added = attr.ib(default=False, validator=instance_of(bool))

    def to_json_data(self):
        text = "delta()"
        definition = {
            'category': 'Transform',
            'name': 'delta',
            'defaultParams': [],
            'params': []}
        return {
            'added': self.added,
            'text': text,
            'def': definition,
            'params': [],
        }


@attr.s
class ZabbixGroupByFunction(object):
    """ZabbixGroupByFunction

    Takes each timeseries and consolidate its points falled in given interval
    into one point using function, which can be one of: avg, min, max, median.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions//#groupBy
    """

    _options = ('avg', 'min', 'max', 'median')
    _default_interval = '1m'
    _default_function = 'avg'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default=_default_interval, validator=is_interval)
    function = attr.ib(default=_default_function,
                       validator=is_in(_options))

    def to_json_data(self):
        text = "groupBy({interval}, {function})"
        definition = {
            'category': 'Transform',
            'name': 'groupBy',
            'defaultParams': [
                self._default_interval,
                self._default_function,
            ],
            'params': [
                {'name': 'interval',
                 'type': 'string'},
                {'name': 'function',
                 'options': self._options,
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval, function=self.function),
            'params': [self.interval, self.function],
            'added': self.added,
        }


@attr.s
class ZabbixScaleFunction(object):
    """ZabbixScaleFunction

    Takes timeseries and multiplies each point by the given factor.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions//#scale
    """

    _default_factor = 100

    added = attr.ib(default=False, validator=instance_of(bool))
    factor = attr.ib(default=_default_factor, validator=instance_of(Number))

    def to_json_data(self):
        text = "scale({factor})"
        definition = {
            'category': 'Transform',
            'name': 'scale',
            'defaultParams': [self._default_factor],
            'params': [
                {'name': 'factor',
                 'options': [100, 0.01, 10, -1],
                 'type': 'float'}]
        }
        return {
            'def': definition,
            'text': text.format(factor=self.factor),
            'params': [self.factor],
            'added': self.added,
        }


@attr.s
class ZabbixAggregateByFunction(object):
    """ZabbixAggregateByFunction

    Takes all timeseries and consolidate all its points falled in given
    interval into one point using function, which can be one of:
    avg, min, max, median.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#aggregateBy
    """

    _options = ('avg', 'min', 'max', 'median')
    _default_interval = '1m'
    _default_function = 'avg'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default=_default_interval, validator=is_interval)
    function = attr.ib(default=_default_function,
                       validator=is_in(_options))

    def to_json_data(self):
        text = "aggregateBy({interval}, {function})"
        definition = {
            'category': 'Aggregate',
            'name': 'aggregateBy',
            'defaultParams': [
                self._default_interval,
                self._default_function,
            ],
            'params': [
                {'name': 'interval',
                 'type': 'string'},
                {'name': 'function',
                 'options': self._options,
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval, function=self.function),
            'params': [self.interval, self.function],
            'added': self.added,
        }


@attr.s
class ZabbixAverageFunction(object):
    """ZabbixAverageFunction

    Deprecated, use aggregateBy(interval, avg) instead.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#average
    """

    _default_interval = '1m'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default=_default_interval, validator=is_interval)

    def to_json_data(self):
        text = "average({interval})"
        definition = {
            'category': "Aggregate",
            "name": "average",
            "defaultParams": [
                self._default_interval,
            ],
            'params': [
                {'name': 'interval',
                 'type': 'string'}]
        }
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval),
            'params': [self.interval],
            'added': self.added,
        }


@attr.s
class ZabbixMaxFunction(object):
    """ZabbixMaxFunction

    Deprecated, use aggregateBy(interval, max) instead.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#max
    """

    _default_interval = '1m'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default=_default_interval, validator=is_interval)

    def to_json_data(self):
        text = "max({interval})"
        definition = {
            'category': 'Aggregate',
            'name': 'max',
            'defaultParams': [
                self._default_interval,
            ],
            'params': [
                {'name': 'interval',
                 'type': 'string'}]
        }
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval),
            'params': [self.interval],
            'added': self.added,
        }


@attr.s
class ZabbixMedianFunction(object):
    """ZabbixMedianFunction

    Deprecated, use aggregateBy(interval, median) instead.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#median
    """

    _default_interval = '1m'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default='1m', validator=is_interval)

    def to_json_data(self):
        text = "median({interval})"
        definition = {
            'category': 'Aggregate',
            'name': 'median',
            'defaultParams': [
                self._default_interval,
            ],
            'params': [
                {'name': 'interval',
                 'type': 'string'}]
        }
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval),
            'params': [self.interval],
            'added': self.added,
        }


@attr.s
class ZabbixMinFunction(object):
    """ZabbixMinFunction

    Deprecated, use aggregateBy(interval, min) instead.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#min
    """

    _default_interval = '1m'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default=_default_interval, validator=is_interval)

    def to_json_data(self):
        text = "min({interval})"
        definition = {
            'category': 'Aggregate',
            'name': 'min',
            'defaultParams': [
                self._default_interval,
            ],
            'params': [
                {'name': 'interval',
                 'type': 'string'}]
        }
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval),
            'params': [self.interval],
            'added': self.added,
        }


@attr.s
class ZabbixSumSeriesFunction(object):
    """ZabbixSumSeriesFunction

    This will add metrics together and return the sum at each datapoint.
    This method required interpolation of each timeseries so it may
    cause high CPU load.
    Try to combine it with groupBy() function to reduce load.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#sumSeries
    """
    added = attr.ib(default=False)

    def to_json_data(self):
        text = "sumSeries()"
        definition = {
            'category': 'Aggregate',
            'name': 'sumSeries',
            'defaultParams': [],
            'params': []}
        return {
            'added': self.added,
            'text': text,
            'def': definition,
            'params': [],
        }


@attr.s
class ZabbixBottomFunction(object):

    _options = ('avg', 'min', 'max', 'median')
    _default_number = 5
    _default_function = 'avg'

    added = attr.ib(default=False, validator=instance_of(bool))
    number = attr.ib(default=_default_number, validator=instance_of(int))
    function = attr.ib(default=_default_function,
                       validator=is_in(_options))

    def to_json_data(self):
        text = "bottom({number}, {function})"
        definition = {
            'category': 'Filter',
            'name': 'bottom',
            'defaultParams': [
                self._default_number,
                self._default_function,
            ],
            'params': [
                {'name': 'number',
                 'type': 'string'},
                {'name': 'function',
                 'options': self._options,
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(
                number=self.number, function=self.function),
            'params': [self.number, self.function],
            'added': self.added,
        }


@attr.s
class ZabbixTopFunction(object):

    _options = ('avg', 'min', 'max', 'median')
    _default_number = 5
    _default_function = 'avg'

    added = attr.ib(default=False, validator=instance_of(bool))
    number = attr.ib(default=_default_number, validator=instance_of(int))
    function = attr.ib(default=_default_function,
                       validator=is_in(_options))

    def to_json_data(self):
        text = "top({number}, {function})"
        definition = {
            'category': 'Filter',
            'name': 'top',
            'defaultParams': [
                self._default_number,
                self._default_function,
            ],
            'params': [
                {'name': 'number',
                 'type': 'string'},
                {'name': 'function',
                 'options': self._options,
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(
                number=self.number, function=self.function),
            'params': [self.number, self.function],
            'added': self.added,
        }


@attr.s
class ZabbixTrendValueFunction(object):
    """ZabbixTrendValueFunction

    Specifying type of trend value returned by Zabbix when
    trends are used (avg, min or max).
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#trendValue
    """

    _options = ('avg', 'min', 'max')
    _default_type = 'avg'
    added = attr.ib(default=False, validator=instance_of(bool))
    type = attr.ib(default=_default_type,
                   validator=is_in(_options))

    def to_json_data(self):
        text = "trendValue({type})"
        definition = {
            'category': 'Trends',
            'name': 'trendValue',
            'defaultParams': [
                self._default_type,
            ],
            'params': [
                {'name': 'type',
                 'options': self._options,
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(
                type=self.type),
            'params': [self.type],
            'added': self.added,
        }


@attr.s
class ZabbixTimeShiftFunction(object):
    """ZabbixTimeShiftFunction

    Draws the selected metrics shifted in time.
    If no sign is given, a minus sign ( - ) is implied which will
    shift the metric back in time.
    If a plus sign ( + ) is given, the metric will be shifted forward in time.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#timeShift
    """

    _options = ('24h', '7d', '1M', '+24h', '-24h')
    _default_interval = '24h'

    added = attr.ib(default=False, validator=instance_of(bool))
    interval = attr.ib(default=_default_interval)

    def to_json_data(self):
        text = "timeShift({interval})"
        definition = {
            'category': 'Time',
            'name': 'timeShift',
            'defaultParams': [
                self._default_interval,
            ],
            'params': [
                {'name': 'interval',
                 'options': self._options,
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(
                interval=self.interval),
            'params': [self.interval],
            'added': self.added,
        }


@attr.s
class ZabbixSetAliasFunction(object):
    """ZabbixSetAliasFunction

    Returns given alias instead of the metric name.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#setAlias
    """
    alias = attr.ib(validator=instance_of(str))
    added = attr.ib(default=False, validator=instance_of(bool))

    def to_json_data(self):
        text = "setAlias({alias})"
        definition = {
            'category': 'Alias',
            'name': 'setAlias',
            'defaultParams': [],
            'params': [
                {'name': 'alias',
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(alias=self.alias),
            'params': [self.alias],
            'added': self.added,
        }


@attr.s
class ZabbixSetAliasByRegexFunction(object):
    """ZabbixSetAliasByRegexFunction

    Returns part of the metric name matched by regex.
    https://alexanderzobnin.github.io/grafana-zabbix/reference/functions/#setAliasByRegex
    """

    regexp = attr.ib(validator=instance_of(str))
    added = attr.ib(default=False, validator=instance_of(bool))

    def to_json_data(self):
        text = "setAliasByRegex({regexp})"
        definition = {
            'category': 'Alias',
            'name': 'setAliasByRegex',
            'defaultParams': [],
            'params': [
                {'name': 'aliasByRegex',
                 'type': 'string'}]}
        return {
            'def': definition,
            'text': text.format(regexp=self.regexp),
            'params': [self.regexp],
            'added': self.added,
        }


def zabbixMetricTarget(application, group, host, item, functions=[]):
    return ZabbixTarget(
        mode=ZABBIX_QMODE_METRICS,
        application=application,
        group=group,
        host=host,
        item=item,
        functions=functions,
    )


def zabbixServiceTarget(service, sla=ZABBIX_SLA_PROP_STATUS):
    return ZabbixTarget(
        mode=ZABBIX_QMODE_SERVICES,
        itService=service,
        slaProperty=sla,
    )


def zabbixTextTarget(application, group, host, item, text,
                     useCaptureGroups=False):
    return ZabbixTarget(
        mode=ZABBIX_QMODE_TEXT,
        application=application,
        group=group,
        host=host,
        item=item,
        textFilter=text,
        useCaptureGroups=useCaptureGroups,
    )


@attr.s
class ZabbixColor(object):
    color = attr.ib(validator=is_color_code)
    priority = attr.ib(validator=instance_of(int))
    severity = attr.ib(validator=instance_of(str))
    show = attr.ib(default=True, validator=instance_of(bool))

    def to_json_data(self):
        return {
            'color': self.color,
            'priority': self.priority,
            'severity': self.severity,
            'show': self.show,
        }


@attr.s
class ZabbixTrigger(object):

    application = attr.ib(default="", validator=instance_of(str))
    group = attr.ib(default="", validator=instance_of(str))
    host = attr.ib(default="", validator=instance_of(str))
    trigger = attr.ib(default="", validator=instance_of(str))

    def to_json_data(self):
        return {
            'application': ZabbixTargetField(self.application),
            'group': ZabbixTargetField(self.group),
            'host': ZabbixTargetField(self.host),
            'trigger': ZabbixTargetField(self.trigger),
        }


@attr.s
class ZabbixTriggersPanel(object):
    """ZabbixTriggersPanel

    :param dataSource: query datasource name
    :param title: panel title
    :param ackEventColor: acknowledged triggers color
    :param customLastChangeFormat: defines last change field data format.
        See momentjs docs for time format:
        http://momentjs.com/docs/#/displaying/format/
    :param description: additional panel description
    :param fontSize: panel font size
    :param height: panel height in Pixels
    :param hideHostsInMaintenance: defines if triggers form hosts
        in maintenance should be shown
    :param hostField: defines if host field should be shown
    :param hostTechNameField: defines if field with host technical name should
        be shown
    :param id: panel identificator
    :param infoField: defines if field with host info should be shown
    :param lastChangeField: defines if field with last change
        time should be shown
    :param limit: defines number of queried triggers
    :param links: list of dashboard links
    :param markAckEvents: defines if acknowledged triggers should be colored
        with different color
    :param minSpan: defines panel minimum spans
    :param okEventColor: defines color for triggers with Ok status
    :param pageSize: defines number of triggers per panel page
    :param scroll: defines if scroll should be shown
    :param severityField: defines if severity field should be shown
    :param showEvents: defines event type to query (Ok, Problems, All)
    :param showTriggers: defines trigger type to query
        (all, acknowledged, unacknowledged)
    :param sortTriggersBy: defines trigger sort policy
    :param span: defines span number for panel
    :param statusField: defines if status field should be shown
    :param transparent: defines if panel should be transparent
    :param triggerSeverity: defines colors for trigger severity,
    :param triggers: trigger query

    """
    dataSource = attr.ib()
    title = attr.ib()

    ackEventColor = attr.ib(default=attr.Factory(lambda: BLANK),
                            validator=instance_of(RGBA))
    ageField = attr.ib(default=True, validator=instance_of(bool))
    customLastChangeFormat = attr.ib(default=False,
                                     validator=instance_of(bool))
    description = attr.ib(default="", validator=instance_of(str))
    fontSize = attr.ib(default=attr.Factory(Percent),
                       validator=instance_of(Percent))
    height = attr.ib(default=DEFAULT_ROW_HEIGHT, validator=instance_of(Pixels))
    hideHostsInMaintenance = attr.ib(default=False,
                                     validator=instance_of(bool))
    hostField = attr.ib(default=True, validator=instance_of(bool))
    hostTechNameField = attr.ib(default=False, validator=instance_of(bool))
    id = attr.ib(default=None)
    infoField = attr.ib(default=True, validator=instance_of(bool))
    lastChangeField = attr.ib(default=True, validator=instance_of(bool))

    lastChangeFormat = attr.ib(default="")
    limit = attr.ib(default=10, validator=instance_of(int))
    links = attr.ib(default=attr.Factory(list),
                    validator=is_list_of(DashboardLink))
    markAckEvents = attr.ib(default=False, validator=instance_of(bool))
    minSpan = attr.ib(default=None)
    okEventColor = attr.ib(default=attr.Factory(lambda: GREEN),
                           validator=instance_of(RGBA))
    pageSize = attr.ib(default=10, validator=instance_of(int))
    repeat = attr.ib(default=None)
    scroll = attr.ib(default=True, validator=instance_of(bool))
    severityField = attr.ib(default=False, validator=instance_of(bool))
    showEvents = attr.ib(default=attr.Factory(lambda: ZABBIX_EVENT_PROBLEMS))
    showTriggers = attr.ib(default=ZABBIX_TRIGGERS_SHOW_ALL)
    sortTriggersBy = attr.ib(
        default=attr.Factory(lambda: ZABBIX_SORT_TRIGGERS_BY_CHANGE),
    )
    span = attr.ib(default=None)
    statusField = attr.ib(default=False, validator=instance_of(bool))
    transparent = attr.ib(default=False, validator=instance_of(bool))
    try:
        triggerSeverity = attr.ib(
            default=ZABBIX_SEVERITY_COLORS,
            converter=convertZabbixSeverityColors,
        )
    except TypeError:
        triggerSeverity = attr.ib(
            default=ZABBIX_SEVERITY_COLORS,
            convert=convertZabbixSeverityColors,
        )
    triggers = attr.ib(
        default=attr.Factory(ZabbixTrigger),
        validator=instance_of(ZabbixTrigger),
    )

    def to_json_data(self):
        return {
            'type': ZABBIX_TRIGGERS_TYPE,
            'datasource': self.dataSource,
            'title': self.title,
            'ackEventColor': self.ackEventColor,
            'ageField': self.ageField,
            'customLastChangeFormat': self.customLastChangeFormat,
            'description': self.description,
            'fontSize': self.fontSize,
            'height': self.height,
            'hideHostsInMaintenance': self.hideHostsInMaintenance,
            'hostField': self.hostField,
            'hostTechNameField': self.hostTechNameField,
            'id': self.id,
            'infoField': self.infoField,
            'lastChangeField': self.lastChangeField,
            'lastChangeFormat': self.lastChangeFormat,
            'limit': self.limit,
            'links': self.links,
            'markAckEvents': self.markAckEvents,
            'minSpan': self.minSpan,
            'okEventColor': self.okEventColor,
            'pageSize': self.pageSize,
            'repeat': self.repeat,
            'scroll': self.scroll,
            'severityField': self.severityField,
            'showEvents': self.showEvents,
            'showTriggers': self.showTriggers,
            'sortTriggersBy': self.sortTriggersBy,
            'span': self.span,
            'statusField': self.statusField,
            'transparent': self.transparent,
            'triggers': self.triggers,
            'triggerSeverity': self.triggerSeverity,
        }
