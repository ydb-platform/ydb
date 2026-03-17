"""Tests for core."""

import random
import grafanalib.core as G
import pytest


def dummy_grid_pos() -> G.GridPos:
    return G.GridPos(h=1, w=2, x=3, y=4)


def dummy_data_link() -> G.DataLink:
    return G.DataLink(
        title='dummy title',
        linkUrl='https://www.dummy-link-url.com',
        isNewTab=True
    )


def dummy_evaluator() -> G.Evaluator:
    return G.Evaluator(
        type=G.EVAL_GT,
        params=42
    )


def dummy_alert_condition() -> G.AlertCondition:
    return G.AlertCondition(
        target=G.Target(
            refId="A",
        ),
        evaluator=G.Evaluator(
            type=G.EVAL_GT,
            params=42),
        timeRange=G.TimeRange(
            from_time='5m',
            to_time='now'
        ),
        operator=G.OP_AND,
        reducerType=G.RTYPE_AVG,
    )


def test_template_defaults():
    t = G.Template(
        name='test',
        query='1m,5m,10m,30m,1h,3h,12h,1d',
        type='interval',
        default='1m',
    )

    assert t.to_json_data()['current']['text'] == '1m'
    assert t.to_json_data()['current']['value'] == '1m'


def test_custom_template_ok():
    t = G.Template(
        name='test',
        query='1,2,3',
        default='1',
        type='custom',
    )

    assert len(t.to_json_data()['options']) == 3
    assert t.to_json_data()['current']['text'] == '1'
    assert t.to_json_data()['current']['value'] == '1'


def test_custom_template_dont_override_options():
    t = G.Template(
        name='test',
        query='1,2,3',
        default='1',
        options=[
            {
                "value": '1',
                "selected": True,
                "text": 'some text 1',
            },
            {
                "value": '2',
                "selected": False,
                "text": 'some text 2',
            },
            {
                "value": '3',
                "selected": False,
                "text": 'some text 3',
            },
        ],
        type='custom',
    )

    assert len(t.to_json_data()['options']) == 3
    assert t.to_json_data()['current']['text'] == 'some text 1'
    assert t.to_json_data()['current']['value'] == '1'


def test_table():
    t = G.Table(
        dataSource='some data source',
        targets=[
            G.Target(expr='some expr'),
        ],
        title='table title',
        transformations=[
            {
                "id": "seriesToRows",
                "options": {}
            },
            {
                "id": "organize",
                "options": {
                    "excludeByName": {
                        "Time": True
                    },
                    "indexByName": {},
                    "renameByName": {
                        "Value": "Dummy"
                    }
                }
            }
        ]
    )
    assert len(t.to_json_data()['transformations']) == 2
    assert t.to_json_data()['transformations'][0]["id"] == "seriesToRows"


def test_stat_no_repeat():
    t = G.Stat(
        title='dummy',
        dataSource='data source',
        targets=[
            G.Target(expr='some expr')
        ]
    )

    assert t.to_json_data()['repeat'] is None
    assert t.to_json_data()['repeatDirection'] is None
    assert t.to_json_data()['maxPerRow'] is None


def test_Text_exception_checks():
    with pytest.raises(TypeError):
        G.Text(content=123)

    with pytest.raises(TypeError):
        G.Text(error=123)

    with pytest.raises(ValueError):
        G.Text(mode=123)


def test_ePictBox():
    t = G.ePictBox()
    json_data = t.to_json_data()

    assert json_data['angle'] == 0
    assert json_data['backgroundColor'] == "#000"
    assert json_data['blinkHigh'] is False
    assert json_data['blinkLow'] is False
    assert json_data['color'] == "#000"
    assert json_data['colorHigh'] == "#000"
    assert json_data['colorLow'] == "#000"
    assert json_data['colorMedium'] == "#000"
    assert json_data['colorSymbol'] is False
    assert json_data['customSymbol'] == ""
    assert json_data['decimal'] == 0
    assert json_data['fontSize'] == 12
    assert json_data['hasBackground'] is False
    assert json_data['hasOrb'] is False
    assert json_data['hasSymbol'] is False
    assert json_data['isUsingThresholds'] is False
    assert json_data['orbHideText'] is False
    assert json_data['orbLocation'] == "Left"
    assert json_data['orbSize'] == 13
    assert json_data['prefix'] == ""
    assert json_data['prefixSize'] == 10
    assert json_data['selected'] is False
    assert json_data['serie'] == ""
    assert json_data['suffix'] == ""
    assert json_data['suffixSize'] == 10
    assert json_data['symbol'] == ""
    assert json_data['symbolDefHeight'] == 32
    assert json_data['symbolDefWidth'] == 32
    assert json_data['symbolHeight'] == 32
    assert json_data['symbolHideText'] is False
    assert json_data['symbolWidth'] == 32
    assert json_data['text'] == "N/A"
    assert json_data['thresholds'] == ""
    assert json_data['url'] == ""
    assert json_data['xpos'] == 0
    assert json_data['ypos'] == 0

    t = G.ePictBox(
        angle=1,
        backgroundColor="#100",
        blinkHigh=True,
        blinkLow=True,
        color="#200",
        colorHigh="#300",
        colorLow="#400",
        colorMedium="#500",
        colorSymbol=True,
        decimal=2,
        fontSize=9,
        hasBackground=True,
        hasOrb=True,
        hasSymbol=True,
        orbHideText=True,
        orbLocation="Right",
        orbSize=10,
        prefix="prefix",
        prefixSize=11,
        selected=True,
        serie="series",
        suffix="suffix",
        suffixSize=12,
        symbol="data:image/svg+xml;base64,...",
        symbolDefHeight=13,
        symbolDefWidth=14,
        symbolHeight=15,
        symbolHideText=True,
        symbolWidth=17,
        text="text",
        thresholds="40,50",
        url="https://google.de",
        xpos=18,
        ypos=19,
    )

    json_data = t.to_json_data()

    assert json_data['angle'] == 1
    assert json_data['backgroundColor'] == "#100"
    assert json_data['blinkHigh'] is True
    assert json_data['blinkLow'] is True
    assert json_data['color'] == "#200"
    assert json_data['colorHigh'] == "#300"
    assert json_data['colorLow'] == "#400"
    assert json_data['colorMedium'] == "#500"
    assert json_data['colorSymbol'] is True
    assert json_data['decimal'] == 2
    assert json_data['fontSize'] == 9
    assert json_data['hasBackground'] is True
    assert json_data['hasOrb'] is True
    assert json_data['hasSymbol'] is True
    assert json_data['isUsingThresholds'] is True
    assert json_data['orbHideText'] is True
    assert json_data['orbLocation'] == "Right"
    assert json_data['orbSize'] == 10
    assert json_data['prefix'] == "prefix"
    assert json_data['prefixSize'] == 11
    assert json_data['selected'] is True
    assert json_data['serie'] == "series"
    assert json_data['suffix'] == "suffix"
    assert json_data['suffixSize'] == 12
    assert json_data['symbol'] == "data:image/svg+xml;base64,..."
    assert json_data['symbolDefHeight'] == 13
    assert json_data['symbolDefWidth'] == 14
    assert json_data['symbolHeight'] == 15
    assert json_data['symbolHideText'] is True
    assert json_data['symbolWidth'] == 17
    assert json_data['text'] == "text"
    assert json_data['thresholds'] == "40,50"
    assert json_data['url'] == "https://google.de"
    assert json_data['xpos'] == 18
    assert json_data['ypos'] == 19


def test_ePictBox_custom_symbole_logic():
    t = G.ePictBox(
        customSymbol="https://foo.bar/foo.jpg",
        symbol="will be overiden",
    )

    json_data = t.to_json_data()

    assert json_data['customSymbol'] == "https://foo.bar/foo.jpg"
    assert json_data['symbol'] == "custom"


def test_ePict():
    t = G.ePict()
    json_data = t.to_json_data()

    assert json_data['type'] == G.EPICT_TYPE
    assert json_data['options']['autoScale'] is True
    assert json_data['options']['bgURL'] == ''
    assert json_data['options']['boxes'] == []

    t = G.ePict(
        autoScale=False,
        bgURL='https://example.com/img.jpg',
        boxes=[
            G.ePictBox(),
            G.ePictBox(angle=123),
        ]
    )
    json_data = t.to_json_data()

    print(json_data)

    assert json_data['type'] == G.EPICT_TYPE
    assert json_data['options']['autoScale'] is False
    assert json_data['options']['bgURL'] == 'https://example.com/img.jpg'
    assert json_data['options']['boxes'] == [
        G.ePictBox(),
        G.ePictBox(angle=123),
    ]


def test_Text():
    t = G.Text()

    json_data = t.to_json_data()
    assert json_data['error'] is False
    assert json_data['options']['content'] == ""
    assert json_data['options']['mode'] == G.TEXT_MODE_MARKDOWN

    t = G.Text(content='foo', error=True, mode=G.TEXT_MODE_HTML)

    json_data = t.to_json_data()
    assert json_data['error'] is True
    assert json_data['options']['content'] == "foo"
    assert json_data['options']['mode'] == G.TEXT_MODE_HTML


def test_DiscreteColorMappingItem_exception_checks():
    with pytest.raises(TypeError):
        G.DiscreteColorMappingItem(123)

    with pytest.raises(TypeError):
        G.DiscreteColorMappingItem("foo", color=123)


def test_DiscreteColorMappingItem():
    t = G.DiscreteColorMappingItem('foo')

    json_data = t.to_json_data()
    assert json_data['text'] == 'foo'
    assert json_data['color'] == G.GREY1

    t = G.DiscreteColorMappingItem('foo', color='bar')

    json_data = t.to_json_data()
    assert json_data['text'] == 'foo'
    assert json_data['color'] == 'bar'


def test_Discrete_exceptions():
    with pytest.raises(ValueError):
        G.Discrete(legendSortBy='foo')

    with pytest.raises(TypeError):
        G.Discrete(rangeMaps=[123, 456])

    with pytest.raises(TypeError):
        G.Discrete(valueMaps=['foo', 'bar'])

    with pytest.raises(TypeError):
        G.Discrete(lineColor=123)

    with pytest.raises(TypeError):
        G.Discrete(highlightOnMouseover=123)


def test_Discrete():
    colorMap = [
        G.DiscreteColorMappingItem('bar', color='baz'),
        G.DiscreteColorMappingItem('foz', color='faz')
    ]

    t = G.Discrete(
        title='foo',
        colorMaps=colorMap,
        lineColor='#aabbcc',
        metricNameColor=G.RGBA(1, 2, 3, .5),
        decimals=123,
        highlightOnMouseover=False,
        showDistinctCount=True,
        showLegendCounts=False,
    )

    json_data = t.to_json_data()
    assert json_data['colorMaps'] == colorMap
    assert json_data['title'] == 'foo'
    assert json_data['type'] == G.DISCRETE_TYPE
    assert json_data['rangeMaps'] == []
    assert json_data['valueMaps'] == []

    assert json_data['backgroundColor'] == G.RGBA(128, 128, 128, 0.1)
    assert json_data['lineColor'] == '#aabbcc'
    assert json_data['metricNameColor'] == G.RGBA(1, 2, 3, .5)
    assert json_data['timeTextColor'] == "#d8d9da"
    assert json_data['valueTextColor'] == "#000000"

    assert json_data['decimals'] == 123
    assert json_data['legendPercentDecimals'] == 0
    assert json_data['rowHeight'] == 50
    assert json_data['textSize'] == 24
    assert json_data['textSizeTime'] == 12

    assert json_data['highlightOnMouseover'] is False
    assert json_data['showLegend'] is True
    assert json_data['showLegendPercent'] is True
    assert json_data['showLegendNames'] is True
    assert json_data['showLegendValues'] is True
    assert json_data['showTimeAxis'] is True
    assert json_data['use12HourClock'] is False
    assert json_data['writeMetricNames'] is False
    assert json_data['writeLastValue'] is True
    assert json_data['writeAllValues'] is False

    assert json_data['showDistinctCount'] is True
    assert json_data['showLegendCounts'] is False
    assert json_data['showLegendTime'] is None
    assert json_data['showTransitionCount'] is None


def test_StatValueMappings_exception_checks():
    with pytest.raises(TypeError):
        G.StatValueMappings(
            G.StatValueMappingItem('foo', '0', 'dark-red'),
            "not of type StatValueMappingItem",
        )


def test_StatValueMappings():
    t = G.StatValueMappings(
        G.StatValueMappingItem('foo', '0', 'dark-red'),  # Value must a string
        G.StatValueMappingItem('bar', '1', 'purple'),
    )

    json_data = t.to_json_data()
    assert json_data['type'] == 'value'
    assert json_data['options']['0']['text'] == 'foo'
    assert json_data['options']['0']['color'] == 'dark-red'
    assert json_data['options']['1']['text'] == 'bar'
    assert json_data['options']['1']['color'] == 'purple'


def test_StatRangeMappings():
    t = G.StatRangeMappings(
        'dummy_text',
        startValue=10,
        endValue=20,
        color='dark-red'
    )

    json_data = t.to_json_data()
    assert json_data['type'] == 'range'
    assert json_data['options']['from'] == 10
    assert json_data['options']['to'] == 20
    assert json_data['options']['result']['text'] == 'dummy_text'
    assert json_data['options']['result']['color'] == 'dark-red'


def test_StatMapping():
    t = G.StatMapping(
        'dummy_text',
        startValue='foo',
        endValue='bar',
    )

    json_data = t.to_json_data()
    assert json_data['text'] == 'dummy_text'
    assert json_data['from'] == 'foo'
    assert json_data['to'] == 'bar'


def test_stat_with_repeat():
    t = G.Stat(
        title='dummy',
        dataSource='data source',
        targets=[
            G.Target(expr='some expr')
        ],
        repeat=G.Repeat(
            variable="repetitionVariable",
            direction='h',
            maxPerRow=10
        )
    )

    assert t.to_json_data()['repeat'] == 'repetitionVariable'
    assert t.to_json_data()['repeatDirection'] == 'h'
    assert t.to_json_data()['maxPerRow'] == 10


def test_single_stat():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    single_stat = G.SingleStat(data_source, targets, title)
    data = single_stat.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title


def test_dashboard_list():
    title = 'dummy title'
    dashboard_list = G.DashboardList(title=title)
    data = dashboard_list.to_json_data()
    assert data['targets'] == []
    assert data['datasource'] is None
    assert data['title'] == title
    assert data['starred'] is True


def test_logs_panel():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    logs = G.Logs(data_source, targets, title)
    data = logs.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert data['options']['showLabels'] is False
    assert data['options']['showCommonLabels'] is False
    assert data['options']['showTime'] is False
    assert data['options']['wrapLogMessage'] is False
    assert data['options']['sortOrder'] == 'Descending'
    assert data['options']['dedupStrategy'] == 'none'
    assert data['options']['enableLogDetails'] is False
    assert data['options']['prettifyLogMessage'] is False


def test_notification():
    uid = 'notification_channel'
    notification = G.Notification(uid)
    data = notification.to_json_data()
    assert data['uid'] == uid


def test_graph_panel():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    graph = G.Graph(data_source, targets, title)
    data = graph.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert 'alert' not in data


def test_panel_extra_json():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    extraJson = {
        'fillGradient': 6,
        'yaxis': {'align': True},
        'legend': {'avg': True},
    }
    graph = G.Graph(data_source, targets, title, extraJson=extraJson)
    data = graph.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert 'alert' not in data
    assert data['fillGradient'] == 6
    assert data['yaxis']['align'] is True
    # Nested non-dict object should also be deep-updated
    assert data['legend']['max'] is False
    assert data['legend']['avg'] is True


def test_graph_panel_threshold():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    thresholds = [
        G.GraphThreshold(20.0),
        G.GraphThreshold(40.2, colorMode="ok")
    ]
    graph = G.Graph(data_source, targets, title, thresholds=thresholds)
    data = graph.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert 'alert' not in data
    assert data['thresholds'] == thresholds


def test_graph_panel_alert():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    alert = [
        G.AlertCondition(G.Target(refId="A"), G.Evaluator('a', 'b'), G.TimeRange('5', '6'), 'd', 'e')
    ]
    thresholds = [
        G.GraphThreshold(20.0),
        G.GraphThreshold(40.2, colorMode="ok")
    ]
    graph = G.Graph(data_source, targets, title, thresholds=thresholds, alert=alert)
    data = graph.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert data['alert'] == alert
    assert data['thresholds'] == []


def test_graph_threshold():
    value = 20.0
    colorMode = "ok"
    threshold = G.GraphThreshold(value, colorMode=colorMode)
    data = threshold.to_json_data()

    assert data['value'] == value
    assert data['colorMode'] == colorMode
    assert data['fill'] is True
    assert data['line'] is True
    assert data['op'] == G.EVAL_GT
    assert 'fillColor' not in data
    assert 'lineColor' not in data


def test_graph_threshold_custom():
    value = 20.0
    colorMode = "custom"
    color = G.GREEN
    threshold = G.GraphThreshold(value, colorMode=colorMode, fillColor=color)
    data = threshold.to_json_data()

    assert data['value'] == value
    assert data['colorMode'] == colorMode
    assert data['fill'] is True
    assert data['line'] is True
    assert data['op'] == G.EVAL_GT
    assert data['fillColor'] == color
    assert data['lineColor'] == G.RED


def test_alert_list():
    alert_list = G.AlertList(
        dashboardTags=['dummy tag'],
        description='dummy description',
        gridPos=dummy_grid_pos(),
        id=random.randint(1, 10),
        links=[dummy_data_link(), dummy_data_link()],
        nameFilter='dummy name filter',
        stateFilter=[G.ALERTLIST_STATE_ALERTING, G.ALERTLIST_STATE_OK],
        title='dummy title'
    )
    alert_list.to_json_data()


def test_SeriesOverride_exception_checks():
    with pytest.raises(TypeError):
        G.SeriesOverride()

    with pytest.raises(TypeError):
        G.SeriesOverride(123)

    with pytest.raises(TypeError):
        G.SeriesOverride('alias', bars=123)

    with pytest.raises(TypeError):
        G.SeriesOverride('alias', lines=123)

    with pytest.raises(ValueError):
        G.SeriesOverride('alias', yaxis=123)
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', yaxis='abc')

    with pytest.raises(TypeError):
        G.SeriesOverride('alias', fillBelowTo=123)

    with pytest.raises(ValueError):
        G.SeriesOverride('alias', fill="foo")
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', fill=123)
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', fill=-2)
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', zindex=5)

    with pytest.raises(TypeError):
        G.SeriesOverride('alias', dashes="foo")

    with pytest.raises(ValueError):
        G.SeriesOverride('alias', dashLength=-2)
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', dashLength=25)
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', spaceLength=-2)
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', spaceLength=25)

    with pytest.raises(ValueError):
        G.SeriesOverride('alias', dashLength="foo")
    with pytest.raises(ValueError):
        G.SeriesOverride('alias', spaceLength="foo")


def test_SeriesOverride():
    t = G.SeriesOverride('alias').to_json_data()

    assert t['alias'] == 'alias'
    assert t['bars'] is False
    assert t['lines'] is True
    assert t['yaxis'] == 1
    assert t['fill'] == 1
    assert t['color'] is None
    assert t['fillBelowTo'] is None
    assert t['dashes'] is False
    assert t['dashLength'] is None
    assert t['spaceLength'] is None
    assert t['zindex'] == 0

    t = G.SeriesOverride(
        'alias',
        bars=True,
        lines=False,
        yaxis=2,
        fill=7,
        color='#abc',
        fillBelowTo='other_alias',
        dashes=True,
        dashLength=12,
        spaceLength=17,
        zindex=-2,
    ).to_json_data()

    assert t['alias'] == 'alias'
    assert t['bars'] is True
    assert t['lines'] is False
    assert t['yaxis'] == 2
    assert t['fill'] == 7
    assert t['color'] == '#abc'
    assert t['fillBelowTo'] == 'other_alias'
    assert t['dashes'] is True
    assert t['dashLength'] == 12
    assert t['spaceLength'] == 17
    assert t['zindex'] == -2


def test_alert():
    alert = G.Alert(
        name='dummy name',
        message='dummy message',
        alertConditions=dummy_alert_condition(),
        alertRuleTags=dict(alert_rul_dummy_key='alert rul dummy value')
    )
    alert.to_json_data()


def test_alertgroup():
    name = "Example Alert Group"
    group = G.AlertGroup(
        name=name,
        rules=[
            G.AlertRulev8(
                title="My Important Alert!",
                triggers=[
                    (
                        G.Target(refId="A"),
                        G.AlertCondition(
                            evaluator=G.LowerThan(1),
                            operator=G.OP_OR,
                        ),
                    ),
                    (
                        G.Target(refId="B"),
                        G.AlertCondition(
                            evaluator=G.GreaterThan(1),
                            operator=G.OP_OR,
                        )
                    )
                ]
            )
        ]
    )

    output = group.to_json_data()

    assert output["name"] == name
    assert output["rules"][0]["grafana_alert"]["rule_group"] == name


def test_alertrulev8():
    title = "My Important Alert!"
    annotations = {"summary": "this alert fires when prod is down!!!"}
    labels = {"severity": "serious"}
    rule = G.AlertRulev8(
        title=title,
        triggers=[
            (
                G.Target(
                    refId="A",
                    datasource="Prometheus",
                ),
                G.AlertCondition(
                    evaluator=G.LowerThan(1),
                    operator=G.OP_OR,
                ),
            ),
            (
                G.Target(
                    refId="B",
                    datasource="Prometheus",
                ),
                G.AlertCondition(
                    evaluator=G.GreaterThan(1),
                    operator=G.OP_OR,
                )
            )
        ],
        annotations=annotations,
        labels=labels,
        evaluateFor="3m",
    )

    data = rule.to_json_data()
    assert data['grafana_alert']['title'] == title
    assert data['annotations'] == annotations
    assert data['labels'] == labels
    assert data['for'] == "3m"


def test_alertrule_invalid_triggers():
    # test that triggers is a list of [(Target, AlertCondition)]

    with pytest.raises(ValueError):
        G.AlertRulev8(
            title="Invalid rule",
            triggers=[
                G.Target(
                    refId="A",
                    datasource="Prometheus",
                ),
            ],
        )

    with pytest.raises(ValueError):
        G.AlertRulev8(
            title="Invalid rule",
            triggers=[
                (
                    "foo",
                    G.AlertCondition(
                        evaluator=G.GreaterThan(1),
                        operator=G.OP_OR,
                    )
                ),
            ],
        )

    with pytest.raises(ValueError):
        G.AlertRulev8(
            title="Invalid rule",
            triggers=[
                (
                    G.Target(
                        refId="A",
                        datasource="Prometheus",
                    ),
                    "bar"
                ),
            ],
        )


def test_alertrulev9():
    title = "My Important Alert!"
    annotations = {"summary": "this alert fires when prod is down!!!"}
    labels = {"severity": "serious"}
    condition = 'C'
    rule = G.AlertRulev9(
        title=title,
        uid='alert1',
        condition=condition,
        triggers=[
            G.Target(
                expr='query',
                refId='A',
                datasource='Prometheus',
            ),
            G.AlertExpression(
                refId='B',
                expressionType=G.EXP_TYPE_CLASSIC,
                expression='A',
                conditions=[
                    G.AlertCondition(
                        evaluator=G.GreaterThan(3),
                        operator=G.OP_AND,
                        reducerType=G.RTYPE_LAST
                    )
                ]
            ),
        ],
        annotations=annotations,
        labels=labels,
        evaluateFor="3m",
    )

    data = rule.to_json_data()
    assert data['annotations'] == annotations
    assert data['labels'] == labels
    assert data['for'] == "3m"
    assert data['grafana_alert']['title'] == title
    assert data['grafana_alert']['condition'] == condition


def test_alertexpression():
    refId = 'D'
    expression = 'C'
    expressionType = G.EXP_TYPE_REDUCE
    reduceFunction = G.EXP_REDUCER_FUNC_MAX
    reduceMode = G.EXP_REDUCER_FUNC_DROP_NN

    alert_exp = G.AlertExpression(
        refId=refId,
        expression=expression,
        expressionType=expressionType,
        reduceFunction=reduceFunction,
        reduceMode=reduceMode
    )

    data = alert_exp.to_json_data()

    assert data['refId'] == refId
    assert data['datasourceUid'] == '-100'
    assert data['model']['conditions'] == []
    assert data['model']['datasource'] == {
        'type': '__expr__',
        'uid': '-100'
    }
    assert data['model']['expression'] == expression
    assert data['model']['refId'] == refId
    assert data['model']['type'] == expressionType
    assert data['model']['reducer'] == reduceFunction
    assert data['model']['settings']['mode'] == reduceMode


def test_alertfilefasedfrovisioning():
    groups = [{
        'foo': 'bar'
    }]

    rules = G.AlertFileBasedProvisioning(
        groups=groups
    )

    data = rules.to_json_data()

    assert data['apiVersion'] == 1
    assert data['groups'] == groups


def test_alertCondition_useNewAlerts_default():
    alert_condition = G.AlertCondition(
        G.Target(refId="A"),
        G.Evaluator('a', 'b'),
        G.TimeRange('5', '6'),
        'd',
        'e'
    )
    data = alert_condition.to_json_data()
    assert data['query']['model'] is not None
    assert len(data['query']['params']) == 3


def test_alertCondition_useNewAlerts_true():
    alert_condition = G.AlertCondition(
        G.Target(refId="A"),
        G.Evaluator('a', 'b'),
        G.TimeRange('5', '6'),
        'd',
        'e',
        useNewAlerts=True
    )
    data = alert_condition.to_json_data()
    assert 'model' not in data['query']
    assert len(data['query']['params']) == 1


def test_worldmap():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    worldmap = G.Worldmap(data_source, targets, title, circleMaxSize=11)
    data = worldmap.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert data['circleMaxSize'] == 11


def test_stateTimeline():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    stateTimeline = G.StateTimeline(data_source, targets, title, rowHeight=0.7)
    data = stateTimeline.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert data['options']['rowHeight'] == 0.7


def test_timeseries():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    timeseries = G.TimeSeries(data_source, targets, title)
    data = timeseries.to_json_data()
    assert data['targets'] == targets
    assert data['datasource'] == data_source
    assert data['title'] == title
    assert data['fieldConfig']['overrides'] == []


def test_timeseries_with_overrides():
    data_source = 'dummy data source'
    targets = ['dummy_prom_query']
    title = 'dummy title'
    overrides = [
        {
            "matcher": {
                "id": "byName",
                "options": "min"
            },
            "properties": [
                {
                    "id": "custom.fillBelowTo",
                    "value": "min"
                },
                {
                    "id": "custom.lineWidth",
                    "value": 0
                }
            ]
        }
    ]
    timeseries = G.TimeSeries(
        dataSource=data_source,
        targets=targets,
        title=title,
        overrides=overrides,
    )
    data = timeseries.to_json_data()
    assert data["targets"] == targets
    assert data["datasource"] == data_source
    assert data["title"] == title
    assert data["fieldConfig"]["overrides"] == overrides


def test_news():
    title = "dummy title"
    feedUrl = "www.example.com"
    news = G.News(title=title, feedUrl=feedUrl)
    data = news.to_json_data()
    assert data["options"]["feedUrl"] == feedUrl
    assert data["title"] == title


def test_pieChartv2():
    data_source = "dummy data source"
    targets = ["dummy_prom_query"]
    title = "dummy title"
    pie = G.PieChartv2(data_source, targets, title)
    data = pie.to_json_data()
    assert data["targets"] == targets
    assert data["datasource"] == data_source
    assert data["title"] == title


def test_histogram():
    data_source = "dummy data source"
    targets = ["dummy_prom_query"]
    title = "dummy title"
    panel = G.Histogram(data_source, targets, title)
    data = panel.to_json_data()
    assert data["targets"] == targets
    assert data["datasource"] == data_source
    assert data["title"] == title
    assert 'bucketSize' not in data['options']

    bucketSize = 5
    panel = G.Histogram(data_source, targets, title, bucketSize=bucketSize)
    data = panel.to_json_data()
    assert data['options']['bucketSize'] == bucketSize


def test_ae3e_plotly():
    data_source = "dummy data source"
    targets = ["dummy_prom_query"]
    title = "dummy title"
    panel = G.Ae3ePlotly(data_source, targets, title)
    data = panel.to_json_data()
    assert data["targets"] == targets
    assert data["datasource"] == data_source
    assert data["title"] == title
    assert bool(data["options"]["configuration"]) is False
    assert bool(data["options"]["layout"]) is False

    config = {
        "displayModeBar": False
    }
    layout = {
        "font": {
            "color": "darkgrey"
        },
    }
    panel = G.Ae3ePlotly(data_source, targets, title, configuration=config, layout=layout)
    data = panel.to_json_data()
    assert data["options"]["configuration"] == config
    assert data["options"]["layout"] == layout


def test_barchart():
    data_source = "dummy data source"
    targets = ["dummy_prom_query"]
    title = "dummy title"
    panel = G.BarChart(data_source, targets, title)
    data = panel.to_json_data()
    assert data["targets"] == targets
    assert data["datasource"] == data_source
    assert data["title"] == title
    assert data["options"] is not None
    assert data["fieldConfig"] is not None
    assert data["options"]["orientation"] == 'auto'
    assert data["fieldConfig"]["defaults"]["color"]["mode"] == 'palette-classic'

    panel = G.BarChart(data_source, targets, title, orientation='horizontal', axisCenteredZero=True, showLegend=False)
    data = panel.to_json_data()
    assert data["options"]["orientation"] == 'horizontal'
    assert data["fieldConfig"]["defaults"]["custom"]["axisCenteredZero"]
    assert not data["options"]["legend"]["showLegend"]


def test_target_invalid():
    with pytest.raises(ValueError, match=r"target should have non-empty 'refId' attribute"):
        return G.AlertCondition(
            target=G.Target(),
            evaluator=G.Evaluator(
                type=G.EVAL_GT,
                params=42),
            timeRange=G.TimeRange(
                from_time='5m',
                to_time='now'
            ),
            operator=G.OP_AND,
            reducerType=G.RTYPE_AVG,
        )


def test_sql_target():
    t = G.Table(
        dataSource="some data source",
        targets=[
            G.SqlTarget(rawSql="SELECT * FROM example"),
        ],
        title="table title",
    )
    assert t.to_json_data()["targets"][0].rawQuery is True
    assert t.to_json_data()["targets"][0].rawSql == "SELECT * FROM example"


def _test_sql_target_with_source_files():
    t = G.Table(
        dataSource="some data source",
        targets=[
            G.SqlTarget(srcFilePath="grafanalib/tests/examples/sqltarget_example_files/example.sql"),
        ],
        title="table title",
    )
    assert t.to_json_data()["targets"][0].rawQuery is True
    assert t.to_json_data()["targets"][0].rawSql == "SELECT example, count(id)\nFROM test\nGROUP BY example;\n"
    print(t.to_json_data()["targets"][0])

    t = G.Table(
        dataSource="some data source",
        targets=[
            G.SqlTarget(srcFilePath="grafanalib/tests/examples/sqltarget_example_files/example_with_params.sql", sqlParams={
                "example": "example",
                "starting_date": "1970-01-01",
                "ending_date": "1971-01-01",
            },),
        ],
        title="table title",
    )
    assert t.to_json_data()["targets"][0].rawQuery is True
    assert t.to_json_data()["targets"][0].rawSql == "SELECT example\nFROM test\nWHERE example='example' AND example_date BETWEEN '1970-01-01' AND '1971-01-01';\n"
    print(t.to_json_data()["targets"][0])


def test_default_heatmap():
    h = G.Heatmap()

    assert h.to_json_data()["options"] == []


class TestDashboardLink():

    def test_validators(self):
        with pytest.raises(ValueError):
            G.DashboardLink(
                type='dashboard',
            )
        with pytest.raises(ValueError):
            G.DashboardLink(
                icon='not an icon'
            )

    def test_initialisation(self):
        dl = G.DashboardLink().to_json_data()
        assert dl['asDropdown'] is False
        assert dl['icon'] == 'external link'
        assert dl['includeVars'] is False
        assert dl['keepTime'] is True
        assert not dl['tags']
        assert dl['targetBlank'] is False
        assert dl['title'] == ""
        assert dl['tooltip'] == ""
        assert dl['type'] == 'dashboards'
        assert dl['url'] == ""

        url = 'https://grafana.com'
        dl = G.DashboardLink(
            uri=url,
            type='link'
        ).to_json_data()
        assert dl['url'] == url
        assert dl['type'] == 'link'
