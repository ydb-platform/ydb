from __future__ import print_function

import sys  # noqa
import json

from tempfile import TemporaryFile

import pytest  # noqa

from library.python.monlib.metric_registry import MetricRegistry, HistogramType
from library.python.monlib.encoder import dump, dumps, TimePrecision, load, loads  # noqa


def test_common_labels(request):
    labels = {'my': 'label'}
    registry = MetricRegistry(labels)
    assert registry.common_labels == labels

    with pytest.raises(TypeError):
        MetricRegistry('foo')

    with pytest.raises(TypeError):
        MetricRegistry([])


def test_json_serialization(request):
    registry = MetricRegistry()
    labels = {'foo': 'gauge'}

    g = registry.gauge(labels)

    g.set(10.0)
    g.set(20)

    c = registry.counter({'foo': 'counter'})
    c.inc()

    r = registry.rate({'foo': 'rate'})
    r.add(10)

    out = dumps(registry, format='json', precision=TimePrecision.Seconds)
    expected = json.loads("""{
      "sensors":
        [
          {
            "kind":"RATE",
            "labels":
              {
                "foo":"rate"
              },
            "value":10
          },
          {
            "kind":"COUNTER",
            "labels":
              {
                "foo":"counter"
              },
            "value":1
          },
          {
            "kind":"GAUGE",
            "labels":
              {
                "foo":"gauge"
              },
            "value":20
          }
        ]
    }
    """)

    j = json.loads(out)
    assert j == expected


EXPECTED_EXPLICIT = json.loads("""
    {
    "sensors":
    [
      {
        "kind":"HIST",
        "labels":
          {
            "foo":"hist"
          },
        "hist":
          {
            "bounds":
              [
                2,
                10,
                500
              ],
            "buckets":
              [
                1,
                0,
                0
              ],
            "inf":1
          }
      }
    ]
    }
""")

EXPECTED_EXPONENTIAL = json.loads("""{
  "sensors":
    [
      {
        "kind":"HIST",
        "labels":
          {
            "foo":"hist"
          },
        "hist":
          {
            "bounds":
              [
                3,
                6,
                12,
                24,
                48
              ],
            "buckets":
              [
                1,
                0,
                0,
                0,
                0
              ],
            "inf":1
          }
      }
    ]
}
""")

EXPECTED_LINEAR = json.loads("""
    { "sensors":
        [
          {
            "kind":"HIST",
            "labels":
              {
                "foo":"hist"
              },
            "hist":
              {
                "bounds":
                  [
                    1
                  ],
                "buckets":
                  [
                    1
                  ],
                "inf":1
              }
          }
        ]
}""")


@pytest.mark.parametrize('type,args,expected', [
    (HistogramType.Linear, dict(bucket_count=2, start_value=1, bucket_width=1), EXPECTED_LINEAR),
    (HistogramType.Explicit, dict(buckets=[2, 10, 500]), EXPECTED_EXPLICIT),
    (HistogramType.Exponential, dict(bucket_count=6, base=2, scale=3), EXPECTED_EXPONENTIAL),
])
@pytest.mark.parametrize('rate', [True, False])
def test_histograms(request, type, args, expected, rate):
    registry = MetricRegistry()
    labels = {'foo': 'hist'}

    h = registry.histogram_counter(labels, type, **args) if not rate else registry.histogram_rate(labels, type, **args)
    h.collect(1)
    h.collect(1000)

    s = dumps(registry, format='json')

    if rate:
        expected['sensors'][0]['kind'] = u'HIST_RATE'
    else:
        expected['sensors'][0]['kind'] = u'HIST'

    assert json.loads(s) == expected


@pytest.mark.parametrize('fmt', ['json', 'spack'])
def test_stream_load(request, fmt):
    expected = json.loads("""{"sensors":[{"kind":"GAUGE","labels":{"foo":"gauge"},"value":42}]}""")
    registry = MetricRegistry()
    labels = {'foo': 'gauge'}

    g = registry.gauge(labels)
    g.set(42)

    with TemporaryFile() as f:
        dump(registry, f, format=fmt)
        f.flush()
        f.seek(0, 0)
        s = load(f, from_format=fmt, to_format='json')
        assert json.loads(s) == expected


@pytest.mark.parametrize('fmt', ['json', 'spack'])
def test_stream_loads(request, fmt):
    expected = json.loads("""{"sensors":[{"kind":"GAUGE","labels":{"foo":"gauge"},"value":42}]}""")
    registry = MetricRegistry()
    labels = {'foo': 'gauge'}

    g = registry.gauge(labels)
    g.set(42)

    s = dumps(registry, format=fmt)
    j = loads(s, from_format=fmt, to_format='json')
    assert json.loads(j) == expected


@pytest.mark.parametrize('fmt', ['json', 'spack'])
def test_utf(request, fmt):
    expected = json.loads(u"""{"sensors":[{"kind":"GAUGE","labels":{"foo":"gaugeह", "bàr":"Münich"},"value":42}]}""")
    registry = MetricRegistry()
    labels = {'foo': u'gaugeह', u'bàr': u'Münich'}

    g = registry.gauge(labels)
    g.set(42)

    s = dumps(registry, format=fmt)
    j = loads(s, from_format=fmt, to_format='json')
    assert json.loads(j) == expected


def test_gauge_sensors():
    registry = MetricRegistry()
    g = registry.gauge({'a': 'b'})
    ig = registry.int_gauge({'c': 'd'})

    g.set(2)
    assert g.add(3.5) == 5.5
    assert g.get() == 5.5

    ig.set(2)
    assert ig.inc() == 3
    assert ig.dec() == 2
    assert ig.add(3) == 5
    assert ig.get() == 5


UNISTAT_DATA = """[
    ["signal1_max", 10],
    ["signal2_hgram", [[0, 100], [50, 200], [200, 300]]],
    ["prj=some-project;signal3_summ", 3],
    ["signal4_summ", 5]
]"""


EXPECTED = json.loads("""
{
  "sensors": [
    {
      "kind": "GAUGE",
      "labels": {
        "sensor": "signal1_max"
      },
      "value": 10
    },
    {
      "hist": {
        "buckets": [
          0,
          100,
          200
        ],
        "bounds": [
          0,
          50,
          200
        ],
        "inf": 300
      },
      "kind": "HIST_RATE",
      "labels": {
        "sensor": "signal2_hgram"
      }
    },
    {
      "kind": "RATE",
      "labels": {
        "sensor": "signal3_summ",
        "prj": "some-project"
      },
      "value": 3
    },
    {
      "kind": "RATE",
      "labels": {
        "sensor": "signal4_summ"
      },
      "value": 5
    }
  ]
}""")


def test_unistat_conversion(request):
    j = loads(UNISTAT_DATA, from_format='unistat', to_format='json')
    assert json.loads(j) == EXPECTED
