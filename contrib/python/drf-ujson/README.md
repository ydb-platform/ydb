Django Rest Framework UJSON Renderer
==================

[![Build Status](https://travis-ci.org/gizmag/drf-ujson-renderer.png?branch=master)](https://travis-ci.org/gizmag/drf-ujson-renderer)

Django Rest Framework renderer using [ujson](https://github.com/esnme/ultrajson)

## Installation

`pip install drf_ujson`

You can then set the `UJSONRenderer` class as your default renderer in your `settings.py`

```python
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': (
        'drf_ujson.renderers.UJSONRenderer',
    ),
    ...
}
```

Also you can set the `UJSONParser` class as your default parser in your `settings.py`

```python
REST_FRAMEWORK = {
    'DEFAULT_PARSER_CLASSES': (
        'drf_ujson.parsers.UJSONParser',
    ),
    ...
}
```

## Benchmarks
This is on average 2.3x faster than the default JSON Serializer.

```python
import timeit

setup = '''
from proposals.models import Proposal
from proposals.serializers import ProposalSerializer
from rest_framework.renderers import JSONRenderer
from drf_ujson.renderers import UJSONRenderer

proposals = Proposal.objects.all()
serialized = ProposalSerializer(proposals, many=True).data
'''

stdlib_test = '''
JSONRenderer().render(serialized)
'''

ujson_test = '''
UJSONRenderer().render(serialized)
'''

stdlib_result = timeit.repeat(stdlib_test, setup=setup, number=1, repeat=10)
ujson_result = timeit.repeat(ujson_test, setup=setup, number=1, repeat=10)

print stdlib_result
print sum(stdlib_result) / 10
print ujson_result
print sum(ujson_result) / 10

# stdlib results
[
0.004502058029174805,
0.004289865493774414,
0.006896018981933594,
0.0048198699951171875,
0.004084110260009766,
0.007154941558837891,
0.003937959671020508,
0.004029035568237305,
0.004770040512084961,
0.004539966583251953
]
# avg
0.00490238666534

# ujson results
[
0.0016620159149169922,
0.001817941665649414,
0.0015261173248291016,
0.0040950775146484375,
0.0021469593048095703,
0.001798868179321289,
0.001569986343383789,
0.0019931793212890625,
0.0017120838165283203,
0.001814126968383789
]
# avg
0.00201363563538
```
