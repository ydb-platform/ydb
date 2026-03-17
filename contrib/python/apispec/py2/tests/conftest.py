# -*- coding: utf-8 -*-
from collections import namedtuple

import pytest

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin



@pytest.fixture(params=(
    ('2.0', True),
    ('2.0', False),
    ('3.0.0', True),
    ('3.0.0', False),
))
def spec(request):
    return APISpec(
        title='Validation',
        version='0.1',
        plugins=(
            # Test both plugin class and deprecated interface
            MarshmallowPlugin()
            if request.param[1] else
            'apispec.ext.marshmallow',
        ),
        openapi_version=request.param[0],
    )


@pytest.fixture(params=('2.0', '3.0.0'))
def spec_fixture(request):
    ma_plugin = MarshmallowPlugin()
    spec = APISpec(
        title='Validation',
        version='0.1',
        plugins=(ma_plugin, ),
        openapi_version=request.param,
    )
    return namedtuple(
        'Spec', ('spec', 'marshmallow_plugin', 'openapi'),
    )(
        spec, ma_plugin, ma_plugin.openapi,
    )
