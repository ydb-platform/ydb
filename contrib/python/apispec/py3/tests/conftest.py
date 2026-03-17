from collections import namedtuple

import pytest

from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin


def make_spec(openapi_version):
    ma_plugin = MarshmallowPlugin()
    spec = APISpec(
        title="Validation",
        version="0.1",
        openapi_version=openapi_version,
        plugins=(ma_plugin,),
    )
    return namedtuple("Spec", ("spec", "marshmallow_plugin", "openapi"))(
        spec, ma_plugin, ma_plugin.converter
    )


@pytest.fixture(params=("2.0", "3.0.0"))
def spec_fixture(request):
    return make_spec(request.param)


@pytest.fixture(params=("2.0", "3.0.0"))
def spec(request):
    return make_spec(request.param).spec


@pytest.fixture(params=("2.0", "3.0.0"))
def openapi(request):
    spec = make_spec(request.param)
    return spec.openapi
