from __future__ import division

from openapi_core.schema.servers import get_server_url


def get_spec_url(spec, index=0):
    servers = spec / 'servers'
    return get_server_url(servers / 0)
