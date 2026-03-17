from socket import AF_INET

from .common import NLAKeyTransform


class ProbeFieldFilter(NLAKeyTransform):
    _nla_prefix = 'PROBE_'

    def finalize(self, context):
        if 'family' not in context:
            context['family'] = AF_INET
        if 'proto' not in context:
            context['proto'] = 1
        if 'port' not in context:
            context['port'] = 0
        if 'dst_len' not in context:
            context['dst_len'] = 32
        if 'kind' not in context:
            context['kind'] = 'ping'
        if 'num' not in context:
            context['num'] = 1
        if 'timeout' not in context:
            context['timeout'] = 1
