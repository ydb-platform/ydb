
def normalize_scheme(scheme=None, default='//'):
    if scheme is None:
        scheme = default
    elif scheme.endswith(':'):
        scheme = '%s//' % scheme
    elif '//' not in scheme:
        scheme = '%s://' % scheme
    return scheme


def normalize_port(port=None):
    if port is None:
        port = ''
    elif ':' in port:
        port = ':%s' % port.strip(':')
    elif port:
        port = ':%s' % port
    return port
