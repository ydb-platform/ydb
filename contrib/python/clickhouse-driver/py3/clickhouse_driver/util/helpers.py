import ssl
from itertools import islice, tee
from urllib.parse import urlparse, parse_qs, unquote


def chunks(seq, n):
    # islice is MUCH slower than slice for lists and tuples.
    if isinstance(seq, (list, tuple)):
        i = 0
        item = seq[i:i+n]
        while item:
            yield list(item)
            i += n
            item = seq[i:i+n]

    else:
        it = iter(seq)
        item = list(islice(it, n))
        while item:
            yield item
            item = list(islice(it, n))


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def column_chunks(columns, n):
    for column in columns:
        if not isinstance(column, (list, tuple)):
            raise TypeError(
                'Unsupported column type: {}. list or tuple is expected.'
                .format(type(column))
            )

    # create chunk generator for every column
    g = [chunks(column, n) for column in columns]

    while True:
        # get next chunk for every column
        item = [next(column, []) for column in g]
        if not any(item):
            break
        yield item


# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError('String is not true/false: %r' % obj)
    return bool(obj)


def parse_url(url):
    """
    Parses url into host and kwargs suitable for further Client construction.
    Return host and kwargs.

    For example::

        clickhouse://[user:password]@localhost:9000/default
        clickhouses://[user:password]@localhost:9440/default

    Three URL schemes are supported:

        * clickhouse:// creates a normal TCP socket connection
        * clickhouses:// creates a SSL wrapped TCP socket connection

    Any additional querystring arguments will be passed along to
    the Connection class's initializer.
    """
    url = urlparse(url)

    settings = {}
    kwargs = {}

    host = url.hostname

    if url.port is not None:
        kwargs['port'] = url.port

    path = url.path.replace('/', '', 1)
    if path:
        kwargs['database'] = path

    if url.username is not None:
        kwargs['user'] = unquote(url.username)

    if url.password is not None:
        kwargs['password'] = unquote(url.password)

    if url.scheme == 'clickhouses':
        kwargs['secure'] = True

    compression_algs = {'lz4', 'lz4hc', 'zstd'}
    timeouts = {
        'connect_timeout',
        'send_receive_timeout',
        'sync_request_timeout'
    }

    for name, value in parse_qs(url.query).items():
        if not value or not len(value):
            continue

        value = value[0]

        if name == 'compression':
            value = value.lower()
            if value in compression_algs:
                kwargs[name] = value
            else:
                kwargs[name] = asbool(value)

        elif name == 'secure':
            kwargs[name] = asbool(value)

        elif name == 'use_numpy':
            settings[name] = asbool(value)

        elif name == 'round_robin':
            kwargs[name] = asbool(value)

        elif name == 'client_name':
            kwargs[name] = value

        elif name in timeouts:
            kwargs[name] = float(value)

        elif name == 'compress_block_size':
            kwargs[name] = int(value)

        elif name == 'settings_is_important':
            kwargs[name] = asbool(value)

        elif name == 'tcp_keepalive':
            try:
                kwargs[name] = asbool(value)
            except ValueError:
                parts = value.split(',')
                kwargs[name] = (
                    int(parts[0]), int(parts[1]), int(parts[2])
                )
        elif name == 'client_revision':
            kwargs[name] = int(value)

        # ssl
        elif name == 'verify':
            kwargs[name] = asbool(value)
        elif name == 'check_hostname':
            kwargs[name] = asbool(value)
        elif name == 'ssl_version':
            kwargs[name] = getattr(ssl, value)
        elif name in ['ca_certs', 'ciphers', 'keyfile', 'keypass', 'certfile',
                      'server_hostname']:
            kwargs[name] = value
        elif name == 'alt_hosts':
            kwargs['alt_hosts'] = value
        else:
            settings[name] = value

    if settings:
        kwargs['settings'] = settings

    return host, kwargs
