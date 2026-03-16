# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Tests configuration
# :Author:    Lele Gaifax <lele@metapensiero.it>
# :License:   MIT License
# :Copyright: © 2016, 2017, 2018 Lele Gaifax
#

import io
import rapidjson as rj


def binary_streaming_dumps(o, **opts):
    stream = io.BytesIO()
    rj.dump(o, stream, **opts)
    return stream.getvalue().decode('utf-8')


def text_streaming_dumps(o, **opts):
    stream = io.StringIO()
    rj.dump(o, stream, **opts)
    return stream.getvalue()


def binary_streaming_encoder(o, **opts):
    stream = io.BytesIO()
    rj.Encoder(**opts)(o, stream=stream)
    return stream.getvalue().decode('utf-8')


def text_streaming_encoder(o, **opts):
    stream = io.StringIO()
    rj.Encoder(**opts)(o, stream=stream)
    return stream.getvalue()


def pytest_generate_tests(metafunc):
    if 'dumps' in metafunc.fixturenames and 'loads' in metafunc.fixturenames:
        metafunc.parametrize('dumps,loads', (
            ((rj.dumps, rj.loads),
             (lambda o,**opts: rj.Encoder(**opts)(o),
              lambda j,**opts: rj.Decoder(**opts)(j)))
        ), ids=('func[string]',
                'class[string]'))
    elif 'dumps' in metafunc.fixturenames:
        metafunc.parametrize('dumps', (
            rj.dumps,
            binary_streaming_dumps,
            text_streaming_dumps,
            lambda o,**opts: rj.Encoder(**opts)(o),
            binary_streaming_encoder,
            text_streaming_encoder,
        ), ids=('func[string]',
                'func[bytestream]',
                'func[textstream]',
                'class[string]',
                'class[binarystream]',
                'class[textstream]'))
    elif 'loads' in metafunc.fixturenames:
        metafunc.parametrize('loads', (
            rj.loads,
            lambda j,**opts: rj.load(io.BytesIO(j.encode('utf-8')
                                           if isinstance(j, str) else j), **opts),
            lambda j,**opts: rj.load(io.StringIO(j), **opts),
            lambda j,**opts: rj.Decoder(**opts)(j),
            lambda j,**opts: rj.Decoder(**opts)(io.BytesIO(j.encode('utf-8')
                                                      if isinstance(j, str) else j)),
            lambda j,**opts: rj.Decoder(**opts)(io.StringIO(j)),
        ), ids=('func[string]',
                'func[bytestream]',
                'func[textstream]',
                'class[string]',
                'class[bytestream]',
                'class[textstream]'))
