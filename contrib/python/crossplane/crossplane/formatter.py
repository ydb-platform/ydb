# -*- coding: utf-8 -*-
from .errors import NgxParserBaseException
from .builder import build
from .parser import parse


def format(filename, indent=4, tabs=False):
    payload = parse(
        filename,
        comments=True,
        single=True,
        check_ctx=False,
        check_args=False
    )

    if payload['status'] != 'ok':
        e = payload['errors'][0]
        raise NgxParserBaseException(e['error'], e['file'], e['line'])

    parsed = payload['config'][0]['parsed']
    output = build(parsed, indent=indent, tabs=tabs)
    return output
