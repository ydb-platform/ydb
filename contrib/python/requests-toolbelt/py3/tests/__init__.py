# -*- coding: utf-8 -*-
import betamax
import yatest.common as yc


def get_betamax(session):
    return betamax.Betamax(
        session,
        cassette_library_dir=yc.source_path("contrib/python/requests-toolbelt/py3/tests/cassettes/"))
