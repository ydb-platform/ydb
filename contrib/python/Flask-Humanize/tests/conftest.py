#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from datetime import (
    datetime, timedelta
)

from flask import (
    Flask, render_template_string
)
from flask_humanize import Humanize


@pytest.fixture
def app():
    app = Flask(__name__)
    app.debug = True
    app.testing = True

    @app.context_processor
    def expose_current_timestamp():
        return {
            'now': datetime.now(),
        }

    @app.route('/naturalday')
    def naturalday():
        return render_template_string("{{ now|humanize('naturalday') }}")

    @app.route('/naturaltime')
    def naturaltime():
        return render_template_string("{{ now|humanize('naturaltime') }}")

    @app.route('/naturaldelta')
    def naturaldelta():
        return render_template_string("{{ value|humanize('naturaldelta') }}",
                                      value=timedelta(days=7))

    return app


@pytest.fixture(autouse=True)
def h(request):
    if 'app' not in request.fixturenames:
        return

    app = request.getfixturevalue('app')
    return Humanize(app)
