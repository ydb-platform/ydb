#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from datetime import (
    date, datetime, timedelta
)

from flask import url_for
from flask_humanize.compat import text_type
import humanize


now = datetime.now()
today = date.today()
one_day = timedelta(days=1)
one_month = timedelta(days=31)
birthday = date(1987, 4, 21)


def b(string):
    return string.encode('utf-8') if isinstance(string, text_type) else string


class TestInit:

    def test_register_themself(self, app, h):
        assert app.extensions['humanize'] is h

    def test_register_before_request_callback(self, app, h):
        assert h._set_locale in app.before_request_funcs[None]

    def test_register_after_request_callback(self, app, h):
        assert h._unset_locale in app.after_request_funcs[None]

    def test_register_template_filter(self, app, h):
        assert h._humanize in app.jinja_env.filters.values()

    def test_defaults(self, config, h):
        assert config['HUMANIZE_DEFAULT_LOCALE'] == 'en'
        assert not config['HUMANIZE_USE_UTC']


@pytest.mark.usefixtures('app')
class TestHumanize:

    def test_naturalday(self, h):
        assert h._humanize(today, 'naturalday') == 'today'
        assert h._humanize(today + one_day, 'naturalday') == 'tomorrow'
        assert h._humanize(today - one_day, 'naturalday') == 'yesterday'

    def test_naturaltime(self, h):
        assert h._humanize(now) == 'now'
        assert h._humanize(now - one_month) == 'a month ago'

    def test_naturaltime_nomonths(self, h):
        assert h._humanize(now - one_month, months=False) == '31 days ago'

    def test_naturaldelta(self, h):
        assert h._humanize(one_day, 'naturaldelta') == 'a day'
        assert h._humanize(one_month, 'naturaldelta') == 'a month'

    def test_naturaldelta_nomonths(self, h):
        assert h._humanize(one_month, 'naturaldelta', months=False) == '31 days'

    def test_naturaldate(self, h):
        assert h._humanize(birthday, 'naturaldate') == 'Apr 21 1987'

    def test_invalid_fname(self, h):
        with pytest.raises(Exception) as exc:
            h._humanize(now, fname='foo')

        assert text_type(exc.value) == (
            "Humanize module does not contains function 'foo'"
        )

    def test_unsupported_arguments(self, h):
        with pytest.raises(ValueError) as exc:
            h._humanize(now, foo=42)

        assert text_type(exc.value) == (
            "An error occured during execution function 'naturaltime'"
        )


class TestL10N:

    def test_locale_selector(self, client, h):
        @h.localeselector
        def get_locale():
            return 'ru_RU'

        assert client.get(url_for('naturalday')).data == b(u'сегодня')
        assert client.get(url_for('naturaltime')).data == b(u'сейчас')
        assert client.get(url_for('naturaldelta')).data == b(u'7 дней')

    @pytest.mark.options(humanize_default_locale='ru_RU')
    def test_default_locale(self, client):
        assert client.get(url_for('naturaltime')).data == b(u'сейчас')

    def test_fallback_to_default_if_no_translations_for_locale(self, client, h):
        @h.localeselector
        def get_locale():
            return 'af'

        assert client.get(url_for('naturaltime')).data == b'now'


@pytest.mark.usefixtures('app')
class TestUTC:

    @pytest.mark.options(humanize_use_utc=True)
    def test_use_utc(self, h):
        assert humanize.time._now == datetime.utcnow

    @pytest.mark.options(humanize_use_utc=False)
    def test_dont_use_utc(self, h):
        assert humanize.time._now == datetime.now

    @pytest.mark.options(humanize_use_utc=True)
    def test_naturaltime_with_utc(self, h):
        assert h._humanize(datetime.utcnow()) == 'now'
