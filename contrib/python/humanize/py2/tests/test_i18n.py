# -*- coding: utf-8 -*-
import datetime as dt

import humanize


def test_i18n():
    three_seconds = dt.timedelta(seconds=3)

    assert humanize.naturaltime(three_seconds) == "3 seconds ago"

    humanize.i18n.activate("ru_RU")
    assert humanize.naturaltime(three_seconds) == "3 секунды назад"

    humanize.i18n.deactivate()
    assert humanize.naturaltime(three_seconds) == "3 seconds ago"
