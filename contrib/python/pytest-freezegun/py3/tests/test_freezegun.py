# -*- coding: utf-8 -*-
from datetime import date, datetime
import re
import time

import pytest


@pytest.fixture
def today():
    return datetime.now().date()


@pytest.mark.freeze_time('2017-05-20 15:42')
def test_freezing_time():
    assert date.today() == date(2017, 5, 20)


@pytest.mark.freeze_time('2017-05-20 15:42')
def test_freezing_time_in_fixture(today):
    assert today == date(2017, 5, 20)


def test_no_mark():
    now = datetime.now()

    assert datetime.now() > now


@pytest.mark.freeze_time
def test_move_to(freezer):
    freezer.move_to('2017-05-20')
    assert date.today() == date(2017, 5, 20)

    freezer.move_to('2017-05-21')
    assert date.today() == date(2017, 5, 21)


def test_fixture_no_mark(freezer):
    now = datetime.now()
    time.sleep(0.1)
    later = datetime.now()

    assert now == later
