import asyncio
import datetime
import itertools
import pathlib

import pytest

import aioftp


def test_parse_directory_response():
    s = 'foo "baz "" test nop" """""fdfs """'
    parsed = aioftp.Client.parse_directory_response(s)
    assert parsed == pathlib.PurePosixPath('baz " test nop')


@pytest.mark.asyncio
async def test_connection_del_future():
    loop = asyncio.new_event_loop()
    c = aioftp.Connection(loop=loop)
    c.foo = "bar"
    del c.future.foo


@pytest.mark.asyncio
async def test_connection_not_in_storage():
    loop = asyncio.new_event_loop()
    c = aioftp.Connection(loop=loop)
    with pytest.raises(AttributeError):
        getattr(c, "foo")


def test_available_connections_too_much_acquires():
    ac = aioftp.AvailableConnections(3)
    ac.acquire()
    ac.acquire()
    ac.acquire()
    with pytest.raises(ValueError):
        ac.acquire()


def test_available_connections_too_much_releases():
    ac = aioftp.AvailableConnections(3)
    ac.acquire()
    ac.release()
    with pytest.raises(ValueError):
        ac.release()


def test_parse_pasv_response():
    p = aioftp.Client.parse_pasv_response
    assert p("(192,168,1,0,1,0)") == ("192.168.1.0", 256)


def test_parse_epsv_response():
    p = aioftp.Client.parse_epsv_response
    assert p("some text (ha-ha) (|||665|) ((((666() (|fd667s).") == (None, 666)
    assert p("some text (ha-ha) (|||665|) (6666666).") == (None, 666)


def _c_locale_time(d, format="%b %d %H:%M"):
    with aioftp.common.setlocale("C"):
        return d.strftime(format)


def test_parse_ls_date_of_leap_year():
    def date_to_p(d):
        return d.strftime("%Y%m%d%H%M00")

    p = aioftp.Client.parse_ls_date
    # Leap year date to test
    d = datetime.datetime(year=2000, month=2, day=29)
    current_and_expected_dates = (
        # 2016 (leap)
        (
            datetime.datetime(year=2016, month=2, day=29),
            datetime.datetime(year=2016, month=2, day=29),
        ),
        # 2017
        (
            datetime.datetime(year=2017, month=2, day=28),
            datetime.datetime(year=2016, month=2, day=29),
        ),
        (
            datetime.datetime(year=2017, month=3, day=1),
            datetime.datetime(year=2016, month=2, day=29),
        ),
        # 2018
        (
            datetime.datetime(year=2018, month=2, day=28),
            datetime.datetime(year=2016, month=2, day=29),
        ),
        (
            datetime.datetime(year=2018, month=3, day=1),
            datetime.datetime(year=2020, month=2, day=29),
        ),
        # 2019
        (
            datetime.datetime(year=2019, month=2, day=28),
            datetime.datetime(year=2020, month=2, day=29),
        ),
        (
            datetime.datetime(year=2019, month=3, day=1),
            datetime.datetime(year=2020, month=2, day=29),
        ),
        # 2020 (leap)
        (
            datetime.datetime(year=2020, month=2, day=29),
            datetime.datetime(year=2020, month=2, day=29),
        ),
    )
    for now, expected in current_and_expected_dates:
        assert p(_c_locale_time(d), now=now) == date_to_p(expected)


def test_parse_ls_date_not_older_than_6_month_format():
    def date_to_p(d):
        return d.strftime("%Y%m%d%H%M00")

    p = aioftp.Client.parse_ls_date
    dates = (
        datetime.datetime(year=2002, month=1, day=1),
        datetime.datetime(year=2002, month=12, day=31),
    )
    dt = datetime.timedelta(seconds=15778476 // 2)
    deltas = (datetime.timedelta(), dt, -dt)
    for now, delta in itertools.product(dates, deltas):
        d = now + delta
        assert p(_c_locale_time(d), now=now) == date_to_p(d)


def test_parse_ls_date_older_than_6_month_format():
    def date_to_p(d):
        return d.strftime("%Y%m%d%H%M00")

    p = aioftp.Client.parse_ls_date
    dates = (
        datetime.datetime(year=2002, month=1, day=1),
        datetime.datetime(year=2002, month=12, day=31),
    )
    dt = datetime.timedelta(seconds=15778476, days=30)
    deltas = (dt, -dt)
    for now, delta in itertools.product(dates, deltas):
        d = now + delta
        if delta.total_seconds() > 0:
            expect = date_to_p(d.replace(year=d.year - 1))
        else:
            expect = date_to_p(d.replace(year=d.year + 1))
        assert p(_c_locale_time(d), now=now) == expect


def test_parse_ls_date_short():
    def date_to_p(d):
        return d.strftime("%Y%m%d%H%M00")

    p = aioftp.Client.parse_ls_date
    dates = (
        datetime.datetime(year=2002, month=1, day=1),
        datetime.datetime(year=2002, month=12, day=31),
    )
    for d in dates:
        s = _c_locale_time(d, format="%b %d  %Y")
        assert p(s) == date_to_p(d)


def test_parse_list_line_unix():
    lines = {
        "file": [
            "-rw-rw-r--  1 poh  poh   6595 Feb 27 04:14 history.rst",
            "lrwxrwxrwx  1 poh  poh      6 Mar 23 05:46 link-tmp.py -> tmp.py",
        ],
        "dir": [
            "drw-rw-r--  1 poh  poh   6595 Feb 27 04:14 history.rst",
            "drw-rw-r--  1 poh  poh   6595 Jan 03 2016  changes.rst",
            "drw-rw-r--  1 poh  poh   6595 Mar 10  1996 README.rst",
        ],
        "unknown": [
            "Erw-rw-r--  1 poh  poh   6595 Feb 27 04:14 history.rst",
        ],
    }
    p = aioftp.Client(encoding="utf-8").parse_list_line_unix
    for t, stack in lines.items():
        for line in stack:
            _, parsed = p(line.encode("utf-8"))
            assert parsed["type"] == t
    with pytest.raises(ValueError):
        p(b"-rw-rw-r--  1 poh  poh   6xx5 Feb 27 04:14 history.rst")
    with pytest.raises(ValueError):
        p(b"-rw-rw-r--  xx poh  poh   6595 Feb 27 04:14 history.rst")

    _, parsed = p(b"lrwxrwxrwx  1 poh  poh      6 Mar 23 05:46 link-tmp.py -> tmp.py")
    assert parsed["type"] == "file"
    assert parsed["link_dst"] == "tmp.py"

    _, parsed = p(b"lrwxrwxrwx  1 poh  poh      6 Mar 23 05:46 link-tmp.py -> /tmp.py")
    assert parsed["type"] == "file"
    assert parsed["link_dst"] == "/tmp.py"


@pytest.mark.parametrize("owner", ["s", "x", "-", "E"])
@pytest.mark.parametrize("group", ["s", "x", "-", "E"])
@pytest.mark.parametrize("others", ["t", "x", "-", "E"])
@pytest.mark.parametrize("read", ["r", "-"])
@pytest.mark.parametrize("write", ["w", "-"])
def test_parse_unix_mode(owner, group, others, read, write):
    s = f"{read}{write}{owner}{read}{write}{group}{read}{write}{others}"
    if "E" in {owner, group, others}:
        with pytest.raises(ValueError):
            aioftp.Client.parse_unix_mode(s)
    else:
        assert isinstance(aioftp.Client.parse_unix_mode(s), int)


def test_parse_list_line_failed():
    with pytest.raises(ValueError):
        aioftp.Client(encoding="utf-8").parse_list_line(b"what a hell?!")


def test_reprs_works():
    repr(aioftp.Throttle())
    repr(aioftp.Permission())
    repr(aioftp.User())


def test_throttle_reset():
    t = aioftp.Throttle(limit=1, reset_rate=1)
    t.append(b"-" * 3, 0)
    assert t._start == 0
    assert t._sum == 3
    t.append(b"-" * 3, 2)
    assert t._start == 2
    assert t._sum == 4


def test_permission_is_parent():
    p = aioftp.Permission("/foo/bar")
    assert p.is_parent(pathlib.PurePosixPath("/foo/bar/baz"))
    assert not p.is_parent(pathlib.PurePosixPath("/bar/baz"))


def test_server_mtime_build():
    now = datetime.datetime(year=2002, month=1, day=1).timestamp()
    past = datetime.datetime(year=2001, month=1, day=1).timestamp()
    b = aioftp.Server.build_list_mtime
    assert b(now, now) == "Jan  1 00:00"
    assert b(past, now) == "Jan  1  2001"


@pytest.mark.asyncio
async def test_get_paths_windows_traverse():
    base_path = pathlib.PureWindowsPath("C:\\ftp")
    user = aioftp.User()
    user.base_path = base_path
    connection = aioftp.Connection(current_directory=base_path, user=user)
    virtual_path = pathlib.PurePosixPath("/foo/C:\\windows")
    real_path, resolved_virtual_path = aioftp.Server.get_paths(
        connection,
        virtual_path,
    )
    assert real_path == pathlib.PureWindowsPath("C:/ftp/foo/C:/windows")
    assert resolved_virtual_path == pathlib.PurePosixPath("/foo/C:\\windows")
