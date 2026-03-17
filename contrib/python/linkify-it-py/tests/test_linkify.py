from pathlib import Path

import pytest

from linkify_it import LinkifyIt

from .utils import read_fixture_file

FIXTURE_PATH = Path(__file__).parent / "fixtures"


def dummy(_):
    pass


@pytest.mark.parametrize(
    "number,line,expected",
    read_fixture_file(FIXTURE_PATH.joinpath("links.txt")),
)
def test_links(number, line, expected):
    linkifyit = LinkifyIt(options={"fuzzy_ip": True})

    linkifyit.normalize = dummy

    assert linkifyit.pretest(line) is True
    assert linkifyit.test("\n" + line + "\n") is True
    assert linkifyit.test(line) is True
    assert linkifyit.match(line)[0].url == expected


@pytest.mark.parametrize(
    "number,line,expected",
    read_fixture_file(FIXTURE_PATH.joinpath("not_links.txt")),
)
def test_not_links(number, line, expected):
    linkifyit = LinkifyIt()

    linkifyit.normalize = dummy

    assert linkifyit.test(line) is False
