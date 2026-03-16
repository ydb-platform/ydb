"""Tests for known XML attacks"""

from pathlib import Path

from psutil import Process

from parsel import Selector

MiB_1 = 1024**2


def _load(attack: str) -> str:
    import yatest.common as yc
    folder_path = Path(yc.source_path(__file__)).parent
    file_path = folder_path / "xml_attacks" / f"{attack}.xml"
    return file_path.read_bytes().decode("utf-8")


# List of known attacks:
# https://github.com/tiran/defusedxml#python-xml-libraries
def test_billion_laughs() -> None:
    process = Process()
    memory_usage_before = process.memory_info().rss
    selector = Selector(text=_load("billion_laughs"))
    lolz = selector.css("lolz::text").get()
    memory_usage_after = process.memory_info().rss
    memory_change = memory_usage_after - memory_usage_before
    assert_message = f"Memory change: {memory_change}B"
    assert memory_change <= MiB_1, assert_message
    assert lolz == "&lol9;"
