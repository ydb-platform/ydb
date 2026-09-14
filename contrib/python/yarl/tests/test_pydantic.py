from typing import TYPE_CHECKING

import pytest

from yarl import URL

if TYPE_CHECKING:
    import pydantic
else:
    pydantic = pytest.importorskip("pydantic")


class TstModel(pydantic.BaseModel):
    url: URL


def test_dump() -> None:
    url = URL("https://example.com")
    m = TstModel(url=url)
    dct = m.model_dump()
    assert dct == {"url": str(url)}
    assert isinstance(dct["url"], str)


def test_validate_valid() -> None:
    url = URL("https://example.com")
    dct = {"url": str(url)}
    m = TstModel.model_validate(dct)
    assert m == TstModel(url=url)
    assert isinstance(m.url, URL)


def test_validate_invalid() -> None:
    dct = {"url": 123}
    with pytest.raises(pydantic.ValidationError, match="url"):
        TstModel.model_validate(dct)


def test_get_schema() -> None:
    schema = TstModel.model_json_schema()
    assert schema == {
        "properties": {"url": {"format": "uri", "title": "Url", "type": "string"}},
        "required": ["url"],
        "title": "TstModel",
        "type": "object",
    }


def test_json_roundtrip_json() -> None:
    url = URL("https://example.com")
    m = TstModel(url=url)
    js = m.model_dump_json()
    m2 = TstModel.model_validate_json(js)
    assert m == m2
    js2 = m2.model_dump_json()
    assert js == js2


def test_fake_cover() -> None:
    # The test exists only for getting ocverage for __get_pydantic_core_schema__,
    # otherwise a call of python code back from rust is not measured
    # by coverage tool

    URL.__get_pydantic_core_schema__(URL, pydantic.GetCoreSchemaHandler())
