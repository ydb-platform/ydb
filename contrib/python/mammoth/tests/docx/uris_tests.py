from mammoth.docx.uris import uri_to_zip_entry_name
from ..testing import assert_equal


def test_when_path_does_not_have_leading_slash_then_path_is_resolved_relative_to_base():
    assert_equal(
        "one/two/three/four",
        uri_to_zip_entry_name("one/two", "three/four"),
    )


def test_when_path_has_leading_slash_then_base_is_ignored():
    assert_equal(
        "three/four",
        uri_to_zip_entry_name("one/two", "/three/four"),
    )
