from mammoth.options import read_options, _default_style_map
from mammoth.styles.parser import read_style_mapping
from .testing import assert_equal


def test_default_style_map_is_used_if_style_map_is_not_set():
    assert_equal(_default_style_map, read_options({}).value["style_map"])


def test_custom_style_mappings_are_prepended_to_default_style_mappings():
    style_map = read_options({
        "style_map": "p.SectionTitle => h2"
    }).value["style_map"]
    assert_equal(read_style_mapping("p.SectionTitle => h2").value, style_map[0])
    assert_equal(_default_style_map, style_map[1:])


def test_default_style_mappings_are_ignored_if_include_default_style_map_is_false():
    style_map = read_options({
        "style_map": "p.SectionTitle => h2",
        "include_default_style_map": False
    }).value["style_map"]
    assert_equal([read_style_mapping("p.SectionTitle => h2").value], style_map)


def test_lines_starting_with_hash_in_custom_style_map_are_ignored():
    style_map = read_options({
        "style_map": "#p.SectionTitle => h3\np.SectionTitle => h2",
        "include_default_style_map": False
    }).value["style_map"]
    assert_equal([read_style_mapping("p.SectionTitle => h2").value], style_map)
