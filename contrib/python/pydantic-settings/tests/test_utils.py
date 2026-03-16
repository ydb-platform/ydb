from pydantic_settings.utils import path_type_label


def test_path_type_label(tmp_path):
    result = path_type_label(tmp_path)
    assert result == 'directory'
