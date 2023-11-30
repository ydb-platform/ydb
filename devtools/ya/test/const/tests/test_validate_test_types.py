from test import const, explore


def get_test_types(name):
    return {suite.get_type() for suite in explore.SUITE_MAP.values() if suite.get_ci_type_name() == name}


def test_validate_style_tests():
    assert get_test_types("style") == set(const.STYLE_TEST_TYPES)


def test_validate_regular_tests():
    types = get_test_types("test")
    # TODO remove when DEVTOOLS-7066 is done
    types.remove('pytest_script')
    # Generated test types are listed below
    types.remove('coverage_extractor')
    types.remove('import_test')
    types.remove('validate_resource')
    types.remove('validate_data_sbr')
    # TODO remove when python2 will be removed from arcadia
    types.add('py2test')
    assert types == set(const.REGULAR_TEST_TYPES)
