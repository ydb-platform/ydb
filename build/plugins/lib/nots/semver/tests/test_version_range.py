from build.plugins.lib.nots.semver import Version, Operator, VersionRange


def test_from_str():
    checklist = [
        (">= 1.2.3", VersionRange, Operator.GE),
        (">=1.2.3", VersionRange, Operator.GE),
        (">=    1.2.3", VersionRange, Operator.GE),
        (" >=    1.2.3 ", VersionRange, Operator.GE),
        ("= 1.2.3", VersionRange, Operator.EQ),
        ("=1.2.3", VersionRange, Operator.EQ),
        ("=    1.2.3", VersionRange, Operator.EQ),
        (" =    1.2.3 ", VersionRange, Operator.EQ),
        (" 1.2.3", VersionRange, Operator.EQ),
        ("1.2.3", VersionRange, Operator.EQ),
        ("    1.2.3", VersionRange, Operator.EQ),
        ("     1.2.3 ", VersionRange, Operator.EQ),
    ]

    for range_str, expected_class, expected_operator in checklist:
        range = VersionRange.from_str(range_str)

        assert isinstance(range, expected_class), f"unexpected class for '{range_str}': '{type(range)}'"
        assert range.operator == expected_operator, f"unexpected operator for '{range_str}': '{range.operator}'"


def test_from_str_error():
    error_template = "Unsupported version range: '{}'. Currently we only support ranges with stable versions and GE / EQ: '>= 1.2.3' / '= 1.2.3' / '1.2.3'"
    checklist = [
        (r"¯\_(ツ)_/¯", ValueError, error_template),
        ("<= 1.2.3", ValueError, error_template),
        ("<=1.2.3", ValueError, error_template),
        ("<=    1.2.3", ValueError, error_template),
        (" <=    1.2.3 ", ValueError, error_template),
        ("< 1.2.3", ValueError, error_template),
        ("<1.2.3", ValueError, error_template),
        ("<    1.2.3", ValueError, error_template),
        (" <    1.2.3 ", ValueError, error_template),
        ("> 1.2.3", ValueError, error_template),
        (">1.2.3", ValueError, error_template),
        (">    1.2.3", ValueError, error_template),
        (" >    1.2.3 ", ValueError, error_template),
        ("0.0.1-beta", ValueError, error_template),
    ]

    for range_str, expected_class, expected_msg_template in checklist:
        try:
            VersionRange.from_str(range_str)
        except Exception as exception:
            error = exception

        assert isinstance(error, expected_class), f"unexpected error class for '{range_str}': '{type(error)}'"
        assert str(error) == expected_msg_template.format(
            range_str
        ), f"unexpected error message for '{range_str}': '{error}'"


def test_init():
    checklist = [
        (Operator.GE, "1.2.3", Operator.GE, Version(1, 2, 3)),
        (Operator.GE, " 1.2.3 ", Operator.GE, Version(1, 2, 3)),
        (Operator.GE, "0.0.1", Operator.GE, Version(0, 0, 1)),
        (Operator.EQ, "1.2.3", Operator.EQ, Version(1, 2, 3)),
        (Operator.EQ, " 1.2.3 ", Operator.EQ, Version(1, 2, 3)),
        (Operator.EQ, "0.0.1", Operator.EQ, Version(0, 0, 1)),
        (None, "1.2.3", Operator.EQ, Version(1, 2, 3)),
        (None, " 1.2.3 ", Operator.EQ, Version(1, 2, 3)),
        (None, "0.0.1", Operator.EQ, Version(0, 0, 1)),
    ]

    for operator_provided, version_provided, expected_operator, expected_version in checklist:
        range = VersionRange(operator_provided, Version.from_str(version_provided))

        assert (
            range.operator == expected_operator
        ), f"unexpected operator for '{operator_provided}', '{version_provided}': '{range.operator}'"
        assert (
            range.version == expected_version
        ), f"unexpected result version for '{operator_provided}', '{version_provided}': '{range.version}'"


def test_is_satisfied():
    checklist = [
        (">= 1.2.3", "1.2.3", True),
        (">= 1.2.3", "1.2.4", True),
        (">= 1.2.3", "1.3.0", True),
        (">= 1.2.3", "2.0.0", True),
        (">= 1.2.3", "5.8.2", True),
        (">= 1.2.3", "1.2.2", False),
        (">= 1.2.3", "0.100.200", False),
        ("= 1.2.3", "1.2.3", True),
        ("1.2.3", "1.2.3", True),
        ("1.2.3", "1.2.2", False),
        ("1.2.3", "1.3.3", False),
        ("1.2.3", "2.2.3", False),
        ("12345.45634.456234", "12345.45634.456234", True),
        ("0.0.0", "0.0.0", True),
    ]

    for range_provided, version_provided, expected_result in checklist:
        version = Version.from_str(version_provided)
        range = VersionRange.from_str(range_provided)

        assert (
            range.is_satisfied_by(version) == expected_result
        ), f"Unexpected is_satisfied_by result for '{range_provided}', '{version_provided}': {(not expected_result)}"
