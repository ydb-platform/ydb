from pytest import CaptureFixture


def test_mypy(capsys: CaptureFixture[str]) -> None:
    """
    Run mypy stub tests for pykdtree.
    This function checks for:
        - Type consistency in the stubs vs the definitions
        - Function / property signatures
        - Missing functions or properties in the stubs
    """
    from mypy import stubtest

    code = stubtest.test_stubs(stubtest.parse_options(["pykdtree.kdtree"]))
    captured = capsys.readouterr()

    assert code == 0, "Mypy stub test failed:\n" + captured.out
