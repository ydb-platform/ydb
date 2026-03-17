def pytest_addoption(parser):
    parser.addoption(
        "--fast", action="store_true", default=False, help="run tests fast"
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow to run (deselect with '-m \"not slow\"')",
    )
