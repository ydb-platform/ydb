from s1 import test_s1


class TestTpchS0_1(test_s1.TestTpchS1):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
