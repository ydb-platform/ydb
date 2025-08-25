import test_s1


class TestTpchS0_1(test_s1.TestTpchS1):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1


class TestTpchS0_2(test_s1.TestTpchS1):
    tables_size: dict[str, int] = {
        'lineitem': 1199969,
    }
    scale: float = 0.2


class TestTpchS0_5(test_s1.TestTpchS1):
    tables_size: dict[str, int] = {
        'lineitem': 2999671,
    }
    scale: float = 0.5
