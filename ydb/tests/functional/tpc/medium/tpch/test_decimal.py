import test_s_float


class TestTpchS0_1Decimal_22_9(test_s_float.TestTpchS0_1):
    float_mode = 'decimal_ydb'
