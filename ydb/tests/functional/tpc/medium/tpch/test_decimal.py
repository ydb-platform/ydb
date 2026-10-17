# ci: widen the increment graph so auto sharding exceeds one hour, 2026-10-05
import test_s_float


class TestTpchS0_1DecimalNative(test_s_float.TestTpchS0_1):
    float_mode = 'decimal'
