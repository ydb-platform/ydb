package ru.yandex.passport.tvmauth;

public enum BlackboxTvmId {
    PROD(222),
    TEST(224),
    PROD_YATEAM(223),
    TEST_YATEAM(225),
    STRESS(226),
    MIMINO(239);

    private final int dstTvmId;

    BlackboxTvmId(int dstTvmId) {
        this.dstTvmId = dstTvmId;
    }

    @Override
    public String toString() {
        return String.valueOf(dstTvmId);
    }

    public int getDstTvmId() {
        return dstTvmId;
    }
}
