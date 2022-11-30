package ru.yandex.passport.tvmauth;

import org.junit.Assert;
import org.junit.Test;

public class BlackboxTvmIdTest {
    @Test
    public void blackboxTvmIdTest() {
        Assert.assertEquals("222", BlackboxTvmId.PROD.toString());
        Assert.assertEquals("224", BlackboxTvmId.TEST.toString());
        Assert.assertEquals("223", BlackboxTvmId.PROD_YATEAM.toString());
        Assert.assertEquals("225", BlackboxTvmId.TEST_YATEAM.toString());
        Assert.assertEquals("226", BlackboxTvmId.STRESS.toString());
        Assert.assertEquals("239", BlackboxTvmId.MIMINO.toString());
    }
}
