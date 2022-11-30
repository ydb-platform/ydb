package ru.yandex.passport.tvmauth;

import org.junit.Assert;
import org.junit.Test;

public class VersionTest {
    @Test
    public void versionGetTest() {
        Assert.assertEquals(true, Version.get().startsWith("java_"));
        Assert.assertTrue(Version.get().length() >= 10);
    }
}
