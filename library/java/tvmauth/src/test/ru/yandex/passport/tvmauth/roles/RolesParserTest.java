package ru.yandex.passport.tvmauth.roles;

import org.junit.Assert;
import org.junit.Test;

public class RolesParserTest {
    @Test(expected = java.lang.IllegalStateException.class)
    public void malformedJsonTest() {
        RolesParser.parse("kek");
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void emptyJsonTest() {
        RolesParser.parse("{}");
    }

    @Test
    public void malformedMetaRolesTest() {
        RolesParser.parse("{\"revision\": 42, \"born_date\": 1612791978}");
    }

    @Test
    public void noRolesTest() {
        String raw = "{\"revision\": \"GYYDEMJUGBQWC\", \"born_date\": 1612791978}";
        Roles roles = RolesParser.parse(raw);
        Assert.assertEquals(raw, roles.getRaw());
    }

    @Test
    public void noRolesWithKeysTest() {
        String raw = "{\"revision\": \"GYYDEMJUGBQWC\",\"born_date\": 1612791978,\"tvm\": {},\"user\": {}}";
        Roles roles = RolesParser.parse(raw);
        Assert.assertEquals(raw, roles.getRaw());
    }

    @Test(expected = NumberFormatException.class)
    public void malformedTvmidTest() {
        String raw = "{\"revision\": \"GYYDEMJUGBQWC\",\"born_date\": 1612791978,\"tvm\": {\"1120000000000493\":{}}}";
        Roles roles = RolesParser.parse(raw);
        Assert.assertEquals(raw, roles.getRaw());
    }

    @Test(expected = NumberFormatException.class)
    public void malformedUidTest() {
        String raw = "{\"revision\": \"GYYDEMJUGBQWC\",\"born_date\": 1612791978,\"user\": {\"asd\":{}}}";
        Roles roles = RolesParser.parse(raw);
        Assert.assertEquals(raw, roles.getRaw());
    }

    @Test
    public void commonTest() {
        String raw = "{\"revision\":\"GYYDEMJUGBQWC\",\"born_date\":1612791978," +
            "\"tvm\":{\"2012192\":{\"/group/system/system_on/abc/role/impersonator/\":[{\"scope\":\"/\"}]}}," +
            "\"user\":{\"1120000000000493\":{\"/group/system/system_on/abc/role/roles_manage/\":[]}}}";
        Roles roles = RolesParser.parse(raw);
        Assert.assertEquals(raw, roles.getRaw());
    }
}
