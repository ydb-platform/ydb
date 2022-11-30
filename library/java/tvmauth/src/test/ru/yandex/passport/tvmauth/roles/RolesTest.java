package ru.yandex.passport.tvmauth.roles;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.passport.tvmauth.BlackboxEnv;
import ru.yandex.passport.tvmauth.TicketStatus;
import ru.yandex.passport.tvmauth.Unittest;
import ru.yandex.passport.tvmauth.exception.NotAllowedException;

public class RolesTest {
    private String rawRoles = "{\"revision\":\"GYYDEMJUGBQWC\",\"born_date\":1612791978," +
            "\"tvm\":{\"2012192\":{\"/group/system/system_on/abc/role/impersonator/\":[{\"scope\":\"/\"}]}}," +
            "\"user\":{\"1120000000000493\":{\"/group/system/system_on/abc/role/roles_manage/\":[]}}}";

    private Roles createRoles() {
        return RolesParser.parse(rawRoles);
    }

    @Test
    public void rawTest() {
        Roles roles = createRoles();
        Assert.assertEquals(rawRoles, roles.getRaw());
    }

    @Test
    public void metaTest() {
        Meta meta = createRoles().getMeta();
        Assert.assertEquals("GYYDEMJUGBQWC", meta.getRevision());
        Assert.assertEquals(new Date(1612791978000L), meta.getBornTime());
    }

    @Test(expected = NotAllowedException.class)
    public void checkInvalidServiceTest() {
        Roles roles = createRoles();
        roles.getRolesForService(
            Unittest.createServiceTicket(TicketStatus.EXPIRED, 42)
        );
    }

    @Test
    public void checkServiceWithoutRolesTest() {
        Roles roles = createRoles();
        ConsumerRoles consumerRoles = roles.getRolesForService(
            Unittest.createServiceTicket(TicketStatus.OK, 42)
        );
        Assert.assertNull(consumerRoles);
    }

    @Test
    public void checkServiceWithRolesTest() {
        Roles roles = createRoles();
        ConsumerRoles consumerRoles = roles.getRolesForService(
            Unittest.createServiceTicket(TicketStatus.OK, 2012192)
        );
        Assert.assertNotNull(consumerRoles);
        Assert.assertEquals(
            "{\n" +
            "  \"/group/system/system_on/abc/role/impersonator/\": [\n" +
            "    {\n" +
            "      \"scope\": \"/\"\n" +
            "    }\n" +
            "  ]\n" +
            "}",
            consumerRoles.debugPrint()
        );
    }

    @Test(expected = NotAllowedException.class)
    public void checkInvalidUserTest() {
        Roles roles = createRoles();
        roles.getRolesForUser(
            Unittest.createUserTicket(TicketStatus.EXPIRED, 42, new String[]{}, new long[]{})
        );
    }

    @Test(expected = NotAllowedException.class)
    public void checkUserFromWrongEnvTest() {
        Roles roles = createRoles();
        roles.getRolesForUser(
            Unittest.createUserTicket(TicketStatus.OK, 42, new String[]{}, new long[]{}, BlackboxEnv.PROD)
        );
    }

    @Test
    public void checkUserWithoutRolesTest() {
        Roles roles = createRoles();
        ConsumerRoles consumerRoles = roles.getRolesForUser(
            Unittest.createUserTicket(TicketStatus.OK, 42, new String[]{}, new long[]{}, BlackboxEnv.PROD_YATEAM)
        );
        Assert.assertNull(consumerRoles);
    }

    @Test
    public void checkUserWithRolesTest() {
        Roles roles = createRoles();
        ConsumerRoles consumerRoles = roles.getRolesForUser(
            Unittest.createUserTicket(
                TicketStatus.OK, 1120000000000493L, new String[]{}, new long[]{}, BlackboxEnv.PROD_YATEAM)
        );
        Assert.assertNotNull(consumerRoles);
        Assert.assertEquals(
            "{\n" +
            "  \"/group/system/system_on/abc/role/roles_manage/\": []\n" +
            "}",
            consumerRoles.debugPrint()
        );
    }

    @Test
    public void checkSelectedUserWithRolesTest() {
        Roles roles = createRoles();
        ConsumerRoles consumerRoles = roles.getRolesForUser(
            Unittest.createUserTicket(
                TicketStatus.OK, 42, new String[]{}, new long[]{1120000000000493L}, BlackboxEnv.PROD_YATEAM),
                1120000000000493L
        );
        Assert.assertNotNull(consumerRoles);
        Assert.assertEquals(
            "{\n" +
            "  \"/group/system/system_on/abc/role/roles_manage/\": []\n" +
            "}",
            consumerRoles.debugPrint()
        );
    }

    @Test
    public void checkServiceRoleTest() {
        Roles roles = createRoles();

        Assert.assertFalse(roles.checkServiceRole(
            Unittest.createServiceTicket(TicketStatus.OK, 42),
            "/group/system/system_on/abc/role/impersonator/"
        ));
        Assert.assertFalse(roles.checkServiceRole(
            Unittest.createServiceTicket(TicketStatus.OK, 2012192),
            "kek"
        ));

        Assert.assertTrue(roles.checkServiceRole(
            Unittest.createServiceTicket(TicketStatus.OK, 2012192),
            "/group/system/system_on/abc/role/impersonator/"
        ));
    }

    @Test
    public void checkUserRoleTest() {
        Roles roles = createRoles();

        Assert.assertFalse(roles.checkUserRole(
            Unittest.createUserTicket(
                TicketStatus.OK, 42, new String[]{}, new long[]{}, BlackboxEnv.PROD_YATEAM),
            "/group/system/system_on/abc/role/roles_manage/"
        ));
        Assert.assertFalse(roles.checkUserRole(
            Unittest.createUserTicket(
                TicketStatus.OK, 1120000000000493L, new String[]{}, new long[]{}, BlackboxEnv.PROD_YATEAM),
            "kek"
        ));

        Assert.assertTrue(roles.checkUserRole(
            Unittest.createUserTicket(
                TicketStatus.OK, 1120000000000493L, new String[]{}, new long[]{}, BlackboxEnv.PROD_YATEAM),
            "/group/system/system_on/abc/role/roles_manage/"
        ));

        Assert.assertFalse(roles.checkUserRole(
            Unittest.createUserTicket(
                TicketStatus.OK, 1120000000000493L, new String[]{}, new long[]{42}, BlackboxEnv.PROD_YATEAM),
            "/group/system/system_on/abc/role/roles_manage/",
            42
        ));
        Assert.assertTrue(roles.checkUserRole(
            Unittest.createUserTicket(
                TicketStatus.OK, 42, new String[]{}, new long[]{1120000000000493L}, BlackboxEnv.PROD_YATEAM),
            "/group/system/system_on/abc/role/roles_manage/",
            1120000000000493L
        ));
    }
}
