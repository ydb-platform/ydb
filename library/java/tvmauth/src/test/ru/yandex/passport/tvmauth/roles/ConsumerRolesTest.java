package ru.yandex.passport.tvmauth.roles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.passport.tvmauth.TicketStatus;
import ru.yandex.passport.tvmauth.Unittest;

public class ConsumerRolesTest {
    private Roles createRoles() {
        return RolesParser.parse("{\"revision\":\"GYYDEMJUGBQWC\",\"born_date\":1612791978," +
            "\"tvm\":{\"2012192\":{\"/group/system/system_on/abc/role/impersonator/\":[{\"scope\":\"/\"}]}}," +
            "\"user\":{\"1120000000000493\":{\"/group/system/system_on/abc/role/roles_manage/\":[]}}}");
    }

    @Test
    public void commonTest() {
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
        Assert.assertFalse(consumerRoles.hasRole("kek"));
        Assert.assertTrue(consumerRoles.hasRole("/group/system/system_on/abc/role/impersonator/"));

        Assert.assertNull(consumerRoles.getEntitiesForRole("kek"));
        Assert.assertEquals(
            new ArrayList<HashMap<String, String>>(Arrays.asList(
                new HashMap<String, String>() {{
                    put("scope", "/");
                }}
            )),
            consumerRoles.getEntitiesForRole("/group/system/system_on/abc/role/impersonator/")
        );
    }
}
