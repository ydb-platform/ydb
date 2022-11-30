package ru.yandex.passport.tvmauth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.passport.tvmauth.roles.ConsumerRoles;
import ru.yandex.passport.tvmauth.roles.Roles;

public class ClientRolesTest {
    static String readAuthToken() throws IOException {
        return new String(Files.readAllBytes(Paths.get("./tvmtool.authtoken")), StandardCharsets.UTF_8);
    }

    static int readPort(String port) throws IOException {
        return Integer.parseInt(
            new String(Files.readAllBytes(Paths.get(port)), StandardCharsets.UTF_8)
        );
    }

    static NativeTvmClient createClientWithTirole(boolean checkSrc, boolean checkDefaultUid) throws IOException {
        TvmApiSettings s = new TvmApiSettings();
        s.setSelfTvmId(1000502);
        s.enableServiceTicketsFetchOptions("e5kL0vM3nP-nPf-388Hi6Q", new int[]{});
        s.fetchRolesForIdmSystemSlug("some_slug_2");
        s.setDiskCacheDir("./");
        s.enableServiceTicketChecking();
        s.enableUserTicketChecking(BlackboxEnv.PROD_YATEAM);
        s.setTvmPortForLocalhost(readPort("./tvmapi.port"));
        s.setTiroleConnectionParams("http://localhost", readPort("./tirole.port"), 1000001);

        s.shouldCheckSrc(checkSrc);
        s.shouldCheckDefaultUid(checkDefaultUid);

        return new NativeTvmClient(s);
    }

    static NativeTvmClient createClientWithTvmtool(boolean checkSrc, boolean checkDefaultUid) throws IOException {
        TvmToolSettings s = TvmToolSettings.create("me");
        s.setPort(readPort("./tvmtool.port"));
        s.setAuthToken(readAuthToken());

        s.shouldCheckSrc(checkSrc);
        s.shouldCheckDefaultUid(checkDefaultUid);

        return new NativeTvmClient(s);
    }

    static void checkServiceNoRoles(TvmClient clientWithAutoCheck, TvmClient clientWithoutAutoCheck) {
        // src=1000000000: tvmknife unittest service -s 1000000000 -d 1000502
        String stWithoutRoles = "3:serv:CBAQ__________9_IgoIgJTr3AMQtog9:" +
            "Sv3SKuDQ4p-2419PKqc1vo9EC128K6Iv7LKck5SyliJZn5gTAqMDAwb9aYWH" +
            "hf49HTR-Qmsjw4i_Lh-sNhge-JHWi5PTGFJm03CZHOCJG9Y0_G1pcgTfodtA" +
            "svDykMxLhiXGB4N84cGhVVqn1pFWz6SPmMeKUPulTt7qH1ifVtQ";

        CheckedServiceTicket checked = clientWithAutoCheck.checkServiceTicket(stWithoutRoles);
        Assert.assertEquals(TicketStatus.NO_ROLES, checked.getStatus());

        checked = clientWithoutAutoCheck.checkServiceTicket(stWithoutRoles);
        Assert.assertEquals(TicketStatus.OK, checked.getStatus());
        Assert.assertNull(clientWithoutAutoCheck.getRoles().getRolesForService(checked));
    }

    static void checkServiceHasRoles(TvmClient clientWithAutoCheck, TvmClient clientWithoutAutoCheck) {
        // src=1000000001: tvmknife unittest service -s 1000000001 -d 1000502
        String stWithRoles = "3:serv:CBAQ__________9_IgoIgZTr3AMQtog9:" +
            "EyPympmoLBM6jyiQLcK8ummNmL5IUAdTvKM1do8ppuEgY6yHfto3s_WAK" +
            "mP9Pf9EiNqPBe18HR7yKmVS7gvdFJY4gP4Ut51ejS-iBPlsbsApJOYTgo" +
            "dQPhkmjHVKIT0ub0pT3fWHQtapb8uimKpGcO6jCfopFQSVG04Ehj7a0jw";

        for (TvmClient cl : new TvmClient[]{clientWithAutoCheck, clientWithoutAutoCheck}) {
            CheckedServiceTicket checked = cl.checkServiceTicket(stWithRoles);
            Assert.assertEquals(TicketStatus.OK, checked.getStatus());

            Roles clientRoles = cl.getRoles();
            ConsumerRoles roles = clientRoles.getRolesForService(checked);
            Assert.assertEquals(
                "{\n" +
                "  \"/role/service/read/\": [\n" +
                "    {}\n" +
                "  ],\n" +
                "  \"/role/service/write/\": [\n" +
                "    {\n" +
                "      \"kek\": \"lol\",\n" +
                "      \"foo\": \"bar\"\n" +
                "    }\n" +
                "  ]\n" +
                "}",
                roles.debugPrint()
             );

            Assert.assertTrue(clientRoles.checkServiceRole(
                checked,
                "/role/service/read/"
            ));
            Assert.assertTrue(clientRoles.checkServiceRole(
                checked,
                "/role/service/write/"
            ));
            Assert.assertFalse(clientRoles.checkServiceRole(
                checked,
                "/role/foo/"
            ));
        }
    }

    static void checkUserNoRoles(TvmClient clientWithAutoCheck, TvmClient clientWithoutAutoCheck) {
        // default_uid=1000000000: tvmknife unittest user -d 1000000000 --env prod_yateam
        String utWithoutRoles = "3:user:CAwQ__________9_GhYKBgiAlOvcAxCAlOvcAyDShdjMBCgC:" +
                "LloRDlCZ4vd0IUTOj6MD1mxBPgGhS6EevnnWvHgyXmxc--2CVVkAtNKNZJqCJ6GtDY4nknEn" +
                "YmWvEu6-MInibD-Uk6saI1DN-2Y3C1Wdsz2SJCq2OYgaqQsrM5PagdyP9PLrftkuV_ZluS_F" +
                "UYebMXPzjJb0L0ALKByMPkCVWuk";

        CheckedUserTicket checked = clientWithAutoCheck.checkUserTicket(utWithoutRoles);
        Assert.assertEquals(TicketStatus.NO_ROLES, checked.getStatus());

        checked = clientWithoutAutoCheck.checkUserTicket(utWithoutRoles);
        Assert.assertEquals(TicketStatus.OK, checked.getStatus());
        Assert.assertNull(clientWithoutAutoCheck.getRoles().getRolesForUser(checked));
    }

    static void checkUserHasRoles(TvmClient clientWithAutoCheck, TvmClient clientWithoutAutoCheck) {
        //  default_uid=1120000000000001: tvmknife unittest user -d 1120000000000001 --env prod_yateam
        String utWithRoles = "3:user:CAwQ__________9_GhwKCQiBgJiRpdT-ARCBgJiRpdT-ASDShdjMBCgC:" +
            "SQV7Z9hDpZ_F62XGkSF6yr8PoZHezRp0ZxCINf_iAbT2rlEiO6j4UfLjzwn3EnRXkAOJxuAtTDCnHlrzd" +
            "h3JgSKK7gciwPstdRT5GGTixBoUU9kI_UlxEbfGBX1DfuDsw_GFQ2eCLu4Svq6jC3ynuqQ41D2RKopYL8Bx8PDZKQc";

        for (TvmClient cl : new TvmClient[]{clientWithAutoCheck, clientWithoutAutoCheck}) {
            CheckedUserTicket checked = cl.checkUserTicket(utWithRoles);
            Assert.assertEquals(TicketStatus.OK, checked.getStatus());

            Roles clientRoles = cl.getRoles();
            ConsumerRoles roles = clientRoles.getRolesForUser(checked);
            Assert.assertEquals(
                "{\n" +
                "  \"/role/user/write/\": [\n" +
                "    {}\n" +
                "  ],\n" +
                "  \"/role/user/read/\": [\n" +
                "    {\n" +
                "      \"kek\": \"lol\",\n" +
                "      \"foo\": \"bar\"\n" +
                "    }\n" +
                "  ]\n" +
                "}",
                roles.debugPrint()
            );

            Assert.assertTrue(clientRoles.checkUserRole(
                checked,
                "/role/user/read/"
            ));
            Assert.assertTrue(clientRoles.checkUserRole(
                checked,
                "/role/user/write/"
            ));
            Assert.assertFalse(clientRoles.checkUserRole(
                checked,
                "/role/foo/"
            ));
        }
    }

    @Test
    public void rolesFromTiroleCheckSrcNoRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTirole(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTirole(false, true);

        checkServiceNoRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }

    @Test
    public void rolesFromTiroleCheckSrcHasRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTirole(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTirole(false, true);

        checkServiceHasRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }

    @Test
    public void rolesFromTiroleCheckDefaultUidNoRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTirole(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTirole(true, false);

        checkUserNoRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }

    @Test
    public void rolesFromTiroleCheckDefaultUidHasRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTirole(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTirole(true, false);

        checkUserHasRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }


    @Test
    public void rolesFromTvmtoolCheckSrcNoRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTvmtool(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTvmtool(false, true);

        checkServiceNoRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }

    @Test
    public void rolesFromTvmtoolCheckSrcHasRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTvmtool(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTvmtool(false, true);

        checkServiceHasRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }

    @Test
    public void rolesFromTvmtoolCheckDefaultUidNoRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTvmtool(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTvmtool(true, false);

        checkUserNoRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }

    @Test
    public void rolesFromTvmtoolCheckDefaultUidHasRoles() throws IOException {
        NativeTvmClient clientWithAutoCheck = createClientWithTvmtool(true, true);
        NativeTvmClient clientWithoutAutoCheck = createClientWithTvmtool(true, false);

        checkUserHasRoles(clientWithAutoCheck, clientWithoutAutoCheck);

        clientWithAutoCheck.close();
        clientWithoutAutoCheck.close();
    }
}
