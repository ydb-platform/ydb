package ru.yandex.passport.tvmauth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.passport.tvmauth.exception.NonRetriableException;

public class TvmToolTest {
    static final String TIME_REGEX = "\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d.\\d{6}Z";

    static String readAuthToken() throws IOException {
        return new String(Files.readAllBytes(Paths.get("./tvmtool.authtoken")), StandardCharsets.UTF_8);
    }

    static int readPort() throws IOException {
        return Integer.parseInt(
            new String(Files.readAllBytes(Paths.get("./tvmtool.port")), StandardCharsets.UTF_8)
        );
    }

    @Test
    public void createSettings() {
        TvmToolSettings s = TvmToolSettings.create("me");
        s.setPort(8080);
        s.setHostname("localhost");
        s.setAuthToken("qwerty");
        s.close();
        s.close();
    }

    @Test(expected = NonRetriableException.class)
    public void createClientWithBadTvmtool_Alias() throws IOException {
        TvmToolSettings s = TvmToolSettings.create("no one");
        s.setPort(readPort());
        s.setAuthToken(readAuthToken());
        NativeTvmClient c = new NativeTvmClient(s);
    }

    @Test(expected = NonRetriableException.class)
    public void createClientWithBadTvmtool_Port() throws IOException {
        TvmToolSettings s = TvmToolSettings.create("me");
        s.setPort(0);
        s.setAuthToken(readAuthToken());
        NativeTvmClient c = new NativeTvmClient(s);
    }

    @Test
    public void createClientWithOkTvmtool() throws IOException {
        TvmToolSettings s = TvmToolSettings.create("me");
        s.setPort(readPort());
        s.setAuthToken(readAuthToken());
        s.overrideBlackboxEnv(BlackboxEnv.PROD);

        NativeTvmClient c = new NativeTvmClient(s);
        Assert.assertEquals(
            ClientStatus.Code.OK,
            c.getStatus().getCode());
        c.close();

        String content = new String(Files.readAllBytes(Paths.get("./common.log")),
                                    StandardCharsets.US_ASCII);
        String exp = "DEBUG Meta info fetched from localhost:" + Integer.toString(readPort()) + "\n" +
                     "INFO Meta: self_tvm_id=1000502, bb_env=ProdYateam, idm_slug=some_slug_2," +
                     " dsts=[(2028120:2028120)]\n" +
                     "INFO Meta: override blackbox env: ProdYateam->Prod\n" +
                     "DEBUG Tickets fetched from tvmtool: XXXXXXXXXXX\n" +
                     "DEBUG Public keys fetched from tvmtool: XXXXXXXXXXX\n" +
                     "DEBUG Succeed to update roles with revision some_revision_2\n" +
                     "DEBUG Thread-worker started\n" +
                     "DEBUG Thread-worker stopped\n";
        content = content.replaceAll(TIME_REGEX, "XXXXXXXXXXX");
        content = content.replaceAll("\\(she:100501\\)\\(he:100500\\)", "(he:100500)(she:100501)");
        System.out.println(content);
        Assert.assertTrue(content.endsWith(exp));
    }
}
