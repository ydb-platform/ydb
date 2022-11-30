package ru.yandex.passport.tvmauth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class DynamicClientTest {
    static final String SRV_TICKET =
        "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8" +
        "B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU";
    static final String PROD_TICKET =
        "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_BEUv1x9CALU7do8irDlDYV" +
        "eVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTyVetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490C" +
        "JFw";
    static final String TEST_TICKET =
        "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEhfDDeCLoVA-sJesxMl2pGW" +
        "4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgtRoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKU" +
        "X54";

    @Test
    public void createClient_full() throws IOException, InterruptedException {
        DynamicClient c = factory();
        Thread.sleep(1000);
        Assert.assertEquals(ClientStatus.Code.OK, c.getStatus().getCode());
        Assert.assertEquals("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", c.getServiceTicketFor("dest"));
        Assert.assertEquals("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", c.getServiceTicketFor(19));
        Assert.assertEquals(TicketStatus.OK, c.checkServiceTicket(SRV_TICKET).getStatus());
        Assert.assertEquals(TicketStatus.OK, c.checkUserTicket(TEST_TICKET).getStatus());
        Assert.assertEquals(TicketStatus.INVALID_BLACKBOX_ENV, c.checkUserTicket(PROD_TICKET).getStatus());

        Assert.assertEquals(Optional.of("3:serv:CBAQ__________9_IgYIKhCUkQY:CX"),
                            c.getOptionalServiceTicketFor(19));
        Assert.assertEquals(Optional.empty(), c.getOptionalServiceTicketFor(100500));
        c.addDsts(new int[]{100500});
        Assert.assertEquals(Optional.empty(), c.getOptionalServiceTicketFor(100500));
        c.close();

        String content = new String(Files.readAllBytes(Paths.get("./common.log")),
                                    StandardCharsets.US_ASCII);
        String exp = "INFO File './service_tickets' was successfully read\n" +
                     "INFO Got 1 service ticket(s) from disk\n" +
                     "INFO Cache was updated with 1 service ticket(s): 2050-01-01T00:00:00.000000Z\n" +
                     "INFO File './public_keys' was successfully read\n" +
                     "INFO Cache was updated with public keys: 2050-01-01T00:00:00.000000Z\n" +
                     "DEBUG File './retry_settings' does not exist\n" +
                     "DEBUG Thread-worker started\n" +
                     "DEBUG Adding dst: got task #1 with 1 dsts\n" +
                     "DEBUG Thread-worker stopped\n";
        System.out.println(content);
        Assert.assertTrue(content.endsWith(exp));
    }

    private static DynamicClient factory() throws IOException {
        TvmApiSettings s = TvmApiSettings.create();
        s.setSelfTvmId(100500);
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put("dest", 19);
        s.enableServiceTicketsFetchOptions("qwe", map);
        s.enableServiceTicketChecking();
        s.enableUserTicketChecking(BlackboxEnv.TEST);
        Files.copy(Paths.get(ru.yandex.devtools.test.Paths.getSourcePath(
                       "/library/cpp/tvmauth/client/ut/files/public_keys"
                   )),
                   Paths.get("./public_keys"),
                   REPLACE_EXISTING);
        Files.copy(Paths.get(ru.yandex.devtools.test.Paths.getSourcePath(
                        "/library/cpp/tvmauth/client/ut/files/service_tickets"
                   )),
                   Paths.get("./service_tickets"),
                   REPLACE_EXISTING);
        s.setDiskCacheDir("./");
        return new DynamicClient(s);
    }
}
