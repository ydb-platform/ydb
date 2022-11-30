package ru.yandex.passport.tvmauth.deprecated;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.passport.tvmauth.CheckedServiceTicket;
import ru.yandex.passport.tvmauth.TicketStatus;
import ru.yandex.passport.tvmauth.Unittest;
import ru.yandex.passport.tvmauth.Utils;
import ru.yandex.passport.tvmauth.exception.EmptyTvmKeysException;
import ru.yandex.passport.tvmauth.exception.MalformedTvmKeysException;
import ru.yandex.passport.tvmauth.exception.MalformedTvmSecretException;
import ru.yandex.passport.tvmauth.exception.NotAllowedException;

public class ServiceTest {
    static final String EMPTY_TVM_KEYS =
        "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38" +
        "NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWNt4X6nCItKIYeVcSK6XJU" +
        "bEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1z_TqZm4i1XkS7jMwZuBxBRw8DGdYei" +
        "0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAggCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtblu" +
        "JoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDorWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxY" +
        "KZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIcNrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7" +
        "KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbwW2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94EN" +
        "FIJaRBhACCpYBCpEBCAMQABqGATCBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRU" +
        "m-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABC" +
        "pYBCpEBCAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPgZni" +
        "NRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBhADCpcBCpIBCAU" +
        "QABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLGgzQ7Gar_uuxfUGKwqEQzopp" +
        "Draw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwg" +
        "YMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-hI55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4we" +
        "DMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuz" +
        "Fcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawU" +
        "lAwp5qp3QQtZyx_se97YIoLzuLr46UkLcLnkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_r" +
        "Nt17_ir8JlWvrtnCWsQd1MAnl5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyo" +
        "AbeSN2Yb1jygtZJQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyi" +
        "U11I8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3N9O2ve" +
        "mjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRvqpq2YZGSIgm_PTyD" +
        "I2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR4mcxnTKThPnRQpbuOlYAnriwia" +
        "sWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAE";
    static final String EXPIRED_SERVICE_TICKET =
        "3:serv:CBAQACIZCOUBEBwaCGJiOnNlc3MxGghiYjpzZXNzMg:IwfMNJYEqStY_SixwqJnyHOMCPR7-3HHk4uylB2oVRkthtezq-OOA7Qiz" +
        "Dvx7VABLs_iTlXuD1r5IjufNei_EiV145eaa3HIg4xCdJXCojMexf2UYJz8mF2b0YzFAy6_KWagU7xo13CyKAqzJuQf5MJcSUf0ecY9hVh3" +
        "6cJ51aw";
    static final String MALFORMED_TVM_KEYS =
        "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38";
    static final String MALFORMED_TVM_SECRET = "adcvxcv./-+";
    static final int NOT_OUR_ID = 27;
    static final int OUR_ID = 28;
    static final String SECRET = "GRMJrKnj4fOVnvOqe-WyD1";
    static final String SERVICE_TICKET_PROTOBUF = "CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My";
    static final int SRC_ID = 229;
    static final String UNSUPPORTED_VERSION_SERVICE_TICKET =
        "2:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXH" +
        "K1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5U" +
        "mDR6xfkJdnmMG94o8";
    static final String VALID_SERVICE_TICKET_1 =
        "3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXH" +
        "K1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5U" +
        "mDR6xfkJdnmMG94o8";
    static final String VALID_SERVICE_TICKET_3 =
        "3:serv:CBAQ__________9_IgUI5QEQHA:Sd6tmA1CNy2Nf7XevC3x7zr2DrGNRmcl-TxUsDtDW2xI3YXyCxBltWeg0-KtDlqyYuPOP5Jd_" +
        "-XXNA12KlOPnNzrz3jm-5z8uQl6CjCcrVHUHJ75pGC8r9UOlS8cOgeXQB5dYP-fOWyo5CNadlozx1S2meCIxncbQRV1kCBi4KU";
    static final String VALID_SERVICE_TICKET_ISSUER =
        "3:serv:CBAQ__________9_IgsI5QEQHCDr1MT4Ag:Gu66XJT_nKnIRJjFy1561wFhIqkJItcSTGftLo7Yvi7i5wIdV-QuKT_-IMPpgjxnn" +
        "Gbt1Dy3Ys2TEoeJAb0TdaCYG1uy3vpoLONmTx9AenN5dx1HHf46cypLK5D3OdiTjxvqI9uGmSIKrSdRxU8gprpu5QiBDPZqVCWhM60FVSY";

    @Test(expected = MalformedTvmKeysException.class)
    public void malformedTvmKeys() {
        ServiceContext.create(OUR_ID, SECRET, MALFORMED_TVM_KEYS);
    }

    @Test(expected = EmptyTvmKeysException.class)
    public void emptyTvmKeys() {
        ServiceContext.create(OUR_ID, SECRET, EMPTY_TVM_KEYS);
    }

    @Test(expected = MalformedTvmSecretException.class)
    public void mslformedSecret() {
        ServiceContext.create(OUR_ID, MALFORMED_TVM_SECRET, Unittest.getTvmknifePublicKeys());
    }

    @Test(expected = MalformedTvmKeysException.class)
    public void malformedTvmKeys_badBase64url() {
        ServiceContext.create(OUR_ID, SECRET, "adcvxcv./-+");
    }

    @Test(expected = MalformedTvmSecretException.class)
    public void checkingCtxTriesToSign() {
        ServiceContext context = ServiceContext.create(OUR_ID, Unittest.getTvmknifePublicKeys());
        context.signCgiParamsForTvm(String.valueOf(Long.MAX_VALUE), "13,28", "");
    }

    @Test
    public void validCheckingCtx() {
        ServiceContext context = ServiceContext.create(OUR_ID, Unittest.getTvmknifePublicKeys());
    }

    @Test(expected = EmptyTvmKeysException.class)
    public void signingCtxTriesToCheck() {
        ServiceContext context = ServiceContext.create(SECRET, OUR_ID);
        context.check("abcde");
    }

    @Test
    public void contextExceptionsTest_ValidSingingCtx() {
        ServiceContext context = ServiceContext.create(SECRET, OUR_ID);
    }

    @Test
    public void contextSignTest() {
        ServiceContext context = ServiceContext.create(OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        Assert.assertEquals(
            "NsPTYak4Cfk-4vgau5lab3W4GPiTtb2etuj3y4MDPrk",
            context.signCgiParamsForTvm(String.valueOf(Long.MAX_VALUE), "13,28", "")
        );
    }

    @Test
    public void ticket1Test() {
        ServiceContext context = ServiceContext.create(OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        CheckedServiceTicket checkedTicket = context.check(VALID_SERVICE_TICKET_1);
        Assert.assertEquals(TicketStatus.OK, checkedTicket.getStatus());
        Assert.assertEquals(SRC_ID, checkedTicket.getSrc());
        Assert.assertEquals(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess2;",
            checkedTicket.debugInfo());
        Assert.assertEquals(0, checkedTicket.getIssuerUid());
    }

    @Test
    public void ticket3Test() {
        ServiceContext context = ServiceContext.create(OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        CheckedServiceTicket checkedTicket = context.check(VALID_SERVICE_TICKET_3);
        Assert.assertEquals(TicketStatus.OK, checkedTicket.getStatus());
        Assert.assertEquals(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;",
            checkedTicket.debugInfo());
        Assert.assertEquals(0, checkedTicket.getIssuerUid());
    }

    @Test
    public void ticketIssuerTest() {
        ServiceContext context = ServiceContext.create(OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        CheckedServiceTicket checkedTicket = context.check(VALID_SERVICE_TICKET_ISSUER);
        Assert.assertEquals(TicketStatus.OK, checkedTicket.getStatus());
        Assert.assertEquals(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;issuer_uid=789654123;",
            checkedTicket.debugInfo());
        Assert.assertEquals(789654123, checkedTicket.getIssuerUid());
    }

    @Test
    public void ticketErrorsTest() {
        ServiceContext context = ServiceContext.create(NOT_OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        CheckedServiceTicket checkedTicket1 = context.check(VALID_SERVICE_TICKET_1);
        Assert.assertEquals(TicketStatus.INVALID_DST, checkedTicket1.getStatus());

        CheckedServiceTicket checkedTicket2 = context.check(UNSUPPORTED_VERSION_SERVICE_TICKET);
        Assert.assertEquals(TicketStatus.UNSUPPORTED_VERSION, checkedTicket2.getStatus());

        CheckedServiceTicket checkedTicket3 = context.check(EXPIRED_SERVICE_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket3.getStatus());
    }

    @Test(expected = NotAllowedException.class)
    public void ticketException2Test() {
        ServiceContext context = ServiceContext.create(OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        CheckedServiceTicket checkedTicket = context.check(EXPIRED_SERVICE_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket.getStatus());
        checkedTicket.getSrc();
    }

    @Test
    public void ticketNoExceptionTest() {
        ServiceContext context = ServiceContext.create(OUR_ID, SECRET, Unittest.getTvmknifePublicKeys());
        CheckedServiceTicket checkedTicket = context.check(EXPIRED_SERVICE_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket.getStatus());
        checkedTicket.booleanValue();
        checkedTicket.debugInfo();
        checkedTicket.getStatus();
    }

    @Test
    public void removeSignatureTest() {
        Assert.assertEquals(
            "1:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds",
            Utils.removeTicketSignature("1:serv:ASDkljbjhsdbfLJHABFJHBslfbsfjs:asdxcvbxcvniueliuweklsvds")
        );
    }

    @Test
    public void createTicketForTests() {
        CheckedServiceTicket s = Unittest.createServiceTicket(TicketStatus.OK, 42, 0);
        Assert.assertEquals(TicketStatus.OK, s.getStatus());
    }
}
