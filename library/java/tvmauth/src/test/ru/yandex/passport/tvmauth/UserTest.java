package ru.yandex.passport.tvmauth.deprecated;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.passport.tvmauth.BlackboxEnv;
import ru.yandex.passport.tvmauth.CheckedUserTicket;
import ru.yandex.passport.tvmauth.TicketStatus;
import ru.yandex.passport.tvmauth.Unittest;
import ru.yandex.passport.tvmauth.Utils;
import ru.yandex.passport.tvmauth.exception.EmptyTvmKeysException;
import ru.yandex.passport.tvmauth.exception.MalformedTvmKeysException;
import ru.yandex.passport.tvmauth.exception.NotAllowedException;

public class UserTest {
    static final String EMPTY_TVM_KEYS =
        "1:EpUBCpIBCAYQABqHATCBhAKBgQCoZkFGm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFBIRuTkYxhs4vUYnWjZrKRAd5bp6_py0csmFmpl_" +
        "5Yh0b-2pdo_E5PNP7LGRzKyKSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr-TPu34DnJq3yv2a6dqiKL9zSCakQY";
    static final String EXPIRED_USER_TICKET =
        "3:user:CA0QABokCgMIyAMKAgh7EMgDGghiYjpzZXNzMRoIYmI6c2VzczIgEigB:D0CmYVwWg91LDYejjeQ2UP8AeiA_mr1q1CUD_lfJ9zQ" +
        "SEYEOYGDTafg4Um2rwOOvQnsD1JHM4zHyMUJ6Jtp9GAm5pmhbXBBZqaCcJpyxLTEC8a81MhJFCCJRvu_G1FiAgRgB25gI3HIbkvHFUEqAIC" +
        "_nANy7NFQnbKk2S-EQPGY";
    static final String MALFORMED_TVM_KEYS =
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
        "sWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAEEpUBCpIBCAYQABqHATCBhAKBgQCoZkFGm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFB" +
        "IRuTkYxhs4vUYnWjZrKRAd5bp6_py0csmFmpl_5Yh0b-2pdo_E5PNP7LGRzKyKSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr" +
        "-TPu34DnJq3yv2a6dqiKL9zSCakQYSlQEKkgEIEBAAGocBMIGEAoGBALhrihbf3EpjDQS2sCQHazoFgN0nBbE9eesnnFTfzQELXb2gnJU9e" +
        "nmV_aDqaHKjgtLIPpCgn40lHrn5k6mvH5OdedyI6cCzE-N-GFp3nAq0NDJyMe0fhtIRD__CbT0ulcvkeow65ubXWfw6dBC2gR_34rdMe_L_" +
        "TGRLMWjDULbNIJ";
    static final String MALFORMED_USER_TICKET =
        "3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXlFrhMW-R4q8mKfXJXCd-RBVBgUQzCOR1Dx2FiOyU-B" +
        "xUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tzrfNUr88otBGAziHASJWgyVDkhyQ3p7YbN38qpb0vGQrYNxlk4" +
        "e2I";
    static final String UNSUPPORTED_VERSION_USER_TICKET =
        "2:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:KJFv5EcXn9krYk19LCvlFrhMW-R4q8mK" +
        "fXJXCd-RBVBgUQzCOR1Dx2FiOyU-BxUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tzrfNUr88otBGAziHASJWg" +
        "yVDkhyQ3p7YbN38qpb0vGQrYNxlk4e2I";
    static final String USER_TICKET_PROTOBUF = "CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE";
    static final String VALID_USER_TICKET_1 =
        "3:user:CA0Q__________9_GjQKAgh7CgkIoN71iqr3gAIQoN71iqr3gAIaCGJiOnNlc3MxGghiYjpzZXNzMiDShdjMBCgB:KJhfloe9EFf" +
        "x1P3fs6JWXipyQuQRuxSM_PCHxdpeLvCcxMZcLU_PG91aRxY2_ieNH5cMJn88JitIIsI6TMeTOKW1wLMoEJHrcSPkE6-EZh9D4agY1wKCEq" +
        "7ArCLjR5Jg38RgMt0b8sUhNumeHphsgdzGxWqRyOOkoI2Qp_y1Z3Y";
    static final String VALID_USER_TICKET_2 =
        "3:user:CA0Q__________9_GhAKAwjIAwoCCHsQyAMgEigB:KRibGYTJUA2ns0Fn7VYqeMZ1-GdscB1o9pRzELyr7QJrJsfsE8Y_HoVvB8N" +
        "pr-oalv6AXOpagSc8HpZjAQz8zKMAVE_tI0tL-9DEsHirpawEbpy7OWV7-k18o1m-RaDaKeTlIB45KHbBul1-9aeKkortBfbbXtz_Qy9r_m" +
        "fFPiQ";

    @Test
    public void contextTest() {
        UserContext.create(BlackboxEnv.PROD, Unittest.getTvmknifePublicKeys());
    }

    @Test(expected = EmptyTvmKeysException.class)
    public void contextExceptions1Test() {
        UserContext.create(BlackboxEnv.PROD, EMPTY_TVM_KEYS);
    }

    @Test(expected = MalformedTvmKeysException.class)
    public void contextExceptions2Test() {
        UserContext.create(BlackboxEnv.PROD, MALFORMED_TVM_KEYS);
    }

    @Test
    public void ticket1Test() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(VALID_USER_TICKET_1);
        Assert.assertEquals(TicketStatus.OK, checkedTicket.getStatus());
        Assert.assertArrayEquals(new long[]{123, 1130000012898080L}, checkedTicket.getUids());
        Assert.assertEquals(1130000012898080L, checkedTicket.getDefaultUid());
        Assert.assertArrayEquals(new String[]{"bb:sess1", "bb:sess2"}, checkedTicket.getScopes());
        Assert.assertTrue(checkedTicket.hasScope("bb:sess1"));
        Assert.assertTrue(checkedTicket.hasScope("bb:sess2"));
        Assert.assertFalse(checkedTicket.hasScope("bb:sess3"));
        Assert.assertEquals(
            "ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess2;" +
            "default_uid=1130000012898080;uid=123;uid=1130000012898080;env=Test;",
            checkedTicket.debugInfo());
    }

    @Test
    public void ticket2Test() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(VALID_USER_TICKET_2);
        Assert.assertEquals(TicketStatus.OK, checkedTicket.getStatus());
        Assert.assertEquals(
            "ticket_type=user;expiration_time=9223372036854775807;default_uid=456;uid=456;uid=123;env=Test;",
            checkedTicket.debugInfo());
    }

    @Test
    public void ticketErrorsTest() {
        UserContext context = UserContext.create(BlackboxEnv.PROD, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket1 = context.check(VALID_USER_TICKET_1);
        Assert.assertEquals(TicketStatus.INVALID_BLACKBOX_ENV, checkedTicket1.getStatus());

        CheckedUserTicket checkedTicket2 = context.check(UNSUPPORTED_VERSION_USER_TICKET);
        Assert.assertEquals(TicketStatus.UNSUPPORTED_VERSION, checkedTicket2.getStatus());

        CheckedUserTicket checkedTicket3 = context.check(EXPIRED_USER_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket3.getStatus());
    }

    @Test(expected = NotAllowedException.class)
    public void ticketException1Test() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(EXPIRED_USER_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket.getStatus());
        checkedTicket.getScopes();
    }

    @Test(expected = NotAllowedException.class)
    public void ticketException2Test() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(EXPIRED_USER_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket.getStatus());
        checkedTicket.getUids();
    }

    @Test(expected = NotAllowedException.class)
    public void ticketException3Test() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(EXPIRED_USER_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket.getStatus());
        checkedTicket.getDefaultUid();
    }

    @Test(expected = NotAllowedException.class)
    public void ticketException4Test() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(EXPIRED_USER_TICKET);
        Assert.assertEquals(TicketStatus.EXPIRED, checkedTicket.getStatus());
        checkedTicket.hasScope("");
    }

    @Test
    public void ticketNoExceptionTest() {
        UserContext context = UserContext.create(BlackboxEnv.TEST, Unittest.getTvmknifePublicKeys());
        CheckedUserTicket checkedTicket = context.check(EXPIRED_USER_TICKET);
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
        CheckedUserTicket u = Unittest.createUserTicket(
            TicketStatus.OK,
            42,
            new String[]{"a", "b", "c"},
            new long[]{24, 57});
        Assert.assertEquals(TicketStatus.OK, u.getStatus());
        Assert.assertArrayEquals(new long[]{24, 42, 57}, u.getUids());
        Assert.assertEquals(42, u.getDefaultUid());
    }
}
