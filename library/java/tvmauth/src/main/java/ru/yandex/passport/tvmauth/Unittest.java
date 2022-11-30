package ru.yandex.passport.tvmauth;

import javax.annotation.Nonnull;

import ru.yandex.passport.tvmauth.internal.JniUtils;

/**
 * Unittest should make easier proccess of creating tests
 */
public class Unittest {
    private static String tvmknifePublicKeys =
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
        "TGRLMWjDULbNIJqRBg";

    private Unittest() {
    }

    public static String getTvmknifePublicKeys() {
        return tvmknifePublicKeys;
    }

    @Nonnull
    public static CheckedServiceTicket createServiceTicket(@Nonnull TicketStatus status, int src) {
        return createServiceTicket(status, src, 0);
    }
    @Nonnull
    public static CheckedServiceTicket createServiceTicket(@Nonnull TicketStatus status,
                                                           int src,
                                                           long issuerUid) {
        return createServiceTicketNative(status.ordinal(), src, issuerUid);
    }

    @Nonnull
    public static CheckedUserTicket createUserTicket(@Nonnull TicketStatus status,
                                                     long defaultUid,
                                                     @Nonnull String[] scopes,
                                                     @Nonnull long[] uids) {
        return createUserTicket(status, defaultUid, scopes, uids, BlackboxEnv.TEST);
    }

    @Nonnull
    public static CheckedUserTicket createUserTicket(@Nonnull TicketStatus status,
                                                     long defaultUid,
                                                     @Nonnull String[] scopes,
                                                     @Nonnull long[] uids,
                                                     BlackboxEnv env) {
        StringBuilder b = new StringBuilder();
        if (scopes != null) {
            for (String v : scopes) {
                b.append(v).append(";");
            }
        }
        StringBuilder b2 = new StringBuilder();
        if (uids != null) {
            for (long v : uids) {
                b2.append(v).append(";");
            }
        }

        return createUserTicketNative(status.ordinal(), defaultUid, b.toString(), b2.toString(), env.ordinal());
    }

    private static native CheckedServiceTicket createServiceTicketNative(int status, int src, long issuerUid);
    private static native CheckedUserTicket createUserTicketNative(int status,
                                                                   long defaultUid,
                                                                   String scopes,
                                                                   String uids,
                                                                   int env);

    static {
        JniUtils.loadLibrary();
    }
}
