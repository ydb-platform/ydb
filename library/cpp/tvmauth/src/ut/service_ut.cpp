#include <library/cpp/tvmauth/src/service_impl.h>
#include <library/cpp/tvmauth/src/utils.h>

#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ServiceTestSuite) { 
    Y_UNIT_TEST_DECLARE(TicketProtoTest); 
}

class TTestServiceTicketImpl: public TCheckedServiceTicket::TImpl {
    using TCheckedServiceTicket::TImpl::TImpl;
    Y_UNIT_TEST_FRIEND(ServiceTestSuite, TicketProtoTest); 
};

Y_UNIT_TEST_SUITE_IMPLEMENTATION(ServiceTestSuite) { 
    static const TString EMPTY_TVM_KEYS = "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWNt4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAggCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDorWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIcNrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbwW2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGATCBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEBCAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPgZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBhADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLGgzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-hI55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcLnkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAnl5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZJQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRvqpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAE";
    static const TString EXPIRED_SERVICE_TICKET = "3:serv:CBAQACIZCOUBEBwaCGJiOnNlc3MxGghiYjpzZXNzMg:IwfMNJYEqStY_SixwqJnyHOMCPR7-3HHk4uylB2oVRkthtezq-OOA7QizDvx7VABLs_iTlXuD1r5IjufNei_EiV145eaa3HIg4xCdJXCojMexf2UYJz8mF2b0YzFAy6_KWagU7xo13CyKAqzJuQf5MJcSUf0ecY9hVh36cJ51aw";
    static const TString MALFORMED_TVM_KEYS = "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWNt4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAggCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDorWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIcNrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbwW2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGATCBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEBCAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPgZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBhADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLGgzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-hI55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcLnkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAnl5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZJQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRvqpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAEEpUBCpIBCAYQABqHATCBhAKBgQCoZkFGm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFBIRuTkYxhs4vUYnWjZrKRAd5bp6_py0csmFmpl_5Yh0b-2pdo_E5PNP7LGRzKyKSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr-TPu34DnJq3yv2a6dqiKL9zSCakQYSlQEKkgEIEBAAGocBMIGEAoGBALhrihbf3EpjDQS2sCQHazoFgN0nBbE9eesnnFTfzQELXb2gnJU9enmV_aDqaHKjgtLIPpCgn40lHrn5k6mvH5OdedyI6cCzE-N-GFp3nAq0NDJyMe0fhtIRD__CbT0ulcvkeow65ubXWfw6dBC2gR_34rdMe_L_TGRLMWjDULbNIJ";
    static const TString MALFORMED_TVM_SECRET = "adcvxcv./-+";
    static const TTvmId NOT_OUR_ID = 27;
    static const TTvmId OUR_ID = 28;
    static const TString SECRET = "GRMJrKnj4fOVnvOqe-WyD1";
    static const TString SERVICE_TICKET_PROTOBUF = "CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My";
    static const TTvmId SRC_ID = 229;
    static const TString UNSUPPORTED_VERSION_SERVICE_TICKET = "2:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8";
    static const TString VALID_SERVICE_TICKET_1 = "3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8";
    static const TString VALID_SERVICE_TICKET_2 = "3:serv:CBAQ__________9_IskICOUBEBwaCGJiOnNlc3MxGgliYjpzZXNzMTAaCmJiOnNlc3MxMDAaCWJiOnNlc3MxMRoJYmI6c2VzczEyGgliYjpzZXNzMTMaCWJiOnNlc3MxNBoJYmI6c2VzczE1GgliYjpzZXNzMTYaCWJiOnNlc3MxNxoJYmI6c2VzczE4GgliYjpzZXNzMTkaCGJiOnNlc3MyGgliYjpzZXNzMjAaCWJiOnNlc3MyMRoJYmI6c2VzczIyGgliYjpzZXNzMjMaCWJiOnNlc3MyNBoJYmI6c2VzczI1GgliYjpzZXNzMjYaCWJiOnNlc3MyNxoJYmI6c2VzczI4GgliYjpzZXNzMjkaCGJiOnNlc3MzGgliYjpzZXNzMzAaCWJiOnNlc3MzMRoJYmI6c2VzczMyGgliYjpzZXNzMzMaCWJiOnNlc3MzNBoJYmI6c2VzczM1GgliYjpzZXNzMzYaCWJiOnNlc3MzNxoJYmI6c2VzczM4GgliYjpzZXNzMzkaCGJiOnNlc3M0GgliYjpzZXNzNDAaCWJiOnNlc3M0MRoJYmI6c2VzczQyGgliYjpzZXNzNDMaCWJiOnNlc3M0NBoJYmI6c2VzczQ1GgliYjpzZXNzNDYaCWJiOnNlc3M0NxoJYmI6c2VzczQ4GgliYjpzZXNzNDkaCGJiOnNlc3M1GgliYjpzZXNzNTAaCWJiOnNlc3M1MRoJYmI6c2VzczUyGgliYjpzZXNzNTMaCWJiOnNlc3M1NBoJYmI6c2VzczU1GgliYjpzZXNzNTYaCWJiOnNlc3M1NxoJYmI6c2VzczU4GgliYjpzZXNzNTkaCGJiOnNlc3M2GgliYjpzZXNzNjAaCWJiOnNlc3M2MRoJYmI6c2VzczYyGgliYjpzZXNzNjMaCWJiOnNlc3M2NBoJYmI6c2VzczY1GgliYjpzZXNzNjYaCWJiOnNlc3M2NxoJYmI6c2VzczY4GgliYjpzZXNzNjkaCGJiOnNlc3M3GgliYjpzZXNzNzAaCWJiOnNlc3M3MRoJYmI6c2VzczcyGgliYjpzZXNzNzMaCWJiOnNlc3M3NBoJYmI6c2Vzczc1GgliYjpzZXNzNzYaCWJiOnNlc3M3NxoJYmI6c2Vzczc4GgliYjpzZXNzNzkaCGJiOnNlc3M4GgliYjpzZXNzODAaCWJiOnNlc3M4MRoJYmI6c2VzczgyGgliYjpzZXNzODMaCWJiOnNlc3M4NBoJYmI6c2Vzczg1GgliYjpzZXNzODYaCWJiOnNlc3M4NxoJYmI6c2Vzczg4GgliYjpzZXNzODkaCGJiOnNlc3M5GgliYjpzZXNzOTAaCWJiOnNlc3M5MRoJYmI6c2VzczkyGgliYjpzZXNzOTMaCWJiOnNlc3M5NBoJYmI6c2Vzczk1GgliYjpzZXNzOTYaCWJiOnNlc3M5NxoJYmI6c2Vzczk4GgliYjpzZXNzOTk:JYmABAVLM6y7_T4n1pRcwBfwDfzMV4JJ3cpbEG617zdGgKRZwL7MalsYn5bq1F2ibujMrsF9nzZf8l4s_e-Ivjkz_xu4KMzSp-pUh9V7XIF_smj0WHYpv6gOvWNuK8uIvlZTTKwtQX0qZOL9m-MEeZiHoQPKZGCfJ_qxMUp-J8I";
    static const TString VALID_SERVICE_TICKET_3 = "3:serv:CBAQ__________9_IgUI5QEQHA:Sd6tmA1CNy2Nf7XevC3x7zr2DrGNRmcl-TxUsDtDW2xI3YXyCxBltWeg0-KtDlqyYuPOP5Jd_-XXNA12KlOPnNzrz3jm-5z8uQl6CjCcrVHUHJ75pGC8r9UOlS8cOgeXQB5dYP-fOWyo5CNadlozx1S2meCIxncbQRV1kCBi4KU";
    static const TString VALID_SERVICE_TICKET_ISSUER = "3:serv:CBAQ__________9_IgsI5QEQHCDr1MT4Ag:Gu66XJT_nKnIRJjFy1561wFhIqkJItcSTGftLo7Yvi7i5wIdV-QuKT_-IMPpgjxnnGbt1Dy3Ys2TEoeJAb0TdaCYG1uy3vpoLONmTx9AenN5dx1HHf46cypLK5D3OdiTjxvqI9uGmSIKrSdRxU8gprpu5QiBDPZqVCWhM60FVSY";

    Y_UNIT_TEST(ContextExceptionsTest) { 
        UNIT_ASSERT_EXCEPTION(TServiceContext::TImpl(SECRET, OUR_ID, MALFORMED_TVM_KEYS), TMalformedTvmKeysException);
        UNIT_ASSERT_EXCEPTION(TServiceContext::TImpl(SECRET, OUR_ID, EMPTY_TVM_KEYS), TEmptyTvmKeysException);
        UNIT_ASSERT_EXCEPTION(TServiceContext::TImpl(MALFORMED_TVM_SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS), TMalformedTvmSecretException);
    }

    Y_UNIT_TEST(ContextSignTest) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        UNIT_ASSERT_VALUES_EQUAL(
            "NsPTYak4Cfk-4vgau5lab3W4GPiTtb2etuj3y4MDPrk",
            context.SignCgiParamsForTvm(IntToString<10>(std::numeric_limits<time_t>::max()), "13,28", ""));
    }

    Y_UNIT_TEST(Ticket1Test) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        auto checkedTicket = context.Check(VALID_SERVICE_TICKET_1);
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, checkedTicket->GetStatus());
        UNIT_ASSERT_EQUAL(std::numeric_limits<time_t>::max(), checkedTicket->GetExpirationTime());
        UNIT_ASSERT_EQUAL(SRC_ID, checkedTicket->GetSrc());
        UNIT_ASSERT_EQUAL(TScopes({"bb:sess1", "bb:sess2"}), checkedTicket->GetScopes());
        UNIT_ASSERT(checkedTicket->HasScope("bb:sess1"));
        UNIT_ASSERT(checkedTicket->HasScope("bb:sess2"));
        UNIT_ASSERT(!checkedTicket->HasScope("bb:sess3"));
        UNIT_ASSERT_EQUAL("ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess2;", checkedTicket->DebugInfo());
        UNIT_ASSERT(!checkedTicket->GetIssuerUid());
    }

    Y_UNIT_TEST(Ticket2Test) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        auto checkedTicket = context.Check(VALID_SERVICE_TICKET_2);
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, checkedTicket->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess10;scope=bb:sess100;scope=bb:sess11;scope=bb:sess12;scope=bb:sess13;scope=bb:sess14;scope=bb:sess15;scope=bb:sess16;scope=bb:sess17;scope=bb:sess18;scope=bb:sess19;scope=bb:sess2;scope=bb:sess20;scope=bb:sess21;scope=bb:sess22;scope=bb:sess23;scope=bb:sess24;scope=bb:sess25;scope=bb:sess26;scope=bb:sess27;scope=bb:sess28;scope=bb:sess29;scope=bb:sess3;scope=bb:sess30;scope=bb:sess31;scope=bb:sess32;scope=bb:sess33;scope=bb:sess34;scope=bb:sess35;scope=bb:sess36;scope=bb:sess37;scope=bb:sess38;scope=bb:sess39;scope=bb:sess4;scope=bb:sess40;scope=bb:sess41;scope=bb:sess42;scope=bb:sess43;scope=bb:sess44;scope=bb:sess45;scope=bb:sess46;scope=bb:sess47;scope=bb:sess48;scope=bb:sess49;scope=bb:sess5;scope=bb:sess50;scope=bb:sess51;scope=bb:sess52;scope=bb:sess53;scope=bb:sess54;scope=bb:sess55;scope=bb:sess56;scope=bb:sess57;scope=bb:sess58;scope=bb:sess59;scope=bb:sess6;scope=bb:sess60;scope=bb:sess61;scope=bb:sess62;scope=bb:sess63;scope=bb:sess64;scope=bb:sess65;scope=bb:sess66;scope=bb:sess67;scope=bb:sess68;scope=bb:sess69;scope=bb:sess7;scope=bb:sess70;scope=bb:sess71;scope=bb:sess72;scope=bb:sess73;scope=bb:sess74;scope=bb:sess75;scope=bb:sess76;scope=bb:sess77;scope=bb:sess78;scope=bb:sess79;scope=bb:sess8;scope=bb:sess80;scope=bb:sess81;scope=bb:sess82;scope=bb:sess83;scope=bb:sess84;scope=bb:sess85;scope=bb:sess86;scope=bb:sess87;scope=bb:sess88;scope=bb:sess89;scope=bb:sess9;scope=bb:sess90;scope=bb:sess91;scope=bb:sess92;scope=bb:sess93;scope=bb:sess94;scope=bb:sess95;scope=bb:sess96;scope=bb:sess97;scope=bb:sess98;scope=bb:sess99;", checkedTicket->DebugInfo());
        UNIT_ASSERT(!checkedTicket->GetIssuerUid());
    }

    Y_UNIT_TEST(Ticket3Test) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        auto checkedTicket = context.Check(VALID_SERVICE_TICKET_3);
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, checkedTicket->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;", checkedTicket->DebugInfo());
        UNIT_ASSERT(!checkedTicket->GetIssuerUid());
    }

    Y_UNIT_TEST(TicketIssuerTest) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        auto checkedTicket = context.Check(VALID_SERVICE_TICKET_ISSUER);
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, checkedTicket->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL("ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;issuer_uid=789654123;",
                                 checkedTicket->DebugInfo());
        UNIT_ASSERT(checkedTicket->GetIssuerUid());
        UNIT_ASSERT_VALUES_EQUAL(789654123, *checkedTicket->GetIssuerUid());
    }

    Y_UNIT_TEST(TicketErrorsTest) { 
        TServiceContext::TImpl context(SECRET, NOT_OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        auto checkedTicket1 = context.Check(VALID_SERVICE_TICKET_1);
        UNIT_ASSERT_EQUAL(ETicketStatus::InvalidDst, checkedTicket1->GetStatus());

        auto checkedTicket2 = context.Check(UNSUPPORTED_VERSION_SERVICE_TICKET);
        UNIT_ASSERT_EQUAL(ETicketStatus::UnsupportedVersion, checkedTicket2->GetStatus());

        auto checkedTicket3 = context.Check(EXPIRED_SERVICE_TICKET);
        UNIT_ASSERT_EQUAL(ETicketStatus::Expired, checkedTicket3->GetStatus());
    }

    Y_UNIT_TEST(TicketExceptionTest) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);

        auto checkedTicket = context.Check(EXPIRED_SERVICE_TICKET);
        UNIT_ASSERT_EQUAL(ETicketStatus::Expired, checkedTicket->GetStatus());

        UNIT_ASSERT_EXCEPTION(checkedTicket->GetScopes(), TNotAllowedException);
        UNIT_ASSERT_EXCEPTION(checkedTicket->GetSrc(), TNotAllowedException);
        UNIT_ASSERT_EXCEPTION(checkedTicket->HasScope(""), TNotAllowedException);
        UNIT_ASSERT_NO_EXCEPTION(bool(*checkedTicket));
        UNIT_ASSERT_NO_EXCEPTION(checkedTicket->DebugInfo());
    }

    Y_UNIT_TEST(TicketProtoTest) { 
        ticket2::Ticket protobufTicket;
        UNIT_ASSERT(protobufTicket.ParseFromString(NUtils::Base64url2bin(SERVICE_TICKET_PROTOBUF)));
        TTestServiceTicketImpl checkedTicket(ETicketStatus::Ok, std::move(protobufTicket));
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, checkedTicket.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(std::numeric_limits<time_t>::max(), checkedTicket.GetExpirationTime());
        UNIT_ASSERT_EQUAL(SRC_ID, checkedTicket.GetSrc());
    }

    Y_UNIT_TEST(ResetKeysTest) { 
        TServiceContext::TImpl context(SECRET, OUR_ID, NUnittest::TVMKNIFE_PUBLIC_KEYS);
        context.ResetKeys(NUnittest::TVMKNIFE_PUBLIC_KEYS);
        auto checkedTicket = context.Check(VALID_SERVICE_TICKET_1);
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, checkedTicket->GetStatus());
    }

    Y_UNIT_TEST(CreateTicketForTests) {
        TCheckedServiceTicket t = NTvmAuth::NUnittest::CreateServiceTicket(ETicketStatus::Ok, 42);
        UNIT_ASSERT_EQUAL(ETicketStatus::Ok, t.GetStatus());
        UNIT_ASSERT_EQUAL(42, t.GetSrc());
        UNIT_ASSERT_VALUES_EQUAL("ticket_type=serv;src=42;dst=100500;", t.DebugInfo());
    }

    Y_UNIT_TEST(CreateForTests) {
        auto t = TCheckedServiceTicket::TImpl::CreateTicketForTests(ETicketStatus::Ok, 456, {});
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok, t->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(456, t->GetSrc());
        UNIT_ASSERT(!t->GetIssuerUid());

        t = TCheckedServiceTicket::TImpl::CreateTicketForTests(ETicketStatus::Ok, 456, 100800);
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok, t->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(456, t->GetSrc());
        UNIT_ASSERT(t->GetIssuerUid());
        UNIT_ASSERT_VALUES_EQUAL(*t->GetIssuerUid(), 100800);

        t = TCheckedServiceTicket::TImpl::CreateTicketForTests(ETicketStatus::Expired, 456, {});
        UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Expired, t->GetStatus());
        UNIT_ASSERT_EXCEPTION_CONTAINS(t->GetSrc(), TNotAllowedException, "Method cannot be used in non-valid ticket");
        UNIT_ASSERT_EXCEPTION_CONTAINS(t->GetIssuerUid(), TNotAllowedException, "Method cannot be used in non-valid ticket");
    }
}
