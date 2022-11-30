// DO_NOT_STYLE
#include <library/c/tvmauth/deprecated.h>
#include <library/c/tvmauth/tvmauth.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/tvmauth/unittest.h>

#include <chrono>
#include <string>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(CInterfaceServiceTestSuite) {
    static const TString EMPTY_TVM_KEYS = "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWNt4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAggCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDorWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIcNrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbwW2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGATCBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEBCAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPgZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBhADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLGgzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-hI55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcLnkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAnl5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZJQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRvqpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAE";
    static const TString EXPIRED_SERVICE_TICKET = "3:serv:CBAQACIZCOUBEBwaCGJiOnNlc3MxGghiYjpzZXNzMg:IwfMNJYEqStY_SixwqJnyHOMCPR7-3HHk4uylB2oVRkthtezq-OOA7QizDvx7VABLs_iTlXuD1r5IjufNei_EiV145eaa3HIg4xCdJXCojMexf2UYJz8mF2b0YzFAy6_KWagU7xo13CyKAqzJuQf5MJcSUf0ecY9hVh36cJ51aw";
    static const TString MALFORMED_TVM_KEYS = "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWNt4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAggCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDorWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIcNrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbwW2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGATCBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEBCAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPgZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBhADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLGgzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-hI55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcLnkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAnl5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZJQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRvqpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAEEpUBCpIBCAYQABqHATCBhAKBgQCoZkFGm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFBIRuTkYxhs4vUYnWjZrKRAd5bp6_py0csmFmpl_5Yh0b-2pdo_E5PNP7LGRzKyKSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr-TPu34DnJq3yv2a6dqiKL9zSCakQYSlQEKkgEIEBAAGocBMIGEAoGBALhrihbf3EpjDQS2sCQHazoFgN0nBbE9eesnnFTfzQELXb2gnJU9enmV_aDqaHKjgtLIPpCgn40lHrn5k6mvH5OdedyI6cCzE-N-GFp3nAq0NDJyMe0fhtIRD__CbT0ulcvkeow65ubXWfw6dBC2gR_34rdMe_L_TGRLMWjDULbNIJ";
    static const TString MALFORMED_TVM_SECRET = "adcvxcv./-+";
    static const TTvmId OUR_ID = 28;
    static const TString SECRET = "GRMJrKnj4fOVnvOqe-WyD1";
    static const TString SERVICE_TICKET_PROTOBUF = "CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My";
    static const TTvmId SRC_ID = 229;
    static const TString UNSUPPORTED_VERSION_SERVICE_TICKET = "2:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8";
    static const TString VALID_SERVICE_TICKET_1 = "3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8";
    static const TString VALID_SERVICE_TICKET_2 = "3:serv:CBAQ__________9_IskICOUBEBwaCGJiOnNlc3MxGgliYjpzZXNzMTAaCmJiOnNlc3MxMDAaCWJiOnNlc3MxMRoJYmI6c2VzczEyGgliYjpzZXNzMTMaCWJiOnNlc3MxNBoJYmI6c2VzczE1GgliYjpzZXNzMTYaCWJiOnNlc3MxNxoJYmI6c2VzczE4GgliYjpzZXNzMTkaCGJiOnNlc3MyGgliYjpzZXNzMjAaCWJiOnNlc3MyMRoJYmI6c2VzczIyGgliYjpzZXNzMjMaCWJiOnNlc3MyNBoJYmI6c2VzczI1GgliYjpzZXNzMjYaCWJiOnNlc3MyNxoJYmI6c2VzczI4GgliYjpzZXNzMjkaCGJiOnNlc3MzGgliYjpzZXNzMzAaCWJiOnNlc3MzMRoJYmI6c2VzczMyGgliYjpzZXNzMzMaCWJiOnNlc3MzNBoJYmI6c2VzczM1GgliYjpzZXNzMzYaCWJiOnNlc3MzNxoJYmI6c2VzczM4GgliYjpzZXNzMzkaCGJiOnNlc3M0GgliYjpzZXNzNDAaCWJiOnNlc3M0MRoJYmI6c2VzczQyGgliYjpzZXNzNDMaCWJiOnNlc3M0NBoJYmI6c2VzczQ1GgliYjpzZXNzNDYaCWJiOnNlc3M0NxoJYmI6c2VzczQ4GgliYjpzZXNzNDkaCGJiOnNlc3M1GgliYjpzZXNzNTAaCWJiOnNlc3M1MRoJYmI6c2VzczUyGgliYjpzZXNzNTMaCWJiOnNlc3M1NBoJYmI6c2VzczU1GgliYjpzZXNzNTYaCWJiOnNlc3M1NxoJYmI6c2VzczU4GgliYjpzZXNzNTkaCGJiOnNlc3M2GgliYjpzZXNzNjAaCWJiOnNlc3M2MRoJYmI6c2VzczYyGgliYjpzZXNzNjMaCWJiOnNlc3M2NBoJYmI6c2VzczY1GgliYjpzZXNzNjYaCWJiOnNlc3M2NxoJYmI6c2VzczY4GgliYjpzZXNzNjkaCGJiOnNlc3M3GgliYjpzZXNzNzAaCWJiOnNlc3M3MRoJYmI6c2VzczcyGgliYjpzZXNzNzMaCWJiOnNlc3M3NBoJYmI6c2Vzczc1GgliYjpzZXNzNzYaCWJiOnNlc3M3NxoJYmI6c2Vzczc4GgliYjpzZXNzNzkaCGJiOnNlc3M4GgliYjpzZXNzODAaCWJiOnNlc3M4MRoJYmI6c2VzczgyGgliYjpzZXNzODMaCWJiOnNlc3M4NBoJYmI6c2Vzczg1GgliYjpzZXNzODYaCWJiOnNlc3M4NxoJYmI6c2Vzczg4GgliYjpzZXNzODkaCGJiOnNlc3M5GgliYjpzZXNzOTAaCWJiOnNlc3M5MRoJYmI6c2VzczkyGgliYjpzZXNzOTMaCWJiOnNlc3M5NBoJYmI6c2Vzczk1GgliYjpzZXNzOTYaCWJiOnNlc3M5NxoJYmI6c2Vzczk4GgliYjpzZXNzOTk:JYmABAVLM6y7_T4n1pRcwBfwDfzMV4JJ3cpbEG617zdGgKRZwL7MalsYn5bq1F2ibujMrsF9nzZf8l4s_e-Ivjkz_xu4KMzSp-pUh9V7XIF_smj0WHYpv6gOvWNuK8uIvlZTTKwtQX0qZOL9m-MEeZiHoQPKZGCfJ_qxMUp-J8I";
    static const TString VALID_SERVICE_TICKET_3 = "3:serv:CBAQ__________9_IgUI5QEQHA:Sd6tmA1CNy2Nf7XevC3x7zr2DrGNRmcl-TxUsDtDW2xI3YXyCxBltWeg0-KtDlqyYuPOP5Jd_-XXNA12KlOPnNzrz3jm-5z8uQl6CjCcrVHUHJ75pGC8r9UOlS8cOgeXQB5dYP-fOWyo5CNadlozx1S2meCIxncbQRV1kCBi4KU";
    static const TString VALID_SERVICE_TICKET_ISSUER = "3:serv:CBAQ__________9_IgsI5QEQHCDr1MT4Ag:Gu66XJT_nKnIRJjFy1561wFhIqkJItcSTGftLo7Yvi7i5wIdV-QuKT_-IMPpgjxnnGbt1Dy3Ys2TEoeJAb0TdaCYG1uy3vpoLONmTx9AenN5dx1HHf46cypLK5D3OdiTjxvqI9uGmSIKrSdRxU8gprpu5QiBDPZqVCWhM60FVSY";

    Y_UNIT_TEST(BlackboxTvmIdTest) {
        UNIT_ASSERT_VALUES_EQUAL("222", TA_BlackboxTvmIdProd);
        UNIT_ASSERT_VALUES_EQUAL("224", TA_BlackboxTvmIdTest);
        UNIT_ASSERT_VALUES_EQUAL("223", TA_BlackboxTvmIdProdYateam);
        UNIT_ASSERT_VALUES_EQUAL("225", TA_BlackboxTvmIdTestYateam);
        UNIT_ASSERT_VALUES_EQUAL("226", TA_BlackboxTvmIdStress);
        UNIT_ASSERT_VALUES_EQUAL("239", TA_BlackboxTvmIdMimino);
    }

    Y_UNIT_TEST(ContextErrorsTest) {
        TA_TServiceContext* context;
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_MALFORMED_TVM_KEYS,
            TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), MALFORMED_TVM_KEYS.c_str(), MALFORMED_TVM_KEYS.size(), &context));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_EMPTY_TVM_KEYS,
            TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), EMPTY_TVM_KEYS.c_str(), EMPTY_TVM_KEYS.size(), &context));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_MALFORMED_TVM_SECRET,
            TA_CreateServiceContext(OUR_ID, MALFORMED_TVM_SECRET.c_str(), MALFORMED_TVM_SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context));

        char signature[512];
        size_t signatureSize;

        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_SMALL_BUFFER,
            TA_SignCgiParamsForTvm(context, "1490000001", 10, "13,19", 5, "", 0, signature, &signatureSize, 1));
        TA_DeleteServiceContext(context);

        TA_CreateServiceContext(OUR_ID, nullptr, 0, NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_MALFORMED_TVM_SECRET,
            TA_SignCgiParamsForTvm(context, "1490000001", 10, "13,19", 5, "", 0, signature, &signatureSize, 1));
        TA_DeleteServiceContext(context);
    }

    Y_UNIT_TEST(ContextSignTest) {
        TA_TServiceContext* context;
        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        char signature[512];
        size_t signatureSize;
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_SignCgiParamsForTvm(context, "1490000001", 10, "13,19", 5, "", 0, signature, &signatureSize, 512));
        UNIT_ASSERT_VALUES_EQUAL("9q5ghpb9jqJocw1GyweNo2LyY_lN47O7sXu2-Oe78V4", signature);
        TA_DeleteServiceContext(context);
    }

    Y_UNIT_TEST(ContextTest) {
        TA_TServiceContext* context;
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_DeleteServiceContext(context));
    }

    Y_UNIT_TEST(StatusToLabelsTest) {
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_OK), "libtvmauth.so: OK");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_DEPRECATED), "libtvmauth.so: Deprecated function");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_EMPTY_TVM_KEYS), "libtvmauth.so: Empty TVM keys");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_EXPIRED_TICKET), "libtvmauth.so: Expired ticket");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_INVALID_BLACKBOX_ENV), "libtvmauth.so: Invalid BlackBox environment");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_INVALID_DST), "libtvmauth.so: Invalid ticket destination");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_INVALID_PARAM), "libtvmauth.so: Invalid function parameter");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_INVALID_TICKET_TYPE), "libtvmauth.so: Invalid ticket type");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_MALFORMED_TICKET), "libtvmauth.so: Malformed ticket");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_MALFORMED_TVM_KEYS), "libtvmauth.so: Malformed TVM keys");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_MALFORMED_TVM_SECRET), "libtvmauth.so: Malformed TVM secret: it is empty or invalid base64url");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_MISSING_KEY), "libtvmauth.so: Context does not have required key to check ticket: public keys are too old");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_NOT_ALLOWED), "libtvmauth.so: Not allowed method");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_SIGN_BROKEN), "libtvmauth.so: Invalid ticket signature");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_SMALL_BUFFER), "libtvmauth.so: Small buffer");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_UNEXPECTED_ERROR), "libtvmauth.so: Unexpected error");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_UNSUPPORTED_VERSION), "libtvmauth.so: Unsupported ticket version");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(TA_EErrorCode::TA_EC_FAILED_TO_START_TVM_CLIENT), "libtvmauth.so: TvmClient failed to start with some reason (need to check logs)");
        UNIT_ASSERT_VALUES_EQUAL(TA_ErrorCodeToString(static_cast<TA_EErrorCode>(31)), "libtvmauth.so: Unknown error");
    }

    Y_UNIT_TEST(TicketErrorsTest) {
        TA_TServiceContext* context;
        TA_TCheckedServiceTicket* ticket;
        char debugInfo[512];
        size_t debugInfoSize;

        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        TA_CheckServiceTicket(context, VALID_SERVICE_TICKET_1.c_str(), VALID_SERVICE_TICKET_1.size(), &ticket);

        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_SMALL_BUFFER,
            TA_GetServiceTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 1));

        TA_DeleteServiceTicket(ticket);
        TA_DeleteServiceContext(context);
    }

    Y_UNIT_TEST(Ticket1Test) {
        TA_TServiceContext* context;
        char debugInfo[512];
        size_t debugInfoSize;
        TA_TCheckedServiceTicket* ticket;
        uint32_t ticketSrc;
        uint64_t uid = 0;

        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckServiceTicket(context, VALID_SERVICE_TICKET_1.c_str(), VALID_SERVICE_TICKET_1.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketSrc(ticket, &ticketSrc));
        UNIT_ASSERT_VALUES_EQUAL(SRC_ID, ticketSrc);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 512));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess2;",
            TStringBuf(debugInfo, debugInfoSize));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketIssuerUid(ticket, &uid));
        UNIT_ASSERT_VALUES_EQUAL(0, uid);
        TA_DeleteServiceTicket(ticket);
        TA_DeleteServiceContext(context);
    }

    Y_UNIT_TEST(Ticket2Test) {
        TA_TServiceContext* context;
        char debugInfo[8192];
        size_t debugInfoSize;
        TA_TCheckedServiceTicket* ticket;
        uint64_t uid = 0;

        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckServiceTicket(context, VALID_SERVICE_TICKET_2.c_str(), VALID_SERVICE_TICKET_2.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 8192));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess10;scope=bb:sess100;scope=bb:sess11;scope=bb:sess12;scope=bb:sess13;scope=bb:sess14;scope=bb:sess15;scope=bb:sess16;scope=bb:sess17;scope=bb:sess18;scope=bb:sess19;scope=bb:sess2;scope=bb:sess20;scope=bb:sess21;scope=bb:sess22;scope=bb:sess23;scope=bb:sess24;scope=bb:sess25;scope=bb:sess26;scope=bb:sess27;scope=bb:sess28;scope=bb:sess29;scope=bb:sess3;scope=bb:sess30;scope=bb:sess31;scope=bb:sess32;scope=bb:sess33;scope=bb:sess34;scope=bb:sess35;scope=bb:sess36;scope=bb:sess37;scope=bb:sess38;scope=bb:sess39;scope=bb:sess4;scope=bb:sess40;scope=bb:sess41;scope=bb:sess42;scope=bb:sess43;scope=bb:sess44;scope=bb:sess45;scope=bb:sess46;scope=bb:sess47;scope=bb:sess48;scope=bb:sess49;scope=bb:sess5;scope=bb:sess50;scope=bb:sess51;scope=bb:sess52;scope=bb:sess53;scope=bb:sess54;scope=bb:sess55;scope=bb:sess56;scope=bb:sess57;scope=bb:sess58;scope=bb:sess59;scope=bb:sess6;scope=bb:sess60;scope=bb:sess61;scope=bb:sess62;scope=bb:sess63;scope=bb:sess64;scope=bb:sess65;scope=bb:sess66;scope=bb:sess67;scope=bb:sess68;scope=bb:sess69;scope=bb:sess7;scope=bb:sess70;scope=bb:sess71;scope=bb:sess72;scope=bb:sess73;scope=bb:sess74;scope=bb:sess75;scope=bb:sess76;scope=bb:sess77;scope=bb:sess78;scope=bb:sess79;scope=bb:sess8;scope=bb:sess80;scope=bb:sess81;scope=bb:sess82;scope=bb:sess83;scope=bb:sess84;scope=bb:sess85;scope=bb:sess86;scope=bb:sess87;scope=bb:sess88;scope=bb:sess89;scope=bb:sess9;scope=bb:sess90;scope=bb:sess91;scope=bb:sess92;scope=bb:sess93;scope=bb:sess94;scope=bb:sess95;scope=bb:sess96;scope=bb:sess97;scope=bb:sess98;scope=bb:sess99;",
            TStringBuf(debugInfo, debugInfoSize));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketIssuerUid(ticket, &uid));
        UNIT_ASSERT_VALUES_EQUAL(0, uid);
        TA_DeleteServiceTicket(ticket);
        TA_DeleteServiceContext(context);
    }

    Y_UNIT_TEST(Ticket3Test) {
        TA_TServiceContext* context;
        char debugInfo[512];
        size_t debugInfoSize;
        TA_TCheckedServiceTicket* ticket;
        uint64_t uid = 0;

        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckServiceTicket(context, VALID_SERVICE_TICKET_3.c_str(), VALID_SERVICE_TICKET_3.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 512));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;",
            TStringBuf(debugInfo, debugInfoSize));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketIssuerUid(ticket, &uid));
        UNIT_ASSERT_VALUES_EQUAL(0, uid);
        TA_DeleteServiceTicket(ticket);
        TA_DeleteServiceContext(context);
    }

    Y_UNIT_TEST(TicketIssuerTest) {
        TA_TServiceContext* context;
        char debugInfo[512];
        size_t debugInfoSize;
        TA_TCheckedServiceTicket* ticket;
        uint64_t uid = 0;

        TA_CreateServiceContext(OUR_ID, SECRET.c_str(), SECRET.size(), NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckServiceTicket(context, VALID_SERVICE_TICKET_ISSUER.c_str(), VALID_SERVICE_TICKET_ISSUER.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 512));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;issuer_uid=789654123;",
            TStringBuf(debugInfo, debugInfoSize));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetServiceTicketIssuerUid(ticket, &uid));
        UNIT_ASSERT_VALUES_EQUAL(789654123, uid);
        TA_DeleteServiceTicket(ticket);
        TA_DeleteServiceContext(context);
    }
}

Y_UNIT_TEST_SUITE(CInterfaceUserTestSuite) {
    static const TString EMPTY_TVM_KEYS = "1:CpkCCpQCCAIQABqJAjCCAQUCggEBAOEQEzn7piw9Z-iAq1uW4mgJfVZXYp3M5pDT46VFsHN4LPD55Aq7XZzxZqQcFt4Ix3UjVNTrhIOQOitaNcYhr_bFPo3fPM0ATnSuhNs8JyZ5CumzbrChNh9gi6oN2jFgpk2vSW80stbju8GvsQqqjVLgpKPtpcr9dh4S9SAiB_edUhw2uKjtVOcWYYZVAJ6lovUeLWA3jIheNVnTo8E9n4PFJG7rE4ZsWt8owl4w-zso7IT7bpDfE5d1MhrVf7Ngi5xN39vFQIvgFVvBbYC9tsWxobshKZV0a7RdROaFezUqsR9RUtEmETgPH32EBlebX8eVgxR4lwxD-gc6FX3gQ0UglpEGEAIKlgEKkQEIAxAAGoYBMIGDAoGAX23ZgkYAmRFEWrp9aGLebVMVbVQ4TR_pmt9iEcCSmoaUqWHRBV95M0-l4mGLvnFfMJ7qhF5FSb7QNuoM2FNKELu4ZS_Ug1idEFBYfoT7kVzletsMVK4ZDDYRiM18fL8d58clfFAoCo-_EEMowqQeBXnxa0zqsLyNGL2x1f-KDY0gl5EGEAEKlgEKkQEIBBAAGoYBMIGDAoGAYHh3p4sZQG_5DoQ8t6ELhL7K4TYcH7-sntaR5jDKn7Eg-iU-t349CZ7a60cHhmClcci784WSwN7_Rs-BmeI1FJVFuXtbj0OBLlhreQx-tgHgOcXkW1rm2fWhXoTDFgPgk42kVN6c4kAs8ZM9rDFcR-if-l6Ic5IG5Ay7f9Wf6XUgmJEGEAMKlwEKkgEIBRAAGocBMIGEAoGBAJMkwoh-Z95mlojtD2uexzb2B1ZrArtOelUpEfCzWJjsRSVhE55Vwx0DASpUzp_wFUIosaDNDsZqv-67F9QYrCoRDOimkOtrDgXvknjrj7sPl_r-glC4YgEdnGpsw420uMEJdFSFBbmzMEuZND9Hepolvm9_6HQA9l-RiGrVxO21IJmRBhAECpcBCpIBCAwQABqHATCBhAKBgQCS7MVx3lMm7uVhZh7aFAsV9RYgiP3UG9BAtr9OGWfhi0YI7yAbUomomb5iWYk5ZAbQHsf_lFWHFTFX0qmYWewNWTNQUo6wIFpgZdzXbE9WKhrBSUDCnmqndBC1nLH-x73tgigvO4uvjpSQtwueQiun4mjjpDNiL9AerjlZObwxDSCWkQYQAgqXAQqSAQgNEAAahwEwgYQCgYEAhTNvE6Sdd_-s23Xv-KvwmVa-u2cJaxB3UwCeXmaACu9q8O0p4FgfOLn8CTsMeUvO4DpgTxbXvkXEsUqk3aGTP6X4zuW1RJ3w3UG8lGEri3pZ99Z8L_XA_KgBt5I3ZhvWPKC1klDzAahcdwlBReIyK3d4U3LKXCwkWPWDMBHgkJ0gl5EGEAEKlgEKkQEIDhAAGoYBMIGDAoGAUxxsSmX26RVHoA79HqwzKJTXUjxvwwFJQIQLXmIqytU7y_-bjv9NFGNY9i1D15nujWhw0kdoZRTuqoicWq0VpWchx4_o2YKxeGOzcwutY9LwhEDLhl_gUqYY7Hc307a96aNTKBn29SJKRQt1Wt5yKdFpPwlDlEbhtQxjThcYjAUgmJEGEAMKlgEKkQEIDxAAGoYBMIGDAoGAakKRb41HeWLwaVG-qmrZhkZIiCb89PIMjZnB_Dr2CuwGY-lUVbjxXDvGgw3fnNyuiMSmJqUH-5DOWktg6-wyXrfgJEi6P6fVATf6BplNNMTyaD4BHiZzGdMpOE-dFClu46VgCeuLCJqxaJAQxsaOr8eZaKhhjEWj1RKWSVCfj70gmZEGEAQSlQEKkgEIBhAAGocBMIGEAoGBAKhmQUab2gtOqN5dkCrqPlLqLsrbRXSU10EEup-YUEhG5ORjGGzi9RidaNmspEB3lunr-nLRyyYWamX_liHRv7al2j8Tk80_ssZHMrIpKIV13LKQorNU5rMfxhiV0MB8TxnkehL17jxqfkmw-DSOv5M-7fgOcmrfK_Zrp2qIov3NIJqRBhKVAQqSAQgQEAAahwEwgYQCgYEAuGuKFt_cSmMNBLawJAdrOgWA3ScFsT156yecVN_NAQtdvaCclT16eZX9oOpocqOC0sg-kKCfjSUeufmTqa8fk5153IjpwLMT434YWnecCrQ0MnIx7R-G0hEP_8JtPS6Vy-R6jDrm5tdZ_Dp0ELaBH_fit0x78v9MZEsxaMNQts0gmpEG";
    static const TString EXPIRED_USER_TICKET = "3:user:CA0QABokCgMIyAMKAgh7EMgDGghiYjpzZXNzMRoIYmI6c2VzczIgEigB:D0CmYVwWg91LDYejjeQ2UP8AeiA_mr1q1CUD_lfJ9zQSEYEOYGDTafg4Um2rwOOvQnsD1JHM4zHyMUJ6Jtp9GAm5pmhbXBBZqaCcJpyxLTEC8a81MhJFCCJRvu_G1FiAgRgB25gI3HIbkvHFUEqAIC_nANy7NFQnbKk2S-EQPGY";
    static const TString MALFORMED_TVM_KEYS = "1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPLlhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWNt4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAggCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDorWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIcNrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbwW2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGATCBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEBCAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPgZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBhADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLGgzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-hI55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcLnkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAnl5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZJQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRvqpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAEEpUBCpIBCAYQABqHATCBhAKBgQCoZkFGm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFBIRuTkYxhs4vUYnWjZrKRAd5bp6_py0csmFmpl_5Yh0b-2pdo_E5PNP7LGRzKyKSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr-TPu34DnJq3yv2a6dqiKL9zSCakQYSlQEKkgEIEBAAGocBMIGEAoGBALhrihbf3EpjDQS2sCQHazoFgN0nBbE9eesnnFTfzQELXb2gnJU9enmV_aDqaHKjgtLIPpCgn40lHrn5k6mvH5OdedyI6cCzE-N-GFp3nAq0NDJyMe0fhtIRD__CbT0ulcvkeow65ubXWfw6dBC2gR_34rdMe_L_TGRLMWjDULbNIJ";
    static const TString UNSUPPORTED_VERSION_USER_TICKET = "2:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:KJFv5EcXn9krYk19LCvlFrhMW-R4q8mKfXJXCd-RBVBgUQzCOR1Dx2FiOyU-BxUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tzrfNUr88otBGAziHASJWgyVDkhyQ3p7YbN38qpb0vGQrYNxlk4e2I";
    static const TString USER_TICKET_PROTOBUF = "CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE";
    static const TString VALID_USER_TICKET_1 = "3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:KJFv5EcXn9krYk19LCvlFrhMW-R4q8mKfXJXCd-RBVBgUQzCOR1Dx2FiOyU-BxUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tzrfNUr88otBGAziHASJWgyVDkhyQ3p7YbN38qpb0vGQrYNxlk4e2I";
    static const TString VALID_USER_TICKET_2 = "3:user:CA0Q__________9_GhAKAwjIAwoCCHsQyAMgEigB:KRibGYTJUA2ns0Fn7VYqeMZ1-GdscB1o9pRzELyr7QJrJsfsE8Y_HoVvB8Npr-oalv6AXOpagSc8HpZjAQz8zKMAVE_tI0tL-9DEsHirpawEbpy7OWV7-k18o1m-RaDaKeTlIB45KHbBul1-9aeKkortBfbbXtz_Qy9r_mfFPiQ";
    static const TString VALID_USER_TICKET_3 = "3:user:CA0Q__________9_Go8bCgIIAAoCCAEKAggCCgIIAwoCCAQKAggFCgIIBgoCCAcKAggICgIICQoCCAoKAggLCgIIDAoCCA0KAggOCgIIDwoCCBAKAggRCgIIEgoCCBMKAggUCgIIFQoCCBYKAggXCgIIGAoCCBkKAggaCgIIGwoCCBwKAggdCgIIHgoCCB8KAgggCgIIIQoCCCIKAggjCgIIJAoCCCUKAggmCgIIJwoCCCgKAggpCgIIKgoCCCsKAggsCgIILQoCCC4KAggvCgIIMAoCCDEKAggyCgIIMwoCCDQKAgg1CgIINgoCCDcKAgg4CgIIOQoCCDoKAgg7CgIIPAoCCD0KAgg-CgIIPwoCCEAKAghBCgIIQgoCCEMKAghECgIIRQoCCEYKAghHCgIISAoCCEkKAghKCgIISwoCCEwKAghNCgIITgoCCE8KAghQCgIIUQoCCFIKAghTCgIIVAoCCFUKAghWCgIIVwoCCFgKAghZCgIIWgoCCFsKAghcCgIIXQoCCF4KAghfCgIIYAoCCGEKAghiCgIIYwoCCGQKAghlCgIIZgoCCGcKAghoCgIIaQoCCGoKAghrCgIIbAoCCG0KAghuCgIIbwoCCHAKAghxCgIIcgoCCHMKAgh0CgIIdQoCCHYKAgh3CgIIeAoCCHkKAgh6CgIIewoCCHwKAgh9CgIIfgoCCH8KAwiAAQoDCIEBCgMIggEKAwiDAQoDCIQBCgMIhQEKAwiGAQoDCIcBCgMIiAEKAwiJAQoDCIoBCgMIiwEKAwiMAQoDCI0BCgMIjgEKAwiPAQoDCJABCgMIkQEKAwiSAQoDCJMBCgMIlAEKAwiVAQoDCJYBCgMIlwEKAwiYAQoDCJkBCgMImgEKAwibAQoDCJwBCgMInQEKAwieAQoDCJ8BCgMIoAEKAwihAQoDCKIBCgMIowEKAwikAQoDCKUBCgMIpgEKAwinAQoDCKgBCgMIqQEKAwiqAQoDCKsBCgMIrAEKAwitAQoDCK4BCgMIrwEKAwiwAQoDCLEBCgMIsgEKAwizAQoDCLQBCgMItQEKAwi2AQoDCLcBCgMIuAEKAwi5AQoDCLoBCgMIuwEKAwi8AQoDCL0BCgMIvgEKAwi_AQoDCMABCgMIwQEKAwjCAQoDCMMBCgMIxAEKAwjFAQoDCMYBCgMIxwEKAwjIAQoDCMkBCgMIygEKAwjLAQoDCMwBCgMIzQEKAwjOAQoDCM8BCgMI0AEKAwjRAQoDCNIBCgMI0wEKAwjUAQoDCNUBCgMI1gEKAwjXAQoDCNgBCgMI2QEKAwjaAQoDCNsBCgMI3AEKAwjdAQoDCN4BCgMI3wEKAwjgAQoDCOEBCgMI4gEKAwjjAQoDCOQBCgMI5QEKAwjmAQoDCOcBCgMI6AEKAwjpAQoDCOoBCgMI6wEKAwjsAQoDCO0BCgMI7gEKAwjvAQoDCPABCgMI8QEKAwjyAQoDCPMBCgMI9AEKAwj1AQoDCPYBCgMI9wEKAwj4AQoDCPkBCgMI-gEKAwj7AQoDCPwBCgMI_QEKAwj-AQoDCP8BCgMIgAIKAwiBAgoDCIICCgMIgwIKAwiEAgoDCIUCCgMIhgIKAwiHAgoDCIgCCgMIiQIKAwiKAgoDCIsCCgMIjAIKAwiNAgoDCI4CCgMIjwIKAwiQAgoDCJECCgMIkgIKAwiTAgoDCJQCCgMIlQIKAwiWAgoDCJcCCgMImAIKAwiZAgoDCJoCCgMImwIKAwicAgoDCJ0CCgMIngIKAwifAgoDCKACCgMIoQIKAwiiAgoDCKMCCgMIpAIKAwilAgoDCKYCCgMIpwIKAwioAgoDCKkCCgMIqgIKAwirAgoDCKwCCgMIrQIKAwiuAgoDCK8CCgMIsAIKAwixAgoDCLICCgMIswIKAwi0AgoDCLUCCgMItgIKAwi3AgoDCLgCCgMIuQIKAwi6AgoDCLsCCgMIvAIKAwi9AgoDCL4CCgMIvwIKAwjAAgoDCMECCgMIwgIKAwjDAgoDCMQCCgMIxQIKAwjGAgoDCMcCCgMIyAIKAwjJAgoDCMoCCgMIywIKAwjMAgoDCM0CCgMIzgIKAwjPAgoDCNACCgMI0QIKAwjSAgoDCNMCCgMI1AIKAwjVAgoDCNYCCgMI1wIKAwjYAgoDCNkCCgMI2gIKAwjbAgoDCNwCCgMI3QIKAwjeAgoDCN8CCgMI4AIKAwjhAgoDCOICCgMI4wIKAwjkAgoDCOUCCgMI5gIKAwjnAgoDCOgCCgMI6QIKAwjqAgoDCOsCCgMI7AIKAwjtAgoDCO4CCgMI7wIKAwjwAgoDCPECCgMI8gIKAwjzAgoDCPQCCgMI9QIKAwj2AgoDCPcCCgMI-AIKAwj5AgoDCPoCCgMI-wIKAwj8AgoDCP0CCgMI_gIKAwj_AgoDCIADCgMIgQMKAwiCAwoDCIMDCgMIhAMKAwiFAwoDCIYDCgMIhwMKAwiIAwoDCIkDCgMIigMKAwiLAwoDCIwDCgMIjQMKAwiOAwoDCI8DCgMIkAMKAwiRAwoDCJIDCgMIkwMKAwiUAwoDCJUDCgMIlgMKAwiXAwoDCJgDCgMImQMKAwiaAwoDCJsDCgMInAMKAwidAwoDCJ4DCgMInwMKAwigAwoDCKEDCgMIogMKAwijAwoDCKQDCgMIpQMKAwimAwoDCKcDCgMIqAMKAwipAwoDCKoDCgMIqwMKAwisAwoDCK0DCgMIrgMKAwivAwoDCLADCgMIsQMKAwiyAwoDCLMDCgMItAMKAwi1AwoDCLYDCgMItwMKAwi4AwoDCLkDCgMIugMKAwi7AwoDCLwDCgMIvQMKAwi-AwoDCL8DCgMIwAMKAwjBAwoDCMIDCgMIwwMKAwjEAwoDCMUDCgMIxgMKAwjHAwoDCMgDCgMIyQMKAwjKAwoDCMsDCgMIzAMKAwjNAwoDCM4DCgMIzwMKAwjQAwoDCNEDCgMI0gMKAwjTAwoDCNQDCgMI1QMKAwjWAwoDCNcDCgMI2AMKAwjZAwoDCNoDCgMI2wMKAwjcAwoDCN0DCgMI3gMKAwjfAwoDCOADCgMI4QMKAwjiAwoDCOMDCgMI5AMKAwjlAwoDCOYDCgMI5wMKAwjoAwoDCOkDCgMI6gMKAwjrAwoDCOwDCgMI7QMKAwjuAwoDCO8DCgMI8AMKAwjxAwoDCPIDCgMI8wMQyAMaCGJiOnNlc3MxGgliYjpzZXNzMTAaCmJiOnNlc3MxMDAaCWJiOnNlc3MxMRoJYmI6c2VzczEyGgliYjpzZXNzMTMaCWJiOnNlc3MxNBoJYmI6c2VzczE1GgliYjpzZXNzMTYaCWJiOnNlc3MxNxoJYmI6c2VzczE4GgliYjpzZXNzMTkaCGJiOnNlc3MyGgliYjpzZXNzMjAaCWJiOnNlc3MyMRoJYmI6c2VzczIyGgliYjpzZXNzMjMaCWJiOnNlc3MyNBoJYmI6c2VzczI1GgliYjpzZXNzMjYaCWJiOnNlc3MyNxoJYmI6c2VzczI4GgliYjpzZXNzMjkaCGJiOnNlc3MzGgliYjpzZXNzMzAaCWJiOnNlc3MzMRoJYmI6c2VzczMyGgliYjpzZXNzMzMaCWJiOnNlc3MzNBoJYmI6c2VzczM1GgliYjpzZXNzMzYaCWJiOnNlc3MzNxoJYmI6c2VzczM4GgliYjpzZXNzMzkaCGJiOnNlc3M0GgliYjpzZXNzNDAaCWJiOnNlc3M0MRoJYmI6c2VzczQyGgliYjpzZXNzNDMaCWJiOnNlc3M0NBoJYmI6c2VzczQ1GgliYjpzZXNzNDYaCWJiOnNlc3M0NxoJYmI6c2VzczQ4GgliYjpzZXNzNDkaCGJiOnNlc3M1GgliYjpzZXNzNTAaCWJiOnNlc3M1MRoJYmI6c2VzczUyGgliYjpzZXNzNTMaCWJiOnNlc3M1NBoJYmI6c2VzczU1GgliYjpzZXNzNTYaCWJiOnNlc3M1NxoJYmI6c2VzczU4GgliYjpzZXNzNTkaCGJiOnNlc3M2GgliYjpzZXNzNjAaCWJiOnNlc3M2MRoJYmI6c2VzczYyGgliYjpzZXNzNjMaCWJiOnNlc3M2NBoJYmI6c2VzczY1GgliYjpzZXNzNjYaCWJiOnNlc3M2NxoJYmI6c2VzczY4GgliYjpzZXNzNjkaCGJiOnNlc3M3GgliYjpzZXNzNzAaCWJiOnNlc3M3MRoJYmI6c2VzczcyGgliYjpzZXNzNzMaCWJiOnNlc3M3NBoJYmI6c2Vzczc1GgliYjpzZXNzNzYaCWJiOnNlc3M3NxoJYmI6c2Vzczc4GgliYjpzZXNzNzkaCGJiOnNlc3M4GgliYjpzZXNzODAaCWJiOnNlc3M4MRoJYmI6c2VzczgyGgliYjpzZXNzODMaCWJiOnNlc3M4NBoJYmI6c2Vzczg1GgliYjpzZXNzODYaCWJiOnNlc3M4NxoJYmI6c2Vzczg4GgliYjpzZXNzODkaCGJiOnNlc3M5GgliYjpzZXNzOTAaCWJiOnNlc3M5MRoJYmI6c2VzczkyGgliYjpzZXNzOTMaCWJiOnNlc3M5NBoJYmI6c2Vzczk1GgliYjpzZXNzOTYaCWJiOnNlc3M5NxoJYmI6c2Vzczk4GgliYjpzZXNzOTkgEigB:CX8PIOrxJnQqFXl7wAsiHJ_1VGjoI-asNlCXb8SE8jtI2vdh9x6CqbAurSgIlAAEgotVP-nuUR38x_a9YJuXzmG5AvJ458apWQtODHIDIX6ZaIwMxjS02R7S5LNqXa0gAuU_R6bCWpZdWe2uLMkdpu5KHbDgW08g-uaP_nceDOk";

    Y_UNIT_TEST(BlackboxEnvTest) {
        UNIT_ASSERT_EQUAL(int(TA_EBlackboxEnv::TA_BE_PROD), int(EBlackboxEnv::Prod));
        UNIT_ASSERT_EQUAL(int(TA_EBlackboxEnv::TA_BE_TEST), int(EBlackboxEnv::Test));
        UNIT_ASSERT_EQUAL(int(TA_EBlackboxEnv::TA_BE_PROD_YATEAM), int(EBlackboxEnv::ProdYateam));
        UNIT_ASSERT_EQUAL(int(TA_EBlackboxEnv::TA_BE_TEST_YATEAM), int(EBlackboxEnv::TestYateam));
        UNIT_ASSERT_EQUAL(int(TA_EBlackboxEnv::TA_BE_STRESS), int(EBlackboxEnv::Stress));
    }

    Y_UNIT_TEST(ContextTest) {
        TA_TUserContext* context;
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CreateUserContext(TA_EBlackboxEnv::TA_BE_TEST, NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_DeleteUserContext(context));
    }

    Y_UNIT_TEST(ContextErrorsTest) {
        TA_TUserContext* context;
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_MALFORMED_TVM_KEYS,
            TA_CreateUserContext(TA_EBlackboxEnv::TA_BE_PROD, MALFORMED_TVM_KEYS.c_str(), MALFORMED_TVM_KEYS.size(), &context));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_EMPTY_TVM_KEYS,
            TA_CreateUserContext(TA_EBlackboxEnv::TA_BE_PROD, EMPTY_TVM_KEYS.c_str(), EMPTY_TVM_KEYS.size(), &context));
    }

    Y_UNIT_TEST(Ticket1Test) {
        int checking;
        TA_TUserContext* context;
        char debugInfo[512];
        size_t debugInfoSize;
        uint64_t defaultUid;
        const char* firstScope;
        uint64_t firstUid;
        size_t scopesCount;
        const char* secondScope;
        uint64_t secondUid;
        TA_TCheckedUserTicket* ticket;
        size_t uidsCount;

        TA_CreateUserContext(TA_EBlackboxEnv::TA_BE_TEST, NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckUserTicket(context, VALID_USER_TICKET_1.c_str(), VALID_USER_TICKET_1.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketDefaultUid(ticket, &defaultUid));
        UNIT_ASSERT_VALUES_EQUAL(456, defaultUid);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketScopesCount(ticket, &scopesCount));
        UNIT_ASSERT_VALUES_EQUAL(2, scopesCount);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketScope(ticket, 0, &firstScope));
        UNIT_ASSERT_VALUES_EQUAL("bb:sess1", firstScope);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketScope(ticket, 1, &secondScope));
        UNIT_ASSERT_VALUES_EQUAL("bb:sess2", secondScope);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketUidsCount(ticket, &uidsCount));
        UNIT_ASSERT_EQUAL(2, uidsCount);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketUid(ticket, 0, &firstUid));
        UNIT_ASSERT_EQUAL(456, firstUid);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketUid(ticket, 1, &secondUid));
        UNIT_ASSERT_EQUAL(123, secondUid);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_HasUserTicketScope(ticket, "bb:sess1", 8, &checking));
        UNIT_ASSERT(checking);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_HasUserTicketScope(ticket, "bb:sess2", 8, &checking));
        UNIT_ASSERT(checking);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_HasUserTicketScope(ticket, "bb:sess3", 8, &checking));
        UNIT_ASSERT(!checking);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 512));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess2;default_uid=456;uid=456;uid=123;env=Test;",
            TStringBuf(debugInfo, debugInfoSize));
        TA_DeleteUserTicket(ticket);
        TA_DeleteUserContext(context);
    }

    Y_UNIT_TEST(Ticket2Test) {
        TA_TUserContext* context;
        char debugInfo[512];
        size_t debugInfoSize;
        size_t scopesCount;
        TA_TCheckedUserTicket* ticket;
        size_t uidsCount;

        TA_CreateUserContext(TA_EBlackboxEnv::TA_BE_TEST, NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckUserTicket(context, VALID_USER_TICKET_2.c_str(), VALID_USER_TICKET_2.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketScopesCount(ticket, &scopesCount));
        UNIT_ASSERT_VALUES_EQUAL(0, scopesCount);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketUidsCount(ticket, &uidsCount));
        UNIT_ASSERT_VALUES_EQUAL(2, uidsCount);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 512));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=user;expiration_time=9223372036854775807;default_uid=456;uid=456;uid=123;env=Test;",
            TStringBuf(debugInfo, debugInfoSize));
        TA_DeleteUserTicket(ticket);
        TA_DeleteUserContext(context);
    }

    Y_UNIT_TEST(Ticket3Test) {
        TA_TUserContext* context;
        char debugInfo[8192];
        size_t debugInfoSize;
        size_t scopesCount;
        TA_TCheckedUserTicket* ticket;
        size_t uidsCount;

        TA_CreateUserContext(TA_EBlackboxEnv::TA_BE_TEST, NUnittest::TVMKNIFE_PUBLIC_KEYS.c_str(), NUnittest::TVMKNIFE_PUBLIC_KEYS.size(), &context);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_CheckUserTicket(context, VALID_USER_TICKET_3.c_str(), VALID_USER_TICKET_3.size(), &ticket));
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketScopesCount(ticket, &scopesCount));
        UNIT_ASSERT_VALUES_EQUAL(100, scopesCount);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketUidsCount(ticket, &uidsCount));
        UNIT_ASSERT_EQUAL(500, uidsCount);
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_GetUserTicketDebugInfo(ticket, debugInfo, &debugInfoSize, 8192));
        UNIT_ASSERT_VALUES_EQUAL(
            "ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess10;scope=bb:sess100;scope=bb:sess11;scope=bb:sess12;scope=bb:sess13;scope=bb:sess14;scope=bb:sess15;scope=bb:sess16;scope=bb:sess17;scope=bb:sess18;scope=bb:sess19;scope=bb:sess2;scope=bb:sess20;scope=bb:sess21;scope=bb:sess22;scope=bb:sess23;scope=bb:sess24;scope=bb:sess25;scope=bb:sess26;scope=bb:sess27;scope=bb:sess28;scope=bb:sess29;scope=bb:sess3;scope=bb:sess30;scope=bb:sess31;scope=bb:sess32;scope=bb:sess33;scope=bb:sess34;scope=bb:sess35;scope=bb:sess36;scope=bb:sess37;scope=bb:sess38;scope=bb:sess39;scope=bb:sess4;scope=bb:sess40;scope=bb:sess41;scope=bb:sess42;scope=bb:sess43;scope=bb:sess44;scope=bb:sess45;scope=bb:sess46;scope=bb:sess47;scope=bb:sess48;scope=bb:sess49;scope=bb:sess5;scope=bb:sess50;scope=bb:sess51;scope=bb:sess52;scope=bb:sess53;scope=bb:sess54;scope=bb:sess55;scope=bb:sess56;scope=bb:sess57;scope=bb:sess58;scope=bb:sess59;scope=bb:sess6;scope=bb:sess60;scope=bb:sess61;scope=bb:sess62;scope=bb:sess63;scope=bb:sess64;scope=bb:sess65;scope=bb:sess66;scope=bb:sess67;scope=bb:sess68;scope=bb:sess69;scope=bb:sess7;scope=bb:sess70;scope=bb:sess71;scope=bb:sess72;scope=bb:sess73;scope=bb:sess74;scope=bb:sess75;scope=bb:sess76;scope=bb:sess77;scope=bb:sess78;scope=bb:sess79;scope=bb:sess8;scope=bb:sess80;scope=bb:sess81;scope=bb:sess82;scope=bb:sess83;scope=bb:sess84;scope=bb:sess85;scope=bb:sess86;scope=bb:sess87;scope=bb:sess88;scope=bb:sess89;scope=bb:sess9;scope=bb:sess90;scope=bb:sess91;scope=bb:sess92;scope=bb:sess93;scope=bb:sess94;scope=bb:sess95;scope=bb:sess96;scope=bb:sess97;scope=bb:sess98;scope=bb:sess99;default_uid=456;uid=0;uid=1;uid=2;uid=3;uid=4;uid=5;uid=6;uid=7;uid=8;uid=9;uid=10;uid=11;uid=12;uid=13;uid=14;uid=15;uid=16;uid=17;uid=18;uid=19;uid=20;uid=21;uid=22;uid=23;uid=24;uid=25;uid=26;uid=27;uid=28;uid=29;uid=30;uid=31;uid=32;uid=33;uid=34;uid=35;uid=36;uid=37;uid=38;uid=39;uid=40;uid=41;uid=42;uid=43;uid=44;uid=45;uid=46;uid=47;uid=48;uid=49;uid=50;uid=51;uid=52;uid=53;uid=54;uid=55;uid=56;uid=57;uid=58;uid=59;uid=60;uid=61;uid=62;uid=63;uid=64;uid=65;uid=66;uid=67;uid=68;uid=69;uid=70;uid=71;uid=72;uid=73;uid=74;uid=75;uid=76;uid=77;uid=78;uid=79;uid=80;uid=81;uid=82;uid=83;uid=84;uid=85;uid=86;uid=87;uid=88;uid=89;uid=90;uid=91;uid=92;uid=93;uid=94;uid=95;uid=96;uid=97;uid=98;uid=99;uid=100;uid=101;uid=102;uid=103;uid=104;uid=105;uid=106;uid=107;uid=108;uid=109;uid=110;uid=111;uid=112;uid=113;uid=114;uid=115;uid=116;uid=117;uid=118;uid=119;uid=120;uid=121;uid=122;uid=123;uid=124;uid=125;uid=126;uid=127;uid=128;uid=129;uid=130;uid=131;uid=132;uid=133;uid=134;uid=135;uid=136;uid=137;uid=138;uid=139;uid=140;uid=141;uid=142;uid=143;uid=144;uid=145;uid=146;uid=147;uid=148;uid=149;uid=150;uid=151;uid=152;uid=153;uid=154;uid=155;uid=156;uid=157;uid=158;uid=159;uid=160;uid=161;uid=162;uid=163;uid=164;uid=165;uid=166;uid=167;uid=168;uid=169;uid=170;uid=171;uid=172;uid=173;uid=174;uid=175;uid=176;uid=177;uid=178;uid=179;uid=180;uid=181;uid=182;uid=183;uid=184;uid=185;uid=186;uid=187;uid=188;uid=189;uid=190;uid=191;uid=192;uid=193;uid=194;uid=195;uid=196;uid=197;uid=198;uid=199;uid=200;uid=201;uid=202;uid=203;uid=204;uid=205;uid=206;uid=207;uid=208;uid=209;uid=210;uid=211;uid=212;uid=213;uid=214;uid=215;uid=216;uid=217;uid=218;uid=219;uid=220;uid=221;uid=222;uid=223;uid=224;uid=225;uid=226;uid=227;uid=228;uid=229;uid=230;uid=231;uid=232;uid=233;uid=234;uid=235;uid=236;uid=237;uid=238;uid=239;uid=240;uid=241;uid=242;uid=243;uid=244;uid=245;uid=246;uid=247;uid=248;uid=249;uid=250;uid=251;uid=252;uid=253;uid=254;uid=255;uid=256;uid=257;uid=258;uid=259;uid=260;uid=261;uid=262;uid=263;uid=264;uid=265;uid=266;uid=267;uid=268;uid=269;uid=270;uid=271;uid=272;uid=273;uid=274;uid=275;uid=276;uid=277;uid=278;uid=279;uid=280;uid=281;uid=282;uid=283;uid=284;uid=285;uid=286;uid=287;uid=288;uid=289;uid=290;uid=291;uid=292;uid=293;uid=294;uid=295;uid=296;uid=297;uid=298;uid=299;uid=300;uid=301;uid=302;uid=303;uid=304;uid=305;uid=306;uid=307;uid=308;uid=309;uid=310;uid=311;uid=312;uid=313;uid=314;uid=315;uid=316;uid=317;uid=318;uid=319;uid=320;uid=321;uid=322;uid=323;uid=324;uid=325;uid=326;uid=327;uid=328;uid=329;uid=330;uid=331;uid=332;uid=333;uid=334;uid=335;uid=336;uid=337;uid=338;uid=339;uid=340;uid=341;uid=342;uid=343;uid=344;uid=345;uid=346;uid=347;uid=348;uid=349;uid=350;uid=351;uid=352;uid=353;uid=354;uid=355;uid=356;uid=357;uid=358;uid=359;uid=360;uid=361;uid=362;uid=363;uid=364;uid=365;uid=366;uid=367;uid=368;uid=369;uid=370;uid=371;uid=372;uid=373;uid=374;uid=375;uid=376;uid=377;uid=378;uid=379;uid=380;uid=381;uid=382;uid=383;uid=384;uid=385;uid=386;uid=387;uid=388;uid=389;uid=390;uid=391;uid=392;uid=393;uid=394;uid=395;uid=396;uid=397;uid=398;uid=399;uid=400;uid=401;uid=402;uid=403;uid=404;uid=405;uid=406;uid=407;uid=408;uid=409;uid=410;uid=411;uid=412;uid=413;uid=414;uid=415;uid=416;uid=417;uid=418;uid=419;uid=420;uid=421;uid=422;uid=423;uid=424;uid=425;uid=426;uid=427;uid=428;uid=429;uid=430;uid=431;uid=432;uid=433;uid=434;uid=435;uid=436;uid=437;uid=438;uid=439;uid=440;uid=441;uid=442;uid=443;uid=444;uid=445;uid=446;uid=447;uid=448;uid=449;uid=450;uid=451;uid=452;uid=453;uid=454;uid=455;uid=456;uid=457;uid=458;uid=459;uid=460;uid=461;uid=462;uid=463;uid=464;uid=465;uid=466;uid=467;uid=468;uid=469;uid=470;uid=471;uid=472;uid=473;uid=474;uid=475;uid=476;uid=477;uid=478;uid=479;uid=480;uid=481;uid=482;uid=483;uid=484;uid=485;uid=486;uid=487;uid=488;uid=489;uid=490;uid=491;uid=492;uid=493;uid=494;uid=495;uid=496;uid=497;uid=498;uid=499;env=Test;",
            TStringBuf(debugInfo, debugInfoSize));
        TA_DeleteUserTicket(ticket);
        TA_DeleteUserContext(context);
    }

    Y_UNIT_TEST(RemoveSignatureTest) {
        const char* removedSignature;
        size_t removedSignatureSize;
        UNIT_ASSERT_EQUAL(
            TA_EErrorCode::TA_EC_OK,
            TA_RemoveTicketSignature(VALID_USER_TICKET_1.c_str(), VALID_USER_TICKET_1.size(), &removedSignature, &removedSignatureSize));
        UNIT_ASSERT_VALUES_EQUAL(
            "3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:",
            TStringBuf(removedSignature, removedSignatureSize));
    }
}
