#!/usr/bin/env python
from __future__ import print_function

import pytest
from ticket_parser2 import (
    ServiceTicket,
    Status,
)
from ticket_parser2.low_level import ServiceContext
from ticket_parser2.exceptions import (
    ContextException,
    EmptyTvmKeysException,
    MalformedTvmKeysException,
    MalformedTvmSecretException,
    TicketParsingException,
)
import ticket_parser2.unittest as tp2u


EMPTY_TVM_KEYS = (
    '1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPL'
    'lhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWN'
    't4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1'
    'z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAg'
    'gCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDo'
    'rWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIc'
    'Nrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbw'
    'W2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGAT'
    'CBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGU'
    'v1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEB'
    'CAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPg'
    'ZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBh'
    'ADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLG'
    'gzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq'
    '1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-h'
    'I55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf'
    '33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8'
    'gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcL'
    'nkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAn'
    'l5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZ'
    'JQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I'
    '8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3'
    'N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRv'
    'qpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR'
    '4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAE'
)
INVALID_SERVICE_TICKET = (
    '3:serv:CBAQ__________9_czEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a4rN-D7JfXHK1Ai4wM4uS'
    'fboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6PpxL5cn0xCksiStILH5U'
    'mDR6xfkJdnmMG94o8'
)
MALFORMED_TVM_KEYS = (
    '1:CpgCCpMCCAEQABqIAjCCAQQCggEAcLEXeH67FQESFUn4_7wnX7wN0PUrBoUsm3QQ4W5vC-qz6sXaEjSwnTV8w1o-z6X9KPL'
    'lhzMQvuS38NCNfK4uvJ4Zvfp3YsXJ25-rYtbnrYJHNvHohD-kPCCw_yZpMp21JdWigzQGuV7CtrxUhF-NNrsnUaJrE5-OpEWN'
    't4X6nCItKIYeVcSK6XJUbEWbrNCRbvkSc4ak2ymFeMuHYJVjxh4eQbk7_ZPzodP0WvF6eUYrYeb42imVEOR8ofVLQWE5DVnb1'
    'z_TqZm4i1XkS7jMwZuBxBRw8DGdYei0lT_sAf7KST2jC0590NySB3vsBgWEVs1OdUUWA6r-Dvx9dsOQtSCVkQYQAAqZAgqUAg'
    'gCEAAaiQIwggEFAoIBAQDhEBM5-6YsPWfogKtbluJoCX1WV2KdzOaQ0-OlRbBzeCzw-eQKu12c8WakHBbeCMd1I1TU64SDkDo'
    'rWjXGIa_2xT6N3zzNAE50roTbPCcmeQrps26woTYfYIuqDdoxYKZNr0lvNLLW47vBr7EKqo1S4KSj7aXK_XYeEvUgIgf3nVIc'
    'Nrio7VTnFmGGVQCepaL1Hi1gN4yIXjVZ06PBPZ-DxSRu6xOGbFrfKMJeMPs7KOyE-26Q3xOXdTIa1X-zYIucTd_bxUCL4BVbw'
    'W2AvbbFsaG7ISmVdGu0XUTmhXs1KrEfUVLRJhE4Dx99hAZXm1_HlYMUeJcMQ_oHOhV94ENFIJaRBhACCpYBCpEBCAMQABqGAT'
    'CBgwKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGU'
    'v1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NIJeRBhABCpYBCpEB'
    'CAQQABqGATCBgwKBgGB4d6eLGUBv-Q6EPLehC4S-yuE2HB-_rJ7WkeYwyp-xIPolPrd-PQme2utHB4ZgpXHIu_OFksDe_0bPg'
    'ZniNRSVRbl7W49DgS5Ya3kMfrYB4DnF5Fta5tn1oV6EwxYD4JONpFTenOJALPGTPawxXEfon_peiHOSBuQMu3_Vn-l1IJiRBh'
    'ADCpcBCpIBCAUQABqHATCBhAKBgQCTJMKIfmfeZpaI7Q9rnsc29gdWawK7TnpVKRHws1iY7EUlYROeVcMdAwEqVM6f8BVCKLG'
    'gzQ7Gar_uuxfUGKwqEQzoppDraw4F75J464-7D5f6_oJQuGIBHZxqbMONtLjBCXRUhQW5szBLmTQ_R3qaJb5vf-h0APZfkYhq'
    '1cTttSCZkQYQBAqWAQqRAQgLEAAahgEwgYMCgYBvvGVH_M2H8qxxv94yaDYUTWbRnJ1uiIYc59KIQlfFimMPhSS7x2tqUa2-h'
    'I55JiII0Xym6GNkwLhyc1xtWChpVuIdSnbvttbrt4weDMLHqTwNOF6qAsVKGKT1Yh8yf-qb-DSmicgvFc74mBQm_6gAY1iQsf'
    '33YX8578ClhKBWHSCVkQYQAAqXAQqSAQgMEAAahwEwgYQCgYEAkuzFcd5TJu7lYWYe2hQLFfUWIIj91BvQQLa_Thln4YtGCO8'
    'gG1KJqJm-YlmJOWQG0B7H_5RVhxUxV9KpmFnsDVkzUFKOsCBaYGXc12xPVioawUlAwp5qp3QQtZyx_se97YIoLzuLr46UkLcL'
    'nkIrp-Jo46QzYi_QHq45WTm8MQ0glpEGEAIKlwEKkgEIDRAAGocBMIGEAoGBAIUzbxOknXf_rNt17_ir8JlWvrtnCWsQd1MAn'
    'l5mgArvavDtKeBYHzi5_Ak7DHlLzuA6YE8W175FxLFKpN2hkz-l-M7ltUSd8N1BvJRhK4t6WffWfC_1wPyoAbeSN2Yb1jygtZ'
    'JQ8wGoXHcJQUXiMit3eFNyylwsJFj1gzAR4JCdIJeRBhABCpYBCpEBCA4QABqGATCBgwKBgFMcbEpl9ukVR6AO_R6sMyiU11I'
    '8b8MBSUCEC15iKsrVO8v_m47_TRRjWPYtQ9eZ7o1ocNJHaGUU7qqInFqtFaVnIceP6NmCsXhjs3MLrWPS8IRAy4Zf4FKmGOx3'
    'N9O2vemjUygZ9vUiSkULdVrecinRaT8JQ5RG4bUMY04XGIwFIJiRBhADCpYBCpEBCA8QABqGATCBgwKBgGpCkW-NR3li8GlRv'
    'qpq2YZGSIgm_PTyDI2Zwfw69grsBmPpVFW48Vw7xoMN35zcrojEpialB_uQzlpLYOvsMl634CRIuj-n1QE3-gaZTTTE8mg-AR'
    '4mcxnTKThPnRQpbuOlYAnriwiasWiQEMbGjq_HmWioYYxFo9USlklQn4-9IJmRBhAEEpUBCpIBCAYQABqHATCBhAKBgQCoZkF'
    'Gm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFBIRuTkYxhs4vUYnWjZrKRAd5bp6_py0csmFmpl_5Yh0b-2pdo_E5PNP7LGRzKy'
    'KSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr-TPu34DnJq3yv2a6dqiKL9zSCakQYSlQEKkgEIEBAAGocBMIGEA'
    'oGBALhrihbf3EpjDQS2sCQHazoFgN0nBbE9eesnnFTfzQELXb2gnJU9enmV_aDqaHKjgtLIPpCgn40lHrn5k6mvH5OdedyI6c'
    'CzE-N-GFp3nAq0NDJyMe0fhtIRD__CbT0ulcvkeow65ubXWfw6dBC2gR_34rdMe_L_TGRLMWjDULbNIJ'
)
MALFORMED_TVM_SECRET = 'adcvxcv./-+'
OUR_ID = 28
SECRET = 'GRMJrKnj4fOVnvOqe-WyD1'
SRC_ID = 229

UNSUPPORTED_VERSION_SERVICE_TICKET = (
    '2:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a'
    '4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6'
    'PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8'
)
VALID_SERVICE_TICKET_1 = (
    '3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a'
    '4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6'
    'PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8'
)
VALID_SERVICE_TICKET_SIGNLESS_1 = '3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:'
VALID_SERVICE_TICKET_2 = (
    '3:serv:CBAQ__________9_IskICOUBEBwaCGJiOnNlc3MxGgliYjpzZXNzMTAaCmJiOnNlc3MxMDAaCWJiOnNlc3MxMRoJYm'
    'I6c2VzczEyGgliYjpzZXNzMTMaCWJiOnNlc3MxNBoJYmI6c2VzczE1GgliYjpzZXNzMTYaCWJiOnNlc3MxNxoJYmI6c2VzczE'
    '4GgliYjpzZXNzMTkaCGJiOnNlc3MyGgliYjpzZXNzMjAaCWJiOnNlc3MyMRoJYmI6c2VzczIyGgliYjpzZXNzMjMaCWJiOnNl'
    'c3MyNBoJYmI6c2VzczI1GgliYjpzZXNzMjYaCWJiOnNlc3MyNxoJYmI6c2VzczI4GgliYjpzZXNzMjkaCGJiOnNlc3MzGgliY'
    'jpzZXNzMzAaCWJiOnNlc3MzMRoJYmI6c2VzczMyGgliYjpzZXNzMzMaCWJiOnNlc3MzNBoJYmI6c2VzczM1GgliYjpzZXNzMz'
    'YaCWJiOnNlc3MzNxoJYmI6c2VzczM4GgliYjpzZXNzMzkaCGJiOnNlc3M0GgliYjpzZXNzNDAaCWJiOnNlc3M0MRoJYmI6c2V'
    'zczQyGgliYjpzZXNzNDMaCWJiOnNlc3M0NBoJYmI6c2VzczQ1GgliYjpzZXNzNDYaCWJiOnNlc3M0NxoJYmI6c2VzczQ4Ggli'
    'YjpzZXNzNDkaCGJiOnNlc3M1GgliYjpzZXNzNTAaCWJiOnNlc3M1MRoJYmI6c2VzczUyGgliYjpzZXNzNTMaCWJiOnNlc3M1N'
    'BoJYmI6c2VzczU1GgliYjpzZXNzNTYaCWJiOnNlc3M1NxoJYmI6c2VzczU4GgliYjpzZXNzNTkaCGJiOnNlc3M2GgliYjpzZX'
    'NzNjAaCWJiOnNlc3M2MRoJYmI6c2VzczYyGgliYjpzZXNzNjMaCWJiOnNlc3M2NBoJYmI6c2VzczY1GgliYjpzZXNzNjYaCWJ'
    'iOnNlc3M2NxoJYmI6c2VzczY4GgliYjpzZXNzNjkaCGJiOnNlc3M3GgliYjpzZXNzNzAaCWJiOnNlc3M3MRoJYmI6c2Vzczcy'
    'GgliYjpzZXNzNzMaCWJiOnNlc3M3NBoJYmI6c2Vzczc1GgliYjpzZXNzNzYaCWJiOnNlc3M3NxoJYmI6c2Vzczc4GgliYjpzZ'
    'XNzNzkaCGJiOnNlc3M4GgliYjpzZXNzODAaCWJiOnNlc3M4MRoJYmI6c2VzczgyGgliYjpzZXNzODMaCWJiOnNlc3M4NBoJYm'
    'I6c2Vzczg1GgliYjpzZXNzODYaCWJiOnNlc3M4NxoJYmI6c2Vzczg4GgliYjpzZXNzODkaCGJiOnNlc3M5GgliYjpzZXNzOTA'
    'aCWJiOnNlc3M5MRoJYmI6c2VzczkyGgliYjpzZXNzOTMaCWJiOnNlc3M5NBoJYmI6c2Vzczk1GgliYjpzZXNzOTYaCWJiOnNl'
    'c3M5NxoJYmI6c2Vzczk4GgliYjpzZXNzOTk:JYmABAVLM6y7_T4n1pRcwBfwDfzMV4JJ3cpbEG617zdGgKRZwL7MalsYn5bq1'
    'F2ibujMrsF9nzZf8l4s_e-Ivjkz_xu4KMzSp-pUh9V7XIF_smj0WHYpv6gOvWNuK8uIvlZTTKwtQX0qZOL9m-MEeZiHoQPKZG'
    'CfJ_qxMUp-J8I'
)
VALID_SERVICE_TICKET_3 = (
    '3:serv:CBAQ__________9_IgUI5QEQHA:Sd6tmA1CNy2Nf7XevC3x7zr2DrGNRmcl-TxUsDtDW2xI3YXyCxBltWeg0-KtDlq'
    'yYuPOP5Jd_-XXNA12KlOPnNzrz3jm-5z8uQl6CjCcrVHUHJ75pGC8r9UOlS8cOgeXQB5dYP-fOWyo5CNadlozx1S2meCIxncb'
    'QRV1kCBi4KU'
)
VALID_SERVICE_TICKET_ISSUER = (
    '3:serv:CBAQ__________9_IgsI5QEQHCDr1MT4Ag:Gu66XJT_nKnIRJjFy1561wFhIqkJItcSTGftLo7Yvi7i5wIdV-QuKT_'
    '-IMPpgjxnnGbt1Dy3Ys2TEoeJAb0TdaCYG1uy3vpoLONmTx9AenN5dx1HHf46cypLK5D3OdiTjxvqI9uGmSIKrSdRxU8gprpu'
    '5QiBDPZqVCWhM60FVSY'
)


def test_context():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    service_context.reset_keys(tp2u.TVMKNIFE_PUBLIC_KEYS)


def test_context_exceptions():
    with pytest.raises(MalformedTvmSecretException):
        ServiceContext(OUR_ID, MALFORMED_TVM_SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    with pytest.raises(MalformedTvmKeysException):
        ServiceContext(OUR_ID, SECRET, MALFORMED_TVM_KEYS)
    with pytest.raises(EmptyTvmKeysException):
        ServiceContext(OUR_ID, SECRET, EMPTY_TVM_KEYS)

    service_context = ServiceContext(OUR_ID, None, tp2u.TVMKNIFE_PUBLIC_KEYS)
    with pytest.raises(MalformedTvmSecretException):
        service_context.sign(1490000001, 13)

    service_context = ServiceContext(OUR_ID, SECRET, None)
    with pytest.raises(EmptyTvmKeysException):
        service_context.check('abcde')

    with pytest.raises(ContextException):
        service_context = ServiceContext(OUR_ID, None, None)


def test_context_sign():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    assert '6H8RjdP4cCrTpMEd3XArBTrKFMQbgXLHbB2FJgQ-yO0' == service_context.sign('1490000001', '13,19', 'bb:sess1')
    assert 'HAes0pEg8wb9M9YmKWPjwxm91mDp-GMTruOb6bzmuRE' == service_context.sign(
        1490000001, [13, 19], ['bb:sess1', 'bb:sess2']
    )
    assert 'JU5tIwr3qS1K4dse2KafQzRXX_TGtlS3jE1inK7QyRM' == service_context.sign(1490000001, 13, [])
    assert 'JU5tIwr3qS1K4dse2KafQzRXX_TGtlS3jE1inK7QyRM' == service_context.sign(1490000001, 13)


def test_ticket1():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    ticket = service_context.check(VALID_SERVICE_TICKET_1)
    assert ticket.src == SRC_ID
    assert ticket.scopes == ['bb:sess1', 'bb:sess2']
    assert ticket.has_scope('bb:sess1')
    assert ticket.has_scope('bb:sess2')
    assert not ticket.has_scope('bb:sess3')
    assert (
        ticket.debug_info()
        == 'ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess2;'
    )
    assert VALID_SERVICE_TICKET_SIGNLESS_1 == ServiceTicket.remove_signature(VALID_SERVICE_TICKET_1)
    assert ticket.issuer_uid is None
    assert (
        repr(ticket)
        == 'ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess2;'
    )
    assert (
        str(ticket)
        == 'ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess2;'
    )


def test_ticket2():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    ticket = service_context.check(VALID_SERVICE_TICKET_2)
    assert (
        ticket.debug_info()
        == 'ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;scope=bb:sess1;scope=bb:sess10;scope=bb:sess100;scope=bb:sess11;scope=bb:sess12;scope=bb:sess13;scope=bb:sess14;scope=bb:sess15;scope=bb:sess16;scope=bb:sess17;scope=bb:sess18;scope=bb:sess19;scope=bb:sess2;scope=bb:sess20;scope=bb:sess21;scope=bb:sess22;scope=bb:sess23;scope=bb:sess24;scope=bb:sess25;scope=bb:sess26;scope=bb:sess27;scope=bb:sess28;scope=bb:sess29;scope=bb:sess3;scope=bb:sess30;scope=bb:sess31;scope=bb:sess32;scope=bb:sess33;scope=bb:sess34;scope=bb:sess35;scope=bb:sess36;scope=bb:sess37;scope=bb:sess38;scope=bb:sess39;scope=bb:sess4;scope=bb:sess40;scope=bb:sess41;scope=bb:sess42;scope=bb:sess43;scope=bb:sess44;scope=bb:sess45;scope=bb:sess46;scope=bb:sess47;scope=bb:sess48;scope=bb:sess49;scope=bb:sess5;scope=bb:sess50;scope=bb:sess51;scope=bb:sess52;scope=bb:sess53;scope=bb:sess54;scope=bb:sess55;scope=bb:sess56;scope=bb:sess57;scope=bb:sess58;scope=bb:sess59;scope=bb:sess6;scope=bb:sess60;scope=bb:sess61;scope=bb:sess62;scope=bb:sess63;scope=bb:sess64;scope=bb:sess65;scope=bb:sess66;scope=bb:sess67;scope=bb:sess68;scope=bb:sess69;scope=bb:sess7;scope=bb:sess70;scope=bb:sess71;scope=bb:sess72;scope=bb:sess73;scope=bb:sess74;scope=bb:sess75;scope=bb:sess76;scope=bb:sess77;scope=bb:sess78;scope=bb:sess79;scope=bb:sess8;scope=bb:sess80;scope=bb:sess81;scope=bb:sess82;scope=bb:sess83;scope=bb:sess84;scope=bb:sess85;scope=bb:sess86;scope=bb:sess87;scope=bb:sess88;scope=bb:sess89;scope=bb:sess9;scope=bb:sess90;scope=bb:sess91;scope=bb:sess92;scope=bb:sess93;scope=bb:sess94;scope=bb:sess95;scope=bb:sess96;scope=bb:sess97;scope=bb:sess98;scope=bb:sess99;'  # noqa
    )
    assert ticket.issuer_uid is None


def test_ticket3():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    ticket = service_context.check(VALID_SERVICE_TICKET_3)
    assert ticket.debug_info() == 'ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;'
    assert ticket.issuer_uid is None


def test_ticket_issuer():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    ticket = service_context.check(VALID_SERVICE_TICKET_ISSUER)
    assert (
        ticket.debug_info()
        == 'ticket_type=serv;expiration_time=9223372036854775807;src=229;dst=28;issuer_uid=789654123;'
    )
    assert 789654123 == ticket.issuer_uid


def test_ticket_exceptions():
    service_context = ServiceContext(OUR_ID, SECRET, tp2u.TVMKNIFE_PUBLIC_KEYS)
    service_context.reset_keys(tp2u.TVMKNIFE_PUBLIC_KEYS)
    with pytest.raises(TicketParsingException) as ex:
        service_context.check(INVALID_SERVICE_TICKET)
    assert ex.value.status == Status.Malformed

    with pytest.raises(TicketParsingException) as ex:
        service_context.check(UNSUPPORTED_VERSION_SERVICE_TICKET)
    assert ex.value.status == Status.UnsupportedVersion


def test_create_ticket_for_tests():
    with pytest.raises(TicketParsingException):
        tp2u.create_service_ticket_for_unittest(Status.Expired, 42)
    s = tp2u.create_service_ticket_for_unittest(Status.Ok, 42)
    assert s
    assert s.src == 42
    assert s.issuer_uid is None
    assert s.debug_info() == 'ticket_type=serv;src=42;dst=100500;'

    s = tp2u.create_service_ticket_for_unittest(Status.Ok, 42, 100501)
    assert s
    assert s.src == 42
    assert s.issuer_uid == 100501
    assert s.debug_info() == 'ticket_type=serv;src=42;dst=100500;issuer_uid=100501;'
