#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import pytest
import six
from tvmauth import (
    BlackboxEnv,
    CheckedUserTicket,
    TicketStatus,
)
from tvmauth.deprecated import UserContext
from tvmauth.exceptions import (
    EmptyTvmKeysException,
    MalformedTvmKeysException,
    TicketParsingException,
)
import tvmauth.unittest as tau
import tvmauth.utils


EMPTY_TVM_KEYS = (
    '1:EpUBCpIBCAYQABqHATCBhAKBgQCoZkFGm9oLTqjeXZAq6j5S6i7K20V0lNdBBLqfmFBIRuTkYxhs4vUYnWjZrKRAd5bp6_p'
    'y0csmFmpl_5Yh0b-2pdo_E5PNP7LGRzKyKSiFddyykKKzVOazH8YYldDAfE8Z5HoS9e48an5JsPg0jr-TPu34DnJq3yv2a6dq'
    'iKL9zSCakQY'
)
EXPIRED_USER_TICKET = (
    '3:user:CA0QABokCgMIyAMKAgh7EMgDGghiYjpzZXNzMRoIYmI6c2VzczIgEigB:D0CmYVwWg91LDYejjeQ2UP8AeiA_mr1q1'
    'CUD_lfJ9zQSEYEOYGDTafg4Um2rwOOvQnsD1JHM4zHyMUJ6Jtp9GAm5pmhbXBBZqaCcJpyxLTEC8a81MhJFCCJRvu_G1FiAgR'
    'gB25gI3HIbkvHFUEqAIC_nANy7NFQnbKk2S-EQPGY'
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
MALFORMED_USER_TICKET = (
    '3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxcXn9krYk19LCvlFrhMW-R4q8mKfXJXCd-RBVBgUQzC'
    'OR1Dx2FiOyU-BxUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tzrfNUr88otBGAziHASJWgyVDkhy'
    'Q3p7YbN38qpb0vGQrYNxlk4e2I'
)
SIGN_BROKEN_USER_TICKET = (
    '3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:KJFv5EcXn9krYk19LCvlFr'
    'hMW-R4q8mKfXJXCd-RBVBgUQzCOR1Dx2FiOyU-BxUoIsaU0PiwI2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tzrfNUr8'
    '8otBGAziHASJWgyVDkhyQ3p7YbN38qpb0vGQrYNxlk4e2'
)
UNSUPPORTED_VERSION_USER_TICKET = (
    '2:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:KJFv5EcXn9krYk19LCvlFr'
    'hMW-R4q8mKfXJXCd-RBVBgUQzCOR1Dx2FiOyU-BxUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tz'
    'rfNUr88otBGAziHASJWgyVDkhyQ3p7YbN38qpb0vGQrYNxlk4e2I'
)
VALID_SERVICE_TICKET = (
    '3:serv:CBAQ__________9_IhkI5QEQHBoIYmI6c2VzczEaCGJiOnNlc3My:WUPx1cTf05fjD1exB35T5j2DCHWH1YaLJon_a'
    '4rN-D7JfXHK1Ai4wM4uSfboHD9xmGQH7extqtlEk1tCTCGm5qbRVloJwWzCZBXo3zKX6i1oBYP_89WcjCNPVe1e8jwGdLsnu6'
    'PpxL5cn0xCksiStILH5UmDR6xfkJdnmMG94o8'
)
VALID_USER_TICKET_1 = (
    '3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:KJFv5EcXn9krYk19LCvlFr'
    'hMW-R4q8mKfXJXCd-RBVBgUQzCOR1Dx2FiOyU-BxUoIsaU0PiwTjbVY5I2onJDilge70Cl5zEPI9pfab2qwklACq_ZBUvD1tz'
    'rfNUr88otBGAziHASJWgyVDkhyQ3p7YbN38qpb0vGQrYNxlk4e2I'
)
VALID_USER_TICKET_SIGNLESS_1 = '3:user:CA0Q__________9_GiQKAwjIAwoCCHsQyAMaCGJiOnNlc3MxGghiYjpzZXNzMiASKAE:'
VALID_USER_TICKET_2 = (
    '3:user:CA0Q__________9_GhAKAwjIAwoCCHsQyAMgEigB:KRibGYTJUA2ns0Fn7VYqeMZ1-GdscB1o9pRzELyr7QJrJsfsE'
    '8Y_HoVvB8Npr-oalv6AXOpagSc8HpZjAQz8zKMAVE_tI0tL-9DEsHirpawEbpy7OWV7-k18o1m-RaDaKeTlIB45KHbBul1-9a'
    'eKkortBfbbXtz_Qy9r_mfFPiQ'
)
VALID_USER_TICKET_3 = (
    '3:user:CA0Q__________9_Go8bCgIIAAoCCAEKAggCCgIIAwoCCAQKAggFCgIIBgoCCAcKAggICgIICQoCCAoKAggLCgIIDA'
    'oCCA0KAggOCgIIDwoCCBAKAggRCgIIEgoCCBMKAggUCgIIFQoCCBYKAggXCgIIGAoCCBkKAggaCgIIGwoCCBwKAggdCgIIHgo'
    'CCB8KAgggCgIIIQoCCCIKAggjCgIIJAoCCCUKAggmCgIIJwoCCCgKAggpCgIIKgoCCCsKAggsCgIILQoCCC4KAggvCgIIMAoC'
    'CDEKAggyCgIIMwoCCDQKAgg1CgIINgoCCDcKAgg4CgIIOQoCCDoKAgg7CgIIPAoCCD0KAgg-CgIIPwoCCEAKAghBCgIIQgoCC'
    'EMKAghECgIIRQoCCEYKAghHCgIISAoCCEkKAghKCgIISwoCCEwKAghNCgIITgoCCE8KAghQCgIIUQoCCFIKAghTCgIIVAoCCF'
    'UKAghWCgIIVwoCCFgKAghZCgIIWgoCCFsKAghcCgIIXQoCCF4KAghfCgIIYAoCCGEKAghiCgIIYwoCCGQKAghlCgIIZgoCCGc'
    'KAghoCgIIaQoCCGoKAghrCgIIbAoCCG0KAghuCgIIbwoCCHAKAghxCgIIcgoCCHMKAgh0CgIIdQoCCHYKAgh3CgIIeAoCCHkK'
    'Agh6CgIIewoCCHwKAgh9CgIIfgoCCH8KAwiAAQoDCIEBCgMIggEKAwiDAQoDCIQBCgMIhQEKAwiGAQoDCIcBCgMIiAEKAwiJA'
    'QoDCIoBCgMIiwEKAwiMAQoDCI0BCgMIjgEKAwiPAQoDCJABCgMIkQEKAwiSAQoDCJMBCgMIlAEKAwiVAQoDCJYBCgMIlwEKAw'
    'iYAQoDCJkBCgMImgEKAwibAQoDCJwBCgMInQEKAwieAQoDCJ8BCgMIoAEKAwihAQoDCKIBCgMIowEKAwikAQoDCKUBCgMIpgE'
    'KAwinAQoDCKgBCgMIqQEKAwiqAQoDCKsBCgMIrAEKAwitAQoDCK4BCgMIrwEKAwiwAQoDCLEBCgMIsgEKAwizAQoDCLQBCgMI'
    'tQEKAwi2AQoDCLcBCgMIuAEKAwi5AQoDCLoBCgMIuwEKAwi8AQoDCL0BCgMIvgEKAwi_AQoDCMABCgMIwQEKAwjCAQoDCMMBC'
    'gMIxAEKAwjFAQoDCMYBCgMIxwEKAwjIAQoDCMkBCgMIygEKAwjLAQoDCMwBCgMIzQEKAwjOAQoDCM8BCgMI0AEKAwjRAQoDCN'
    'IBCgMI0wEKAwjUAQoDCNUBCgMI1gEKAwjXAQoDCNgBCgMI2QEKAwjaAQoDCNsBCgMI3AEKAwjdAQoDCN4BCgMI3wEKAwjgAQo'
    'DCOEBCgMI4gEKAwjjAQoDCOQBCgMI5QEKAwjmAQoDCOcBCgMI6AEKAwjpAQoDCOoBCgMI6wEKAwjsAQoDCO0BCgMI7gEKAwjv'
    'AQoDCPABCgMI8QEKAwjyAQoDCPMBCgMI9AEKAwj1AQoDCPYBCgMI9wEKAwj4AQoDCPkBCgMI-gEKAwj7AQoDCPwBCgMI_QEKA'
    'wj-AQoDCP8BCgMIgAIKAwiBAgoDCIICCgMIgwIKAwiEAgoDCIUCCgMIhgIKAwiHAgoDCIgCCgMIiQIKAwiKAgoDCIsCCgMIjA'
    'IKAwiNAgoDCI4CCgMIjwIKAwiQAgoDCJECCgMIkgIKAwiTAgoDCJQCCgMIlQIKAwiWAgoDCJcCCgMImAIKAwiZAgoDCJoCCgM'
    'ImwIKAwicAgoDCJ0CCgMIngIKAwifAgoDCKACCgMIoQIKAwiiAgoDCKMCCgMIpAIKAwilAgoDCKYCCgMIpwIKAwioAgoDCKkC'
    'CgMIqgIKAwirAgoDCKwCCgMIrQIKAwiuAgoDCK8CCgMIsAIKAwixAgoDCLICCgMIswIKAwi0AgoDCLUCCgMItgIKAwi3AgoDC'
    'LgCCgMIuQIKAwi6AgoDCLsCCgMIvAIKAwi9AgoDCL4CCgMIvwIKAwjAAgoDCMECCgMIwgIKAwjDAgoDCMQCCgMIxQIKAwjGAg'
    'oDCMcCCgMIyAIKAwjJAgoDCMoCCgMIywIKAwjMAgoDCM0CCgMIzgIKAwjPAgoDCNACCgMI0QIKAwjSAgoDCNMCCgMI1AIKAwj'
    'VAgoDCNYCCgMI1wIKAwjYAgoDCNkCCgMI2gIKAwjbAgoDCNwCCgMI3QIKAwjeAgoDCN8CCgMI4AIKAwjhAgoDCOICCgMI4wIK'
    'AwjkAgoDCOUCCgMI5gIKAwjnAgoDCOgCCgMI6QIKAwjqAgoDCOsCCgMI7AIKAwjtAgoDCO4CCgMI7wIKAwjwAgoDCPECCgMI8'
    'gIKAwjzAgoDCPQCCgMI9QIKAwj2AgoDCPcCCgMI-AIKAwj5AgoDCPoCCgMI-wIKAwj8AgoDCP0CCgMI_gIKAwj_AgoDCIADCg'
    'MIgQMKAwiCAwoDCIMDCgMIhAMKAwiFAwoDCIYDCgMIhwMKAwiIAwoDCIkDCgMIigMKAwiLAwoDCIwDCgMIjQMKAwiOAwoDCI8'
    'DCgMIkAMKAwiRAwoDCJIDCgMIkwMKAwiUAwoDCJUDCgMIlgMKAwiXAwoDCJgDCgMImQMKAwiaAwoDCJsDCgMInAMKAwidAwoD'
    'CJ4DCgMInwMKAwigAwoDCKEDCgMIogMKAwijAwoDCKQDCgMIpQMKAwimAwoDCKcDCgMIqAMKAwipAwoDCKoDCgMIqwMKAwisA'
    'woDCK0DCgMIrgMKAwivAwoDCLADCgMIsQMKAwiyAwoDCLMDCgMItAMKAwi1AwoDCLYDCgMItwMKAwi4AwoDCLkDCgMIugMKAw'
    'i7AwoDCLwDCgMIvQMKAwi-AwoDCL8DCgMIwAMKAwjBAwoDCMIDCgMIwwMKAwjEAwoDCMUDCgMIxgMKAwjHAwoDCMgDCgMIyQM'
    'KAwjKAwoDCMsDCgMIzAMKAwjNAwoDCM4DCgMIzwMKAwjQAwoDCNEDCgMI0gMKAwjTAwoDCNQDCgMI1QMKAwjWAwoDCNcDCgMI'
    '2AMKAwjZAwoDCNoDCgMI2wMKAwjcAwoDCN0DCgMI3gMKAwjfAwoDCOADCgMI4QMKAwjiAwoDCOMDCgMI5AMKAwjlAwoDCOYDC'
    'gMI5wMKAwjoAwoDCOkDCgMI6gMKAwjrAwoDCOwDCgMI7QMKAwjuAwoDCO8DCgMI8AMKAwjxAwoDCPIDCgMI8wMQyAMaCGJiOn'
    'Nlc3MxGgliYjpzZXNzMTAaCmJiOnNlc3MxMDAaCWJiOnNlc3MxMRoJYmI6c2VzczEyGgliYjpzZXNzMTMaCWJiOnNlc3MxNBo'
    'JYmI6c2VzczE1GgliYjpzZXNzMTYaCWJiOnNlc3MxNxoJYmI6c2VzczE4GgliYjpzZXNzMTkaCGJiOnNlc3MyGgliYjpzZXNz'
    'MjAaCWJiOnNlc3MyMRoJYmI6c2VzczIyGgliYjpzZXNzMjMaCWJiOnNlc3MyNBoJYmI6c2VzczI1GgliYjpzZXNzMjYaCWJiO'
    'nNlc3MyNxoJYmI6c2VzczI4GgliYjpzZXNzMjkaCGJiOnNlc3MzGgliYjpzZXNzMzAaCWJiOnNlc3MzMRoJYmI6c2VzczMyGg'
    'liYjpzZXNzMzMaCWJiOnNlc3MzNBoJYmI6c2VzczM1GgliYjpzZXNzMzYaCWJiOnNlc3MzNxoJYmI6c2VzczM4GgliYjpzZXN'
    'zMzkaCGJiOnNlc3M0GgliYjpzZXNzNDAaCWJiOnNlc3M0MRoJYmI6c2VzczQyGgliYjpzZXNzNDMaCWJiOnNlc3M0NBoJYmI6'
    'c2VzczQ1GgliYjpzZXNzNDYaCWJiOnNlc3M0NxoJYmI6c2VzczQ4GgliYjpzZXNzNDkaCGJiOnNlc3M1GgliYjpzZXNzNTAaC'
    'WJiOnNlc3M1MRoJYmI6c2VzczUyGgliYjpzZXNzNTMaCWJiOnNlc3M1NBoJYmI6c2VzczU1GgliYjpzZXNzNTYaCWJiOnNlc3'
    'M1NxoJYmI6c2VzczU4GgliYjpzZXNzNTkaCGJiOnNlc3M2GgliYjpzZXNzNjAaCWJiOnNlc3M2MRoJYmI6c2VzczYyGgliYjp'
    'zZXNzNjMaCWJiOnNlc3M2NBoJYmI6c2VzczY1GgliYjpzZXNzNjYaCWJiOnNlc3M2NxoJYmI6c2VzczY4GgliYjpzZXNzNjka'
    'CGJiOnNlc3M3GgliYjpzZXNzNzAaCWJiOnNlc3M3MRoJYmI6c2VzczcyGgliYjpzZXNzNzMaCWJiOnNlc3M3NBoJYmI6c2Vzc'
    'zc1GgliYjpzZXNzNzYaCWJiOnNlc3M3NxoJYmI6c2Vzczc4GgliYjpzZXNzNzkaCGJiOnNlc3M4GgliYjpzZXNzODAaCWJiOn'
    'Nlc3M4MRoJYmI6c2VzczgyGgliYjpzZXNzODMaCWJiOnNlc3M4NBoJYmI6c2Vzczg1GgliYjpzZXNzODYaCWJiOnNlc3M4Nxo'
    'JYmI6c2Vzczg4GgliYjpzZXNzODkaCGJiOnNlc3M5GgliYjpzZXNzOTAaCWJiOnNlc3M5MRoJYmI6c2VzczkyGgliYjpzZXNz'
    'OTMaCWJiOnNlc3M5NBoJYmI6c2Vzczk1GgliYjpzZXNzOTYaCWJiOnNlc3M5NxoJYmI6c2Vzczk4GgliYjpzZXNzOTkgEigB:'
    'CX8PIOrxJnQqFXl7wAsiHJ_1VGjoI-asNlCXb8SE8jtI2vdh9x6CqbAurSgIlAAEgotVP-nuUR38x_a9YJuXzmG5AvJ458apW'
    'QtODHIDIX6ZaIwMxjS02R7S5LNqXa0gAuU_R6bCWpZdWe2uLMkdpu5KHbDgW08g-uaP_nceDOk'
)


def test_context():
    UserContext(BlackboxEnv.Test, tau.TVMKNIFE_PUBLIC_KEYS)


def test_context_exceptions():
    with pytest.raises(MalformedTvmKeysException):
        UserContext(BlackboxEnv.Test, MALFORMED_TVM_KEYS)
    with pytest.raises(EmptyTvmKeysException):
        UserContext(BlackboxEnv.Stress, EMPTY_TVM_KEYS)


def test_ticket():
    user_context = UserContext(BlackboxEnv.Test, tau.TVMKNIFE_PUBLIC_KEYS)
    ticket = user_context.check(VALID_USER_TICKET_1)
    assert ticket.scopes == ['bb:sess1', 'bb:sess2']
    assert ticket.has_scope('bb:sess1')
    assert ticket.has_scope('bb:sess2')
    assert not ticket.has_scope('bb:sess3')
    assert ticket.uids == [456, 123]
    assert ticket.default_uid == 456
    assert (
        ticket.debug_info
        == 'ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess2;default_uid=456;uid=456;uid=123;env=Test;'
    )
    assert VALID_USER_TICKET_SIGNLESS_1 == tvmauth.utils.remove_ticket_signature(VALID_USER_TICKET_1)
    assert (
        repr(ticket)
        == 'ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess2;default_uid=456;uid=456;uid=123;env=Test;'
    )
    assert (
        str(ticket)
        == 'ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess2;default_uid=456;uid=456;uid=123;env=Test;'
    )


def test_ticket_exceptions():
    user_context = UserContext(BlackboxEnv.Test, tau.TVMKNIFE_PUBLIC_KEYS)
    with pytest.raises(TicketParsingException) as ex:
        user_context.check(SIGN_BROKEN_USER_TICKET)
    assert ex.value.status == TicketStatus.SignBroken
    assert (
        ex.value.debug_info
        == 'ticket_type=user;expiration_time=9223372036854775807;scope=bb:sess1;scope=bb:sess2;default_uid=456;uid=456;uid=123;env=Test;'
    )

    with pytest.raises(TicketParsingException) as ex:
        user_context.check(MALFORMED_USER_TICKET)
    assert ex.value.status == TicketStatus.Malformed
    assert ex.value.debug_info == 'status=malformed;'

    with pytest.raises(TicketParsingException) as ex:
        user_context.check(VALID_SERVICE_TICKET)
    assert ex.value.status == TicketStatus.InvalidTicketType
    assert ex.value.debug_info == 'ticket_type=not-user;'

    user_context = UserContext(BlackboxEnv.Prod, tau.TVMKNIFE_PUBLIC_KEYS)
    with pytest.raises(TicketParsingException) as ex:
        user_context.check(VALID_USER_TICKET_1)
    assert ex.value.status == TicketStatus.InvalidBlackboxEnv


def test_create_ticket_for_tests():
    with pytest.raises(TicketParsingException):
        tau.create_user_ticket_for_unittest(TicketStatus.Expired, 42, ['ololo', 'abc'])
    u = tau.create_user_ticket_for_unittest(TicketStatus.Ok, 42, ['ololo', 'abc'], [23, 56])
    assert u
    assert u.default_uid == 42
    assert u.scopes == ['abc', 'ololo']
    assert u.uids == [23, 42, 56]
    assert u.debug_info == 'ticket_type=user;scope=abc;scope=ololo;default_uid=42;uid=23;uid=42;uid=56;env=Test;'

    with pytest.raises(Exception):
        tau.create_user_ticket_for_unittest(TicketStatus.Ok, 0)


def test_non_ascii():
    class _Ins(object):
        def debug_info(self):
            return u'Люблю яблоки'

    u = CheckedUserTicket(_Ins())
    assert str(u) == 'Люблю яблоки'
    if six.PY2:
        assert unicode(u) == u'Люблю яблоки'  # noqa
