#include <library/cpp/tvmauth/src/rw/keys.h>
#include <library/cpp/tvmauth/src/rw/rw.h>

#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/openssl/include/openssl/bn.h>
#include <contrib/libs/openssl/include/openssl/evp.h>

namespace NTvmAuth {
    /*
 returns 0 in case of error
 */
    int MakeKeysRw(TRwKey** skey, TRwKey** vkey) {
        int result = 0;

        TRwKey* rw = RwNew();

        do {
            RwGenerateKey(rw, 2048);

            if (rw == nullptr) {
                printf("RwGenerateKey failed\n");
                break; /* failed */
            }

            printf("RW key bits: %d\n", BN_num_bits(rw->N));

            /* Set signing key */
            *skey = RwPrivateKeyDup(rw);
            if (*skey == nullptr) {
                printf("RwPrivateKeyDup failed\n");
                break;
            }

            /* Set verifier key */
            *vkey = RwPublicKeyDup(rw);
            if (*vkey == nullptr) {
                printf("RwPublicKeyDup failed\n");
                break;
            }

            result = 1;

        } while (0);

        if (rw) {
            RwFree(rw);
            rw = nullptr;
        }

        return result;
    }

    static void PrintIt(const char* label, const unsigned char* buff, size_t len) {
        if (!buff || !len)
            return;

        if (label)
            printf("%s: ", label);

        for (size_t i = 0; i < len; ++i)
            printf("%02X", buff[i]);

        printf("\n");
    }

    int TestSignVerify() {
        TRwKey *skey = nullptr, *vkey = nullptr;
        const char* msg = "Test test test test test";
        unsigned int msg_len = 0;
        int res = 0;

        msg_len = (unsigned int)strlen(msg);
        if (MakeKeysRw(&skey, &vkey)) {
            unsigned char* sign = new unsigned char[RwModSize(skey) + 10];
            int sign_len;
            printf("RwModSize(skey) returned %d\n", RwModSize(skey));
            memset(sign, 0x00, RwModSize(skey) + 10);

            printf("--- Signing call ---\n");
            if ((sign_len = RwPssrSignMsg(msg_len, (unsigned char*)msg, sign, skey, (EVP_MD*)EVP_sha256())) != 0) {
#ifdef RW_PRINT_DEBUG
                BIGNUM* s = BN_new();
#endif
                printf("\n");
                PrintIt("Signature", sign, RwModSize(skey));

#ifdef RW_PRINT_DEBUG
                BN_bin2bn(sign, RW_mod_size(skey), s);

                print_bn("Signature BN", s);

                BN_free(s);
#endif

                printf("--- Verification call ---\n");
                if (RwPssrVerifyMsg(msg_len, (unsigned char*)msg, sign, sign_len, vkey, (EVP_MD*)EVP_sha256())) {
                    printf("Verification: success!\n");
                    res = 1;
                } else {
                    printf("Verification: failed!\n");
                    printf("RwPssrVerifyMsg failed!\n");
                    return 1;
                }
            } else {
                printf("RwPssrSignMsg failed!\n");
                return 1;
            }

            if (sign != nullptr)
                delete[] sign;

        } else {
            printf("MakeKeysRw failed!\n");
            return 1;
        }

        if (skey != nullptr) {
            RwFree(skey);
        }
        if (vkey != nullptr)
            RwFree(vkey);

        return res;
    }
}

using namespace NTvmAuth;
Y_UNIT_TEST_SUITE(Rw) { 
    Y_UNIT_TEST(SignVerify) { 
        for (int i = 1; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(1, TestSignVerify());
        }
    }

    Y_UNIT_TEST(TKeysPriv) { 
        NRw::TRwPrivateKey priv(Base64Decode("MIIEmwKCAQBwsRd4frsVARIVSfj_vCdfvA3Q9SsGhSybdBDhbm8L6rPqxdoSNLCdNXzDWj7Ppf0o8uWHMxC-5Lfw0I18ri68nhm9-ndixcnbn6ti1uetgkc28eiEP6Q8ILD_JmkynbUl1aKDNAa5XsK2vFSEX402uydRomsTn46kRY23hfqcIi0ohh5VxIrpclRsRZus0JFu-RJzhqTbKYV4y4dglWPGHh5BuTv9k_Oh0_Ra8Xp5Rith5vjaKZUQ5Hyh9UtBYTkNWdvXP9OpmbiLVeRLuMzBm4HEFHDwMZ1h6LSVP-wB_spJPaMLTn3Q3JIHe-wGBYRWzU51RRYDqv4O_H12w5C1AoGBALAwCQ7fdAPG1lGclL7iWFjUofwPCFwPyDjicDT_MRRu6_Ta4GjqOGO9zuOp0o_ePgvR-7nA0fbaspM4LZNrPZwmoYBCJMtKXetg68ylu2DO-RRSN2SSh1AIZSA_8UTABk69bPzNL31j4PyZWxrgZ3zP9uZvzggveuKt5ZhCMoB7AoGBAKO9oC2AZjLdh2RaEFotTL_dY6lVcm38VA6PnigB8gB_TMuSrd4xtRw5BxvHpOCnBcUAJE0dN4_DDe5mrotKYMD2_3_lcq9PaLZadrPDCSDL89wtoVxNQNAJTqFjBFXYNu4Ze63lrsqg45TF5XmVRemyBHzXw3erd0pJaeoUDaSPAoGAJhGoHx_nVw8sDoLzeRkOJ1_6-uh_wVmVr6407_LPjrrySEq-GiYu43M3-QDp8J_J9e3S1Rpm4nQX2bEf5Gx9n4wKz7Hp0cwkOqBOWhvrAu6YLpv59wslEtkx0LYcJy6yQk5mpU8l29rPO7b50NyLnfnE2za-9DyK038FKlr5VgICgYAUd7QFsAzGW7Dsi0ILRamX-6x1Kq5Nv4qB0fPFAD5AD-mZclW7xjajhyDjePScFOC4oASJo6bx-GG9zNXRaUwYHt_v_K5V6e0Wy07WeGEkGX57hbQriagaASnULGCKuwbdwy91vLXZVBxymLyvMqi9NkCPmvhu9W7pSS09QoG0kgKBgBYGASHb7oB42sozkpfcSwsalD-B4QuB-QccTgaf5iKN3X6bXA0dRwx3udx1OlH7x8F6P3c4Gj7bVlJnBbJtZ7OE1DAIRJlpS71sHXmUt2wZ3yKKRuySUOoBDKQH_iiYAMnXrZ-Zpe-sfB-TK2NcDO-Z_tzN-cEF71xVvLMIRlAPAoGAdeikZPh1O57RxnVY72asiMRZheMBhK-9uSNPyYEZv3bUnIjg4XdMYStF2yTHNu014XvkDSQTe-drv2BDs9ExKplM4xFOtDtPQQ3mMB3GoK1qVhM_9n1QEElreurMicahkalnPo6tU4Z6PFL7PTpjRnCN67lJp0J0fxNDL13YSagCgYBA9VJrMtPjzcAx5ZCIYJjrYUPqEG_ttQN2RJIHN3MVpdpLAMIgX3tnlfyLwQFVKK45D1JgFa_1HHcxTWGtdIX4nsIjPWt-cWCCCkkw9rM5_Iqcb-YLSood6IP2OK0w0XLD1STnFRy_BRwdjPbGOYmp6YrJDZAlajDkFSdRvsz9Vg=="),
                                0);
        NRw::TRwPrivateKey priv2(Base64Decode("MIIEnAKCAQEA4RATOfumLD1n6ICrW5biaAl9VldinczmkNPjpUWwc3gs8PnkCrtdnPFmpBwW3gjHdSNU1OuEg5A6K1o1xiGv9sU-jd88zQBOdK6E2zwnJnkK6bNusKE2H2CLqg3aMWCmTa9JbzSy1uO7wa-xCqqNUuCko-2lyv12HhL1ICIH951SHDa4qO1U5xZhhlUAnqWi9R4tYDeMiF41WdOjwT2fg8UkbusThmxa3yjCXjD7OyjshPtukN8Tl3UyGtV_s2CLnE3f28VAi-AVW8FtgL22xbGhuyEplXRrtF1E5oV7NSqxH1FS0SYROA8ffYQGV5tfx5WDFHiXDEP6BzoVfeBDRQKBgQDzidelKZNFMWar_yj-r_cniMkZXNaNVEQbMg1A401blGjkU1r-ufGH5mkdNx4IgEoCEYBTM834Z88fYV1lOVfdT0OqtiVoC9NkLu3xhQ1r9_r6RMaAenwsV7leH8jWMOKvhkB0KNI49oznTGDqLp0AbDbtP66xdNH4dr3rw3WFywKBgQDslDdv4sdnRKN27h2drhn4Pp_Lgw2U-6MfHiyjp6BKR8Qtlld3hdb-ZjU9F0h38DqECmFIEe35_flKfd7X21CBQs9EuKR8EdaF3OAgzA-TRWeQhyHmaV7Fas1RlNqZHm8lckaZT8dX9Ygsxn0I_vUbm9pkFivwGvQnnwNQ7Te5LwKBgCVMYOzLHW911l6EbCZE6XU2HUrTKEd1bdqWCgtxPEmDl3BZcXpnyKpqSHmlH1F7s65WBfejxDM2hjin3OnXSog_x35ql_-Azu93-79QAzbQc6Z13BuWPpQxV8iw4ijqRRhzjD2pcvXlIxgebp5-H0eDt-Md2Y8rkrzyhm8EH7mwAoGAHZKG7fxY7OiUbt3Ds7XDPwfT-XBhsp90Y-PFlHT0CUj4hbLK7vC638zGp6LpDv4HUIFMKQI9vz-_KU-72vtqEChZ6JcUj4I60LucBBmB8mis8hDkPM0r2K1ZqjKbUyPN5K5I0yn46v6xBZjPoR_eo3N7TILFfgNehPPgah2m9yYCgYAecTr0pTJopizVf-Uf1f7k8RkjK5rRqoiDZkGoHGmrco0cimtf1z4w_M0jpuPBEAlAQjAKZnm_DPnj7Cuspyr7qeh1VsStAXpshd2-MKGtfv9fSJjQD0-Fivcrw_kaxhxV8MgOhRpHHtGc6YwdRdOgDYbdp_XWLpo_Dte9eG6wuQKBgDzo0e8d8pTyvCP23825rVzvrSHBZkliGkCEu0iggDnfKOreejFhQN9JeBo8sYdQFCRBptEU6k4b5O6J3NQ1Sspiez15ddqmFMD4uhJY6VsV-JFnL9YhLqVd355xZCyU4b07mReU9-LuqK2m2chjxH_HDAgUoEvO_yzR9EDYqHbNAoGAf529Ah9HIT5aG6IGTlwQdk-M7guy63U4vj4uC7z98qgvFEsV6cr4miT6RE8Aw5yAeN5pW59rZNjBNr9i-8n8kouasho2xNMTPKP8YuSNg2PNNS5T1Ou56mgsBCY5i10TIHKNIm2RVSUgzJ97BMEOZY6jQRytFfwgYkvnFzbuA9c="),
                                 0);
        NRw::TRwPrivateKey priv3(Base64Decode("MIICVAKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NAkEAg1xBDL_UkHy347HwioMscJFP-6eKeim3LoG9rd1EvOycxkoStZ4299OdyzzEXC9cjLdq401BXe-LairiMUgZawJBALn5ziBCc2ycMaYjZDon2EN55jBEe0tJdUy4mOi0ozTV9OLcBANds0nMYPjZFOY3QymzU0LcOa_An3JknI0C2ucCQGxtwTb3h7ux5Ld8jkeRYzkNoB2Y6Is5fqCYVRIJZmz0IcQFb2iW0EX92U7_BpgVuKlvSDTP9LuaxuPfmY6WXEECQBc_OcQITm2ThjTEbIdE-whvPMYIj2lpLqmXEx0WlGaavpxbgIBrtmk5jB8bIpzG6GU2amhbhzX4E-5Mk5GgW10CQBBriCGX-pIPlvx2PhFQZY4SKf908U9FNuXQN7W7qJedk5jJQlazxt76c7lnmIuF65GW7VxpqCu98W1FXEYpAy0CQG-lpihdvxaZ8SkHqNFZGnXhELT2YesLs7GehZSTwuUwx1iTpVm88PVROLYBDZqoGM316s9aZEJBALe5zEpxQTQCQQCDMszX1cQlbBCP08isuMQ2ac3S-qNd0mfRXDCRfMm4s7iuJ5MeHU3uPUVlA_MR4ULRbg1d97TGio912z4KPgjE"),
                                 0);

        UNIT_ASSERT_EXCEPTION(NRw::TRwPrivateKey("asdzxcv", 0), yexception);
        UNIT_ASSERT_EXCEPTION(NRw::TRwPrivateKey(Base64Decode("AKBgF9t2YJGAJkRRFq6fWhi3m1TFW1UOE0f6ZrfYhHAkpqGlKlh0QVfeTNPpeJhi75xXzCe6oReRUm-0DbqDNhTShC7uGUv1INYnRBQWH6E-5Fc5XrbDFSuGQw2EYjNfHy_HefHJXxQKAqPvxBDKMKkHgV58WtM6rC8jRi9sdX_ig2NAkEAg1xBDL_UkHy347HwioMscJFP-6eKeim3LoG9rd1EvOycxkoStZ4299OdyzzEXC9cjLdq401BXe-LairiMUgZawJBALn5ziBCc2ycMaYjZDon2EN55jBEe0tJdUy4mOi0ozTV9OLcBANds0nMYPjZFOY3QymzU0LcOa_An3JknI0C2ucCQGxtwTb3h7ux5Ld8jkeRYzkNoB2Y6Is5fqCYVRIJZmz0IcQFb2iW0EX92U7_BpgVuKlvSDTP9LuaxuPfmY6WXEECQBc_OcQITm2ThjTEbIdE-whvPMYIj2lpLqmXEx0WlGaavpxbgIBrtmk5jB8bIpzG6GU2amhbhzX4E-5Mk5GgW10CQBBriCGX-pIPlvx2PhFQZY4SKf908U9FNuXQN7W7qJedk5jJQlazxt76c7lnmIuF65GW7VxpqCu98W1FXEYpAy0CQG-lpihdvxaZ8SkHqNFZGnXhELT2YesLs7GehZSTwuUwx1iTpVm88PVROLYBDZqoGM316s9aZEJBALe5zEpxQTQCQQCDMszX1cQlbBCP08isuMQ2ac3S-qNd0mfRXDCRfMm4s7iuJ5MeHU3uPUVlA_MR4ULRbg1d97TGio912z4KP"),
                                                 0),
                              yexception);

        UNIT_ASSERT(!priv.SignTicket("").empty());
    }

    Y_UNIT_TEST(TKeysPub) { 
        NRw::TRwPublicKey pub(Base64Decode("MIIBBAKCAQBwsRd4frsVARIVSfj_vCdfvA3Q9SsGhSybdBDhbm8L6rPqxdoSNLCdNXzDWj7Ppf0o8uWHMxC-5Lfw0I18ri68nhm9-ndixcnbn6ti1uetgkc28eiEP6Q8ILD_JmkynbUl1aKDNAa5XsK2vFSEX402uydRomsTn46kRY23hfqcIi0ohh5VxIrpclRsRZus0JFu-RJzhqTbKYV4y4dglWPGHh5BuTv9k_Oh0_Ra8Xp5Rith5vjaKZUQ5Hyh9UtBYTkNWdvXP9OpmbiLVeRLuMzBm4HEFHDwMZ1h6LSVP-wB_spJPaMLTn3Q3JIHe-wGBYRWzU51RRYDqv4O_H12w5C1"));
        NRw::TRwPublicKey pub2(Base64Decode("MIIBBQKCAQEA4RATOfumLD1n6ICrW5biaAl9VldinczmkNPjpUWwc3gs8PnkCrtdnPFmpBwW3gjHdSNU1OuEg5A6K1o1xiGv9sU-jd88zQBOdK6E2zwnJnkK6bNusKE2H2CLqg3aMWCmTa9JbzSy1uO7wa-xCqqNUuCko-2lyv12HhL1ICIH951SHDa4qO1U5xZhhlUAnqWi9R4tYDeMiF41WdOjwT2fg8UkbusThmxa3yjCXjD7OyjshPtukN8Tl3UyGtV_s2CLnE3f28VAi-AVW8FtgL22xbGhuyEplXRrtF1E5oV7NSqxH1FS0SYROA8ffYQGV5tfx5WDFHiXDEP6BzoVfeBDRQ=="));
        NRw::TRwPublicKey pub3(Base64Decode("MIGDAoGAX23ZgkYAmRFEWrp9aGLebVMVbVQ4TR_pmt9iEcCSmoaUqWHRBV95M0-l4mGLvnFfMJ7qhF5FSb7QNuoM2FNKELu4ZS_Ug1idEFBYfoT7kVzletsMVK4ZDDYRiM18fL8d58clfFAoCo-_EEMowqQeBXnxa0zqsLyNGL2x1f-KDY0="));

        UNIT_ASSERT_EXCEPTION(NRw::TRwPublicKey("asdzxcv"), yexception);
        UNIT_ASSERT_EXCEPTION(NRw::TRwPublicKey(Base64Decode("AoGAX23ZgkYAmRFEWrp9aGLebVMVbVQ4TR_pmt9iEcCSmoaUqWHRBV95M0-l4mGLvnFfMJ7qhF5FSb7QNuoM2FNKELu4ZS_Ug1idEFBYfoT7kVzletsMVK40")), yexception);

        UNIT_ASSERT(!pub.CheckSign("~~~", "~~~"));
    }

    Y_UNIT_TEST(TKeys) { 
        NRw::TRwPrivateKey priv(Base64Decode("MIIEmwKCAQBwsRd4frsVARIVSfj_vCdfvA3Q9SsGhSybdBDhbm8L6rPqxdoSNLCdNXzDWj7Ppf0o8uWHMxC-5Lfw0I18ri68nhm9-ndixcnbn6ti1uetgkc28eiEP6Q8ILD_JmkynbUl1aKDNAa5XsK2vFSEX402uydRomsTn46kRY23hfqcIi0ohh5VxIrpclRsRZus0JFu-RJzhqTbKYV4y4dglWPGHh5BuTv9k_Oh0_Ra8Xp5Rith5vjaKZUQ5Hyh9UtBYTkNWdvXP9OpmbiLVeRLuMzBm4HEFHDwMZ1h6LSVP-wB_spJPaMLTn3Q3JIHe-wGBYRWzU51RRYDqv4O_H12w5C1AoGBALAwCQ7fdAPG1lGclL7iWFjUofwPCFwPyDjicDT_MRRu6_Ta4GjqOGO9zuOp0o_ePgvR-7nA0fbaspM4LZNrPZwmoYBCJMtKXetg68ylu2DO-RRSN2SSh1AIZSA_8UTABk69bPzNL31j4PyZWxrgZ3zP9uZvzggveuKt5ZhCMoB7AoGBAKO9oC2AZjLdh2RaEFotTL_dY6lVcm38VA6PnigB8gB_TMuSrd4xtRw5BxvHpOCnBcUAJE0dN4_DDe5mrotKYMD2_3_lcq9PaLZadrPDCSDL89wtoVxNQNAJTqFjBFXYNu4Ze63lrsqg45TF5XmVRemyBHzXw3erd0pJaeoUDaSPAoGAJhGoHx_nVw8sDoLzeRkOJ1_6-uh_wVmVr6407_LPjrrySEq-GiYu43M3-QDp8J_J9e3S1Rpm4nQX2bEf5Gx9n4wKz7Hp0cwkOqBOWhvrAu6YLpv59wslEtkx0LYcJy6yQk5mpU8l29rPO7b50NyLnfnE2za-9DyK038FKlr5VgICgYAUd7QFsAzGW7Dsi0ILRamX-6x1Kq5Nv4qB0fPFAD5AD-mZclW7xjajhyDjePScFOC4oASJo6bx-GG9zNXRaUwYHt_v_K5V6e0Wy07WeGEkGX57hbQriagaASnULGCKuwbdwy91vLXZVBxymLyvMqi9NkCPmvhu9W7pSS09QoG0kgKBgBYGASHb7oB42sozkpfcSwsalD-B4QuB-QccTgaf5iKN3X6bXA0dRwx3udx1OlH7x8F6P3c4Gj7bVlJnBbJtZ7OE1DAIRJlpS71sHXmUt2wZ3yKKRuySUOoBDKQH_iiYAMnXrZ-Zpe-sfB-TK2NcDO-Z_tzN-cEF71xVvLMIRlAPAoGAdeikZPh1O57RxnVY72asiMRZheMBhK-9uSNPyYEZv3bUnIjg4XdMYStF2yTHNu014XvkDSQTe-drv2BDs9ExKplM4xFOtDtPQQ3mMB3GoK1qVhM_9n1QEElreurMicahkalnPo6tU4Z6PFL7PTpjRnCN67lJp0J0fxNDL13YSagCgYBA9VJrMtPjzcAx5ZCIYJjrYUPqEG_ttQN2RJIHN3MVpdpLAMIgX3tnlfyLwQFVKK45D1JgFa_1HHcxTWGtdIX4nsIjPWt-cWCCCkkw9rM5_Iqcb-YLSood6IP2OK0w0XLD1STnFRy_BRwdjPbGOYmp6YrJDZAlajDkFSdRvsz9Vg=="),
                                0);
        NRw::TRwPublicKey pub(Base64Decode("MIIBBAKCAQBwsRd4frsVARIVSfj_vCdfvA3Q9SsGhSybdBDhbm8L6rPqxdoSNLCdNXzDWj7Ppf0o8uWHMxC-5Lfw0I18ri68nhm9-ndixcnbn6ti1uetgkc28eiEP6Q8ILD_JmkynbUl1aKDNAa5XsK2vFSEX402uydRomsTn46kRY23hfqcIi0ohh5VxIrpclRsRZus0JFu-RJzhqTbKYV4y4dglWPGHh5BuTv9k_Oh0_Ra8Xp5Rith5vjaKZUQ5Hyh9UtBYTkNWdvXP9OpmbiLVeRLuMzBm4HEFHDwMZ1h6LSVP-wB_spJPaMLTn3Q3JIHe-wGBYRWzU51RRYDqv4O_H12w5C1"));

        const TString data = "my magic data";

        UNIT_ASSERT(pub.CheckSign(data, priv.SignTicket(data)));
        UNIT_ASSERT(!pub.CheckSign("~~~~" + data, priv.SignTicket(data)));
        UNIT_ASSERT(!pub.CheckSign(data, "~~~~" + priv.SignTicket(data)));

        UNIT_ASSERT(pub.CheckSign(data,
                                  Base64Decode("EC5hZunmK3hOJZeov_XlNIXcwj5EsgX94lMd-tQJTNUO4NR6bCO7qQkKjEeFJmI2QFYXGY-iSf9WeMJ_brECAMyYAix-L8sZqcMPXD945QgkPsNQKyC0DX9FkgfSh6ZKkA-UvFSHrkn3QbeE9omk3-yXpqR-M8DlVqmp3mwdYlYRq0NdfTaD3AMXVA4aZTbW3OmhJoLJ8AxJ3w1oG5q_lk8dpW9vvqfIzsfPABme6sY5XyPmsjYaRDf9z4ZJgR-wTkG06_N_YzIklS5T2s_4FUKLz5gLMhsnVlNUpgZyRN9sXTAn9-zMJnCwAC8WRgykWnljPGDDJCjk-Xwsg7AOLQ==")));
        UNIT_ASSERT(pub.CheckSign(data,
                                  Base64Decode("JbHSn1QEQeOEvzyt-LpawbQv4vPEEE05bWhjB2-MkoV-tyq9FykSqGqhP3ZFc1_FPrqguwEYrHibI2l5w3q8wnI1fcyRUoNuJxmBSzf2f_Uzn9ZoUSc7D9pTGSvK_hhZoL4YMc_VfbdEdnDuvHZNlZyaDPH9EbmUqyXjnXTEwRoK0fAU1rhlHvSZvnp0ctVBWSkaQsaU8dJTKDBtIQVP1D5Py2pKB2NBF_Ytz2thWt7iLjbTyjtis6DC-JKwjFBqv6nQf42sKalHQqWFuIvBCIfNUswEw4_sGfwWVSBBmFplf7FmD7sN8znUahYUPGCe1uFNly6WwpPJsm8VtiU80g==")));
        UNIT_ASSERT(pub.CheckSign(data,
                                  Base64Decode("FeMZtDP-yuoNqK2HYw3JxTV9v7p8IoQEuRMtuHddafh4bq1ZOeEqg7g7Su6M3iq_kN9DZ_fVhuhuVcbZmNYPIvJ8oL5DE80KI3d1Qbs9mS8_X4Oq2TJpZgNfFG-z_LPRZSNRP9Q8sQhlAoSZHOSZkBFcYj1EuqEp6nSSSbX8Ji4Se-TfhIh3YFQkr-Ivk_3NmSXhDXUaW7CHo2rVm58QJ2cgSEuxzBH-Q8E8tGDCEmk4p3_iot9XY8RRN-_j0yi15etmXCUIKFbpDogtHdT8CyAEVHMYvsLqkLux9pzy3RdvNQmoPjol3wIm-H0wMtF_pMw4G2QLNev6he6xWeckxw==")));
    }

    Y_UNIT_TEST(Keygen) { 
        for (size_t idx = 0; idx < 100; ++idx) {
            NRw::TKeyPair pair = NRw::GenKeyPair(1024);
            NRw::TRwPrivateKey priv(pair.Private, 0);
            NRw::TRwPublicKey pub(pair.Public);

            const TString data = "my magic data";
            TStringStream s;
            s << "data='" << data << "'.";
            s << "private='" << Base64Encode(pair.Private) << "'.";
            s << "public='" << Base64Encode(pair.Public) << "'.";
            TString sign;
            UNIT_ASSERT_NO_EXCEPTION_C(sign = priv.SignTicket(data), s.Str());
            s << "sign='" << Base64Encode(sign) << "'.";
            UNIT_ASSERT_C(pub.CheckSign(data, sign), s.Str());
        }
    }
}
