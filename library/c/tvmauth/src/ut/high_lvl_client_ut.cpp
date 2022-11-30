// DO_NOT_STYLE

#include <library/c/tvmauth/high_lvl_client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/tvmauth/client/ut/common.h>

#include <util/stream/str.h>

Y_UNIT_TEST_SUITE(CHighLvlClient) {
    static ui32 OK_CLIENT = 100500;
    static const TString SRV_TICKET = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU";
    static const TString TEST_TICKET = "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:AuTECbsGGH-jkLJsKjdHL-jvoOMiBLxBDi_kkZXgWnxvdLQkhXGKXKlG6oHCB6aYfISq3cHdJ2QuyJceqkpi2220-YK1jm68K1-llyApKC7ps5LQ213zuAxxN0fJcTUL4Ys02pkCUkSBft094rXYHciZBUjABU7-8Laj0Ag9j30";
    static const TString PROD_TICKET = "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_BEUv1x9CALU7do8irDlDYVeVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTyVetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490CJFw";
    static const TString PROD_YATEAM_TICKET = "3:user:CAwQ__________9_Gg4KAgh7EHsg0oXYzAQoAg:G2wloFRSi8--RLb2GDSro_sKXPF2JSdL5CVOuOHgUcRvLm-3OxIPn0NUqbJ9DWDmhPplOqEiblIbLK85My1VMJ2aG5SLbRNKEtwfmxLvkwNpl_gUEwWPJm9_8Khslfj71P3hccxtEEqM9bJSMwHueVAY-a9HSzFo-uMFMeSgQ-k";

    Y_UNIT_TEST(Settings) {
        TA_TTvmApiClientSettings* s = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Create(&s));

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableServiceTicketChecking(s));

        UNIT_ASSERT_EQUAL(TA_EC_INVALID_PARAM, TA_TvmApiClientSettings_SetSelfTvmId(s, 0));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetSelfTvmId(s, OK_CLIENT));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableServiceTicketChecking(s));

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableUserTicketChecking(s, TA_BE_PROD));

        UNIT_ASSERT_EQUAL(TA_EC_INVALID_PARAM, TA_TvmApiClientSettings_SetDiskCacheDir(s, nullptr, 7));
        UNIT_ASSERT_EQUAL(TA_EC_INVALID_PARAM, TA_TvmApiClientSettings_SetDiskCacheDir(s, "abc", 0));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetDiskCacheDir(s, "/abc", 4));
        const TString dir = GetCachePath();
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetDiskCacheDir(s, dir.data(), dir.size()));

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Delete(s));
    }

    static TStringStream STREAM;
    static void Log(int lvl, const char* msg) {
        STREAM << lvl << ": " << msg << Endl;
    }

    Y_UNIT_TEST(Client) {
        STREAM.Clear();
        TA_TTvmApiClientSettings* s = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Create(&s));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetSelfTvmId(s, OK_CLIENT));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableServiceTicketChecking(s));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableUserTicketChecking(s, TA_BE_PROD));
        const TString dir = GetCachePath();
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetDiskCacheDir(s, dir.data(), dir.size()));

        struct TA_TTvmClient* c = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Create(s, Log, &c));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Delete(s));

        TA_TTvmClientStatus* status;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_GetStatus(c, &status));
        TA_ETvmClientStatusCode code;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Status_GetCode(status, &code));
        UNIT_ASSERT_EQUAL(TA_TCSC_OK, code);
        const char* lastError = nullptr;
        size_t lastErrorSize = 0;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Status_GetLastError(status, &lastError, &lastErrorSize));
        UNIT_ASSERT_VALUES_EQUAL("OK", TStringBuf(lastError, lastErrorSize));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_DeleteStatus(status));

        TA_TCheckedServiceTicket* st = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_CheckServiceTicket(c, SRV_TICKET.data(), SRV_TICKET.size(), &st));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_DeleteServiceTicket(st));

        TA_TCheckedUserTicket* ut = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_CheckUserTicket(c, PROD_TICKET.data(), PROD_TICKET.size(), &ut));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_DeleteUserTicket(ut));

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Delete(c));

        UNIT_ASSERT_C(STREAM.Str().find("was successfully read") != TString::npos, STREAM.Str());
        UNIT_ASSERT_C(STREAM.Str().find("was successfully fetched") == TString::npos, STREAM.Str());
    }

    Y_UNIT_TEST(CreateClientWithError) {
        STREAM.Clear();
        // TODO
    }

    Y_UNIT_TEST(ClientDstId) {
        STREAM.Clear();
        TA_TTvmApiClientSettings* s = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Create(&s));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetSelfTvmId(s, OK_CLIENT));
        const TString dir = GetCachePath();
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetDiskCacheDir(s, dir.data(), dir.size()));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithTvmIds(
                              s,
                              "aaaaaaaaaaaaaaaa",
                              16,
                              "19",
                              2));

        struct TA_TTvmClient* c = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Create(s, Log, &c));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Delete(s));

        TA_TTvmClientStatus* status;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_GetStatus(c, &status));
        TA_ETvmClientStatusCode code;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Status_GetCode(status, &code));
        UNIT_ASSERT_EQUAL(TA_TCSC_OK, code);
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_DeleteStatus(status));

        char t[512];
        size_t size = 0;

        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 20, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 19, 512, t, &size));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(t, size), "3:serv:CBAQ__________9_IgYIKhCUkQY:CX");

        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForAlias(c, "ololo", 5, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForAlias(c, "bar", 3, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForAlias(c, "foo", 3, 512, t, &size));

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Delete(c));

        UNIT_ASSERT_C(STREAM.Str().find("was successfully read") != TString::npos, STREAM.Str());
        UNIT_ASSERT_C(STREAM.Str().find("was successfully fetched") == TString::npos, STREAM.Str());
    }

    Y_UNIT_TEST(ClientDstAlias) {
        STREAM.Clear();
        TA_TTvmApiClientSettings* s = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Create(&s));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetSelfTvmId(s, OK_CLIENT));
        const TString dir = GetCachePath();
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_SetDiskCacheDir(s, dir.data(), dir.size()));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_EnableServiceTicketsFetchOptionsWithAliases(
                              s,
                              "aaaaaaaaaaaaaaaa",
                              16,
                              "foo:19",
                              6));

        struct TA_TTvmClient* c = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Create(s, Log, &c));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmApiClientSettings_Delete(s));

        TA_TTvmClientStatus* status;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_GetStatus(c, &status));
        TA_ETvmClientStatusCode code;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Status_GetCode(status, &code));
        UNIT_ASSERT_EQUAL(TA_TCSC_OK, code);
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_DeleteStatus(status));

        char t[512];
        size_t size = 0;

        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 20, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 19, 512, t, &size));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(t, size), "3:serv:CBAQ__________9_IgYIKhCUkQY:CX");

        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForAlias(c, "ololo", 5, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForAlias(c, "foo", 3, 512, t, &size));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(t, size), "3:serv:CBAQ__________9_IgYIKhCUkQY:CX");

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Delete(c));

        UNIT_ASSERT_C(STREAM.Str().find("was successfully read") != TString::npos, STREAM.Str());
        UNIT_ASSERT_C(STREAM.Str().find("was successfully fetched") == TString::npos, STREAM.Str());
    }

    Y_UNIT_TEST(ToolClient) {
        STREAM.Clear();
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(port, []() { return new TTvmTool; });

        TA_TTvmToolClientSettings* s = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_Create("me", 2, &s));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_SetPort(s, port));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_SetHostname(s, "localhost", 9));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_SetAuthToken(s, AUTH_TOKEN.data(), AUTH_TOKEN.size()));

        struct TA_TTvmClient* c = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_CreateForTvmtool(s, Log, &c));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_Delete(s));

        TA_TTvmClientStatus* status;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_GetStatus(c, &status));
        TA_ETvmClientStatusCode code;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Status_GetCode(status, &code));
        UNIT_ASSERT_EQUAL(TA_TCSC_OK, code);
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_DeleteStatus(status));

        TA_TCheckedServiceTicket* st = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_CheckServiceTicket(c, SRV_TICKET.data(), SRV_TICKET.size(), &st));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_DeleteServiceTicket(st));

        TA_TCheckedUserTicket* ut = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_CheckUserTicket(c, PROD_YATEAM_TICKET.data(), PROD_YATEAM_TICKET.size(), &ut));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_DeleteUserTicket(ut));

        ut = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_INVALID_BLACKBOX_ENV, TA_TvmClient_CheckUserTicketWithOverridedEnv(c, PROD_YATEAM_TICKET.data(), PROD_YATEAM_TICKET.size(), TA_BE_PROD, &ut));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_DeleteUserTicket(ut));

        char t[512];
        size_t size = 0;

        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 20, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 242, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForTvmId(c, 11, 512, t, &size));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(t, size), "3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8");

        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS,
                          TA_TvmClient_GetServiceTicketForAlias(c, "ololo", 5, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForAlias(c, "bbox", 4, 512, t, &size));
        UNIT_ASSERT_EQUAL(TA_EC_OK,
                          TA_TvmClient_GetServiceTicketForAlias(c, "pass_likers", 11, 512, t, &size));
        UNIT_ASSERT_STRINGS_EQUAL(TStringBuf(t, size), "3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8");

        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmClient_Delete(c));

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "7: Meta info fetched from localhost:" << port << "\n"
                << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, idm_slug=<NULL>, dsts=[(pass_likers:11)(bbox:242)]\n"
                << "7: Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            STREAM.Str());
    }

    Y_UNIT_TEST(ToolClient_BadOverride) {
        STREAM.Clear();
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(port, []() { return new TTvmTool; });

        TA_TTvmToolClientSettings* s = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_Create("me", 2, &s));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_SetPort(s, port));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_SetHostname(s, "localhost", 9));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_SetAuthToken(s, AUTH_TOKEN.data(), AUTH_TOKEN.size()));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_OverrideBlackboxEnv(s, TA_BE_STRESS));

        struct TA_TTvmClient* c = nullptr;
        UNIT_ASSERT_EQUAL(TA_EC_BROKEN_TVM_CLIENT_SETTINGS, TA_TvmClient_CreateForTvmtool(s, Log, &c));
        UNIT_ASSERT_EQUAL(TA_EC_OK, TA_TvmToolClientSettings_Delete(s));
    }
}
