// DO_NOT_STYLE

#include <library/c/tvmauth/high_lvl_wrapper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/tvmauth/client/ut/common.h>

#include <util/stream/str.h>

Y_UNIT_TEST_SUITE(CHighLvlWarapper) {
    static ui32 OK_CLIENT = 100500;
    static const TString SRV_TICKET = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU";
    static const TString PROD_TICKET = "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_BEUv1x9CALU7do8irDlDYVeVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTyVetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490CJFw";

    Y_UNIT_TEST(Settings) {
        NTvmAuthWrapper::TTvmApiClientSettings s;

        UNIT_ASSERT_EXCEPTION(s.SetSelfTvmId(0), std::runtime_error);
        UNIT_ASSERT_NO_EXCEPTION(s.EnableServiceTicketChecking());

        UNIT_ASSERT_NO_EXCEPTION(s.SetSelfTvmId(OK_CLIENT));
        UNIT_ASSERT_NO_EXCEPTION(s.EnableServiceTicketChecking());

        UNIT_ASSERT_EXCEPTION(s.SetDiskCacheDir(""), std::runtime_error);
    }

    static TStringStream STREAM;
    static void Log(int lvl, const char* msg) {
        STREAM << lvl << ": " << msg << Endl;
    }

    NTvmAuthWrapper::TTvmClient GetClient(const NTvmAuthWrapper::TTvmApiClientSettings& s, TA_ETvmClientStatusCode code = TA_TCSC_OK) {
        STREAM.Clear();
        NTvmAuthWrapper::TTvmClient f(s, Log);
        Sleep(TDuration::MilliSeconds(300));
        UNIT_ASSERT_EQUAL(code, f.GetStatus().Code);
        UNIT_ASSERT_C(STREAM.Str().find("was successfully read") != TString::npos, STREAM.Str());
        UNIT_ASSERT_C(STREAM.Str().find("was successfully fetched") == TString::npos, STREAM.Str());
        return f;
    }

    Y_UNIT_TEST(Service) {
        NTvmAuthWrapper::TTvmApiClientSettings s;
        s.SetSelfTvmId(OK_CLIENT);
        s.EnableServiceTicketChecking();
        s.SetDiskCacheDir(GetCachePath());
        NTvmAuthWrapper::TTvmClient f = GetClient(s);

        UNIT_ASSERT(f.CheckServiceTicket(SRV_TICKET));
        UNIT_ASSERT_EXCEPTION(f.CheckUserTicket(PROD_TICKET), std::runtime_error);
    }

    Y_UNIT_TEST(User) {
        NTvmAuthWrapper::TTvmApiClientSettings s;
        s.EnableUserTicketChecking(TA_BE_PROD);
        s.SetDiskCacheDir(GetCachePath());

        NTvmAuthWrapper::TTvmClient f = GetClient(s);
        UNIT_ASSERT_EXCEPTION(f.CheckServiceTicket(SRV_TICKET), std::runtime_error);
        UNIT_ASSERT(f.CheckUserTicket(PROD_TICKET));
    }

    Y_UNIT_TEST(Consts) {
        UNIT_ASSERT_VALUES_EQUAL("222", NTvmAuthWrapper::NBlackboxTvmId::Prod);
        UNIT_ASSERT_VALUES_EQUAL("224", NTvmAuthWrapper::NBlackboxTvmId::Test);
        UNIT_ASSERT_VALUES_EQUAL("223", NTvmAuthWrapper::NBlackboxTvmId::ProdYateam);
        UNIT_ASSERT_VALUES_EQUAL("225", NTvmAuthWrapper::NBlackboxTvmId::TestYateam);
        UNIT_ASSERT_VALUES_EQUAL("226", NTvmAuthWrapper::NBlackboxTvmId::Stress);
        UNIT_ASSERT_VALUES_EQUAL("239", NTvmAuthWrapper::NBlackboxTvmId::Mimino);
    }

    Y_UNIT_TEST(GetTicketTvmId) {
        NTvmAuthWrapper::TTvmApiClientSettings s;
        s.SetSelfTvmId(OK_CLIENT);
        s.EnableServiceTicketsFetchOptions("aaaaaaaaaaaaaaaa",
                                           NTvmAuthWrapper::TTvmApiClientSettings::TDstVector(1, 19));
        s.SetDiskCacheDir(GetCachePath());
        NTvmAuthWrapper::TTvmClient f = GetClient(s);

        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor(20), std::runtime_error, "TVM settings are broken");
        UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", f.GetServiceTicketFor(19));

        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor("ololo"), std::runtime_error, "TVM settings are broken");
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor("foo"), std::runtime_error, "TVM settings are broken");
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor("bar"), std::runtime_error, "TVM settings are broken");
    }

    Y_UNIT_TEST(GetTicketAlias) {
        NTvmAuthWrapper::TTvmApiClientSettings s;
        s.SetSelfTvmId(OK_CLIENT);
        s.EnableServiceTicketsFetchOptions("aaaaaaaaaaaaaaaa",
                                           {
                                               {"foo", 19},
                                           });
        s.SetDiskCacheDir(GetCachePath());
        NTvmAuthWrapper::TTvmClient f = GetClient(s);

        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor(20), std::runtime_error, "TVM settings are broken");
        UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", f.GetServiceTicketFor(19));

        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor("ololo"), std::runtime_error, "TVM settings are broken");
        UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", f.GetServiceTicketFor("foo"));
    }

    Y_UNIT_TEST(Tvmtool) {
        STREAM.Clear();
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(port, []() { return new TTvmTool; });

        NTvmAuthWrapper::TTvmToolClientSettings s("me");
        s.SetPort(port);
        s.SetHostname("localhost");
        s.SetAuthtoken(AUTH_TOKEN);
        s.OverrideBlackboxEnv(TA_BE_PROD);

        {
            NTvmAuthWrapper::TTvmClient c(s, Log);
            UNIT_ASSERT_EQUAL(TA_TCSC_OK, c.GetStatus().Code);
            UNIT_ASSERT_VALUES_EQUAL("OK", c.GetStatus().LastError);

            NTvmAuthWrapper::TCheckedServiceTicket st = c.CheckServiceTicket(SRV_TICKET);
            UNIT_ASSERT_EQUAL(TA_EC_OK, st.GetStatus());

            NTvmAuthWrapper::TCheckedUserTicket ut = c.CheckUserTicket(PROD_TICKET);
            UNIT_ASSERT_EQUAL(TA_EC_OK, ut.GetStatus());
            UNIT_ASSERT_EQUAL(TA_EC_INVALID_BLACKBOX_ENV, c.CheckUserTicket(PROD_TICKET, TA_BE_PROD_YATEAM).GetStatus());

            UNIT_ASSERT_EXCEPTION_CONTAINS(c.GetServiceTicketFor(20), std::runtime_error, "TVM settings are broken");
            UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc",
                                      c.GetServiceTicketFor(242));
            UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8",
                                      c.GetServiceTicketFor(11));

            UNIT_ASSERT_EXCEPTION_CONTAINS(c.GetServiceTicketFor("ololo"), std::runtime_error, "TVM settings are broken");
            UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc",
                                      c.GetServiceTicketFor("bbox"));
            UNIT_ASSERT_STRINGS_EQUAL("3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8",
                                      c.GetServiceTicketFor("pass_likers"));

            Sleep(TDuration::MilliSeconds(300));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "7: Meta info fetched from localhost:" << port << "\n"
                << "6: Meta: self_tvm_id=100500, bb_env=ProdYateam, idm_slug=<NULL>, dsts=[(pass_likers:11)(bbox:242)]\n"
                << "6: Meta: override blackbox env: ProdYateam->Prod\n"
                << "7: Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            STREAM.Str());
    }

    Y_UNIT_TEST(DummyLogger) {
        {
            NTvmAuthWrapper::TTvmApiClientSettings s;
            s.SetSelfTvmId(OK_CLIENT);
            s.EnableServiceTicketChecking();
            s.SetDiskCacheDir(GetCachePath());

            UNIT_ASSERT_EXCEPTION_CONTAINS(NTvmAuthWrapper::TTvmClient(s, nullptr),
                                           std::runtime_error,
                                           "Invalid function parameter");

            NTvmAuthWrapper::TTvmClient f(s, TA_NoopLogger);
        }

        {
            TPortManager pm;
            ui16 port = pm.GetPort(80);
            NMock::TMockServer server(port, []() { return new TTvmTool; });

            NTvmAuthWrapper::TTvmToolClientSettings s("me");
            s.SetPort(port);
            s.SetHostname("localhost");
            s.SetAuthtoken(AUTH_TOKEN);
            s.OverrideBlackboxEnv(TA_BE_PROD);

            UNIT_ASSERT_EXCEPTION_CONTAINS(NTvmAuthWrapper::TTvmClient(s, nullptr),
                                           std::runtime_error,
                                           "Invalid function parameter");

            NTvmAuthWrapper::TTvmClient c(s, TA_NoopLogger);
        }
    }
}
