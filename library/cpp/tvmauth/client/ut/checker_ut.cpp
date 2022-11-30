#include "common.h"

#include <library/cpp/tvmauth/client/mocked_updater.h>
#include <library/cpp/tvmauth/client/misc/checker.h>
#include <library/cpp/tvmauth/client/misc/getter.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>

#include <library/cpp/tvmauth/type.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ClientChecker) {
    static const TTvmId OK_CLIENT = 100500;
    static const TString PROD_TICKET = "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_BEUv1x9CALU7do8irDlDYVeVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTyVetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490CJFw";
    static const TString TEST_TICKET = "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEhfDDeCLoVA-sJesxMl2pGW4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgtRoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKUX54";
    static const TString PROD_YATEAM_TICKET = "3:user:CAwQ__________9_Gg4KAgh7EHsg0oXYzAQoAg:M9dEFEWHLHXiL7brCsyfYlm254PE6VeshUjI62u2qMDRzt6-0jAoJTIdDiogerItht1YFYSn8fSqmMf23_rueGj-wkmvyNzbcBSk3jtK2U5sai_W0bK6OwukR9tzWzi1Gcgg9DrNEeIKFvs1EBqYCF4mPHWo5bgk0CR580Cgit4";
    static const TString TEST_YATEAM_TICKET = "3:user:CA4Q__________9_Gg4KAgh7EHsg0oXYzAQoAw:IlaV3htk3jYrviIOz3k3Dfwz7p-bYYpbrgdn53GiUrMGdrT9eobHeuzNvPLrWB0yuYZAD46C3MGxok4GGmHhT73mki4XOCX8yWT4jW_hzcHBik1442tjWwh8IWqV_7q5j5496suVuLWjnZORWbb7I-2iwdIlU1BUiDfhoAolCq8";
    static const TString STRESS_TICKET = "3:user:CA8Q__________9_Gg4KAgh7EHsg0oXYzAQoBA:GBuG_TLo6SL2OYFxp7Zly04HPNzmAF7Fu2E8E9SnwQDoxq9rf7VThSPtTmnBSAl5UVRRPkMsRtzzHZ87qtj6l-PvF0K7PrDu7-yS_xiFTgAl9sEfXAIHJVzZLoksGRgpoBtpBUg9vVaJsPns0kWFKJgq8M-Mk9agrSk7sb2VUeQ";
    static const TString SRV_TICKET = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU";

    Y_UNIT_TEST(User) {
        NTvmApi::TClientSettings s{
            .DiskCacheDir = GetCachePath(),
            .SelfTvmId = OK_CLIENT,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            UNIT_ASSERT_EXCEPTION(TUserTicketChecker::Check("kek", u->GetCachedUserContext()), TBrokenTvmClientSettings);
        }
        UNIT_ASSERT_C(l->Stream.Str().find("was successfully fetched") == TString::npos, l->Stream.Str());

        s.CheckUserTicketsWithBbEnv = EBlackboxEnv::Prod;
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedUserContext({});
            UNIT_ASSERT(TUserTicketChecker::Check(PROD_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(STRESS_TICKET, c));

            c = u->GetCachedUserContext(EBlackboxEnv::ProdYateam);
            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_TICKET, c));
            UNIT_ASSERT(TUserTicketChecker::Check(PROD_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(STRESS_TICKET, c));

            UNIT_ASSERT_EXCEPTION(TUserTicketChecker::Check(PROD_TICKET, u->GetCachedUserContext(EBlackboxEnv::Stress)), TBrokenTvmClientSettings);
        }

        s.CheckUserTicketsWithBbEnv = EBlackboxEnv::Test;
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedUserContext();

            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_TICKET, c));
            UNIT_ASSERT(TUserTicketChecker::Check(TEST_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(STRESS_TICKET, c));
        }

        s.CheckUserTicketsWithBbEnv = EBlackboxEnv::ProdYateam;
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedUserContext();

            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_TICKET, c));
            UNIT_ASSERT(TUserTicketChecker::Check(PROD_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(STRESS_TICKET, c));
        }

        s.CheckUserTicketsWithBbEnv = EBlackboxEnv::TestYateam;
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedUserContext();

            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_YATEAM_TICKET, c));
            UNIT_ASSERT(TUserTicketChecker::Check(TEST_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(STRESS_TICKET, c));
        }

        s.CheckUserTicketsWithBbEnv = EBlackboxEnv::Stress;
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedUserContext();

            UNIT_ASSERT(TUserTicketChecker::Check(PROD_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(PROD_YATEAM_TICKET, c));
            UNIT_ASSERT(!TUserTicketChecker::Check(TEST_YATEAM_TICKET, c));
            UNIT_ASSERT(TUserTicketChecker::Check(STRESS_TICKET, c));
        }
    }

    Y_UNIT_TEST(Service) {
        NTvmApi::TClientSettings s{
            .DiskCacheDir = GetCachePath(),
            .SelfTvmId = OK_CLIENT,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Stress,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            UNIT_ASSERT_EXCEPTION(TServiceTicketChecker::Check(SRV_TICKET, u->GetCachedServiceContext()), TBrokenTvmClientSettings);
        }
        UNIT_ASSERT_C(l->Stream.Str().find("was successfully fetched") == TString::npos, l->Stream.Str());

        s.CheckServiceTickets = true;
        l = MakeIntrusive<TLogger>();
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedServiceContext();

            UNIT_ASSERT(TServiceTicketChecker::Check(SRV_TICKET, c));
            UNIT_ASSERT(!TServiceTicketChecker::Check(PROD_TICKET, c));
        }
        UNIT_ASSERT_C(l->Stream.Str().find("was successfully fetched") == TString::npos, l->Stream.Str());

        s.SelfTvmId = 17;
        l = MakeIntrusive<TLogger>();
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());

            auto c = u->GetCachedServiceContext();

            UNIT_ASSERT(!TServiceTicketChecker::Check(SRV_TICKET, c));
            UNIT_ASSERT(!TServiceTicketChecker::Check(PROD_TICKET, c));
        }
        UNIT_ASSERT_C(l->Stream.Str().find("was successfully fetched") == TString::npos, l->Stream.Str());
    }

    Y_UNIT_TEST(Tickets) {
        NTvmApi::TClientSettings s{
            .DiskCacheDir = GetCachePath(),
            .SelfTvmId = OK_CLIENT,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}},
        };

        auto l = MakeIntrusive<TLogger>();
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus().GetCode());
            auto c = u->GetCachedServiceTickets();
            UNIT_ASSERT_VALUES_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", TServiceTicketGetter::GetTicket("blackbox", c));
            UNIT_ASSERT_EXCEPTION_CONTAINS(TServiceTicketGetter::GetTicket("blackbox2", c),
                                           TBrokenTvmClientSettings,
                                           "Destination 'blackbox2' was not specified in settings. Check your settings (if you use Qloud/YP/tvmtool - check it's settings)");
        }
        UNIT_ASSERT_C(l->Stream.Str().find("was successfully fetched") == TString::npos, l->Stream.Str());
    }

    Y_UNIT_TEST(ErrorForDst) {
        TAsyncUpdaterPtr u = new TMockedUpdater();
        auto c = u->GetCachedServiceTickets();

        UNIT_ASSERT_VALUES_EQUAL(TMockedUpdater::TSettings::CreateDeafult().Backends.at(0).Value,
                                 TServiceTicketGetter::GetTicket("my_dest", c));
        UNIT_ASSERT_VALUES_EQUAL(TMockedUpdater::TSettings::CreateDeafult().Backends.at(0).Value,
                                 TServiceTicketGetter::GetTicket(42, c));
        UNIT_ASSERT_EXCEPTION_CONTAINS(TServiceTicketGetter::GetTicket("my_bad_dest", c),
                                       TMissingServiceTicket,
                                       "Failed to get ticket for 'my_bad_dest': Dst is not found");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TServiceTicketGetter::GetTicket(43, c),
                                       TMissingServiceTicket,
                                       "Failed to get ticket for '43': Dst is not found");
    }
}
