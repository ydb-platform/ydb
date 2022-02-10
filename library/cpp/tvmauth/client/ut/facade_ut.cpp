#include "common.h"

#include <library/cpp/tvmauth/client/facade.h>
#include <library/cpp/tvmauth/client/mocked_updater.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ClientFacade) {
    static const TTvmId OK_CLIENT = 100500;
    static const TString SRV_TICKET_123 = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU";
    static const TString SRV_TICKET_456 = "3:serv:CBAQ__________9_IgcIyAMQlJEG:VrnqRhpoiDnJeAQbySJluJ1moQ5Kemic99iWzOrHLGfuh7iTw_xMT7KewRAmZMUwDKzE6otj7V86Xsnxbv5xZl8746wbvNcyUXu-nGWmbByZjO7xpSIcY07sISqEhP9n9C_yMSvqDP7ho_PRIfpGCDMXxKlFZ_BhBLLp0kHEvw4";
    static const TString PROD_TICKET = "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_BEUv1x9CALU7do8irDlDYVeVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTyVetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490CJFw";
    static const TString TEST_TICKET = "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEhfDDeCLoVA-sJesxMl2pGW4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgtRoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKUX54";

    TTvmClient GetClient(const NTvmApi::TClientSettings& s) {
        auto l = MakeIntrusive<TLogger>();
        TTvmClient f(s, l);
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, f.GetStatus());
        Sleep(TDuration::MilliSeconds(300));
        TString logs = l->Stream.Str();
        UNIT_ASSERT_C(logs.find("was successfully read") != TString::npos, logs);
        UNIT_ASSERT_C(logs.find("was successfully fetched") == TString::npos, logs);
        return f;
    }

    Y_UNIT_TEST(Service) {
        NTvmApi::TClientSettings s;
        s.SetSelfTvmId(OK_CLIENT);
        s.EnableServiceTicketChecking();
        s.SetDiskCacheDir(GetCachePath());
        TTvmClient f = GetClient(s);

        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(2524608000), f.GetUpdateTimeOfPublicKeys());
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), f.GetUpdateTimeOfServiceTickets());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(2525126400), f.GetInvalidationTimeOfPublicKeys());
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), f.GetInvalidationTimeOfServiceTickets());

        UNIT_ASSERT(f.CheckServiceTicket(SRV_TICKET_123));
        UNIT_ASSERT_EXCEPTION(f.CheckUserTicket(PROD_TICKET), yexception);
        UNIT_ASSERT_EXCEPTION(f.CheckUserTicket(TEST_TICKET), yexception);
    }

    Y_UNIT_TEST(User) {
        NTvmApi::TClientSettings s;
        s.EnableUserTicketChecking(EBlackboxEnv::Prod);
        s.SetDiskCacheDir(GetCachePath());

        TTvmClient f = GetClient(s);
        UNIT_ASSERT_EXCEPTION(f.CheckServiceTicket(SRV_TICKET_123), yexception);
        UNIT_ASSERT(f.CheckUserTicket(PROD_TICKET));
        UNIT_ASSERT(!f.CheckUserTicket(TEST_TICKET));
    }

    Y_UNIT_TEST(Ctors) {
        NTvmApi::TClientSettings s;
        s.EnableUserTicketChecking(EBlackboxEnv::Prod);
        s.SetDiskCacheDir(GetCachePath());

        TTvmClient f = GetClient(s);
        f = GetClient(s);

        TVector<TTvmClient> v;
        v.push_back(std::move(f));
        v.front() = std::move(*v.begin());
    }

    Y_UNIT_TEST(Tickets) {
        NTvmApi::TClientSettings s;
        s.SetSelfTvmId(OK_CLIENT);
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}});
        s.SetDiskCacheDir(GetCachePath());
        TTvmClient f = GetClient(s);

        UNIT_ASSERT_VALUES_EQUAL(TInstant(), f.GetUpdateTimeOfPublicKeys());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::ParseIso8601("2050-01-01T00:00:00.000000Z"), f.GetUpdateTimeOfServiceTickets());
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), f.GetInvalidationTimeOfPublicKeys());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(std::numeric_limits<size_t>::max()), f.GetInvalidationTimeOfServiceTickets());

        UNIT_ASSERT_VALUES_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", f.GetServiceTicketFor("blackbox"));
        UNIT_ASSERT_VALUES_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", f.GetServiceTicketFor(19));
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor("blackbox2"),
                                       TBrokenTvmClientSettings,
                                       "Destination 'blackbox2' was not specified in settings. Check your settings (if you use Qloud/YP/tvmtool - check it's settings)");
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor(20),
                                       TBrokenTvmClientSettings,
                                       "Destination '20' was not specified in settings. Check your settings (if you use Qloud/YP/tvmtool - check it's settings)");
    }

    Y_UNIT_TEST(Tool) {
        TPortManager pm;
        ui16 port = pm.GetPort(80);
        NMock::TMockServer server(port, []() { return new TTvmTool; });

        NTvmTool::TClientSettings s("push-client");
        s.SetPort(port);
        s.SetAuthToken(AUTH_TOKEN);
        auto l = MakeIntrusive<TLogger>();
        {
            TTvmClient f(s, l);

            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(14380887840), f.GetUpdateTimeOfPublicKeys());
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(14380887840), f.GetUpdateTimeOfServiceTickets());
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(14381406240), f.GetInvalidationTimeOfPublicKeys());
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(std::numeric_limits<time_t>::max()), f.GetInvalidationTimeOfServiceTickets());

            UNIT_ASSERT_VALUES_EQUAL(SERVICE_TICKET_PC, f.GetServiceTicketFor("pass_likers"));
            UNIT_ASSERT_VALUES_EQUAL(SERVICE_TICKET_PC, f.GetServiceTicketFor(100502));
            UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor("blackbox"),
                                           TBrokenTvmClientSettings,
                                           "Destination 'blackbox' was not specified in settings. Check your settings (if you use Qloud/YP/tvmtool - check it's settings)");
            UNIT_ASSERT_EXCEPTION_CONTAINS(f.GetServiceTicketFor(242),
                                           TBrokenTvmClientSettings,
                                           "Destination '242' was not specified in settings. Check your settings (if you use Qloud/YP/tvmtool - check it's settings)");
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "7: Meta info fetched from localhost:" << port << "\n"
                << "6: Meta: self_tvm_id=100501, bb_env=ProdYateam, dsts=[(pass_likers:100502)]\n"
                << "7: Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
                << "7: Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            l->Stream.Str());
    }

    Y_UNIT_TEST(CheckRoles) {
        { // roles not configured
            TTvmClient f(new TMockedUpdater(TMockedUpdater::TSettings{
                .SelfTvmId = OK_CLIENT,
            }));

            UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok,
                                     f.CheckServiceTicket(SRV_TICKET_123).GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok,
                                     f.CheckServiceTicket(SRV_TICKET_456).GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Malformed,
                                     f.CheckServiceTicket("asdfg").GetStatus());
        }

        { // roles configured
            NRoles::TRolesPtr roles = std::make_shared<NRoles::TRoles>(
                NRoles::TRoles::TMeta{},
                NRoles::TRoles::TTvmConsumers{
                    {123, std::make_shared<NRoles::TConsumerRoles>(
                              THashMap<TString, NRoles::TEntitiesPtr>())},
                },
                NRoles::TRoles::TUserConsumers{},
                std::make_shared<TString>());
            TTvmClient f(new TMockedUpdater(TMockedUpdater::TSettings{
                .SelfTvmId = OK_CLIENT,
                .Roles = roles,
            }));

            UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Ok,
                                     f.CheckServiceTicket(SRV_TICKET_123).GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::NoRoles,
                                     f.CheckServiceTicket(SRV_TICKET_456).GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(ETicketStatus::Malformed,
                                     f.CheckServiceTicket("asdfg").GetStatus());
        }
    }
}
