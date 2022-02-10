#include <library/cpp/tvmauth/client/misc/api/dynamic_dst/tvm_client.h> 
 
#include <library/cpp/tvmauth/client/misc/disk_cache.h> 
 
#include <library/cpp/tvmauth/unittest.h> 
 
#include <library/cpp/testing/unittest/registar.h>
 
#include <util/stream/file.h> 
#include <util/system/fs.h> 
 
#include <regex> 
 
using namespace NTvmAuth; 
using namespace NTvmAuth::NDynamicClient; 
 
Y_UNIT_TEST_SUITE(DynamicClient) { 
    static const std::regex TIME_REGEX(R"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d.\d{6}Z)"); 
    static const TString CACHE_DIR = "./tmp/"; 
 
    static void WriteFile(TString name, TStringBuf body, TInstant time) { 
        NFs::Remove(CACHE_DIR + name); 
        TFileOutput f(CACHE_DIR + name); 
        f << TDiskWriter::PrepareData(time, body); 
    } 
 
    static void CleanCache() { 
        NFs::RemoveRecursive(CACHE_DIR); 
        NFs::MakeDirectoryRecursive(CACHE_DIR); 
    } 
 
    class TLogger: public NTvmAuth::ILogger { 
    public: 
        void Log(int lvl, const TString& msg) override { 
            Cout << TInstant::Now() << " lvl=" << lvl << " msg: " << msg << "\n"; 
            Stream << lvl << ": " << msg << Endl; 
        } 
 
        TStringStream Stream; 
    }; 
 
    class TOfflineUpdater: public NDynamicClient::TTvmClient { 
    public: 
        TOfflineUpdater(const NTvmApi::TClientSettings& settings, 
                        TIntrusivePtr<TLogger> l, 
                        bool fail = true, 
                        std::vector<TString> tickets = {}) 
            : TTvmClient(settings, l) 
            , Fail(fail) 
            , Tickets(std::move(tickets)) 
        { 
            Init(); 
            ExpBackoff_.SetEnabled(false); 
        } 
 
        NUtils::TFetchResult FetchServiceTicketsFromHttp(const TString& req) const override { 
            if (Fail) { 
                throw yexception() << "tickets: alarm"; 
            } 
 
            TString response; 
            if (!Tickets.empty()) { 
                response = Tickets.front(); 
                Tickets.erase(Tickets.begin()); 
            } 
 
            Cout << "*** FetchServiceTicketsFromHttp. request: " << req << ". response: " << response << Endl; 
            return {200, {}, "/2/ticket", response, ""}; 
        } 
 
        NUtils::TFetchResult FetchPublicKeysFromHttp() const override { 
            if (Fail) { 
                throw yexception() << "keysalarm"; 
            } 
            Cout << "*** FetchPublicKeysFromHttp" << Endl; 
            return {200, {}, "/2/keys", PublicKeys, ""}; 
        } 
 
        using TTvmClient::GetDsts; 
        using TTvmClient::ProcessTasks; 
        using TTvmClient::SetResponseForTask; 
        using TTvmClient::Worker; 
 
        bool Fail = true; 
        TString PublicKeys = NUnittest::TVMKNIFE_PUBLIC_KEYS; 
        mutable std::vector<TString> Tickets; 
    }; 
 
    Y_UNIT_TEST(StartWithIncompleteTicketsSet) {
        TInstant now = TInstant::Now(); 
        CleanCache(); 
        WriteFile("./service_tickets", 
                  R"({"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})" 
                  "\t100500", 
                  now); 
 
        NTvmApi::TClientSettings s; 
        s.SetSelfTvmId(100500); 
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}, {"kolmo", 213}}, false);
        s.SetDiskCacheDir(CACHE_DIR);

        auto l = MakeIntrusive<TLogger>();

        {
            TOfflineUpdater client(s, 
                                   l, 
                                   false, 
                                   { 
                                       R"({"213"  : { "error" : "some error"}})", 
                                       R"({"123"  : { "ticket" : "service_ticket_3"}})", 
                                   }); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::IncompleteTicketsSet, client.GetStatus());
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->ErrorsById.contains(213)); 

            NThreading::TFuture<TAddResponse> fut = client.Add({123});
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::IncompleteTicketsSet, client.GetStatus());

            client.Worker();
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::IncompleteTicketsSet, client.GetStatus());

            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 

            UNIT_ASSERT(fut.HasValue());
            TAddResponse resp{
                {123, {EDstStatus::Success, ""}},
            };
            UNIT_ASSERT_VALUES_EQUAL(resp, fut.GetValue());

            UNIT_ASSERT(client.Tickets.empty()); 

            TDsts dsts{19, 123, 213};
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts());

            UNIT_ASSERT_EXCEPTION_CONTAINS(client.GetOptionalServiceTicketFor(213), TMissingServiceTicket, "some error");
        }
    }

    Y_UNIT_TEST(StartWithEmptyTicketsSet) {
        CleanCache(); 

        NTvmApi::TClientSettings s;
        s.SetSelfTvmId(100500);
        s.EnableServiceTicketsFetchOptions("qwerty", {{"kolmo", 213}}, false);
        s.SetDiskCacheDir(CACHE_DIR);

        auto l = MakeIntrusive<TLogger>();

        {
            TOfflineUpdater client(s, 
                                   l, 
                                   false, 
                                   { 
                                       R"({"213"  : { "error" : "some error"}})", 
                                       R"({"123"  : { "ticket" : "3:serv:CBAQ__________9_IgYIlJEGEHs:CcafYQH-FF5XaXMuJrgLZj98bIC54cs1ZkcFS9VV_9YM9iOM_0PXCtMkdg85rFjxE_BMpg7bE8ZuoqNfdw0FPt0BAKNeISwlydj4o0IjY82--LZBpP8CRn-EpAnkRaDShdlfrcF2pk1SSmEX8xdyZVQEnkUPY0cHGlFnu231vnE"}})", 
                                   }); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::IncompleteTicketsSet, client.GetStatus());
            UNIT_ASSERT(!client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(client.GetOptionalServiceTicketFor(213), TMissingServiceTicket, "some error");

            NThreading::TFuture<TAddResponse> fut = client.Add({123});
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::IncompleteTicketsSet, client.GetStatus());

            client.Worker();
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::IncompleteTicketsSet, client.GetStatus());

            UNIT_ASSERT(!client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 

            UNIT_ASSERT(fut.HasValue());
            TAddResponse resp{
                {123, {EDstStatus::Success, ""}},
            };
            UNIT_ASSERT_VALUES_EQUAL(resp, fut.GetValue());

            UNIT_ASSERT(client.Tickets.empty()); 

            TDsts dsts{123, 213};
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts());

            UNIT_ASSERT_EXCEPTION_CONTAINS(client.GetOptionalServiceTicketFor(213), TMissingServiceTicket, "some error");
        }
    };
    Y_UNIT_TEST(StartWithIncompleteCacheAndAdd) {
        TInstant now = TInstant::Now();
        CleanCache(); 
        WriteFile("./service_tickets", 
                  R"({"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})"
                  "\t100500",
                  now);

        NTvmApi::TClientSettings s;
        s.SetSelfTvmId(100500);
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}, {"kolmo", 213}}); 
        s.SetDiskCacheDir(CACHE_DIR); 
 
        auto l = MakeIntrusive<TLogger>(); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS(TOfflineUpdater(s, l), 
                                       TRetriableException, 
                                       "Failed to start TvmClient. You can retry: ServiceTickets: tickets: alarm"); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/service_tickets' was successfully read\n" 
                << "6: Got 1 service ticket(s) from disk\n" 
                << "6: Cache was updated with 1 service ticket(s): XXXXXXXXXXX\n" 
                << "7: File './tmp/retry_settings' does not exist\n" 
                << "4: Failed to get ServiceTickets: tickets: alarm\n" 
                << "4: Failed to get ServiceTickets: tickets: alarm\n" 
                << "4: Failed to get ServiceTickets: tickets: alarm\n" 
                << "4: Failed to update service tickets: tickets: alarm\n", 
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX")); 
        l->Stream.Str().clear(); 
 
        { 
            TOfflineUpdater client(s, 
                                   l, 
                                   false, 
                                   { 
                                       R"({"213" : { "ticket" : "service_ticket_2"}})", 
                                       R"({"123"  : { "ticket" : "service_ticket_3"}})", 
                                   }); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
 
            NThreading::TFuture<TAddResponse> fut = client.Add({123}); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
 
            client.Worker(); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 
 
            UNIT_ASSERT(fut.HasValue()); 
            TAddResponse resp{ 
                {123, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp, fut.GetValue()); 
 
            UNIT_ASSERT(client.Tickets.empty()); 
 
            TDsts dsts{19, 123, 213}; 
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts()); 
        } 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/service_tickets' was successfully read\n" 
                << "6: Got 1 service ticket(s) from disk\n" 
                << "6: Cache was updated with 1 service ticket(s): " << TInstant::Seconds(now.Seconds()) << "\n" 
                << "7: File './tmp/retry_settings' does not exist\n" 
                << "7: Response with service tickets for 1 destination(s) was successfully fetched from https://tvm-api.yandex.net\n" 
                << "7: Got responses with service tickets with 1 pages for 1 destination(s)\n" 
                << "6: Cache was partly updated with 1 service ticket(s). total: 2\n" 
                << "6: File './tmp/service_tickets' was successfully written\n" 
                << "7: Adding dst: got task #1 with 1 dsts\n" 
                << "7: Response with service tickets for 1 destination(s) was successfully fetched from https://tvm-api.yandex.net\n" 
                << "7: Got responses with service tickets with 1 pages for 1 destination(s)\n" 
                << "6: Cache was partly updated with 1 service ticket(s). total: 3\n" 
                << "6: File './tmp/service_tickets' was successfully written\n" 
                << "7: Adding dst: task #1: dst=123 got ticket\n" 
                << "7: Adding dst: task #1: set value\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(StartWithCacheAndAdd) { 
        TInstant now = TInstant::Now(); 
        CleanCache(); 
        WriteFile("./service_tickets", 
                  R"({"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})" 
                  "\t100500", 
                  now); 
 
        NTvmApi::TClientSettings s; 
        s.SetSelfTvmId(100500); 
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}}); 
        s.SetDiskCacheDir(CACHE_DIR); 
 
        auto l = MakeIntrusive<TLogger>(); 
        { 
            TOfflineUpdater client(s, l); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
 
            client.Fail = false; 
            client.Tickets = { 
                R"({"123"  : { "ticket" : "service_ticket_3"}, "213" : { "ticket" : "service_ticket_2"}})", 
            }; 
            NThreading::TFuture<TAddResponse> fut = client.Add({123, 213}); 
 
            client.Worker(); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 
 
            UNIT_ASSERT(fut.HasValue()); 
            TAddResponse resp{ 
                {123, {EDstStatus::Success, ""}}, 
                {213, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp, fut.GetValue()); 
 
            UNIT_ASSERT(client.Tickets.empty()); 
 
            TDsts dsts{19, 123, 213}; 
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts()); 
        } 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/service_tickets' was successfully read\n" 
                << "6: Got 1 service ticket(s) from disk\n" 
                << "6: Cache was updated with 1 service ticket(s): " << TInstant::Seconds(now.Seconds()) << "\n" 
                << "7: File './tmp/retry_settings' does not exist\n" 
                << "7: Adding dst: got task #1 with 2 dsts\n" 
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n" 
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n" 
                << "6: Cache was partly updated with 2 service ticket(s). total: 3\n" 
                << "6: File './tmp/service_tickets' was successfully written\n" 
                << "7: Adding dst: task #1: dst=123 got ticket\n" 
                << "7: Adding dst: task #1: dst=213 got ticket\n" 
                << "7: Adding dst: task #1: set value\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(StartWithCacheAndAddSeveral) { 
        TInstant now = TInstant::Now(); 
        CleanCache(); 
        WriteFile("./service_tickets", 
                  R"({"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})" 
                  "\t100500", 
                  now); 
 
        NTvmApi::TClientSettings s; 
        s.SetSelfTvmId(100500); 
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}}); 
        s.SetDiskCacheDir(CACHE_DIR); 
 
        auto l = MakeIntrusive<TLogger>(); 
        { 
            TOfflineUpdater client(s, l); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
 
            client.Fail = false; 
            client.Tickets = { 
                R"({"123"  : { "ticket" : "service_ticket_3"}, "213" : { "ticket" : "service_ticket_2"}})", 
            }; 
            NThreading::TFuture<TAddResponse> fut1 = client.Add({123}); 
            NThreading::TFuture<TAddResponse> fut2 = client.Add({213}); 
 
            client.Worker(); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 
 
            UNIT_ASSERT(fut1.HasValue()); 
            TAddResponse resp1{ 
                {123, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp1, fut1.GetValue()); 
 
            UNIT_ASSERT(fut2.HasValue()); 
            TAddResponse resp2{ 
                {213, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp2, fut2.GetValue()); 
 
            UNIT_ASSERT(client.Tickets.empty()); 
 
            TDsts dsts{19, 123, 213}; 
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts()); 
        } 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/service_tickets' was successfully read\n" 
                << "6: Got 1 service ticket(s) from disk\n" 
                << "6: Cache was updated with 1 service ticket(s): " << TInstant::Seconds(now.Seconds()) << "\n" 
                << "7: File './tmp/retry_settings' does not exist\n" 
                << "7: Adding dst: got task #1 with 1 dsts\n" 
                << "7: Adding dst: got task #2 with 1 dsts\n" 
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n" 
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n" 
                << "6: Cache was partly updated with 2 service ticket(s). total: 3\n" 
                << "6: File './tmp/service_tickets' was successfully written\n" 
                << "7: Adding dst: task #1: dst=123 got ticket\n" 
                << "7: Adding dst: task #1: set value\n" 
                << "7: Adding dst: task #2: dst=213 got ticket\n" 
                << "7: Adding dst: task #2: set value\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(StartWithCacheAndAddSeveralWithErrors) { 
        TInstant now = TInstant::Now(); 
        CleanCache(); 
        WriteFile("./service_tickets", 
                  R"({"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})" 
                  "\t100500", 
                  now); 
 
        NTvmApi::TClientSettings s; 
        s.SetSelfTvmId(100500); 
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}}); 
        s.SetDiskCacheDir(CACHE_DIR); 
 
        auto l = MakeIntrusive<TLogger>(); 
        { 
            TOfflineUpdater client(s, l); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
 
            UNIT_ASSERT(client.GetOptionalServiceTicketFor(19)); 
            UNIT_ASSERT_VALUES_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", 
                                     *client.GetOptionalServiceTicketFor(19)); 
            UNIT_ASSERT(!client.GetOptionalServiceTicketFor(456)); 
 
            client.Fail = false; 
            client.Tickets = { 
                R"({ 
                    "123"  : { "ticket" : "service_ticket_3"}, 
                    "213" : { "ticket" : "service_ticket_2"}, 
                    "456"  : { "error" : "error_3"} 
                })", 
            }; 
            NThreading::TFuture<TAddResponse> fut1 = client.Add({123, 213}); 
            NThreading::TFuture<TAddResponse> fut2 = client.Add({213, 456}); 
 
            client.Worker(); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->TicketsById.contains(456)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->ErrorsById.contains(456)); 
 
            UNIT_ASSERT(client.GetOptionalServiceTicketFor(19)); 
            UNIT_ASSERT_VALUES_EQUAL("3:serv:CBAQ__________9_IgYIKhCUkQY:CX", 
                                     *client.GetOptionalServiceTicketFor(19)); 
            UNIT_ASSERT_EXCEPTION_CONTAINS(client.GetOptionalServiceTicketFor(456), 
                                           TMissingServiceTicket, 
                                           "Failed to get ticket for '456': error_3"); 
 
            UNIT_ASSERT(fut1.HasValue()); 
            TAddResponse resp1{ 
                {123, {EDstStatus::Success, ""}}, 
                {213, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp1, fut1.GetValue()); 
 
            UNIT_ASSERT(fut2.HasValue()); 
            TAddResponse resp2{ 
                {213, {EDstStatus::Success, ""}}, 
                {456, {EDstStatus::Fail, "error_3"}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp2, fut2.GetValue()); 
 
            UNIT_ASSERT(client.Tickets.empty()); 
 
            TDsts dsts{19, 123, 213}; 
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts()); 
        } 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/service_tickets' was successfully read\n" 
                << "6: Got 1 service ticket(s) from disk\n" 
                << "6: Cache was updated with 1 service ticket(s): " << TInstant::Seconds(now.Seconds()) << "\n" 
                << "7: File './tmp/retry_settings' does not exist\n" 
                << "7: Adding dst: got task #1 with 2 dsts\n" 
                << "7: Adding dst: got task #2 with 2 dsts\n" 
                << "7: Response with service tickets for 3 destination(s) was successfully fetched from https://tvm-api.yandex.net\n" 
                << "7: Got responses with service tickets with 1 pages for 3 destination(s)\n" 
                << "3: Failed to get service ticket for dst=456: error_3\n" 
                << "6: Cache was partly updated with 2 service ticket(s). total: 3\n" 
                << "6: File './tmp/service_tickets' was successfully written\n" 
                << "7: Adding dst: task #1: dst=123 got ticket\n" 
                << "7: Adding dst: task #1: dst=213 got ticket\n" 
                << "7: Adding dst: task #1: set value\n" 
                << "7: Adding dst: task #2: dst=213 got ticket\n" 
                << "4: Adding dst: task #2: dst=456 failed to get ticket: error_3\n" 
                << "7: Adding dst: task #2: set value\n", 
            l->Stream.Str()); 
    } 
 
    Y_UNIT_TEST(WithException) { 
        TInstant now = TInstant::Now(); 
        CleanCache(); 
        WriteFile("./service_tickets", 
                  R"({"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})" 
                  "\t100500", 
                  now); 
 
        NTvmApi::TClientSettings s; 
        s.SetSelfTvmId(100500); 
        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}}); 
        s.SetDiskCacheDir(CACHE_DIR); 
 
        auto l = MakeIntrusive<TLogger>(); 
        { 
            TOfflineUpdater client(s, l); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
 
            client.Fail = false; 
            client.Tickets = { 
                R"({ 
                    "123"  : { "ticket" : "service_ticket_3"}, 
                    "213" : { "ticket" : "service_ticket_2"}, 
                    "456"  : { "error" : "error_3"}, 
                    "789" : { "ticket" : "service_ticket_4"} 
                })", 
            }; 
            NThreading::TFuture<TAddResponse> fut1 = client.Add({123, 213}); 
            NThreading::TFuture<TAddResponse> fut2 = client.Add({213, 456}); 
            NThreading::TFuture<TAddResponse> fut3 = client.Add({789}); 
 
            fut2.Subscribe([](const auto&) { 
                throw yexception() << "planed exc"; 
            }); 
            fut3.Subscribe([](const auto&) { 
                throw 5; 
            }); 
 
            UNIT_ASSERT_NO_EXCEPTION(client.Worker()); 
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, client.GetStatus()); 
 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(19)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(213)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->TicketsById.contains(123)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->TicketsById.contains(456)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(19)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(213)); 
            UNIT_ASSERT(!client.GetCachedServiceTickets()->ErrorsById.contains(123)); 
            UNIT_ASSERT(client.GetCachedServiceTickets()->ErrorsById.contains(456)); 
 
            UNIT_ASSERT(fut1.HasValue()); 
            TAddResponse resp1{ 
                {123, {EDstStatus::Success, ""}}, 
                {213, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp1, fut1.GetValue()); 
 
            UNIT_ASSERT(fut2.HasValue()); 
            TAddResponse resp2{ 
                {213, {EDstStatus::Success, ""}}, 
                {456, {EDstStatus::Fail, "error_3"}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp2, fut2.GetValue()); 
 
            UNIT_ASSERT(fut3.HasValue()); 
            TAddResponse resp3{ 
                {789, {EDstStatus::Success, ""}}, 
            }; 
            UNIT_ASSERT_VALUES_EQUAL(resp3, fut3.GetValue()); 
 
            UNIT_ASSERT(client.Tickets.empty()); 
 
            TDsts dsts{19, 123, 213, 789}; 
            UNIT_ASSERT_VALUES_EQUAL(dsts, client.GetDsts()); 
        } 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/service_tickets' was successfully read\n" 
                << "6: Got 1 service ticket(s) from disk\n" 
                << "6: Cache was updated with 1 service ticket(s): " << TInstant::Seconds(now.Seconds()) << "\n" 
                << "7: File './tmp/retry_settings' does not exist\n" 
                << "7: Adding dst: got task #1 with 2 dsts\n" 
                << "7: Adding dst: got task #2 with 2 dsts\n" 
                << "7: Adding dst: got task #3 with 1 dsts\n" 
                << "7: Response with service tickets for 4 destination(s) was successfully fetched from https://tvm-api.yandex.net\n" 
                << "7: Got responses with service tickets with 1 pages for 4 destination(s)\n" 
                << "3: Failed to get service ticket for dst=456: error_3\n" 
                << "6: Cache was partly updated with 3 service ticket(s). total: 4\n" 
                << "6: File './tmp/service_tickets' was successfully written\n" 
                << "7: Adding dst: task #1: dst=123 got ticket\n" 
                << "7: Adding dst: task #1: dst=213 got ticket\n" 
                << "7: Adding dst: task #1: set value\n" 
                << "7: Adding dst: task #2: dst=213 got ticket\n" 
                << "4: Adding dst: task #2: dst=456 failed to get ticket: error_3\n" 
                << "7: Adding dst: task #2: set value\n" 
                << "3: Adding dst: task #2: exception: planed exc\n" 
                << "7: Adding dst: task #3: dst=789 got ticket\n" 
                << "7: Adding dst: task #3: set value\n" 
                << "3: Adding dst: task #3: exception: unknown error\n", 
            l->Stream.Str()); 
    } 
} 
 
template <> 
void Out<NTvmAuth::NDynamicClient::TDstResponse>(IOutputStream& out, const NTvmAuth::NDynamicClient::TDstResponse& m) { 
    out << m.Status << " (" << m.Error << ")"; 
} 
 
template <> 
void Out<NTvmAuth::NTvmApi::TClientSettings::TDst>(IOutputStream& out, const NTvmAuth::NTvmApi::TClientSettings::TDst& m) { 
    out << m.Id; 
} 
