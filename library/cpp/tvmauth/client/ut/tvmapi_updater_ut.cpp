#include "common.h"

#include <library/cpp/tvmauth/client/mocked_updater.h>
#include <library/cpp/tvmauth/client/misc/disk_cache.h>
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>
#include <util/string/subst.h>
#include <util/system/fs.h>

#include <regex>

using namespace NTvmAuth;
static const std::regex TIME_REGEX(R"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d.\d{6}Z)");

Y_UNIT_TEST_SUITE(ApiUpdater) {
    static const TString SRV_TICKET = "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRpQOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU";
    static const TString TEST_TICKET = "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEhfDDeCLoVA-sJesxMl2pGW4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgtRoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKUX54";
    static const TString TVM_RESPONSE =
        R"({
            "19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
            "213" : { "ticket" : "service_ticket_2"},
            "234" : { "error" : "Dst is not found" },
            "185" : { "ticket" : "service_ticket_3"},
            "deprecated" : { "ticket" : "deprecated_ticket" }
        })";

    static const TString CACHE_DIR = "./tmp/";

    static void CleanCache() {
        NFs::RemoveRecursive(CACHE_DIR);
        NFs::MakeDirectoryRecursive(CACHE_DIR);
    }

    Y_UNIT_TEST(MockedUpdater) {
        TMockedUpdater m;
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, m.GetStatus());
        UNIT_ASSERT(m.GetCachedServiceContext()->Check(SRV_TICKET));
        UNIT_ASSERT(m.GetCachedUserContext()->Check(TEST_TICKET));
    }

    Y_UNIT_TEST(Updater) {
        NTvmApi::TClientSettings s{
            .DiskCacheDir = GetCachePath(),
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            auto u = NTvmApi::TThreadedUpdater::Create(s, l);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u->GetStatus());
        }

        UNIT_ASSERT_C(l->Stream.Str().find("was successfully read") != TString::npos, l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().find("were successfully fetched") == TString::npos, l->Stream.Str());
    }

    Y_UNIT_TEST(Updater_badConfig) {
        NTvmApi::TClientSettings s;
        UNIT_ASSERT_EXCEPTION(NTvmApi::TThreadedUpdater::Create(s, TDevNullLogger::IAmBrave()), yexception);
        s.SelfTvmId = 100500;
        UNIT_ASSERT_EXCEPTION(NTvmApi::TThreadedUpdater::Create(s, TDevNullLogger::IAmBrave()), yexception);
        s.DiskCacheDir = GetCachePath();
        UNIT_ASSERT_EXCEPTION(NTvmApi::TThreadedUpdater::Create(s, TDevNullLogger::IAmBrave()), yexception);
    }

    class TOfflineUpdater: public NTvmApi::TThreadedUpdater {
        bool Enabled_;
        TString PublicKeys_;

    public:
        TOfflineUpdater(const NTvmApi::TClientSettings& settings,
                        TIntrusivePtr<TLogger> l,
                        bool enabled = false,
                        TString keys = NUnittest::TVMKNIFE_PUBLIC_KEYS)
            : NTvmApi::TThreadedUpdater(settings, l)
            , Enabled_(enabled)
            , PublicKeys_(keys)
        {
            Init();
            StartWorker();
        }

        NUtils::TFetchResult FetchServiceTicketsFromHttp(const TString&) const override {
            if (!Enabled_) {
                throw yexception() << "alarm";
            }
            return {200, {}, "/2/ticket", TVM_RESPONSE, ""};
        }

        NUtils::TFetchResult FetchPublicKeysFromHttp() const override {
            if (!Enabled_) {
                throw yexception() << "alarm";
            }
            return {200, {}, "/2/keys", PublicKeys_, ""};
        }
    };

    Y_UNIT_TEST(StartWithoutCache) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TOfflineUpdater(s, l),
                                       TRetriableException,
                                       "Failed to start TvmClient. You can retry:");

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n"
                << "4: Failed to get ServiceTickets: alarm\n"
                << "4: Failed to get ServiceTickets: alarm\n"
                << "4: Failed to get ServiceTickets: alarm\n"
                << "4: Failed to update service tickets: alarm\n"
                << "3: Service tickets have not been refreshed for too long period\n",
            l->Stream.Str());
    }

    static void WriteFile(TString name, TStringBuf body, TInstant time) {
        NFs::Remove(CACHE_DIR + name);
        TFileOutput f(CACHE_DIR + name);
        f << TDiskWriter::PrepareData(time, body);
    }

    Y_UNIT_TEST(StartWithOldCache) {
        CleanCache();
        WriteFile("./public_keys",
                  NUnittest::TVMKNIFE_PUBLIC_KEYS,
                  TInstant::Now() - TDuration::Days(30)); // too old
        WriteFile("./service_tickets",
                  R"({"19":{"ticket":"3:serv:CBAQACIGCJSRBhAL:Fi"}})"
                  "\t100500",
                  TInstant::Now()); // too old

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOfflineUpdater u(s, l, true);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }

        UNIT_ASSERT_C(l->Stream.Str().find("Disk cache (public keys) is too old") != TString::npos, l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().find("Disk cache (service tickets) is too old") != TString::npos, l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().find("were successfully fetched") != TString::npos, l->Stream.Str());
    }

    Y_UNIT_TEST(StartWithMissingCache) {
        NTvmApi::TClientSettings s{
            .DiskCacheDir = "../",
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TOfflineUpdater(s, l),
                                       TRetriableException,
                                       "Failed to start TvmClient. You can retry: ");

        UNIT_ASSERT_C(l->Stream.Str().find("does not exist") != TString::npos, l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().find("were successfully fetched") == TString::npos, l->Stream.Str());
    }

    Y_UNIT_TEST(StartWithBadCache_Tickets) {
        CleanCache();
        WriteFile("./service_tickets",
                  TVM_RESPONSE,
                  TInstant::Now());

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOfflineUpdater u(s, l, true);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/service_tickets' was successfully read\n"
                << "4: Failed to read service tickets from disk: YYYYYYYYYYYYYYY\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n"
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n"
                << "6: Cache was updated with 2 service ticket(s): XXXXXXXXXXX\n"
                << "6: File './tmp/service_tickets' was successfully written\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"),
                               std::regex(R"(Failed to read service tickets from disk: [^\n]+)"),
                               "Failed to read service tickets from disk: YYYYYYYYYYYYYYY"));
    }

    Y_UNIT_TEST(StartWithBadCache_PublicKeys) {
        CleanCache();
        WriteFile("./public_keys",
                  "ksjdafnlskdjzfgbhdl",
                  TInstant::Now());

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TOfflineUpdater(s, l),
                                       TRetriableException,
                                       "Failed to start TvmClient. You can retry:");

        UNIT_ASSERT_C(l->Stream.Str().find("4: Failed to read public keys from disk: Malformed TVM keys") != TString::npos, l->Stream.Str());
    }

    Y_UNIT_TEST(StartWithCacheForAnotherTvmId) {
        CleanCache();
        WriteFile("./service_tickets",
                  TVM_RESPONSE + "\t" + "100499",
                  TInstant::Now());

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOfflineUpdater u(s, l, true);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/service_tickets' was successfully read\n"
                << "4: Disk cache is for another tvmId (100499). Self=100500\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n"
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n"
                << "6: Cache was updated with 2 service ticket(s): XXXXXXXXXXX\n"
                << "6: File './tmp/service_tickets' was successfully written\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

    Y_UNIT_TEST(StartWithCacheForAnotherDsts) {
        CleanCache();
        TInstant now = TInstant::Now();
        WriteFile("./service_tickets",
                  R"({"213" : { "ticket" : "3:serv:CBAQ__________9_IgYIlJEGEAs:T-"}})"
                  "\t"
                  "100500",
                  now);

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOfflineUpdater u(s, l, true);
            auto cache = u.GetCachedServiceTickets();
            UNIT_ASSERT(cache->TicketsById.contains(213));
            UNIT_ASSERT(cache->TicketsById.contains(19));
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
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
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            l->Stream.Str());
        l->Stream.Clear();

        {
            TOfflineUpdater u(s, l, true);
            auto cache = u.GetCachedServiceTickets();
            UNIT_ASSERT(cache->TicketsById.contains(213));
            UNIT_ASSERT(cache->TicketsById.contains(19));
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/service_tickets' was successfully read\n"
                << "4: tvm_id in cache is not integer: deprecated\n"
                << "6: Got 3 service ticket(s) from disk\n"
                << "6: Cache was updated with 2 service ticket(s): XXXXXXXXXXX\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

    Y_UNIT_TEST(StartWithNotFreshCacheForAnotherDsts) {
        CleanCache();
        TInstant now = TInstant::Now();
        WriteFile("./service_tickets",
                  R"({"213" : { "ticket" : "3:serv:CBAQ__________9_IgYIlJEGEAs:T-"}})"
                  "\t"
                  "100500",
                  now - TDuration::Hours(2));

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOfflineUpdater u(s, l, true);
            auto cache = u.GetCachedServiceTickets();
            UNIT_ASSERT(cache->TicketsById.contains(213));
            UNIT_ASSERT(cache->TicketsById.contains(19));
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/service_tickets' was successfully read\n"
                << "6: Got 1 service ticket(s) from disk\n"
                << "6: Cache was updated with 1 service ticket(s): XXXXXXXXXXX\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n"
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n"
                << "6: Cache was updated with 2 service ticket(s): XXXXXXXXXXX\n"
                << "6: File './tmp/service_tickets' was successfully written\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
        l->Stream.Clear();

        {
            TOfflineUpdater u(s, l, true);
            auto cache = u.GetCachedServiceTickets();
            UNIT_ASSERT(cache->TicketsById.contains(213));
            UNIT_ASSERT(cache->TicketsById.contains(19));
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/service_tickets' was successfully read\n"
                << "4: tvm_id in cache is not integer: deprecated\n"
                << "6: Got 3 service ticket(s) from disk\n"
                << "6: Cache was updated with 2 service ticket(s): XXXXXXXXXXX\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

    Y_UNIT_TEST(StartWithPartialDiskCache) {
        CleanCache();
        WriteFile("./public_keys",
                  NUnittest::TVMKNIFE_PUBLIC_KEYS,
                  TInstant::Now());

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"kolmo", 213}},
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOfflineUpdater u(s, l, true);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "7: File './tmp/service_tickets' does not exist\n"
                << "6: File './tmp/public_keys' was successfully read\n"
                << "6: Cache was updated with public keys: XXXXXXXXXXX\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n"
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n"
                << "6: Cache was updated with 2 service ticket(s): XXXXXXXXXXX\n"
                << "6: File './tmp/service_tickets' was successfully written\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

    Y_UNIT_TEST(StartFromHttpAndRestartFromDisk) {
        CleanCache();

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
        };

        {
            auto l = MakeIntrusive<TLogger>();
            {
                TOfflineUpdater u(s, l, true);
                UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
            }

            UNIT_ASSERT_VALUES_EQUAL(
                TStringBuilder()
                    << "7: File './tmp/service_tickets' does not exist\n"
                    << "7: File './tmp/public_keys' does not exist\n"
                    << "7: File './tmp/retry_settings' does not exist\n"
                    << "7: Response with service tickets for 1 destination(s) was successfully fetched from https://tvm-api.yandex.net\n"
                    << "7: Got responses with service tickets with 1 pages for 1 destination(s)\n"
                    << "6: Cache was updated with 1 service ticket(s): XXXXXXXXXXX\n"
                    << "6: File './tmp/service_tickets' was successfully written\n"
                    << "7: Public keys were successfully fetched from https://tvm-api.yandex.net\n"
                    << "6: Cache was updated with public keys: XXXXXXXXXXX\n"
                    << "6: File './tmp/public_keys' was successfully written\n"
                    << "7: Thread-worker started\n"
                    << "7: Thread-worker stopped\n",
                std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
        }

        {
            auto l = MakeIntrusive<TLogger>();
            {
                TOfflineUpdater u(s, l, true);
                UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
            }

            UNIT_ASSERT_VALUES_EQUAL(
                TStringBuilder()
                    << "6: File './tmp/service_tickets' was successfully read\n"
                    << "4: tvm_id in cache is not integer: deprecated\n"
                    << "6: Got 3 service ticket(s) from disk\n"
                    << "6: Cache was updated with 1 service ticket(s): XXXXXXXXXXX\n"
                    << "6: File './tmp/public_keys' was successfully read\n"
                    << "6: Cache was updated with public keys: XXXXXXXXXXX\n"
                    << "7: File './tmp/retry_settings' does not exist\n"
                    << "7: Thread-worker started\n"
                    << "7: Thread-worker stopped\n",
                std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
        }
    }

    class TUnstableUpdater: public NTvmApi::TThreadedUpdater {
        mutable int V1_ = 0;
        mutable int V2_ = 0;

    public:
        TUnstableUpdater(const NTvmApi::TClientSettings& settings, TIntrusivePtr<TLogger> l)
            : NTvmApi::TThreadedUpdater(settings, l)
        {
            UNIT_ASSERT_NO_EXCEPTION_C(Init(), l->Stream.Str());
            ExpBackoff_.SetEnabled(false);
            StartWorker();

            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Ok, GetStatus(), l->Stream.Str());

            Sleep(TDuration::MicroSeconds(100));
            PublicKeysDurations_.Expiring = TDuration::MicroSeconds(100);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus(TClientStatus::Warning, "Internal client error: failed to collect last useful error message, please report this message to tvm-dev@yandex-team.ru"),
                                       GetStatus(),
                                       l->Stream.Str());

            PublicKeysDurations_.Invalid = TDuration::MicroSeconds(20);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Error, GetStatus(), l->Stream.Str());

            PublicKeysDurations_.Expiring = TDuration::Seconds(100);
            PublicKeysDurations_.Invalid = TDuration::Seconds(200);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Ok, GetStatus(), l->Stream.Str());

            ServiceTicketsDurations_.Expiring = TDuration::MicroSeconds(100);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Warning, GetStatus(), l->Stream.Str());

            ServiceTicketsDurations_.Invalid = TDuration::MicroSeconds(20);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Warning, GetStatus(), l->Stream.Str());

            const TInstant* inv = &GetCachedServiceTickets()->InvalidationTime;
            *const_cast<TInstant*>(inv) = TInstant::Now() + TDuration::Seconds(30);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Error, GetStatus(), l->Stream.Str());
        }

        NUtils::TFetchResult FetchServiceTicketsFromHttp(const TString&) const override {
            Y_ENSURE_EX(++V1_ > 1, yexception() << "++v1_ > 1:" << V1_);
            return {200, {}, "/2/ticket", TVM_RESPONSE, ""};
        }

        NUtils::TFetchResult FetchPublicKeysFromHttp() const override {
            Y_ENSURE_EX(++V2_ > 2, yexception() << "++v2_ > 2:" << V2_);
            return {200, {}, "/2/keys", NUnittest::TVMKNIFE_PUBLIC_KEYS, ""};
        }
    };

    Y_UNIT_TEST(StartFromUnstableHttp) {
        CleanCache();

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TUnstableUpdater u(s, l);
        }

        UNIT_ASSERT_C(l->Stream.Str().Contains("++v1_ > 1"), l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().Contains("++v2_ > 2"), l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().Contains("7: Response with service tickets for 1 destination(s) was successfully fetched from https://tvm-api.yandex.net"), l->Stream.Str());
        UNIT_ASSERT_C(l->Stream.Str().Contains("7: Public keys were successfully fetched"), l->Stream.Str());
    }

    Y_UNIT_TEST(GetUpdateTimeOfServiceTickets) {
        CleanCache();
        TInstant ins = TInstant::Now();
        WriteFile("./service_tickets",
                  TVM_RESPONSE + "\t" + "100500",
                  ins);

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}},
        };

        auto l = MakeIntrusive<TLogger>();
        TOfflineUpdater u(s, l, true);
        UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), u.GetUpdateTimeOfPublicKeys());
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(ins.Seconds()), u.GetUpdateTimeOfServiceTickets());
    }

    class TSignalingUpdater: public NTvmApi::TThreadedUpdater {
        mutable int V_ = 0;
        TAutoEvent& Ev_;
        const TStringBuf PublicKeys_;

    public:
        TSignalingUpdater(const NTvmApi::TClientSettings& settings,
                          TLoggerPtr l,
                          TAutoEvent& ev,
                          const TStringBuf keys = NUnittest::TVMKNIFE_PUBLIC_KEYS)
            : NTvmApi::TThreadedUpdater(settings, l)
            , Ev_(ev)
            , PublicKeys_(keys)
        {
            WorkerAwakingPeriod_ = TDuration::MilliSeconds(300);
            PublicKeysDurations_.RefreshPeriod = TDuration::MilliSeconds(700);
            Init();
            ExpBackoff_.SetEnabled(false);
            StartWorker();
        }

        NUtils::TFetchResult FetchPublicKeysFromHttp() const override {
            if (++V_ >= 2) {
                Ev_.Signal();
            }
            return {200, {}, "/2/keys", TString(PublicKeys_), ""};
        }
    };

    Y_UNIT_TEST(StartWorker) {
        class TSignalingUpdater: public NTvmApi::TThreadedUpdater {
            mutable int V_ = 0;
            TAutoEvent& Ev_;

        public:
            TSignalingUpdater(const NTvmApi::TClientSettings& settings, TLoggerPtr l, TAutoEvent& ev)
                : NTvmApi::TThreadedUpdater(settings, l)
                , Ev_(ev)
            {
                WorkerAwakingPeriod_ = TDuration::MilliSeconds(300);
                PublicKeysDurations_.RefreshPeriod = TDuration::MilliSeconds(700);
                Init();
                ExpBackoff_.SetEnabled(false);
                StartWorker();
            }

            void Worker() override {
                NTvmApi::TThreadedUpdater::Worker();
                Ev_.Signal();
            }

            NUtils::TFetchResult FetchPublicKeysFromHttp() const override {
                if (++V_ < 4) {
                    return {500, {}, "/2/keys", "lol", ""};
                }
                return {200, {}, "/2/keys", NUnittest::TVMKNIFE_PUBLIC_KEYS, "CAEQChkAAAAAAAD4PyGamZmZmZm5PyhkMAE4B0BGSAI"};
            }
        };

        CleanCache();
        TInstant expiringPubKeys = TInstant::Now() - TDuration::Days(3);
        WriteFile("./public_keys", NUnittest::TVMKNIFE_PUBLIC_KEYS, expiringPubKeys);

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        TAutoEvent ev;
        {
            TSignalingUpdater u(s, l, ev);
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus(TClientStatus::Warning, "PublicKeys: Path:/2/keys.Code=500: lol"),
                                     u.GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(TInstant::Seconds(expiringPubKeys.Seconds()), u.GetUpdateTimeOfPublicKeys());
            UNIT_ASSERT_VALUES_EQUAL(TInstant(), u.GetUpdateTimeOfServiceTickets());

            UNIT_ASSERT(ev.WaitT(TDuration::Seconds(15)));
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Ok, u.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/public_keys' was successfully read\n"
                << "6: Cache was updated with public keys: XXXXXXXXXXX\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "4: Failed to get PublicKeys: Path:/2/keys.Code=500: lol\n"
                << "4: Failed to get PublicKeys: Path:/2/keys.Code=500: lol\n"
                << "4: Failed to get PublicKeys: Path:/2/keys.Code=500: lol\n"
                << "4: Failed to update public keys: Path:/2/keys.Code=500: lol\n"
                << "3: Public keys have not been refreshed for too long period\n"
                << "7: Thread-worker started\n"
                << "7: Retry settings were updated: exponential_backoff_min:0.000000s->1.000000s;exponential_backoff_max:60.000000s->10.000000s;exponential_backoff_factor:2->1.5;exponential_backoff_jitter:0.5->0.1;max_random_sleep_default:5.000000s->0.100000s;retries_on_start:3->1;worker_awaking_period:10.000000s->7.000000s;dsts_limit:300->70;\n"
                << "6: File './tmp/retry_settings' was successfully written\n"
                << "7: Public keys were successfully fetched from https://tvm-api.yandex.net\n"
                << "6: Cache was updated with public keys: XXXXXXXXXXX\n"
                << "6: File './tmp/public_keys' was successfully written\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

#if defined(_unix_)
    Y_UNIT_TEST(StartFromCacheAndBadPublicKeysFromHttp) {
        CleanCache();
        TInstant now = TInstant::Now();
        WriteFile("public_keys", NUnittest::TVMKNIFE_PUBLIC_KEYS, now - TDuration::Days(3)); // expiring public keys

        NTvmApi::TClientSettings s{
            .DiskCacheDir = CACHE_DIR,
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TAutoEvent ev;
            TSignalingUpdater u(s, l, ev, "malformed keys");
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus(TClientStatus::Warning, "PublicKeys: Malformed TVM keys"),
                                     u.GetStatus());

            UNIT_ASSERT(ev.WaitT(TDuration::Seconds(15)));
            UNIT_ASSERT_VALUES_EQUAL(TClientStatus::Warning, u.GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: File './tmp/public_keys' was successfully read\n"
                << "6: Cache was updated with public keys: " << TInstant::Seconds((now - TDuration::Days(3)).Seconds()) << "\n"
                << "7: File './tmp/retry_settings' does not exist\n"
                << "7: Public keys were successfully fetched from https://tvm-api.yandex.net\n"
                << "4: Failed to update public keys: Malformed TVM keys\n"
                << "3: Public keys have not been refreshed for too long period\n"
                << "7: Thread-worker started\n"
                << "7: Public keys were successfully fetched from https://tvm-api.yandex.net\n"
                << "4: Failed to update public keys: Malformed TVM keys\n"
                << "3: Public keys have not been refreshed for too long period\n"
                << "7: Thread-worker stopped\n",
            l->Stream.Str());
    }
#endif

    Y_UNIT_TEST(StartWithBadPublicKeysFromHttp) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        TAutoEvent ev;
        UNIT_ASSERT_EXCEPTION_CONTAINS(TOfflineUpdater(s, l, true, "some public keys"),
                                       TRetriableException,
                                       "Failed to start TvmClient. You can retry:");
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n"
                << "7: Public keys were successfully fetched from https://tvm-api.yandex.net\n"
                << "4: Failed to update public keys: Malformed TVM keys\n"
                << "3: Public keys have not been refreshed for too long period\n",
            l->Stream.Str());
    }

    class TNotInitedUpdater: public NTvmApi::TThreadedUpdater {
    public:
        TNotInitedUpdater(const NTvmApi::TClientSettings& settings, TLoggerPtr l = TDevNullLogger::IAmBrave())
            : NTvmApi::TThreadedUpdater(settings, l)
        {
            this->ExpBackoff_.SetEnabled(false);
        }

        using NTvmApi::TThreadedUpdater::AppendToJsonArray;
        using NTvmApi::TThreadedUpdater::AreServicesTicketsOk;
        using NTvmApi::TThreadedUpdater::CreateJsonArray;
        using NTvmApi::TThreadedUpdater::FindMissingDsts;
        using NTvmApi::TThreadedUpdater::GetPublicKeysFromHttp;
        using NTvmApi::TThreadedUpdater::GetServiceTicketsFromHttp;
        using NTvmApi::TThreadedUpdater::Init;
        using NTvmApi::TThreadedUpdater::IsServiceContextOk;
        using NTvmApi::TThreadedUpdater::IsTimeToUpdatePublicKeys;
        using NTvmApi::TThreadedUpdater::IsTimeToUpdateServiceTickets;
        using NTvmApi::TThreadedUpdater::IsUserContextOk;
        using NTvmApi::TThreadedUpdater::ParseTicketsFromDisk;
        using NTvmApi::TThreadedUpdater::ParseTicketsFromResponse;
        using NTvmApi::TThreadedUpdater::PrepareRequestForServiceTickets;
        using NTvmApi::TThreadedUpdater::PrepareTicketsForDisk;
        using NTvmApi::TThreadedUpdater::SetServiceContext;
        using NTvmApi::TThreadedUpdater::SetServiceTickets;
        using NTvmApi::TThreadedUpdater::SetUserContext;
        using NTvmApi::TThreadedUpdater::THttpResult;
        using NTvmApi::TThreadedUpdater::TPairTicketsErrors;
        using TAsyncUpdaterBase::IsServiceTicketMapOk;
    };

    Y_UNIT_TEST(IsCacheComplete_Empty) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"blackbox2", 20}},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
        };

        TNotInitedUpdater u(s);
        UNIT_ASSERT(!u.AreServicesTicketsOk());
    }

    Y_UNIT_TEST(IsCacheComplete_Tickets) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"blackbox2", 20}},
        };

        TNotInitedUpdater u(s);
        UNIT_ASSERT(!u.AreServicesTicketsOk());

        u.SetServiceTickets(MakeIntrusiveConst<TServiceTickets>(
            TServiceTickets::TMapIdStr({{1, "mega_ticket"}}),
            TServiceTickets::TMapIdStr({{2, "mega_error"}}),
            TServiceTickets::TMapAliasId()));
        UNIT_ASSERT(!u.AreServicesTicketsOk());

        u.SetServiceTickets(MakeIntrusiveConst<TServiceTickets>(
            TServiceTickets::TMapIdStr({
                {1, "mega_ticket"},
                {2, "mega_ticket2"},
            }),
            TServiceTickets::TMapIdStr({
                {3, "mega_error3"},
            }),
            TServiceTickets::TMapAliasId()));
        UNIT_ASSERT(u.AreServicesTicketsOk());
    }

    Y_UNIT_TEST(IsCacheComplete_Service) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        TNotInitedUpdater u(s);
        UNIT_ASSERT(!u.IsServiceContextOk());

        u.SetServiceContext(MakeIntrusiveConst<TServiceContext>(
            TServiceContext::CheckingFactory(100500, NUnittest::TVMKNIFE_PUBLIC_KEYS)));
        UNIT_ASSERT(u.IsServiceContextOk());
    }

    Y_UNIT_TEST(IsCacheComplete_User) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
        };

        TNotInitedUpdater u(s);
        UNIT_ASSERT(!u.IsUserContextOk());

        u.SetUserContext(NUnittest::TVMKNIFE_PUBLIC_KEYS);
        UNIT_ASSERT(u.IsUserContextOk());
    }

    Y_UNIT_TEST(TicketsOnDisk) {
        TString res = R"({
            "19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
            "213" : { "ticket" : "service_ticket_2"},
            "234" : { "error" : "Dst is not found" },
            "185" : { "ticket" : "service_ticket_3"},
            "deprecated" : { "ticket" : "deprecated_ticket" }
        })";
        res.append("\t100500");

        UNIT_ASSERT_VALUES_EQUAL(res, TNotInitedUpdater::PrepareTicketsForDisk(TVM_RESPONSE, 100500));

        auto pair = TNotInitedUpdater::ParseTicketsFromDisk(res);
        UNIT_ASSERT_VALUES_EQUAL(pair.first, TVM_RESPONSE);
        UNIT_ASSERT_VALUES_EQUAL(pair.second, 100500);

        res.push_back('a');
        UNIT_ASSERT_EXCEPTION(TNotInitedUpdater::ParseTicketsFromDisk(res), yexception);
    }

    Y_UNIT_TEST(IsTimeToUpdatePublicKeys) {
        NTvmApi::TClientSettings s{
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
        };

        TNotInitedUpdater u(s);

        UNIT_ASSERT(!u.IsTimeToUpdatePublicKeys(TInstant::Now()));
        UNIT_ASSERT(!u.IsTimeToUpdatePublicKeys(TInstant::Now() - TDuration::Hours(23)));
        UNIT_ASSERT(u.IsTimeToUpdatePublicKeys(TInstant::Now() - TDuration::Days(1) - TDuration::MilliSeconds(1)));
    }

    Y_UNIT_TEST(IsTimeToUpdateServiceTickets) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"blackbox2", 20}},
        };

        TNotInitedUpdater u(s);

        UNIT_ASSERT(!u.IsTimeToUpdateServiceTickets(TInstant::Now() - TDuration::Minutes(59)));
        UNIT_ASSERT(u.IsTimeToUpdateServiceTickets(TInstant::Now() - TDuration::Hours(1) - TDuration::MilliSeconds(1)));
    }

    Y_UNIT_TEST(StartWithIncompliteCache) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}, {"blackbox2", 20}},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
        };

        auto l = MakeIntrusive<TLogger>();
        UNIT_ASSERT_EXCEPTION_CONTAINS(TOfflineUpdater(s, l, true),
                                       TNonRetriableException,
                                       "Failed to get ServiceTicket for 20: Missing tvm_id in response, should never happend: 20");

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from https://tvm-api.yandex.net\n"
                << "7: Got responses with service tickets with 1 pages for 2 destination(s)\n"
                << "3: Failed to get service ticket for dst=20: Missing tvm_id in response, should never happend: 20\n"
                << "6: Cache was updated with 1 service ticket(s): XXXXXXXXXXX\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

    Y_UNIT_TEST(PrepareRequestForServiceTickets) {
        const TServiceContext ctx = TServiceContext::SigningFactory("AAAAAAAAAAAAAAAAAAAAAA");

        TString s = TNotInitedUpdater::PrepareRequestForServiceTickets(117,
                                                                       ctx,
                                                                       {19, 20},
                                                                       NUtils::TProcInfo{
                                                                           "__some_pid__",
                                                                           "__some_pname__",
                                                                           "kar",
                                                                       },
                                                                       100700);
        SubstGlobal(s.resize(s.size() - 5), "deb_", "");
        UNIT_ASSERT_VALUES_EQUAL("grant_type=client_credentials&src=117&dst=19,20&ts=100700&sign=XTz2Obd6PII_BHxswzWPJTjju9SrKsN6hyu1VsyxBvU&get_retry_settings=yes&_pid=__some_pid__&_procces_name=__some_pname__&lib_version=client_kar",
                                 s);

        s = TNotInitedUpdater::PrepareRequestForServiceTickets(118,
                                                               ctx,
                                                               {19},
                                                               NUtils::TProcInfo{
                                                                   "__some_pid__",
                                                                   {},
                                                                   "kva_",
                                                               },
                                                               100900);
        SubstGlobal(s.resize(s.size() - 5), "deb_", "");
        UNIT_ASSERT_VALUES_EQUAL("grant_type=client_credentials&src=118&dst=19&ts=100900&sign=-trBo9AtBLjp2ihy6cFAdMAQ6S9afHj23rFzYQ32jkQ&get_retry_settings=yes&_pid=__some_pid__&lib_version=client_kva_",
                                 s);
    }

    Y_UNIT_TEST(ParseTicketsFromResponse) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        TNotInitedUpdater u(s, l);

        TNotInitedUpdater::TPairTicketsErrors t;
        UNIT_ASSERT_EXCEPTION_CONTAINS(u.ParseTicketsFromResponse("{", NTvmApi::TDstSet{19}, t),
                                       yexception,
                                       "Invalid json from tvm-api");

        t = {};
        u.ParseTicketsFromResponse(TVM_RESPONSE, NTvmApi::TDstSet{19}, t);

        TNotInitedUpdater::TPairTicketsErrors expected{{{19, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}}, {}};
        UNIT_ASSERT_VALUES_EQUAL("6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n",
                                 l->Stream.Str());
        UNIT_ASSERT_EQUAL(expected, t);

        t = {};
        u.ParseTicketsFromResponse(TVM_RESPONSE,
                                   NTvmApi::TDstSet{19, 213, 234, 235},
                                   t);
        expected = {{{19, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
                     {213, "service_ticket_2"}},
                    {{234, "Dst is not found"},
                     {235, "Missing tvm_id in response, should never happend: 235"}}};
        UNIT_ASSERT_EQUAL(expected, t);
        UNIT_ASSERT_VALUES_EQUAL("6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n",
                                 l->Stream.Str());

        t = {};
        u.ParseTicketsFromResponse(
            R"([
                {"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},"234" : { "error" : "Dst is not found" }},
                {"213" : { "ticket" : "service_ticket_2"},"185" : { "ticket" : "service_ticket_3"}},
                {"deprecated" : { "ticket" : "deprecated_ticket" }}
            ])",
            NTvmApi::TDstSet{19, 213, 234, 235},
            t);
        expected = {{{19, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
                     {213, "service_ticket_2"}},
                    {{234, "Dst is not found"},
                     {235, "Missing tvm_id in response, should never happend: 235"}}};
        UNIT_ASSERT_EQUAL(expected, t);
        UNIT_ASSERT_VALUES_EQUAL("6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n",
                                 l->Stream.Str());
    }

    Y_UNIT_TEST(ParseTicketsFromResponseAsArray) {
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
        };

        auto l = MakeIntrusive<TLogger>();
        TNotInitedUpdater u(s, l);

        TNotInitedUpdater::TPairTicketsErrors t;
        UNIT_ASSERT_EXCEPTION_CONTAINS(u.ParseTicketsFromResponse("[", NTvmApi::TDstSet{19}, t),
                                       yexception,
                                       "Invalid json from tvm-api");

        u.ParseTicketsFromResponse(R"([])", NTvmApi::TDstSet{19}, t);
        UNIT_ASSERT_VALUES_EQUAL("6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n",
                                 l->Stream.Str());
        TNotInitedUpdater::TPairTicketsErrors expected = {
            {}, {{19, "Missing tvm_id in response, should never happend: 19"}}};
        UNIT_ASSERT_VALUES_EQUAL(expected, t);
        l->Stream.Clear();

        t = {};
        u.ParseTicketsFromResponse(
            R"([{},{"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}}, {"213" : { "ticket" : "service_ticket_2"}}])",
            NTvmApi::TDstSet{19},
            t);
        UNIT_ASSERT_VALUES_EQUAL("", l->Stream.Str());
        expected = {{{19, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}}, {}};
        UNIT_ASSERT_EQUAL(expected, t);

        t = {};
        u.ParseTicketsFromResponse(
            R"([{
                    "19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}
                },
                {
                    "213" : { "ticket" : "service_ticket_2"},
                    "234" : { "error" : "Dst is not found" }
                },
                {
                    "185" : { "ticket" : "service_ticket_3"},
                    "deprecated" : { "ticket" : "deprecated_ticket" }
                }
            ])",
            NTvmApi::TDstSet{19, 213, 234, 235},
            t);
        expected = {{{19, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
                     {213, "service_ticket_2"}},
                    {{234, "Dst is not found"},
                     {235, "Missing tvm_id in response, should never happend: 235"}}};
        UNIT_ASSERT_EQUAL(expected, t);
        UNIT_ASSERT_VALUES_EQUAL("", l->Stream.Str());
    }

    class TReplier: public TRequestReplier {
    public:
        HttpCodes Code = HTTP_OK;

        bool DoReply(const TReplyParams& params) override {
            TParsedHttpFull fl(params.Input.FirstLine());

            THttpResponse resp(Code);
            if (fl.Path == "/2/keys") {
                resp.SetContent(NUnittest::TVMKNIFE_PUBLIC_KEYS);
            } else if (fl.Path == "/2/ticket") {
                resp.SetContent(TVM_RESPONSE);
            } else {
                UNIT_ASSERT(false);
            }
            resp.OutTo(params.Output);

            return true;
        }
    };

    class TOnlineUpdater: public NTvmApi::TThreadedUpdater {
    public:
        TOnlineUpdater(const NTvmApi::TClientSettings& settings, TIntrusivePtr<TLogger> l)
            : NTvmApi::TThreadedUpdater(settings, l)
        {
            Init();
            ExpBackoff_.SetEnabled(false);
            StartWorker();
        }
    };

    Y_UNIT_TEST(MocServerOk) {
        TPortManager pm;
        ui16 tvmPort = pm.GetPort(80);
        NMock::TMockServer server(tvmPort, []() { return new TReplier; });

        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
            .TvmHost = "http://localhost",
            .TvmPort = tvmPort,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TOnlineUpdater u(s, l);
            UNIT_ASSERT_VALUES_EQUAL_C(TClientStatus::Ok, u.GetStatus(), l->Stream.Str());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n"
                << "7: Response with service tickets for 1 destination(s) was successfully fetched from http://localhost\n"
                << "7: Got responses with service tickets with 1 pages for 1 destination(s)\n"
                << "6: Cache was updated with 1 service ticket(s): XXXXXXXXXXX\n"
                << "7: Public keys were successfully fetched from http://localhost\n"
                << "6: Cache was updated with public keys: XXXXXXXXXXX\n"
                << "7: Thread-worker started\n"
                << "7: Thread-worker stopped\n",
            std::regex_replace(std::string(l->Stream.Str()), TIME_REGEX, "XXXXXXXXXXX"));
    }

    Y_UNIT_TEST(MocServerBad) {
        TPortManager pm;
        ui16 tvmPort = pm.GetPort(80);
        NMock::TMockServer server(tvmPort,
                                  []() {
                                      auto p = new TReplier;
                                      p->Code = HTTP_BAD_REQUEST;
                                      return p;
                                  });

        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDstsWithAliases = {{"blackbox", 19}},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
            .TvmHost = "http://localhost",
            .TvmPort = tvmPort,
        };

        auto l = MakeIntrusive<TLogger>();
        UNIT_ASSERT_EXCEPTION_CONTAINS_C(TOnlineUpdater(s, l),
                                         TNonRetriableException,
                                         "Failed to start TvmClient. Do not retry: ServiceTickets: Path:/2/ticket.Code=400:",
                                         l->Stream.Str());
    }

    Y_UNIT_TEST(MocServerPaginated) {
        class TReplier: public TRequestReplier {
        public:
            TString Response;
            TReplier(TString response)
                : Response(response)
            {
            }

            bool DoReply(const TReplyParams& params) override {
                TParsedHttpFull fl(params.Input.FirstLine());
                if (fl.Path != "/2/ticket") {
                    UNIT_ASSERT_C(false, fl.Path);
                }

                THttpResponse resp(HTTP_OK);
                resp.SetContent(Response);
                resp.OutTo(params.Output);
                return true;
            }
        };

        TPortManager pm;
        ui16 tvmPort = pm.GetPort(80);
        TVector<TString> responses = {
            R"({"15" : { "ticket" : "service_ticket_3" },"19" : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})",
            R"({"222" : { "ticket" : "service_ticket_2"}, "239" : { "error" : "Dst is not found" }})",
            R"({"185" : { "ticket" : "service_ticket_3"}})",
        };
        NMock::TMockServer server(tvmPort, [&responses]() {
            if (responses.empty()) {
                return new TReplier("<NULL>");
            }
            TString r = responses.front();
            responses.erase(responses.begin());
            return new TReplier(r);
        });

        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .Secret = (TStringBuf) "qwerty",
            .FetchServiceTicketsForDsts = {19, 222, 239, 100500, 15},
            .CheckServiceTickets = true,
            .CheckUserTicketsWithBbEnv = EBlackboxEnv::Test,
            .TvmHost = "http://localhost",
            .TvmPort = tvmPort,
        };

        auto l = MakeIntrusive<TLogger>();
        {
            TNotInitedUpdater u(s, l);
            TNotInitedUpdater::THttpResult result = u.GetServiceTicketsFromHttp(NTvmApi::TDstSet{19, 222, 239, 100500, 15}, 2);
            UNIT_ASSERT_VALUES_EQUAL(TSmallVec<TString>({
                                         R"({"15" : { "ticket" : "service_ticket_3" },"19" : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}})",
                                         R"({"222" : { "ticket" : "service_ticket_2"}, "239" : { "error" : "Dst is not found" }})",
                                         R"({"185" : { "ticket" : "service_ticket_3"}})",
                                     }),
                                     result.Responses);
            TNotInitedUpdater::TPairTicketsErrors expected{
                {
                    {19, "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
                    {222, "service_ticket_2"},
                    {15, "service_ticket_3"},
                },
                {
                    {239, "Dst is not found"},
                    {100500, "Missing tvm_id in response, should never happend: 100500"},
                },
            };
            UNIT_ASSERT_VALUES_EQUAL(expected, result.TicketsWithErrors);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuilder()
                << "6: Disk cache disabled. Please set disk cache directory in settings for best reliability\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from http://localhost\n"
                << "7: Response with service tickets for 2 destination(s) was successfully fetched from http://localhost\n"
                << "7: Response with service tickets for 1 destination(s) was successfully fetched from http://localhost\n"
                << "7: Got responses with service tickets with 3 pages for 5 destination(s)\n"
                << "3: Failed to get service ticket for dst=100500: Missing tvm_id in response, should never happend: 100500\n"
                << "3: Failed to get service ticket for dst=239: Dst is not found\n",
            l->Stream.Str());
    }

    Y_UNIT_TEST(FindMissingDsts) {
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector({6, 9}),
                                 TNotInitedUpdater::FindMissingDsts({1, 2, 3, 4}, {1, 4, 6, 9}));
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector(),
                                 TNotInitedUpdater::FindMissingDsts({1, 2, 3, 4, 5, 6, 7, 8, 9}, {1, 4, 6, 9}));
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector({1, 4, 6, 9}),
                                 TNotInitedUpdater::FindMissingDsts(NTvmApi::TDstSet(), {1, 4, 6, 9}));
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector(1, 19),
                                 TNotInitedUpdater::FindMissingDsts({213}, {19, 213}));

        auto make = [](TVector<int> ids) {
            TServiceTickets::TMapIdStr m;
            for (auto i : ids) {
                m.insert({i, ""});
            }
            return MakeIntrusiveConst<TServiceTickets>(std::move(m), TServiceTickets::TMapIdStr{}, TServiceTickets::TMapAliasId{});
        };

        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector({6, 9}),
                                 TNotInitedUpdater::FindMissingDsts(make({1, 2, 3, 4}), {1, 4, 6, 9}));
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector(),
                                 TNotInitedUpdater::FindMissingDsts(make({1, 2, 3, 4, 5, 6, 7, 8, 9}), {1, 4, 6, 9}));
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector({1, 4, 6, 9}),
                                 TNotInitedUpdater::FindMissingDsts(make({}), {1, 4, 6, 9}));
        UNIT_ASSERT_VALUES_EQUAL(NTvmApi::TClientSettings::TDstVector(1, 19),
                                 TNotInitedUpdater::FindMissingDsts(make({213}), {19, 213}));
    }

    Y_UNIT_TEST(CreateJsonArray) {
        UNIT_ASSERT_VALUES_EQUAL("[]", TNotInitedUpdater::CreateJsonArray({}));
        UNIT_ASSERT_VALUES_EQUAL("[sdlzkjvbsdljhfbsdajlhfbsakjdfb]",
                                 TNotInitedUpdater::CreateJsonArray({"sdlzkjvbsdljhfbsdajlhfbsakjdfb"}));
        UNIT_ASSERT_VALUES_EQUAL("[sdlzkjvbsdljhfbsdajlhfbsakjdfb,o92q83yh2uhq2eri23r]",
                                 TNotInitedUpdater::CreateJsonArray({"sdlzkjvbsdljhfbsdajlhfbsakjdfb",
                                                                     "o92q83yh2uhq2eri23r"}));
    }

    Y_UNIT_TEST(AppendArrayToJson) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(TNotInitedUpdater::AppendToJsonArray("", {}),
                                       yexception,
                                       "previous body required");
        UNIT_ASSERT_EXCEPTION_CONTAINS(TNotInitedUpdater::AppendToJsonArray("[kek", {}),
                                       yexception,
                                       "array is broken:");

        UNIT_ASSERT_VALUES_EQUAL("[kek]", TNotInitedUpdater::AppendToJsonArray("kek", {}));

        UNIT_ASSERT_VALUES_EQUAL(
            "[kek,sdlzkjvbsdljhfbsdajlhfbsakjdfb]",
            TNotInitedUpdater::AppendToJsonArray("kek",
                                                 {"sdlzkjvbsdljhfbsdajlhfbsakjdfb"}));
        UNIT_ASSERT_VALUES_EQUAL(
            "[kek,sdlzkjvbsdljhfbsdajlhfbsakjdfb,o92q83yh2uhq2eri23r]",
            TNotInitedUpdater::AppendToJsonArray("kek",
                                                 {"sdlzkjvbsdljhfbsdajlhfbsakjdfb", "o92q83yh2uhq2eri23r"}));

        UNIT_ASSERT_VALUES_EQUAL(
            "[kek,sdlzkjvbsdljhfbsdajlhfbsakjdfb]",
            TNotInitedUpdater::AppendToJsonArray("[kek]",
                                                 {"sdlzkjvbsdljhfbsdajlhfbsakjdfb"}));
        UNIT_ASSERT_VALUES_EQUAL(
            "[kek,sdlzkjvbsdljhfbsdajlhfbsakjdfb,o92q83yh2uhq2eri23r]",
            TNotInitedUpdater::AppendToJsonArray("[kek]",
                                                 {"sdlzkjvbsdljhfbsdajlhfbsakjdfb", "o92q83yh2uhq2eri23r"}));
    }

    Y_UNIT_TEST(UpdaterTimeouts) {
        const auto timeout = TDuration::MilliSeconds(10);
        NTvmApi::TClientSettings s{
            .SelfTvmId = 100500,
            .CheckServiceTickets = true,
            .TvmHost = "localhost",
            .TvmPort = GetRandomPort(),
            .TvmSocketTimeout = timeout,
            .TvmConnectTimeout = timeout,
        };

        {
            auto l = MakeIntrusive<TLogger>();
            auto startTs = ::Now();
            UNIT_ASSERT_EXCEPTION(NTvmApi::TThreadedUpdater::Create(s, l), yexception);
            UNIT_ASSERT_LT(::Now() - startTs, timeout * 2);
        }
    }
}

template <>
void Out<TSmallVec<TString>>(IOutputStream& out, const TSmallVec<TString>& m) {
    for (const TString& s : m) {
        out << s << ";";
    }
}

template <>
void Out<TServiceTickets::TMapIdStr>(
    IOutputStream& out,
    const TServiceTickets::TMapIdStr& m) {
    for (const auto& pair : m) {
        out << pair.first << " -> " << pair.second << ";";
    }
}

template <>
void Out<NTestSuiteApiUpdater::TNotInitedUpdater::TPairTicketsErrors>(
    IOutputStream& out,
    const NTestSuiteApiUpdater::TNotInitedUpdater::TPairTicketsErrors& m) {
    out << m.Tickets << "\n";
    out << m.Errors << "\n";
}

template <>
void Out<NTvmAuth::NTvmApi::TClientSettings::TDst>(IOutputStream& out, const NTvmAuth::NTvmApi::TClientSettings::TDst& m) {
    out << m.Id;
}
