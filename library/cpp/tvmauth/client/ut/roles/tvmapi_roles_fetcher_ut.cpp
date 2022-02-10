#include <library/cpp/tvmauth/client/ut/common.h> 
 
#include <library/cpp/tvmauth/client/misc/disk_cache.h> 
#include <library/cpp/tvmauth/client/misc/api/roles_fetcher.h> 
 
#include <library/cpp/tvmauth/unittest.h> 
 
#include <library/cpp/testing/unittest/registar.h> 
 
#include <util/stream/file.h> 
#include <util/system/fs.h> 
 
using namespace NTvmAuth; 
using namespace NTvmAuth::NTvmApi; 
 
Y_UNIT_TEST_SUITE(TvmApiRolesFetcher) { 
    static const TString ROLES = R"({"revision": "100501", "born_date": 42})"; 
 
    static const TString CACHE_DIR = "./tmp/"; 
 
    static void CleanCache() { 
        NFs::RemoveRecursive(CACHE_DIR); 
        NFs::MakeDirectoryRecursive(CACHE_DIR); 
    } 
 
    Y_UNIT_TEST(ReadFromDisk) { 
        CleanCache(); 
        auto logger = MakeIntrusive<TLogger>(); 
 
        TRolesFetcherSettings s; 
        s.CacheDir = CACHE_DIR; 
        s.SelfTvmId = 111111; 
        s.IdmSystemSlug = "fem\tida"; 
        TRolesFetcher fetcher(s, logger); 
 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
 
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), fetcher.ReadFromDisk()); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "7: File './tmp/roles' does not exist\n", 
            logger->Stream.Str()); 
        logger->Stream.clear(); 
 
        const TInstant now = TInstant::Seconds(TInstant::Now().Seconds()); 
 
        TDiskWriter wr(CACHE_DIR + "roles"); 
        UNIT_ASSERT(wr.Write("kek", now)); 
        UNIT_ASSERT_NO_EXCEPTION(fetcher.ReadFromDisk()); 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/roles' was successfully read\n" 
                << "4: Roles in disk cache are for another slug (kek). Self=fem\tida\n", 
            logger->Stream.Str()); 
        logger->Stream.clear(); 
 
        UNIT_ASSERT(wr.Write(TRolesFetcher::PrepareDiskFormat(ROLES, "femida_test"), now)); 
        UNIT_ASSERT_VALUES_EQUAL(TInstant(), fetcher.ReadFromDisk()); 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/roles' was successfully read\n" 
                   "4: Roles in disk cache are for another slug (femida_test). Self=fem\tida\n", 
            logger->Stream.Str()); 
        logger->Stream.clear(); 
 
        UNIT_ASSERT(wr.Write(TRolesFetcher::PrepareDiskFormat(ROLES, "fem\tida"), now)); 
        UNIT_ASSERT_VALUES_EQUAL(now, fetcher.ReadFromDisk()); 
        UNIT_ASSERT(fetcher.AreRolesOk()); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            TStringBuilder() 
                << "6: File './tmp/roles' was successfully read\n" 
                   "7: Succeed to read roles with revision 100501 from ./tmp/roles\n", 
            logger->Stream.Str()); 
        logger->Stream.clear(); 
    } 
 
    Y_UNIT_TEST(IsTimeToUpdate) { 
        TRetrySettings settings; 
        settings.RolesUpdatePeriod = TDuration::Minutes(123); 
 
        UNIT_ASSERT(!TRolesFetcher::IsTimeToUpdate(settings, TDuration::Seconds(5))); 
        UNIT_ASSERT(TRolesFetcher::IsTimeToUpdate(settings, TDuration::Hours(5))); 
    } 
 
    Y_UNIT_TEST(ShouldWarn) { 
        TRetrySettings settings; 
        settings.RolesWarnPeriod = TDuration::Minutes(123); 
 
        UNIT_ASSERT(!TRolesFetcher::ShouldWarn(settings, TDuration::Seconds(5))); 
        UNIT_ASSERT(TRolesFetcher::ShouldWarn(settings, TDuration::Hours(5))); 
    } 
 
    Y_UNIT_TEST(Update) { 
        CleanCache(); 
        auto logger = MakeIntrusive<TLogger>(); 
 
        TRolesFetcherSettings s; 
        s.CacheDir = CACHE_DIR; 
        s.SelfTvmId = 111111; 
        TRolesFetcher fetcher(s, logger); 
 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
 
        NUtils::TFetchResult fetchResult; 
        fetchResult.Code = 304; 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            fetcher.Update(NUtils::TFetchResult(fetchResult)), 
            yexception, 
            "tirole did not return any roles because current roles are actual, but there are no roles in memory"); 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
        UNIT_ASSERT(!NFs::Exists(CACHE_DIR + "roles")); 
 
        fetchResult.Code = 206; 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            fetcher.Update(NUtils::TFetchResult(fetchResult)), 
            yexception, 
            "Unexpected code from tirole: 206."); 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
        UNIT_ASSERT(!NFs::Exists(CACHE_DIR + "roles")); 
 
        fetchResult.Code = 200; 
        fetchResult.Response = "kek"; 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            fetcher.Update(NUtils::TFetchResult(fetchResult)), 
            yexception, 
            "Invalid json. 'kek'"); 
        UNIT_ASSERT(!fetcher.AreRolesOk()); 
        UNIT_ASSERT(!NFs::Exists(CACHE_DIR + "roles")); 
 
        fetchResult.Response = ROLES; 
        UNIT_ASSERT_NO_EXCEPTION(fetcher.Update(NUtils::TFetchResult(fetchResult))); 
        UNIT_ASSERT(fetcher.AreRolesOk()); 
        UNIT_ASSERT(NFs::Exists(CACHE_DIR + "roles")); 
        { 
            TFileInput f(CACHE_DIR + "roles"); 
            TString body = f.ReadAll(); 
            UNIT_ASSERT_C(body.Contains(ROLES), "got body: '" << body << "'"); 
        } 
 
        fetchResult.Code = 304; 
        fetchResult.Response.clear(); 
        UNIT_ASSERT_NO_EXCEPTION(fetcher.Update(NUtils::TFetchResult(fetchResult))); 
        UNIT_ASSERT(fetcher.AreRolesOk()); 
        UNIT_ASSERT(NFs::Exists(CACHE_DIR + "roles")); 
 
        fetchResult.Code = 200; 
        fetchResult.Headers.AddHeader("X-Tirole-Compression", "kek"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            fetcher.Update(NUtils::TFetchResult(fetchResult)), 
            yexception, 
            "unknown codec format version; known: 1; got: kek"); 
    } 
 
    Y_UNIT_TEST(CreateTiroleRequest) { 
        CleanCache(); 
        auto logger = MakeIntrusive<TLogger>(); 
 
        TRolesFetcherSettings s; 
        s.CacheDir = CACHE_DIR; 
        s.SelfTvmId = 111111; 
        s.IdmSystemSlug = "some sys"; 
        TRolesFetcher fetcher(s, logger); 
 
        TRolesFetcher::TRequest req = fetcher.CreateTiroleRequest("some_ticket"); 
        UNIT_ASSERT_VALUES_EQUAL( 
            "/v1/get_actual_roles?system_slug=some+sys&_pid=&lib_version=client_", 
            TStringBuf(req.Url).Chop(5)); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TKeepAliveHttpClient::THeaders({ 
                {"X-Ya-Service-Ticket", "some_ticket"}, 
            }), 
            req.Headers); 
 
        TDiskWriter wr(CACHE_DIR + "roles"); 
        UNIT_ASSERT(wr.Write(TRolesFetcher::PrepareDiskFormat( 
            R"({"revision": "asd&qwe", "born_date": 42})", 
            "some sys"))); 
        UNIT_ASSERT_NO_EXCEPTION(fetcher.ReadFromDisk()); 
 
        req = fetcher.CreateTiroleRequest("some_ticket"); 
        UNIT_ASSERT_VALUES_EQUAL( 
            "/v1/get_actual_roles?system_slug=some+sys&_pid=&lib_version=client_", 
            TStringBuf(req.Url).Chop(5)); 
        UNIT_ASSERT_VALUES_EQUAL( 
            TKeepAliveHttpClient::THeaders({ 
                {"If-None-Match", R"("asd&qwe")"}, 
                {"X-Ya-Service-Ticket", "some_ticket"}, 
            }), 
            req.Headers); 
    } 
} 
