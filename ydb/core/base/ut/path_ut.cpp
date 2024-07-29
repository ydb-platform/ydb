#include "path.h"

#include <ydb/core/base/events.h>
#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <locale>
#include <iostream>

//#define UT_PERF_TEST

using namespace NKikimr;

namespace {

const TVector<std::pair<TString, TString>> Data = {
    {"", ""},
    {"/", ""},
    {"//", ""},
    {"///", ""},
    {"////", ""},
    {"F", "/F"},
    {"/F", "/F"},
    {"/F/", "/F"},
    {"/F//o", "/F/o"},
    {"/F//o//o", "/F/o/o"},
    {"/F///o//o/", "/F/o/o"},
    {"/Foo", "/Foo"},
    {"//Foo", "/Foo"},
    {"///Foo", "/Foo"},
    {"////Foo", "/Foo"},
    {"////Foo//b", "/Foo/b"},
    {"Foo",  "/Foo"},
    {"Foo/", "/Foo"},
    {"Foo//", "/Foo"},
    {"Foo///", "/Foo"},
    {"Foo/Bar", "/Foo/Bar"},
    {"Foo//Bar", "/Foo/Bar"},
    {"/Foo//Bar", "/Foo/Bar"},
    {"//Foo//Bar", "/Foo/Bar"},
    {"//Foo//Bar/", "/Foo/Bar"},
    {"//Foo//Bar//", "/Foo/Bar"},
    {"///Foo///Bar///", "/Foo/Bar"},
    {"///Foo//////Bar///", "/Foo/Bar"},
    {"///Foo//////Bar///FooBar", "/Foo/Bar/FooBar"},
    {"/Foo/Bar/FooBar", "/Foo/Bar/FooBar"},
    {"/Foo//Barqwertyuiopasdfghjklzxcvbnm/FooBar123456789123456789", "/Foo/Barqwertyuiopasdfghjklzxcvbnm/FooBar123456789123456789"},
};

TString DoCanonizePathFast(const TString& path) {
    return CanonizePath(path);
}

TString DoCanonizePathOld(const TString& path)
{
    if (!path)
        return TString();

    const auto parts = SplitPath(path);
    return CanonizePath(parts);
}

Y_UNIT_TEST_SUITE(Path) {
    Y_UNIT_TEST(CanonizeOld) {
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathOld(Data[i].first);
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
    }

    Y_UNIT_TEST(CanonizeFast) {
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathFast(Data[i].first);
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
    }

    Y_UNIT_TEST(CanonizedStringIsSame1) {
        const TString in = "/Foo/Bar";
        const TString& result = DoCanonizePathFast(in);
        UNIT_ASSERT_VALUES_EQUAL((void*)in.data(), (void*)result.data());
    }

    Y_UNIT_TEST(CanonizedStringIsSame2) {
        const TString in = "/Foo";
        const TString& result = DoCanonizePathFast(in);
        UNIT_ASSERT_VALUES_EQUAL((void*)in.data(), (void*)result.data());
    }

#ifdef UT_PERF_TEST
    Y_UNIT_TEST(CanonizeOldPerf) {
        int count = 10000000;
        i64 x = 0;
        while (count--)
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathOld(Data[i].first);
            x += result.size();
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
        Cerr << x << Endl;
    }

    Y_UNIT_TEST(CanonizeFastPerf) {
        int count = 10000000;
        i64 x = 0;
        while (count--)
        for (size_t i = 0; i < Data.size(); i++) {
            const TString& result = DoCanonizePathFast(Data[i].first);
            x += result.size();
            UNIT_ASSERT_VALUES_EQUAL(result, Data[i].second);
        }
        Cerr << x << Endl;
    }
#endif

    Y_UNIT_TEST(Name_EnglishAlphabet) {
        const TString pathPart = "NameInEnglish";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.end());
    }

    Y_UNIT_TEST(Name_RussianAlphabet) {
        const TString pathPart = "НазваниеНаРусском";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.begin());
    }

    class TLocaleGuard {
    public:
        explicit TLocaleGuard(const std::locale& targetLocale)
            : OriginalLocale_(std::locale::global(targetLocale))
        {
        }
        ~TLocaleGuard() {
            std::locale::global(OriginalLocale_);
        }
    
    private:
        const std::locale OriginalLocale_;
    };

    Y_UNIT_TEST(Name_RussianAlphabet_SetLocale_C) {
        TLocaleGuard localeGuard(std::locale("C"));
        const TString pathPart = "НазваниеНаРусском";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.begin());
    }

    Y_UNIT_TEST(Name_RussianAlphabet_SetLocale_C_UTF8) {
        try {
            TLocaleGuard localeGuard(std::locale("C.UTF-8"));
            const TString pathPart = "НазваниеНаРусском";
            UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), pathPart.begin());
        } catch (std::runtime_error) {
            // basic utf-8 locale is absent in the system, abort the test
        }
    }

    Y_UNIT_TEST(Name_AllSymbols) {
        const auto isAllowed = [](char symbol) {
            constexpr std::array<char, 3> allowedSymbols = {'-', '_', '.'};
            return std::isalnum(symbol, std::locale::classic())
                || std::find(allowedSymbols.begin(), allowedSymbols.end(), symbol) != allowedSymbols.end();
        };

        for (char symbol = std::numeric_limits<char>::min(); ; ++symbol) {
            const TString pathPart(1, symbol);
            UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), isAllowed(symbol) ? pathPart.end() : pathPart.begin());
            
            if (symbol == std::numeric_limits<char>::max()) {
                break;
            }
        }
    }

    // This ctype facet classifies 'z' letter as not alphabetic.
    // Code is taken from https://en.cppreference.com/w/cpp/locale/ctype_char.
    struct TWeirdCtypeFacet : std::ctype<char>
    {
        static const mask* MakeTable()
        {
            // make a copy of the "C" locale table
            static std::vector<mask> weirdTable(classic_table(), classic_table() + table_size);

            // reclassify 'z'
            weirdTable['z'] &= ~alpha;
            return &weirdTable[0];
        }
    
        TWeirdCtypeFacet(std::size_t refs = 0) : ctype(MakeTable(), false, refs) {}
    };

    Y_UNIT_TEST(Name_WeirdLocale_RegularName) {
        TLocaleGuard localeGuard(std::locale(std::locale::classic(), new TWeirdCtypeFacet));
        const TString regularName = "a";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(regularName), regularName.end());

    }

    Y_UNIT_TEST(Name_WeirdLocale_WeirdName) {
        TLocaleGuard localeGuard(std::locale(std::locale::classic(), new TWeirdCtypeFacet));
        UNIT_ASSERT(!std::isalnum('z', std::locale()));

        const TString weirdName = "z";
        // path part should not be considered to be broken, we should ignore the global locale
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(weirdName), weirdName.end());
    }

    Y_UNIT_TEST(Name_ExtraSymbols) {
        const TString pathPart = "this string contains whitespaces";
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart), std::find(pathPart.begin(), pathPart.end(), ' '));
        UNIT_ASSERT_EQUAL(PathPartBrokenAt(pathPart, " "), pathPart.end());
    }

    Y_UNIT_TEST(TestMy) {
        std::cout << "ES_KIKIMR_ES_BEGIN = " << NKikimr::TKikimrEvents::ES_KIKIMR_ES_BEGIN << ", " << std::endl;
        std::cout << "ES_STATESTORAGE = " << NKikimr::TKikimrEvents::ES_STATESTORAGE << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4098 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4098 << ", " << std::endl;
        std::cout << "ES_BLOBSTORAGE = " << NKikimr::TKikimrEvents::ES_BLOBSTORAGE << ", " << std::endl;
        std::cout << "ES_HIVE = " << NKikimr::TKikimrEvents::ES_HIVE << ", " << std::endl;
        std::cout << "ES_TABLETBASE = " << NKikimr::TKikimrEvents::ES_TABLETBASE << ", " << std::endl;
        std::cout << "ES_TABLET = " << NKikimr::TKikimrEvents::ES_TABLET << ", " << std::endl;
        std::cout << "ES_TABLETRESOLVER = " << NKikimr::TKikimrEvents::ES_TABLETRESOLVER << ", " << std::endl;
        std::cout << "ES_LOCAL = " << NKikimr::TKikimrEvents::ES_LOCAL << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4105 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4105 << ", " << std::endl;
        std::cout << "ES_TX_PROXY = " << NKikimr::TKikimrEvents::ES_TX_PROXY << ", " << std::endl;
        std::cout << "ES_TX_COORDINATOR = " << NKikimr::TKikimrEvents::ES_TX_COORDINATOR << ", " << std::endl;
        std::cout << "ES_TX_MEDIATOR = " << NKikimr::TKikimrEvents::ES_TX_MEDIATOR << ", " << std::endl;
        std::cout << "ES_TX_PROCESSING = " << NKikimr::TKikimrEvents::ES_TX_PROCESSING << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4110 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4110 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4111 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4111 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4112 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4112 << ", " << std::endl;
        std::cout << "ES_TX_DATASHARD = " << NKikimr::TKikimrEvents::ES_TX_DATASHARD << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4114 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4114 << ", " << std::endl;
        std::cout << "ES_TX_USERPROXY = " << NKikimr::TKikimrEvents::ES_TX_USERPROXY << ", " << std::endl;
        std::cout << "ES_SCHEME_CACHE = " << NKikimr::TKikimrEvents::ES_SCHEME_CACHE << ", " << std::endl;
        std::cout << "ES_TX_PROXY_REQ = " << NKikimr::TKikimrEvents::ES_TX_PROXY_REQ << ", " << std::endl;
        std::cout << "ES_TABLET_PIPE = " << NKikimr::TKikimrEvents::ES_TABLET_PIPE << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4118 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4118 << ", " << std::endl;
        std::cout << "ES_TABLET_COUNTERS_AGGREGATOR = " << NKikimr::TKikimrEvents::ES_TABLET_COUNTERS_AGGREGATOR << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4121 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4121 << ", " << std::endl;
        std::cout << "ES_PROXY_BUS = " << NKikimr::TKikimrEvents::ES_PROXY_BUS << ", " << std::endl;
        std::cout << "ES_BOOTSTRAPPER = " << NKikimr::TKikimrEvents::ES_BOOTSTRAPPER << ", " << std::endl;
        std::cout << "ES_TX_MEDIATORTIMECAST = " << NKikimr::TKikimrEvents::ES_TX_MEDIATORTIMECAST << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4125 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4125 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4126 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4126 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4127 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4127 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4128 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4128 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4129 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4129 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4130 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4130 << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4131 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4131 << ", " << std::endl;
        std::cout << "ES_KEYVALUE = " << NKikimr::TKikimrEvents::ES_KEYVALUE << ", " << std::endl;
        std::cout << "ES_MSGBUS_TRACER = " << NKikimr::TKikimrEvents::ES_MSGBUS_TRACER << ", " << std::endl;
        std::cout << "ES_RTMR_TABLET = " << NKikimr::TKikimrEvents::ES_RTMR_TABLET << ", " << std::endl;
        std::cout << "ES_FLAT_EXECUTOR = " << NKikimr::TKikimrEvents::ES_FLAT_EXECUTOR << ", " << std::endl;
        std::cout << "ES_NODE_WHITEBOARD = " << NKikimr::TKikimrEvents::ES_NODE_WHITEBOARD << ", " << std::endl;
        std::cout << "ES_FLAT_TX_SCHEMESHARD = " << NKikimr::TKikimrEvents::ES_FLAT_TX_SCHEMESHARD << ", " << std::endl;
        std::cout << "ES_PQ = " << NKikimr::TKikimrEvents::ES_PQ << ", " << std::endl;
        std::cout << "ES_YQL_KIKIMR_PROXY = " << NKikimr::TKikimrEvents::ES_YQL_KIKIMR_PROXY << ", " << std::endl;
        std::cout << "ES_PQ_META_CACHE = " << NKikimr::TKikimrEvents::ES_PQ_META_CACHE << ", " << std::endl;
        std::cout << "ES_DEPRECATED_4141 = " << NKikimr::TKikimrEvents::ES_DEPRECATED_4141 << ", " << std::endl;
        std::cout << "ES_PQ_L2_CACHE = " << NKikimr::TKikimrEvents::ES_PQ_L2_CACHE << ", " << std::endl;
        std::cout << "ES_TOKEN_BUILDER = " << NKikimr::TKikimrEvents::ES_TOKEN_BUILDER << ", " << std::endl;
        std::cout << "ES_TICKET_PARSER = " << NKikimr::TKikimrEvents::ES_TICKET_PARSER << ", " << std::endl;
        std::cout << "ES_KQP = " << NKikimr::TKikimrEvents::ES_KQP << ", " << std::endl;
        std::cout << "ES_BLACKBOX_VALIDATOR = " << NKikimr::TKikimrEvents::ES_BLACKBOX_VALIDATOR << ", " << std::endl;
        std::cout << "ES_SELF_PING = " << NKikimr::TKikimrEvents::ES_SELF_PING << ", " << std::endl;
        std::cout << "ES_PIPECACHE = " << NKikimr::TKikimrEvents::ES_PIPECACHE << ", " << std::endl;
        std::cout << "ES_PQ_PROXY = " << NKikimr::TKikimrEvents::ES_PQ_PROXY << ", " << std::endl;
        std::cout << "ES_CMS = " << NKikimr::TKikimrEvents::ES_CMS << ", " << std::endl;
        std::cout << "ES_NODE_BROKER = " << NKikimr::TKikimrEvents::ES_NODE_BROKER << ", " << std::endl;
        std::cout << "ES_TX_ALLOCATOR = " << NKikimr::TKikimrEvents::ES_TX_ALLOCATOR << ", " << std::endl;
        // std::cout << "// reserve event space for each RTMR process = " << NKikimr::TKikimrEvents::// reserve event space for each RTMR process << ", " << std::endl;
        std::cout << "ES_RTMR_STORAGE = " << NKikimr::TKikimrEvents::ES_RTMR_STORAGE << ", " << std::endl;
        std::cout << "ES_RTMR_PROXY = " << NKikimr::TKikimrEvents::ES_RTMR_PROXY << ", " << std::endl;
        std::cout << "ES_RTMR_PUSHER = " << NKikimr::TKikimrEvents::ES_RTMR_PUSHER << ", " << std::endl;
        std::cout << "ES_RTMR_HOST = " << NKikimr::TKikimrEvents::ES_RTMR_HOST << ", " << std::endl;
        std::cout << "ES_RESOURCE_BROKER = " << NKikimr::TKikimrEvents::ES_RESOURCE_BROKER << ", " << std::endl;
        std::cout << "ES_VIEWER = " << NKikimr::TKikimrEvents::ES_VIEWER << ", " << std::endl;
        std::cout << "ES_SUB_DOMAIN = " << NKikimr::TKikimrEvents::ES_SUB_DOMAIN << ", " << std::endl;
        std::cout << "ES_GRPC_PROXY_STATUS = " << NKikimr::TKikimrEvents::ES_GRPC_PROXY_STATUS << ", " << std::endl;
        std::cout << "ES_SQS = " << NKikimr::TKikimrEvents::ES_SQS << ", " << std::endl;
        std::cout << "ES_BLOCKSTORE = " << NKikimr::TKikimrEvents::ES_BLOCKSTORE << ", " << std::endl;
        std::cout << "ES_RTMR_ICBUS = " << NKikimr::TKikimrEvents::ES_RTMR_ICBUS << ", " << std::endl;
        std::cout << "ES_TENANT_POOL = " << NKikimr::TKikimrEvents::ES_TENANT_POOL << ", " << std::endl;
        std::cout << "ES_USER_REGISTRY = " << NKikimr::TKikimrEvents::ES_USER_REGISTRY << ", " << std::endl;
        std::cout << "ES_TVM_SETTINGS_UPDATER = " << NKikimr::TKikimrEvents::ES_TVM_SETTINGS_UPDATER << ", " << std::endl;
        std::cout << "ES_PQ_CLUSTERS_UPDATER = " << NKikimr::TKikimrEvents::ES_PQ_CLUSTERS_UPDATER << ", " << std::endl;
        std::cout << "ES_TENANT_SLOT_BROKER = " << NKikimr::TKikimrEvents::ES_TENANT_SLOT_BROKER << ", " << std::endl;
        std::cout << "ES_GRPC_CALLS = " << NKikimr::TKikimrEvents::ES_GRPC_CALLS << ", " << std::endl;
        std::cout << "ES_CONSOLE = " << NKikimr::TKikimrEvents::ES_CONSOLE << ", " << std::endl;
        std::cout << "ES_KESUS_PROXY = " << NKikimr::TKikimrEvents::ES_KESUS_PROXY << ", " << std::endl;
        std::cout << "ES_KESUS = " << NKikimr::TKikimrEvents::ES_KESUS << ", " << std::endl;
        std::cout << "ES_CONFIGS_DISPATCHER = " << NKikimr::TKikimrEvents::ES_CONFIGS_DISPATCHER << ", " << std::endl;
        std::cout << "ES_IAM_SERVICE = " << NKikimr::TKikimrEvents::ES_IAM_SERVICE << ", " << std::endl;
        std::cout << "ES_FOLDER_SERVICE = " << NKikimr::TKikimrEvents::ES_FOLDER_SERVICE << ", " << std::endl;
        std::cout << "ES_GRPC_MON = " << NKikimr::TKikimrEvents::ES_GRPC_MON << ", " << std::endl;
        std::cout << "ES_QUOTA = " << NKikimr::TKikimrEvents::ES_QUOTA << ", " << std::endl;
        std::cout << "ES_COORDINATED_QUOTA = " << NKikimr::TKikimrEvents::ES_COORDINATED_QUOTA << ", " << std::endl;
        std::cout << "ES_ACCESS_SERVICE = " << NKikimr::TKikimrEvents::ES_ACCESS_SERVICE << ", " << std::endl;
        std::cout << "ES_USER_ACCOUNT_SERVICE = " << NKikimr::TKikimrEvents::ES_USER_ACCOUNT_SERVICE << ", " << std::endl;
        std::cout << "ES_PQ_PROXY_NEW = " << NKikimr::TKikimrEvents::ES_PQ_PROXY_NEW << ", " << std::endl;
        std::cout << "ES_GRPC_STREAMING = " << NKikimr::TKikimrEvents::ES_GRPC_STREAMING << ", " << std::endl;
        std::cout << "ES_SCHEME_BOARD = " << NKikimr::TKikimrEvents::ES_SCHEME_BOARD << ", " << std::endl;
        std::cout << "ES_FLAT_TX_SCHEMESHARD_PROTECTED = " << NKikimr::TKikimrEvents::ES_FLAT_TX_SCHEMESHARD_PROTECTED << ", " << std::endl;
        std::cout << "ES_GRPC_REQUEST_PROXY = " << NKikimr::TKikimrEvents::ES_GRPC_REQUEST_PROXY << ", " << std::endl;
        std::cout << "ES_EXPORT_SERVICE = " << NKikimr::TKikimrEvents::ES_EXPORT_SERVICE << ", " << std::endl;
        std::cout << "ES_TX_ALLOCATOR_CLIENT = " << NKikimr::TKikimrEvents::ES_TX_ALLOCATOR_CLIENT << ", " << std::endl;
        std::cout << "ES_PQ_CLUSTER_TRACKER = " << NKikimr::TKikimrEvents::ES_PQ_CLUSTER_TRACKER << ", " << std::endl;
        std::cout << "ES_NET_CLASSIFIER = " << NKikimr::TKikimrEvents::ES_NET_CLASSIFIER << ", " << std::endl;
        std::cout << "ES_SYSTEM_VIEW = " << NKikimr::TKikimrEvents::ES_SYSTEM_VIEW << ", " << std::endl;
        std::cout << "ES_TENANT_NODE_ENUMERATOR = " << NKikimr::TKikimrEvents::ES_TENANT_NODE_ENUMERATOR << ", " << std::endl;
        std::cout << "ES_SERVICE_ACCOUNT_SERVICE = " << NKikimr::TKikimrEvents::ES_SERVICE_ACCOUNT_SERVICE << ", " << std::endl;
        std::cout << "ES_INDEX_BUILD = " << NKikimr::TKikimrEvents::ES_INDEX_BUILD << ", " << std::endl;
        std::cout << "ES_BLOCKSTORE_PRIVATE = " << NKikimr::TKikimrEvents::ES_BLOCKSTORE_PRIVATE << ", " << std::endl;
        std::cout << "ES_YT_WRAPPER = " << NKikimr::TKikimrEvents::ES_YT_WRAPPER << ", " << std::endl;
        std::cout << "ES_S3_WRAPPER = " << NKikimr::TKikimrEvents::ES_S3_WRAPPER << ", " << std::endl;
        std::cout << "ES_FILESTORE = " << NKikimr::TKikimrEvents::ES_FILESTORE << ", " << std::endl;
        std::cout << "ES_FILESTORE_PRIVATE = " << NKikimr::TKikimrEvents::ES_FILESTORE_PRIVATE << ", " << std::endl;
        std::cout << "ES_YDB_METERING = " << NKikimr::TKikimrEvents::ES_YDB_METERING << ", " << std::endl;
        std::cout << "ES_IMPORT_SERVICE = " << NKikimr::TKikimrEvents::ES_IMPORT_SERVICE << ", " << std::endl;
        std::cout << "ES_TX_OLAPSHARD = " << NKikimr::TKikimrEvents::ES_TX_OLAPSHARD << ", " << std::endl;
        std::cout << "ES_TX_COLUMNSHARD = " << NKikimr::TKikimrEvents::ES_TX_COLUMNSHARD << ", " << std::endl;
        std::cout << "ES_CROSSREF = " << NKikimr::TKikimrEvents::ES_CROSSREF << ", " << std::endl;
        std::cout << "ES_SCHEME_BOARD_MON = " << NKikimr::TKikimrEvents::ES_SCHEME_BOARD_MON << ", " << std::endl;
        std::cout << "ES_YQL_ANALYTICS_PROXY = " << NKikimr::TKikimrEvents::ES_YQL_ANALYTICS_PROXY << ", " << std::endl;
        std::cout << "ES_BLOB_CACHE = " << NKikimr::TKikimrEvents::ES_BLOB_CACHE << ", " << std::endl;
        std::cout << "ES_LONG_TX_SERVICE = " << NKikimr::TKikimrEvents::ES_LONG_TX_SERVICE << ", " << std::endl;
        std::cout << "ES_TEST_SHARD = " << NKikimr::TKikimrEvents::ES_TEST_SHARD << ", " << std::endl;
        std::cout << "ES_DATASTREAMS_PROXY = " << NKikimr::TKikimrEvents::ES_DATASTREAMS_PROXY << ", " << std::endl;
        std::cout << "ES_IAM_TOKEN_SERVICE = " << NKikimr::TKikimrEvents::ES_IAM_TOKEN_SERVICE << ", " << std::endl;
        std::cout << "ES_HEALTH_CHECK = " << NKikimr::TKikimrEvents::ES_HEALTH_CHECK << ", " << std::endl;
        std::cout << "ES_DQ = " << NKikimr::TKikimrEvents::ES_DQ << ", " << std::endl;
        std::cout << "ES_YQ = " << NKikimr::TKikimrEvents::ES_YQ << ", " << std::endl;
        std::cout << "ES_CHANGE_EXCHANGE_DATASHARD = " << NKikimr::TKikimrEvents::ES_CHANGE_EXCHANGE_DATASHARD << ", " << std::endl;
        std::cout << "ES_DATABASE_SERVICE = " << NKikimr::TKikimrEvents::ES_DATABASE_SERVICE << ", " << std::endl;
        std::cout << "ES_SEQUENCESHARD = " << NKikimr::TKikimrEvents::ES_SEQUENCESHARD << ", " << std::endl;
        std::cout << "ES_SEQUENCEPROXY = " << NKikimr::TKikimrEvents::ES_SEQUENCEPROXY << ", " << std::endl;
        std::cout << "ES_CLOUD_STORAGE = " << NKikimr::TKikimrEvents::ES_CLOUD_STORAGE << ", " << std::endl;
        std::cout << "ES_CLOUD_STORAGE_PRIVATE = " << NKikimr::TKikimrEvents::ES_CLOUD_STORAGE_PRIVATE << ", " << std::endl;
        std::cout << "ES_FOLDER_SERVICE_ADAPTER = " << NKikimr::TKikimrEvents::ES_FOLDER_SERVICE_ADAPTER << ", " << std::endl;
        std::cout << "ES_PQ_PARTITION_WRITER = " << NKikimr::TKikimrEvents::ES_PQ_PARTITION_WRITER << ", " << std::endl;
        std::cout << "ES_YDB_PROXY = " << NKikimr::TKikimrEvents::ES_YDB_PROXY << ", " << std::endl;
        std::cout << "ES_REPLICATION_CONTROLLER = " << NKikimr::TKikimrEvents::ES_REPLICATION_CONTROLLER << ", " << std::endl;
        std::cout << "ES_HTTP_PROXY = " << NKikimr::TKikimrEvents::ES_HTTP_PROXY << ", " << std::endl;
        std::cout << "ES_BLOB_DEPOT = " << NKikimr::TKikimrEvents::ES_BLOB_DEPOT << ", " << std::endl;
        std::cout << "ES_DATASHARD_LOAD = " << NKikimr::TKikimrEvents::ES_DATASHARD_LOAD << ", " << std::endl;
        std::cout << "ES_METADATA_PROVIDER = " << NKikimr::TKikimrEvents::ES_METADATA_PROVIDER << ", " << std::endl;
        std::cout << "ES_INTERNAL_REQUEST = " << NKikimr::TKikimrEvents::ES_INTERNAL_REQUEST << ", " << std::endl;
        std::cout << "ES_BACKGROUND_TASKS = " << NKikimr::TKikimrEvents::ES_BACKGROUND_TASKS << ", " << std::endl;
        std::cout << "ES_TIERING = " << NKikimr::TKikimrEvents::ES_TIERING << ", " << std::endl;
        std::cout << "ES_METADATA_INITIALIZER = " << NKikimr::TKikimrEvents::ES_METADATA_INITIALIZER << ", " << std::endl;
        std::cout << "ES_YDB_AUDIT_LOG = " << NKikimr::TKikimrEvents::ES_YDB_AUDIT_LOG << ", " << std::endl;
        std::cout << "ES_METADATA_MANAGER = " << NKikimr::TKikimrEvents::ES_METADATA_MANAGER << ", " << std::endl;
        std::cout << "ES_METADATA_SECRET = " << NKikimr::TKikimrEvents::ES_METADATA_SECRET << ", " << std::endl;
        std::cout << "ES_TEST_LOAD = " << NKikimr::TKikimrEvents::ES_TEST_LOAD << ", " << std::endl;
        std::cout << "ES_GRPC_CANCELATION = " << NKikimr::TKikimrEvents::ES_GRPC_CANCELATION << ", " << std::endl;
        std::cout << "ES_DISCOVERY = " << NKikimr::TKikimrEvents::ES_DISCOVERY << ", " << std::endl;
        std::cout << "ES_EXT_INDEX = " << NKikimr::TKikimrEvents::ES_EXT_INDEX << ", " << std::endl;
        std::cout << "ES_CONVEYOR = " << NKikimr::TKikimrEvents::ES_CONVEYOR << ", " << std::endl;
        std::cout << "ES_KQP_SCAN_EXCHANGE = " << NKikimr::TKikimrEvents::ES_KQP_SCAN_EXCHANGE << ", " << std::endl;
        std::cout << "ES_IC_NODE_CACHE = " << NKikimr::TKikimrEvents::ES_IC_NODE_CACHE << ", " << std::endl;
        std::cout << "ES_DATA_OPERATIONS = " << NKikimr::TKikimrEvents::ES_DATA_OPERATIONS << ", " << std::endl;
        std::cout << "ES_KAFKA = " << NKikimr::TKikimrEvents::ES_KAFKA << ", " << std::endl;
        std::cout << "ES_STATISTICS = " << NKikimr::TKikimrEvents::ES_STATISTICS << ", " << std::endl;
        std::cout << "ES_LDAP_AUTH_PROVIDER = " << NKikimr::TKikimrEvents::ES_LDAP_AUTH_PROVIDER << ", " << std::endl;
        std::cout << "ES_DB_METADATA_CACHE = " << NKikimr::TKikimrEvents::ES_DB_METADATA_CACHE << ", " << std::endl;
        std::cout << "ES_TABLE_CREATOR = " << NKikimr::TKikimrEvents::ES_TABLE_CREATOR << ", " << std::endl;
        std::cout << "ES_PQ_PARTITION_CHOOSER = " << NKikimr::TKikimrEvents::ES_PQ_PARTITION_CHOOSER << ", " << std::endl;
        std::cout << "ES_GRAPH = " << NKikimr::TKikimrEvents::ES_GRAPH << ", " << std::endl;
        std::cout << "ES_REPLICATION_SERVICE = " << NKikimr::TKikimrEvents::ES_REPLICATION_SERVICE << ", " << std::endl;
        std::cout << "ES_CHANGE_EXCHANGE = " << NKikimr::TKikimrEvents::ES_CHANGE_EXCHANGE << ", " << std::endl;
        std::cout << "ES_LIMITER = " << NKikimr::TKikimrEvents::ES_LIMITER << ", " << std::endl;
        UNIT_ASSERT_EQUAL(42, 43);
    }

}

}
