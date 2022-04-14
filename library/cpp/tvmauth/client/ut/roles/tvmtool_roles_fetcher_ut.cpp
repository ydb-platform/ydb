#include <library/cpp/tvmauth/client/ut/common.h>

#include <library/cpp/tvmauth/client/misc/tool/roles_fetcher.h>

#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;
using namespace NTvmAuth::NTvmTool;

Y_UNIT_TEST_SUITE(TvmToolRolesFetcher) {
    static const TString ROLES = R"({"revision": "100501", "born_date": 42})";

    Y_UNIT_TEST(IsTimeToUpdate) {
        TRolesFetcher rf(
            TRolesFetcherSettings{.UpdatePeriod = TDuration::Minutes(1)},
            new TLogger);

        UNIT_ASSERT(!rf.IsTimeToUpdate(TDuration::Seconds(3)));
        UNIT_ASSERT(!rf.IsTimeToUpdate(TDuration::Seconds(60)));
        UNIT_ASSERT(rf.IsTimeToUpdate(TDuration::Seconds(61)));
        UNIT_ASSERT(rf.IsTimeToUpdate(TDuration::Seconds(600)));
    }

    Y_UNIT_TEST(ShouldWarn) {
        TRolesFetcher rf(
            TRolesFetcherSettings{.WarnPeriod = TDuration::Minutes(20)},
            new TLogger);

        UNIT_ASSERT(!rf.ShouldWarn(TDuration::Minutes(3)));
        UNIT_ASSERT(!rf.ShouldWarn(TDuration::Minutes(20)));
        UNIT_ASSERT(rf.ShouldWarn(TDuration::Minutes(21)));
        UNIT_ASSERT(rf.ShouldWarn(TDuration::Minutes(600)));
    }

    Y_UNIT_TEST(Common) {
        auto logger = MakeIntrusive<TLogger>();
        TRolesFetcher rf(
            TRolesFetcherSettings{.SelfAlias = "some_alias"},
            logger);
        UNIT_ASSERT(!rf.AreRolesOk());
        UNIT_ASSERT(!rf.GetCurrentRoles());

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            rf.Update(NUtils::TFetchResult{.Code = HTTP_NOT_MODIFIED}),
            yexception,
            "tvmtool did not return any roles because current roles are actual, but there are no roles in memory - this should never happen");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            rf.Update(NUtils::TFetchResult{.Code = HTTP_BAD_REQUEST, .Response = "kek"}),
            yexception,
            "Unexpected code from tvmtool: 400. kek");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            rf.Update(NUtils::TFetchResult{.Code = HTTP_OK, .Response = "kek"}),
            yexception,
            "Invalid json. 'kek'");

        UNIT_ASSERT_NO_EXCEPTION(rf.Update(NUtils::TFetchResult{.Code = HTTP_OK, .Response = ROLES}));
        UNIT_ASSERT(rf.AreRolesOk());
        UNIT_ASSERT(rf.GetCurrentRoles());
        UNIT_ASSERT_VALUES_EQUAL("100501", rf.GetCurrentRoles()->GetMeta().Revision);

        UNIT_ASSERT_NO_EXCEPTION(rf.Update(NUtils::TFetchResult{.Code = HTTP_NOT_MODIFIED}));
        UNIT_ASSERT_VALUES_EQUAL("100501", rf.GetCurrentRoles()->GetMeta().Revision);

        UNIT_ASSERT_VALUES_EQUAL(
            "7: Succeed to update roles with revision 100501\n",
            logger->Stream.Str());
    }

    Y_UNIT_TEST(CreateRequest) {
        struct TTestFetcher: TRolesFetcher {
            using TRolesFetcher::CreateRequest;
            using TRolesFetcher::TRequest;
            using TRolesFetcher::TRolesFetcher;
        };

        TTestFetcher rf(
            TRolesFetcherSettings{.SelfAlias = "some_&alias"},
            new TLogger);

        TTestFetcher::TRequest request = rf.CreateRequest({{"some_header", "some_value"}});
        UNIT_ASSERT_VALUES_EQUAL(
            "/v2/roles?self=some_%26alias",
            request.Url);
        UNIT_ASSERT_VALUES_EQUAL(
            TKeepAliveHttpClient::THeaders({{"some_header", "some_value"}}),
            request.Headers);

        UNIT_ASSERT_NO_EXCEPTION(rf.Update(NUtils::TFetchResult{.Code = HTTP_OK, .Response = ROLES}));

        request = rf.CreateRequest({{"some_header", "some_value"}});
        UNIT_ASSERT_VALUES_EQUAL(
            "/v2/roles?self=some_%26alias",
            request.Url);
        UNIT_ASSERT_VALUES_EQUAL(
            TKeepAliveHttpClient::THeaders({
                {"some_header", "some_value"},
                {"If-None-Match", R"("100501")"},
            }),
            request.Headers);
    }
}
