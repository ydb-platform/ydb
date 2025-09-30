#include <ydb/core/mon/audit/audit.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NMonitoring::NAudit;

namespace {

struct TRequestHolder : public NHttp::THttpIncomingRequest {
    TStringBuf Store(TString value) {
        return Storage.emplace_back(std::move(value));
    }
private:
    TVector<TString> Storage;
};

NHttp::THttpIncomingRequestPtr MakeRequest(TString method, TString url) {
    auto request = MakeIntrusive<TRequestHolder>();
    request->Method = request->Store(std::move(method));
    request->URL = request->Store(std::move(url));
    return std::move(request);
}

} // namespace

Y_UNIT_TEST_SUITE(TAuditTest) {
    Y_UNIT_TEST(AuditDisabledWithoutAppData) {
        UNIT_ASSERT(!TAuditCtx::AuditEnabled(NKikimrConfig::TAuditConfig::TLogClassConfig::Completed, NACLibProto::SUBJECT_TYPE_ANONYMOUS));
    }

    Y_UNIT_TEST(ModifyingMethodsAlwaysAuditable) {
        TAuditCtx ctx;
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("POST", "/path")));
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("PUT", "/path")));
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("DELETE", "/path")));

        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("POST", "/counters")));
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("PUT", "/counters")));
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("DELETE", "/counters")));
    }

    Y_UNIT_TEST(OptionsRequestsAreNotAudited) {
        TAuditCtx ctx;
        UNIT_ASSERT(!ctx.AuditableRequest(MakeRequest("OPTIONS", "/path")));
    }

    Y_UNIT_TEST(BlacklistedPathsAreNotAudited) {
        TAuditCtx ctx;
        UNIT_ASSERT(!ctx.AuditableRequest(MakeRequest("GET", "/counters")));
        UNIT_ASSERT(!ctx.AuditableRequest(MakeRequest("GET", "/viewer/subpage")));
        UNIT_ASSERT(!ctx.AuditableRequest(MakeRequest("GET", "/viewer?mode=overview")));
        UNIT_ASSERT(!ctx.AuditableRequest(MakeRequest("GET", "/monitoring/cluster/static/js/24615.12b53f26.chunk.js")));
    }

    Y_UNIT_TEST(OtherGetRequestsAreAudited) {
        TAuditCtx ctx;
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("GET", "/other")));
        UNIT_ASSERT(ctx.AuditableRequest(MakeRequest("GET", "/viewerstats?mode=overview")));
    }
}
