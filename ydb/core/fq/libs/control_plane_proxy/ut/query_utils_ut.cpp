#include <ydb/core/fq/libs/control_plane_proxy/actors/query_utils.h>

#include <ydb/public/api/protos/draft/fq.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <utility>

namespace NFq::NPrivate {

namespace {

FederatedQuery::BindingContent MakeBindingContent() {
    FederatedQuery::BindingContent content;
    content.set_name("binding");
    content.mutable_setting()->mutable_object_storage()->add_subset()->set_path_pattern("path");
    return content;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryUtils) {
    Y_UNIT_TEST(EscapesConnectionNameInExternalTableDdl) {
        const TVector<std::pair<TString, TString>> testCases = {
            {R"(\at-start)", R"(DATA_SOURCE = "\\at-start")"},
            {R"(in\middle)", R"(DATA_SOURCE = "in\\middle")"},
            {R"(at-end\)", R"(DATA_SOURCE = "at-end\\")"},
            {R"(two\\slashes)", R"(DATA_SOURCE = "two\\\\slashes")"},
            {R"(at-end\\)", R"(DATA_SOURCE = "at-end\\\\")"},
            {R"("at-start)", R"(DATA_SOURCE = "\"at-start")"},
            {R"(in"middle)", R"(DATA_SOURCE = "in\"middle")"},
            {R"(at-end")", R"(DATA_SOURCE = "at-end\"")"},
        };

        for (const auto& [connectionName, expectedDataSource] : testCases) {
            const TString query = MakeCreateExternalDataTableQuery(
                MakeBindingContent(), connectionName, false);

            UNIT_ASSERT_STRING_CONTAINS_C(query, expectedDataSource, query);
        }
    }
}

} // namespace NFq::NPrivate
