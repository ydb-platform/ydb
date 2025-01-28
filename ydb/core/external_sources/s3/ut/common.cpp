#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb-cpp-sdk/client/operation/operation.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/types/operation/operation.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace fmt::literals;

TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

TString GetExternalPort(const TString& service, const TString& port) {
    auto dockerComposeBin = BinaryPath("library/recipes/docker_compose/bin/docker-compose");
    auto composeFileYml = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "docker-compose.yml");
    auto result = StringSplitter(Exec(dockerComposeBin + " -f " + composeFileYml + " port " + service + " " + port)).Split(':').ToList<TString>();
    return result ? Strip(result.back()) : TString{};
}

void WaitBucket(std::shared_ptr<TKikimrRunner> kikimr, const TString& externalDataSourceName) {
    auto db = kikimr->GetQueryClient();
    for (size_t i = 0; i < 100; i++) {
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            SELECT * FROM `{external_source}`.`/a/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            )
        )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        if (readyOp.Metadata().ExecStatus == EExecStatus::Completed) {
            return;
        }
        Sleep(TDuration::Seconds(1));
    }
    UNIT_FAIL("Bucket isn't ready");
}

} // namespace NKikimr::NKqp
