#include "ut_utils.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/lib/fq/scope.h>
#include <ydb/core/fq/libs/actors/proxy.h>
#include <ydb/core/fq/libs/events/events.h>

#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/library/yql/utils/bind_in_range.h>

#include <util/system/file.h>
#include <util/stream/str.h>
#include <util/string/printf.h>
#include <util/string/builder.h>

#include <library/cpp/protobuf/json/proto2json.h>

using namespace NYdb;
using namespace NYdb::NFq;
using namespace NActors;

void UpsertToExistingTable(TDriver& driver, const TString& location){
    Y_UNUSED(location);
    const TString tablePrefix = "Root/yq";
    NYdb::NTable::TClientSettings settings;
    NYdb::NTable::TTableClient client(driver, settings);
    auto sessionOp = client.CreateSession().ExtractValueSync();
    if (!sessionOp.IsSuccess()) {
        ythrow yexception() << sessionOp.GetStatus() << "\n" << sessionOp.GetIssues().ToString();
    }
    auto session = sessionOp.GetSession();
    auto timeProvider = CreateDefaultTimeProvider();
    auto now = timeProvider->Now();
    NYdb::TParamsBuilder paramsBuilder;

    paramsBuilder.AddParam("$now").Timestamp(now).Build();
    auto params = paramsBuilder.Build();

    const TString scope = TScope("some_folder_id").ToString();

    {
        auto result = session.ExecuteSchemeQuery(
            Sprintf(R"___(
            --!syntax_v1
            CREATE TABLE `%s/%s` (
                id String,
                PRIMARY KEY (id)
            );
        )___", tablePrefix.c_str(), "empty_table")
            ).ExtractValueSync();
        if (!result.IsSuccess()) {
            ythrow yexception() << result.GetStatus() << "\n" << result.GetIssues().ToString();
        }
    }

    {
        NYdb::TParamsBuilder paramsBuilder;
        auto result = session.ExecuteDataQuery(
        Sprintf(R"___(
            --!syntax_v1
            upsert into `%s/empty_table`
                (id)
            values
                ("test_id1"), ("test_id2");
        )___", tablePrefix.c_str()),
        NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
        paramsBuilder.Build(),
        NYdb::NTable::TExecDataQuerySettings()).ExtractValueSync();
        if (!result.IsSuccess()) {
            ythrow yexception() << result.GetStatus() << "\n" << result.GetIssues().ToString();
        }
    }

    {
        NYdb::TParamsBuilder paramsBuilder;
        auto result = session.ExecuteDataQuery(
        Sprintf(R"___(
            --!syntax_v1
            delete from `%s/empty_table`;
        )___", tablePrefix.c_str()),
        NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
        paramsBuilder.Build(),
        NYdb::NTable::TExecDataQuerySettings()).ExtractValueSync();
        if (!result.IsSuccess()) {
            ythrow yexception() << result.GetStatus() << "\n" << result.GetIssues().ToString();
        }
    }
}
