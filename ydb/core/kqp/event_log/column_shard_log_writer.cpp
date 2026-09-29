#include "column_shard_log_writer.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <ydb/core/kqp/ut/olap/combinatory/variator.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/typed_local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/wrappers/fake_storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NKqp::NSchematizedLog {

namespace {

using TEvCreateSessionRequest = NGRpcService::TGrpcRequestOperationCall<
    Ydb::Table::CreateSessionRequest,
    Ydb::Table::CreateSessionResponse>;
using TEvExecuteSchemeQueryRequest = NGRpcService::TGrpcRequestOperationCall<
    Ydb::Table::ExecuteSchemeQueryRequest,
    Ydb::Table::ExecuteSchemeQueryResponse>;

constexpr TStringBuf DatabasePath = "/Root";

template<typename TResponse>
TResponse WaitLocalRpc(TKikimrRunner& runner, NThreading::TFuture<TResponse> future) {
    return runner.GetTestServer().GetRuntime()->WaitFuture(std::move(future));
}

} // namespace

TColumnShardLogWriter::TColumnShardLogWriter(
    TKikimrRunner& runner,
    TLogMessageFilter filter,
    TDatabaseSettings settings,
    TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
    : TBaseEventLogWriter(std::move(filter), std::move(columns))
    , Settings(std::move(settings))
    , Runner(runner)
{
}

bool TColumnShardLogWriter::Write(const NActors::NStructuredLog::TLogMessage& message) {
    if (!TBaseEventLogWriter::Write(message)) {
        return false;
    }
    CurrentBatchSize++;
    if (Settings.MaxBatchSize.has_value() && CurrentBatchSize == Settings.MaxBatchSize.value()) {
        Flush();
    }
    return true;
}

void TColumnShardLogWriter::Flush() {
    TBaseEventLogWriter::Flush();
    CurrentBatchSize = 0;
}

TString TColumnShardLogWriter::GetCreateStoreQuery() {
    TStringBuilder sb;
    sb << " CREATE TABLESTORE `/Root/" << Settings.StoreName << "` (";

    for (const auto& column : Columns) {
        sb << column->Name << " " << column->Type;

        if (column->Settings.IsNotNull || column->Settings.IsPK) {
            sb << " NOT NULL";
        }

        if (column->Settings.IsDictionary) {
            sb << " ENCODING(DICT)";
        }
        sb << ", ";
    }
    sb << " PRIMARY KEY(";
    bool first = true;
    for (const auto& column : Columns) {
        if (!column->Settings.IsPK) continue;

        if (!first) {
            sb << ", ";
        } else {
            first = false;
        }
        sb << column->Name;
    }
    sb << ") ) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Settings.StoreShardsCount << ");";
    return sb;
}

TString TColumnShardLogWriter::GetCreateTableQuery() {

    TStringBuilder sb;

    sb << " CREATE TABLE `/Root/" << Settings.StoreName << "/" << Settings.TableName<< "` (";
    for (const auto& column : Columns) {
        sb << column->Name << " " << column->Type;

        if (column->Settings.IsNotNull || column->Settings.IsPK) {
            sb << " NOT NULL";
        }

        if (column->Settings.IsDictionary) {
            sb << " ENCODING(DICT)";
        }
        sb << ", ";
    }
    sb << " PRIMARY KEY(";
    bool first = true;
    for (const auto& column : Columns) {
        if (!column->Settings.IsPK) continue;

        if (!first) {
            sb << ", ";
        } else {
            first = false;
        }
        sb << column->Name;
    }
    sb << ") ) WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Settings.TableShardsCount << ");";
    return sb;
}

bool TColumnShardLogWriter::CheckStorageExists() const {
    // @todo Будет ли работать в production
    // @todo Не слишком ли - ходить через клиента?
    auto schemeClient = GetRunner().GetSchemeClient();

    const TString storePath = "/Root/" + Settings.StoreName;
    const auto store = schemeClient.DescribePath(storePath).GetValueSync();
    if (!store.IsSuccess() || store.GetEntry().Type != NYdb::NScheme::ESchemeEntryType::ColumnStore) {
        return false;
    }

    const TString tablePath = storePath + "/" + Settings.TableName;
    const auto table = schemeClient.DescribePath(tablePath).GetValueSync();
    return table.IsSuccess() && table.GetEntry().Type == NYdb::NScheme::ESchemeEntryType::ColumnTable;
}

bool TColumnShardLogWriter::ExecuteSchemeQuery(const TString& sessionId, const TString& query) {
    Ydb::Table::ExecuteSchemeQueryRequest request;
    request.set_session_id(sessionId);
    request.set_yql_text(query);

    const auto response = WaitLocalRpc(
        Runner,
        NRpcService::DoLocalRpc<TEvExecuteSchemeQueryRequest>(
            std::move(request), TString(DatabasePath), "", TActivationContext::ActorSystem()));
    return response.operation().status() == Ydb::StatusIds::SUCCESS;
}

void TColumnShardLogWriter::CreateStorage(TAfterFunc afterFunc) {
    Ydb::Table::CreateSessionRequest request;

    const auto response = WaitLocalRpc(
        GetRunner(),
        NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
            std::move(request), TString(DatabasePath), "", TActivationContext::ActorSystem()));
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        Cerr << "FAILED to create session" << Endl;
    }

    Ydb::Table::CreateSessionResult result;
    if (!response.operation().result().UnpackTo(&result)) {
        Cerr << "FAILED to create session" << Endl;
    }
    const TString sessionId = result.session_id();
    if (sessionId.empty()) {
        Cerr << "FAILED to create session" << Endl;
        return ;
    }

    const auto storeQuery = GetCreateStoreQuery();
    Cerr << "QUERY: " << storeQuery << Endl;
    if (!ExecuteSchemeQuery(sessionId, storeQuery)) {
        Cerr << "FAILED to create table store" << Endl;
        return;
    }

    const auto tableQuery = GetCreateTableQuery();
    Cerr << "QUERY: " << tableQuery << Endl;
    if (!ExecuteSchemeQuery(sessionId, tableQuery)) {
        Cerr << "FAILED to create table" << Endl;
    }

    if (afterFunc) {
        afterFunc();
    }
}

void TColumnShardLogWriter::CreateOrUpdateStorage(TAfterFunc afterFunc) {
    if (!CheckStorageExists()) {
        CreateStorage(afterFunc);
        StorageExists = true;
    } else {
        if (afterFunc) {
            afterFunc();
        }
    }
}

void TColumnShardLogWriter::WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    auto data = NKikimr::NArrow::SerializeBatchNoCompression(batch);
    TString serializedSchema = NKikimr::NArrow::SerializeSchema(*batch->schema());

    Ydb::Table::BulkUpsertRequest request;
    request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
    request.set_data(data);
    request.set_table(Sprintf("/Root/%s/%s", Settings.StoreName.c_str(), Settings.TableName.c_str()));

    // std::atomic<size_t> responses = 0;
    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(std::move(request), "", "", TActivationContext::ActorSystem());
    future.Subscribe([&](const NThreading::TFuture<Ydb::Table::BulkUpsertResponse> f) {
        Y_UNUSED(f);
        /* auto op = f.GetValueSync().operation();
        TStringBuilder issues;
        if (op.status() != Ydb::StatusIds::SUCCESS) {
            for (auto& issue : op.issues()) {
                issues << issue.message() << " ";
            }
            issues << "\n";
        }
        responses.fetch_add(1); */
    });
}

} // namespace NKikimr::NKqp::NSchematizedLog
