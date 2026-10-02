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

namespace NKikimr::NKqp::NEventLog {

namespace {

using TEvCreateSessionRequest = NGRpcService::TGrpcRequestOperationCall<
    Ydb::Table::CreateSessionRequest,
    Ydb::Table::CreateSessionResponse>;
using TEvExecuteSchemeQueryRequest = NGRpcService::TGrpcRequestOperationCall<
    Ydb::Table::ExecuteSchemeQueryRequest,
    Ydb::Table::ExecuteSchemeQueryResponse>;
using TEvDescribeTableRequest = NGRpcService::TGrpcRequestOperationCall<
    Ydb::Table::DescribeTableRequest,
    Ydb::Table::DescribeTableResponse>;

template<typename TEvent, typename TResult>
std::optional<TResult> CallLocalRpc(
    typename TEvent::TRequest&& request,
    const TString& database)
{
    auto future = NRpcService::DoLocalRpc<TEvent>(
        std::move(request), database, "", TActivationContext::ActorSystem());
    const auto response = future.GetValueSync();
    if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
        return std::nullopt;
    }

    TResult result;
    if (!response.operation().result().UnpackTo(&result)) {
        return std::nullopt;
    }
    return result;
}

std::optional<Ydb::Table::DescribeTableResult> DescribeTable(
    const TString& database,
    const TString& path)
{
    Ydb::Table::DescribeTableRequest request;
    request.set_path(path);
    return CallLocalRpc<TEvDescribeTableRequest, Ydb::Table::DescribeTableResult>(
        std::move(request), database);
}

} // namespace

TColumnShardLogWriter::TColumnShardLogWriter(
    TDatabaseSettings settings,
    TVector<std::shared_ptr<TSchematizedLogColumn>> columns)
    : TBaseEventLogWriter(std::move(columns), settings.FlushTimeout)
    , Settings(std::move(settings))
{
}

bool TColumnShardLogWriter::Write(const NActors::NStructuredLog::TLogMessage& message) {
    if (!TBaseEventLogWriter::Write(message)) {
        return false;
    }

    if (Settings.MaxBatchSize.has_value() && CurrentBatchSize >= Settings.MaxBatchSize.value()) {
        Flush();
    }
    return true;
}

TString TColumnShardLogWriter::GetCreateStoreQuery() {
    TStringBuilder sb;
    sb << " CREATE TABLESTORE `" << Settings.Path << "/" << Settings.StoreName << "` (";

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

    sb << " CREATE TABLE `" << Settings.Path << "/" << Settings.StoreName << "/" << Settings.TableName<< "` (";
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

TString TColumnShardLogWriter::GetStorePath() const {
    return TStringBuilder() << Settings.Path << "/" << Settings.StoreName;
}

TString TColumnShardLogWriter::GetTablePath() const {
    return TStringBuilder() << Settings.Path << "/" << Settings.StoreName << "/" << Settings.TableName;
}

std::optional<TVector<TString>> TColumnShardLogWriter::GetTableColumnNames() const {
    const auto tableDescription = DescribeTable(Settings.Path, GetTablePath());
    if (!tableDescription) {
        return std::nullopt;
    }

    TVector<TString> columnNames;
    columnNames.reserve(tableDescription->columns_size());
    for (const auto& column : tableDescription->columns()) {
        columnNames.push_back(column.name());
    }
    return columnNames;
}

bool TColumnShardLogWriter::CheckStorageExists() {
    if (CreationState.load() == TCreationState::Exists) {
        return true;
    }

    const auto columnNames = GetTableColumnNames();
    if (!columnNames || columnNames->empty()) {
        return false;
    }

    Cerr << "DEBUG: table exists, fields:";
    for (const auto& columnName : *columnNames) {
        Cerr << " " << columnName;
    }
    Cerr << Endl;

    CreationState.store(TCreationState::Exists);
    return true;
}

bool TColumnShardLogWriter::ExecuteSchemeQuery(const TString& sessionId, const TString& query) {
    Ydb::Table::ExecuteSchemeQueryRequest request;
    request.set_session_id(sessionId);
    request.set_yql_text(query);

    auto future = NRpcService::DoLocalRpc<TEvExecuteSchemeQueryRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    const auto response = future.GetValueSync();
    return response.operation().status() == Ydb::StatusIds::SUCCESS;
}

void TColumnShardLogWriter::CreateStorage() {
    Cerr << "DEBUG: CreateStorage" <<  Endl;

    Ydb::Table::CreateSessionRequest request;
    auto future = NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    const auto response = future.GetValueSync();
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
    Cerr << "DEBUG: QUERY: " << storeQuery << Endl;
    if (!ExecuteSchemeQuery(sessionId, storeQuery)) {
        Cerr << "FAILED to create table store" << Endl;
        return;
    }

    const auto tableQuery = GetCreateTableQuery();
    Cerr << "DEBUG: QUERY: " << tableQuery << Endl;
    if (!ExecuteSchemeQuery(sessionId, tableQuery)) {
        Cerr << "FAILED to create table" << Endl;
    }

    Cerr << "DEBUG: Set CreationState = TCreationState::Exists" <<  Endl;
    CreationState.store(TCreationState::Exists);
    //@ todo force call Flush immediatelly?
}

void TColumnShardLogWriter::CreateOrUpdateStorage() {
    Cerr << "DEBUG: CreateOrUpdateStorage" <<  Endl;
    if (!CheckStorageExists()) {
        CreateStorage();
    }
}

void TColumnShardLogWriter::WriteBatch(std::shared_ptr<arrow::RecordBatch> batch) {
    auto data = NKikimr::NArrow::SerializeBatchNoCompression(batch);
    TString serializedSchema = NKikimr::NArrow::SerializeSchema(*batch->schema());

    Ydb::Table::BulkUpsertRequest request;
    request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
    request.set_data(data);
    request.set_table(Sprintf("%s/%s/%s", Settings.Path.c_str(), Settings.StoreName.c_str(), Settings.TableName.c_str()));

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

} // namespace NKikimr::NKqp::NEventLog
