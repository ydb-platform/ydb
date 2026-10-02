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
    if (State.load() == TState::Working) {
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

    State.store(TState::Working);
    return true;
}

void TColumnShardLogWriter::ExecuteSchemeQuery(const TString& sessionId, const TString& query, std::function<void()> handle) {
    Ydb::Table::ExecuteSchemeQueryRequest request;
    request.set_session_id(sessionId);
    request.set_yql_text(query);

    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    auto future = NRpcService::DoLocalRpc<TEvExecuteSchemeQueryRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    future.Subscribe([pThis, query, handle](const NThreading::TFuture<Ydb::Table::ExecuteSchemeQueryResponse> f) {
        const auto response = f.GetValueSync();
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            Cerr << "DEBUG: FAILED to execute query " << query << Endl;
            pThis->State.store(TState::CreateError);
            return;
        }
        Cerr << "DEBUG: SUCCESS to execute query " << query << Endl;
        handle();
    });
}

void TColumnShardLogWriter::CreateSession() {
    Cerr << "DEBUG: CreateSession" << Endl;
    Ydb::Table::CreateSessionRequest request;

    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    auto future = NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    future.Subscribe([pThis](const NThreading::TFuture<Ydb::Table::CreateSessionResponse> f) {

        Cerr << "DEBUG: Enter response handler" << Endl;

        const auto response = f.GetValueSync();
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            Cerr << "DEBUG: FAILED to create session" << Endl;
            pThis->State.store(TState::CreateError);
            return;
        }

        Ydb::Table::CreateSessionResult result;
        if (!response.operation().result().UnpackTo(&result)) {
            Cerr << "DEBUG: FAILED to create session" << Endl;
            pThis->State.store(TState::CreateError);
            return;
        }
        const TString sessionId = result.session_id();
        if (sessionId.empty()) {
            Cerr << "FAILED to create session" << Endl;
            pThis->State.store(TState::CreateError);
            return;
        }
        Cerr << "DEBUG: Subscribe sessionId=" << sessionId << Endl;
        pThis->CreateStorage(sessionId);
    });
}

void TColumnShardLogWriter::CreateStorage(const TString& sessionId) {
    Cerr << "DEBUG: CreateStorage" <<  Endl;

    const auto storeQuery = GetCreateStoreQuery();
    Cerr << "DEBUG: QUERY: " << storeQuery << Endl;
    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    ExecuteSchemeQuery(sessionId, storeQuery,
        [pThis, sessionId]() {
            pThis->CreateTable(sessionId);
        });
}

void TColumnShardLogWriter::CreateTable(const TString& sessionId) {

    const auto tableQuery = GetCreateTableQuery();
    Cerr << "DEBUG: QUERY: " << tableQuery << Endl;
    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    ExecuteSchemeQuery(sessionId, tableQuery,
        [pThis, sessionId]() {
            Cerr << "DEBUG: Set CreationState = TState::Exists" <<  Endl;
            pThis->State.store(TState::Working);
            // @todo sync call Flush
            pThis->Flush();
        });
}

void TColumnShardLogWriter::CreateOrUpdateStorage() {
    Cerr << "DEBUG: CreateOrUpdateStorage" <<  Endl;
    if (!CheckStorageExists()) {
        CreateSession();
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
