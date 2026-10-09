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

#define YDB_LOG_THIS_FILE_COMPONENT KQP_SLOW_LOG

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

} // namespace

TColumnShardLogWriter::TColumnShardLogWriter(
    TDatabaseSettings settings,
    TVector<std::shared_ptr<TEventLogColumn>> columns)
    : TBaseEventLogWriter(std::move(columns), settings.MaxBatchSize, settings.FlushTimeout)
    , Settings(std::move(settings))
{
}

bool TColumnShardLogWriter::Write(const NActors::NStructuredLog::TLogMessage& message) {
    if (!TBaseEventLogWriter::Write(message)) {
        return false;
    }

    if (Settings.FlushBatchSize.has_value() && CurrentBatchSize >= Settings.FlushBatchSize.value()) {
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

void TColumnShardLogWriter::CreateSession() {
    YDB_LOG_DEBUG("TColumnShardLogWriter: Create session");
    Ydb::Table::CreateSessionRequest request;

    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    auto future = NRpcService::DoLocalRpc<TEvCreateSessionRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    future.Subscribe([pThis](const NThreading::TFuture<Ydb::Table::CreateSessionResponse> f) {
        YDB_LOG_DEBUG("TColumnShardLogWriter: Create session response");

        if (pThis->State.load().Kind == TStateKind::Stop) {
            return ;
        }

        const auto response = f.GetValueSync();
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            YDB_LOG_ERROR("TColumnShardLogWriter: Can't create session",
                {"storeName", pThis->Settings.StoreName},
                {"tableName", pThis->Settings.TableName});
            pThis->SetCreateError();
            return;
        }

        Ydb::Table::CreateSessionResult result;
        if (!response.operation().result().UnpackTo(&result)) {
            YDB_LOG_ERROR("TColumnShardLogWriter: Can't unpack session description",
                {"storeName", pThis->Settings.StoreName},
                {"tableName", pThis->Settings.TableName});
            pThis->SetCreateError();
            return;
        }

        const TString sessionId = result.session_id();
        if (sessionId.empty()) {
            YDB_LOG_ERROR("TColumnShardLogWriter: Received empty session id",
                {"storeName", pThis->Settings.StoreName},
                {"tableName", pThis->Settings.TableName});
            pThis->SetCreateError();
            return;
        }

        YDB_LOG_DEBUG("TColumnShardLogWriter: Session is created",
            {"sessionId", sessionId});
        pThis->CreateStorage(sessionId);
    });
}
void TColumnShardLogWriter::ExecuteSchemeQuery(const TString& sessionId, const TString& query, std::function<void()> handle) {
    Ydb::Table::ExecuteSchemeQueryRequest request;
    request.set_session_id(sessionId);
    request.set_yql_text(query);

    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    auto future = NRpcService::DoLocalRpc<TEvExecuteSchemeQueryRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    future.Subscribe([pThis, query, handle, sessionId](const NThreading::TFuture<Ydb::Table::ExecuteSchemeQueryResponse> f) {
        if (pThis->State.load().Kind == TStateKind::Stop) {
            return ;
        }

        const auto response = f.GetValueSync();
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            YDB_LOG_ERROR("TColumnShardLogWriter: Failed to execute query",
                {"sessionId", sessionId},
                {"storeName", pThis->Settings.StoreName},
                {"tableName", pThis->Settings.TableName},
                {"query", query});
            pThis->SetCreateError();
            return;
        }

        YDB_LOG_NOTICE("TColumnShardLogWriter: Success query execution",
            {"sessionId", sessionId},
            {"storeName", pThis->Settings.StoreName},
            {"tableName", pThis->Settings.TableName},
            {"query", query});
        handle();
    });
}

void TColumnShardLogWriter::CreateStorage(const TString& sessionId) {
    YDB_LOG_NOTICE("TColumnShardLogWriter: Try to create storage",
        {"sessionId", sessionId},
        {"storeName", Settings.StoreName},
        {"tableName", Settings.TableName});

    const auto storeQuery = GetCreateStoreQuery();
    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    ExecuteSchemeQuery(sessionId, storeQuery,
        [pThis, sessionId]() {
            if (pThis->State.load().Kind == TStateKind::Stop) {
                return ;
            }
            pThis->CreateTable(sessionId);
        });
}

void TColumnShardLogWriter::CreateTable(const TString& sessionId) {
    YDB_LOG_NOTICE("TColumnShardLogWriter: Try to create table",
        {"sessionId", sessionId},
        {"storeName", Settings.StoreName},
        {"tableName", Settings.TableName});

    const auto tableQuery = GetCreateTableQuery();
    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    ExecuteSchemeQuery(sessionId, tableQuery,
        [pThis, sessionId]() {
            if (pThis->State.load().Kind == TStateKind::Stop) {
                return ;
            }

            YDB_LOG_NOTICE("TColumnShardLogWriter: Table created. Start to write log messages",
                {"sessionId", sessionId},
                {"storeName", pThis->Settings.StoreName},
                {"tableName", pThis->Settings.TableName});
            pThis->State.store(TState(TStateKind::Working));
            pThis->Flush();
        });
}

void TColumnShardLogWriter::CreateOrUpdateStorage() {
    YDB_LOG_NOTICE("TColumnShardLogWriter: Check storage and table are exists");
    Ydb::Table::DescribeTableRequest request;
    request.set_path(GetTablePath());

    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    auto future = NRpcService::DoLocalRpc<TEvDescribeTableRequest>(
        std::move(request), Settings.Path, "", TActivationContext::ActorSystem());
    future.Subscribe([pThis](const NThreading::TFuture<Ydb::Table::DescribeTableResponse> f) {
        if (pThis->State.load().Kind == TStateKind::Stop) {
            return ;
        }

        YDB_LOG_DEBUG("TColumnShardLogWriter: Check storage and table are exists response");
        const auto response = f.GetValueSync();
        if (response.operation().status() != Ydb::StatusIds::SUCCESS) {
            YDB_LOG_NOTICE("TColumnShardLogWriter: Storage and table seem to be absent. Try create it");
            pThis->CreateSession();
            return;
        }

        Ydb::Table::DescribeTableResult tableDescription;
        if (!response.operation().result().UnpackTo(&tableDescription)) {
            YDB_LOG_ERROR("TColumnShardLogWriter: Can't unpack table description",
                {"storeName", pThis->Settings.StoreName},
                {"tableName", pThis->Settings.TableName});
            pThis->SetCreateError();
        }

        if (tableDescription.columns().empty()) {
            YDB_LOG_NOTICE("TColumnShardLogWriter: There are no table columns. Try create it");
            pThis->CreateSession();
        } else {
            YDB_LOG_NOTICE("TColumnShardLogWriter: Try to update table columns");

            pThis->State.store(TStateKind::Working);
            pThis->Flush();
        }
    });
}

void TColumnShardLogWriter::SetCreateError() {
    auto oldState = State.load();
    auto newState = oldState;
    if (newState.CreateAttempCount != 0) {
        newState.CreateAttempCount--;
        newState.Kind = TStateKind::Started;
    } else {
        newState.Kind = TStateKind::StorageCreateError;
    }
    State.compare_exchange_strong(oldState, newState);
}

void TColumnShardLogWriter::WriteBatch(std::shared_ptr<arrow::RecordBatch> batch)  {
    TWriteBatch writeBatch;
    writeBatch.Data = batch;

    ProcessWriteBatch(writeBatch);
}

void TColumnShardLogWriter::ProcessWriteBatch(const TWriteBatch& writeBatch) {
    unsigned activeCount;
    do {
        activeCount = ActiveWriteCount.load();
        if (activeCount > Settings.MaxActiveWrites) {
            YDB_LOG_ERROR("TColumnShardLogWriter: Max active writes reached, chunk cancelled",
                {"maxActiveWriteCount", Settings.MaxActiveWrites});
            return ;
        }
    } while (!ActiveWriteCount.compare_exchange_strong(activeCount, activeCount + 1));

    auto data = NKikimr::NArrow::SerializeBatchNoCompression(writeBatch.Data);
    TString serializedSchema = NKikimr::NArrow::SerializeSchema(*(writeBatch.Data->schema()));

    Ydb::Table::BulkUpsertRequest request;
    request.mutable_arrow_batch_settings()->set_schema(serializedSchema);
    request.set_data(data);
    request.set_table(Sprintf("%s/%s/%s", Settings.Path.c_str(), Settings.StoreName.c_str(), Settings.TableName.c_str()));

    // std::atomic<size_t> responses = 0;
    auto pThis = std::dynamic_pointer_cast<TColumnShardLogWriter>(shared_from_this());
    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>;
    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(std::move(request), "", "", TActivationContext::ActorSystem());
    future.Subscribe([pThis, writeBatch](const NThreading::TFuture<Ydb::Table::BulkUpsertResponse> f) {
        pThis->ActiveWriteCount--;

        if (pThis->State.load().Kind == TStateKind::Stop) {
            return ;
        }

        auto op = f.GetValueSync().operation();
        if (op.status() != Ydb::StatusIds::SUCCESS) {
            auto newWriteBatch = writeBatch;
            newWriteBatch.AttemptNum++;
            if (newWriteBatch.AttemptNum < pThis->Settings.MaxWriteAttempts) {
                pThis->ProcessWriteBatch(newWriteBatch);
            } else {
                YDB_LOG_ERROR("TColumnShardLogWriter: Max write attempts reached, chunk cancelled",
                    {"maxActiveWriteCount", pThis->Settings.MaxWriteAttempts});
            }
        }
    });
}

} // namespace NKikimr::NKqp::NEventLog
