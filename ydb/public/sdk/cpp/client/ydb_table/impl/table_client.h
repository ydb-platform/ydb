#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/session_client/session_client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/scheme_helpers/helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/table_helpers/helpers.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/session_pool/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include "client_session.h"
#include "data_query.h"
#include "request_migrator.h"
#include "readers.h"


namespace NYdb {
namespace NTable {

//How ofter run host scan to perform session balancing
constexpr TDuration HOSTSCAN_PERIODIC_ACTION_INTERVAL = TDuration::Seconds(2);
constexpr TDuration KEEP_ALIVE_CLIENT_TIMEOUT = TDuration::Seconds(5);

TDuration GetMinTimeToTouch(const TSessionPoolSettings& settings);
TDuration GetMaxTimeToTouch(const TSessionPoolSettings& settings);

class TTableClient::TImpl: public TClientImplCommon<TTableClient::TImpl>, public ISessionClient {
public:
    using TReadTableStreamProcessorPtr = TTablePartIterator::TReaderImpl::TStreamProcessorPtr;
    using TScanQueryProcessorPtr = TScanQueryPartIterator::TReaderImpl::TStreamProcessorPtr;

    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings);
    ~TImpl();

    bool LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag);
    void InitStopper();
    NThreading::TFuture<void> Drain();
    NThreading::TFuture<void> Stop();
    void ScheduleTaskUnsafe(std::function<void()>&& fn, TDuration timeout);
    void StartPeriodicSessionPoolTask();
    static ui64 ScanForeignLocations(std::shared_ptr<TTableClient::TImpl> client);
    static std::pair<ui64, size_t> ScanLocation(std::shared_ptr<TTableClient::TImpl> client,
        std::unordered_map<ui64, size_t>& sessions, bool allNodes);
    static NMath::TStats CalcCV(const std::unordered_map<ui64, size_t>& in);
    void StartPeriodicHostScanTask();

    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings);
    i64 GetActiveSessionCount() const;
    i64 GetActiveSessionsLimit() const;
    i64 GetCurrentPoolSize() const;
    TAsyncCreateSessionResult CreateSession(const TCreateSessionSettings& settings, bool standalone,
        TString preferredLocation = TString());
    TAsyncKeepAliveResult KeepAlive(const TSession::TImpl* session, const TKeepAliveSettings& settings);

    TFuture<TStatus> CreateTable(Ydb::Table::CreateTableRequest&& request, const TCreateTableSettings& settings);
    TFuture<TStatus> AlterTable(Ydb::Table::AlterTableRequest&& request, const TAlterTableSettings& settings);
    TAsyncOperation AlterTableLong(Ydb::Table::AlterTableRequest&& request, const TAlterTableSettings& settings);
    TFuture<TStatus> CopyTable(const TString& sessionId, const TString& src, const TString& dst,
        const TCopyTableSettings& settings);
    TFuture<TStatus> CopyTables(Ydb::Table::CopyTablesRequest&& request, const TCopyTablesSettings& settings);
    TFuture<TStatus> RenameTables(Ydb::Table::RenameTablesRequest&& request, const TRenameTablesSettings& settings);
    TFuture<TStatus> DropTable(const TString& sessionId, const TString& path, const TDropTableSettings& settings);
    TAsyncDescribeTableResult DescribeTable(const TString& sessionId, const TString& path, const TDescribeTableSettings& settings);

    template<typename TParamsType>
    TAsyncDataQueryResult ExecuteDataQuery(TSession& session, const TString& query, const TTxControl& txControl,
        TParamsType params, const TExecDataQuerySettings& settings) {
        auto maybeQuery = session.SessionImpl_->GetQueryFromCache(query, Settings_.AllowRequestMigration_);
        if (maybeQuery) {
            TDataQuery dataQuery(session, query, maybeQuery->QueryId, maybeQuery->ParameterTypes);
            return ExecuteDataQuery(session, dataQuery, txControl, params, settings, true);
        }

        CacheMissCounter.Inc();

        return ::NYdb::NSessionPool::InjectSessionStatusInterception(session.SessionImpl_,
            ExecuteDataQueryInternal(session, query, txControl, params, settings, false),
            true, GetMinTimeToTouch(Settings_.SessionPoolSettings_));
    }

    template<typename TParamsType>
    TAsyncDataQueryResult ExecuteDataQuery(TSession& session, const TDataQuery& dataQuery, const TTxControl& txControl,
        TParamsType params, const TExecDataQuerySettings& settings,
        bool fromCache) {
        TString queryKey = dataQuery.Impl_->GetTextHash();
        auto cb = [queryKey](const TDataQueryResult& result, TKqpSessionCommon& session) {
            if (result.GetStatus() == EStatus::NOT_FOUND) {
                static_cast<TSession::TImpl&>(session).InvalidateQueryInCache(queryKey);
            }
        };

        return ::NYdb::NSessionPool::InjectSessionStatusInterception<TDataQueryResult>(
            session.SessionImpl_,
            session.Client_->ExecuteDataQueryInternal(session, dataQuery, txControl, params, settings, fromCache),
            true,
            GetMinTimeToTouch(session.Client_->Settings_.SessionPoolSettings_),
            cb);
    }

    TAsyncPrepareQueryResult PrepareDataQuery(const TSession& session, const TString& query,
        const TPrepareDataQuerySettings& settings);
    TAsyncStatus ExecuteSchemeQuery(const TString& sessionId, const TString& query,
        const TExecSchemeQuerySettings& settings);

    TAsyncBeginTransactionResult BeginTransaction(const TSession& session, const TTxSettings& txSettings,
        const TBeginTxSettings& settings);
    TAsyncCommitTransactionResult CommitTransaction(const TSession& session, const TTransaction& tx,
        const TCommitTxSettings& settings);
    TAsyncStatus RollbackTransaction(const TSession& session, const TTransaction& tx,
        const TRollbackTxSettings& settings);

    TAsyncExplainDataQueryResult ExplainDataQuery(const TSession& session, const TString& query,
        const TExplainDataQuerySettings& settings);

    static void SetTypedValue(Ydb::TypedValue* protoValue, const TValue& value);

    NThreading::TFuture<std::pair<TPlainStatus, TReadTableStreamProcessorPtr>> ReadTable(
        const TString& sessionId,
        const TString& path,
        const TReadTableSettings& settings);
    TAsyncReadRowsResult ReadRows(const TString& path, TValue&& keys, const TVector<TString>& columns, const TReadRowsSettings& settings);

    TAsyncStatus Close(const TKqpSessionCommon* sessionImpl, const TCloseSessionSettings& settings);
    TAsyncStatus CloseInternal(const TKqpSessionCommon* sessionImpl);

    bool ReturnSession(TKqpSessionCommon* sessionImpl) override;
    void DeleteSession(TKqpSessionCommon* sessionImpl) override;
    ui32 GetSessionRetryLimit() const;

    void SetStatCollector(const NSdkStats::TStatCollector::TClientStatCollector& collector);

    TAsyncBulkUpsertResult BulkUpsert(const TString& table, TValue&& rows, const TBulkUpsertSettings& settings);
    TAsyncBulkUpsertResult BulkUpsert(const TString& table, EDataFormat format,
        const TString& data, const TString& schema, const TBulkUpsertSettings& settings);

    TFuture<std::pair<TPlainStatus, TScanQueryProcessorPtr>> StreamExecuteScanQueryInternal(const TString& query,
        const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        const TStreamExecScanQuerySettings& settings);
    TAsyncScanQueryPartIterator StreamExecuteScanQuery(const TString& query,
        const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        const TStreamExecScanQuerySettings& settings);
    void CollectRetryStatAsync(EStatus status);
    void CollectRetryStatSync(EStatus status);

public:
    TClientSettings Settings_;

private:
    static void SetParams(
        ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        Ydb::Table::ExecuteDataQueryRequest* request);

    static void SetParams(
        const ::google::protobuf::Map<TString, Ydb::TypedValue>& params,
        Ydb::Table::ExecuteDataQueryRequest* request);

    static void CollectParams(
        ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
        NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> histgoram);

    static void CollectParams(
        const ::google::protobuf::Map<TString, Ydb::TypedValue>& params,
        NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> histgoram);

    static void CollectQuerySize(const TString& query, NSdkStats::TAtomicHistogram<::NMonitoring::THistogram>& querySizeHistogram);

    static void CollectQuerySize(const TDataQuery&, NSdkStats::TAtomicHistogram<::NMonitoring::THistogram>&);

    template <typename TQueryType, typename TParamsType>
    TAsyncDataQueryResult ExecuteDataQueryInternal(const TSession& session, const TQueryType& query,
        const TTxControl& txControl, TParamsType params,
        const TExecDataQuerySettings& settings, bool fromCache
    ) {
        auto request = MakeOperationRequest<Ydb::Table::ExecuteDataQueryRequest>(settings);
        request.set_session_id(session.GetId());
        auto txControlProto = request.mutable_tx_control();
        txControlProto->set_commit_tx(txControl.CommitTx_);
        if (txControl.TxId_) {
            txControlProto->set_tx_id(*txControl.TxId_);
        } else {
            SetTxSettings(txControl.BeginTx_, txControlProto->mutable_begin_tx());
        }

        request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));

        SetQuery(query, request.mutable_query());
        CollectQuerySize(query, QuerySizeHistogram);

        SetParams(params, &request);
        CollectParams(params, ParamsSizeHistogram);

        SetQueryCachePolicy(query, settings, request.mutable_query_cache_policy());

        auto promise = NewPromise<TDataQueryResult>();
        bool keepInCache = settings.KeepInQueryCache_ && settings.KeepInQueryCache_.GetRef();

        // We don't want to delay call of TSession dtor, so we can't capture it by copy
        // otherwise we break session pool and other clients logic.
        // Same problem with TDataQuery and TTransaction
        //
        // The fast solution is:
        // - create copy of TSession out of lambda
        // - capture pointer
        // - call free just before SetValue call
        auto sessionPtr = new TSession(session);
        auto extractor = [promise, sessionPtr, query, fromCache, keepInCache]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                TVector<TResultSet> res;
                TMaybe<TTransaction> tx;
                TMaybe<TDataQuery> dataQuery;
                TMaybe<TQueryStats> queryStats;

                auto queryText = GetQueryText(query);
                if (any) {
                    Ydb::Table::ExecuteQueryResult result;
                    any->UnpackTo(&result);

                    for (size_t i = 0; i < result.result_setsSize(); i++) {
                        res.push_back(TResultSet(*result.mutable_result_sets(i)));
                    }

                    if (result.has_tx_meta()) {
                        tx = TTransaction(*sessionPtr, result.tx_meta().id());
                    }

                    if (result.has_query_meta()) {
                        if (queryText) {
                            auto& query_meta = result.query_meta();
                            dataQuery = TDataQuery(*sessionPtr, *queryText, query_meta.id(), query_meta.parameters_types());
                        }
                    }

                    if (result.has_query_stats()) {
                        queryStats = TQueryStats(result.query_stats());
                    }
                }

                if (keepInCache && dataQuery && queryText) {
                    sessionPtr->SessionImpl_->AddQueryToCache(*dataQuery);
                }

                TDataQueryResult dataQueryResult(TStatus(std::move(status)),
                    std::move(res), tx, dataQuery, fromCache, queryStats);

                delete sessionPtr;
                tx.Clear();
                dataQuery.Clear();
                promise.SetValue(std::move(dataQueryResult));
            };

        Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>(
            std::move(request),
            extractor,
            &Ydb::Table::V1::TableService::Stub::AsyncExecuteDataQuery,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey())
            );

        return promise.GetFuture();
    }

    static void SetTxSettings(const TTxSettings& txSettings, Ydb::Table::TransactionSettings* proto);

    static void SetQuery(const TString& queryText, Ydb::Table::Query* query);

    static void SetQuery(const TDataQuery& queryData, Ydb::Table::Query* query);

    static void SetQueryCachePolicy(const TString&, const TExecDataQuerySettings& settings,
        Ydb::Table::QueryCachePolicy* queryCachePolicy);

    static void SetQueryCachePolicy(const TDataQuery&, const TExecDataQuerySettings& settings,
        Ydb::Table::QueryCachePolicy* queryCachePolicy);

    static TMaybe<TString> GetQueryText(const TString& queryText);

    static TMaybe<TString> GetQueryText(const TDataQuery& queryData);

public:
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> CacheMissCounter;
    NSdkStats::TStatCollector::TClientRetryOperationStatCollector RetryOperationStatCollector;
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> QuerySizeHistogram;
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> ParamsSizeHistogram;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> SessionRemovedDueBalancing;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> RequestMigrated;

private:
    NSessionPool::TSessionPool SessionPool_;
    TRequestMigrator RequestMigrator_;
    static const TKeepAliveSettings KeepAliveSettings;
};

}
}
