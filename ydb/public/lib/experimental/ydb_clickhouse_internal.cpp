#include "ydb_clickhouse_internal.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_clickhouse_internal_v1.grpc.pb.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

// TODO: Bad dependency???
#include <ydb/core/scheme/scheme_tablecell.h>

namespace NYdb {
namespace NClickhouseInternal {

using namespace NThreading;

///////////////////////////////////////////////////////////////////////////
class TScanResult::TResultImpl {
public:
    TResultImpl(Ydb::ClickhouseInternal::ScanResult&& result)
        : Result(std::move(result))
    {}

    const Ydb::ClickhouseInternal::ScanResult Result;
};

TScanResult::TScanResult(TResultImpl* impl, TStatus&& status)
    : TStatus(std::move(status))
    , ResultImpl(impl)
{}

TScanResult::TScanResult(TScanResult&& other)
    : TStatus(std::move(other))
    , ResultImpl(std::move(other.ResultImpl))
{}

TScanResult& TScanResult::operator = (TScanResult&& other) {
    (TStatus&)*this = std::move(other);
    ResultImpl = std::move(other.ResultImpl);
    return *this;
}

TScanResult::~TScanResult() {
}

size_t TScanResult::GetBuffersCount() const {
    return ResultImpl->Result.blocks_size();
}

TString TScanResult::GetBuffer(size_t idx) const {
    return ResultImpl->Result.blocks(idx);
}

bool TScanResult::IsEos() const {
    return ResultImpl->Result.eos();
}

std::pair<TString, bool> TScanResult::GetLastKey() const {
    return {ResultImpl->Result.last_key(), ResultImpl->Result.last_key_inclusive()};
}

class TScanClient::TImpl : public TClientImplCommon<TScanClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TAsyncScanResult Scan(
            const TString& table, const TVector<TString>& columns,
            ui64 maxRows, ui64 maxBytes,
            const TString& fromKey, bool fromKeyInclusive,
            const TScanSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ClickhouseInternal::ScanRequest>(settings);
        request.set_table(table);
        for (TString col : columns) {
            request.add_columns(col);
        }
        request.set_from_key(fromKey);
        request.set_from_key_inclusive(fromKeyInclusive);
        request.set_max_rows(maxRows);
        request.set_max_bytes(maxBytes);
        if (settings.SnapshotId_) {
            request.set_snapshot_id(settings.SnapshotId_);
        }
//        Cerr << request << Endl;

        auto promise = NThreading::NewPromise<TScanResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::ClickhouseInternal::ScanResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
//                Cerr << result << Endl;

                TScanResult val(new TScanResult::TResultImpl(std::move(result)),
                   TStatus(std::move(status)));

                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::ClickhouseInternal::V1::ClickhouseInternalService, Ydb::ClickhouseInternal::ScanRequest, Ydb::ClickhouseInternal::ScanResponse>(
            std::move(request),
            extractor,
            &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub::AsyncScan,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings, TEndpointKey(settings.Endpoint_, 0))
            );

        return promise.GetFuture();
    }
};


TScanClient::TScanClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncScanResult TScanClient::Scan(const TString& path, const TVector<TString>& columns,
        ui64 maxRows, ui64 maxBytes, const TString& fromKey, bool fromKeyInclusive, const TScanSettings& settings) {
    return Impl_->Scan(path, columns, maxRows, maxBytes, fromKey, fromKeyInclusive, settings);
}

bool RangeFinished(const TString& lastReadKey, const TString& endKey, const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes) {
    if (lastReadKey.empty()) // +inf
        return true;

    if (endKey.empty())
        return false;

    NKikimr::TSerializedCellVec last(lastReadKey);
    Y_ABORT_UNLESS(last.GetCells().size() <= keyColumnTypes.size());

    NKikimr::TSerializedCellVec end(endKey);
    Y_ABORT_UNLESS(end.GetCells().size() <= keyColumnTypes.size());

    int cmp = NKikimr::CompareTypedCellVectors(
                last.GetCells().data(), end.GetCells().data(),
                keyColumnTypes.data(),
                last.GetCells().size(), end.GetCells().size());

    return cmp >= 0;
}

TScanIterator::TScanIterator(const TDriver& driver, const TString &database, const TString &endpoint, const TString &token, bool ssl, const TString& path,
                                           const TVector<TString>& columns,
                                           const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
                                           ui64 maxRowsInRequest, ui64 maxBytesInRequest,
                                           const TString& keyFrom, const TString& keyTo,
                                           const TScanSettings& settings)
    : Path(path)
    , Columns(columns)
    , KeyColumnTypes(keyColumnTypes)
    , MaxRows(maxRowsInRequest)
    , MaxBytes(maxBytesInRequest)
    , Settings(settings)
    , Connection(driver, NYdb::TCommonClientSettings().Database(database).AuthToken(token).DiscoveryEndpoint(endpoint).DiscoveryMode(NYdb::EDiscoveryMode::Async).SslCredentials(NYdb::TSslCredentials(ssl)))
    , LastReadKey(keyFrom.empty() ? NKikimr::TSerializedCellVec::Serialize(TVector<NKikimr::TCell>(KeyColumnTypes.size())) : keyFrom)
    , LastReadKeyInclusive(false)
    , EndKey(keyTo)
    , RequestsDone(!EndKey.empty() && RangeFinished(LastReadKey, EndKey, KeyColumnTypes))
    , MaxRetries(20)
    , Retried(0)
{
    MakeRequest();
}

TScanIterator::TScanIterator(const TDriver& driver, const TString &database, const TString &token, const TString& path,
                                           const TVector<TString>& columns,
                                           const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
                                           ui64 maxRowsInRequest, ui64 maxBytesInRequest,
                                           const TString& keyFrom, const TString& keyTo,
                                           const TScanSettings& settings)
    : Path(path)
    , Columns(columns)
    , KeyColumnTypes(keyColumnTypes)
    , MaxRows(maxRowsInRequest)
    , MaxBytes(maxBytesInRequest)
    , Settings(settings)
    , Connection(driver, NYdb::TCommonClientSettings().Database(database).AuthToken(token))
    , LastReadKey(keyFrom.empty() ? NKikimr::TSerializedCellVec::Serialize(TVector<NKikimr::TCell>(KeyColumnTypes.size())) : keyFrom)
    , LastReadKeyInclusive(false)
    , EndKey(keyTo)
    , RequestsDone(!EndKey.empty() && RangeFinished(LastReadKey, EndKey, KeyColumnTypes))
    , MaxRetries(20)
    , Retried(0)
{
    MakeRequest();
}

TString TScanIterator::GetBlocks() {
    while (Blocks.empty()) {
        if (RequestsDone)
            return TString();

        WaitResult();
        MakeRequest();
    }

    TString block = Blocks.front();
    Blocks.pop_front();
    return block;
}

void TScanIterator::MakeRequest() {
    if (RequestsDone)
        return;

    // TODO: check that previous request is finished

    NextResult = Connection.Scan(Path, Columns, MaxRows, MaxBytes, LastReadKey, !LastReadKeyInclusive, Settings);
}

bool IsRetriable(EStatus status) {
    switch (status) {
    case EStatus::BAD_REQUEST:
    case EStatus::SCHEME_ERROR:
    case EStatus::UNAUTHORIZED:
    case EStatus::NOT_FOUND:
        return false;
    default:
        return true;
    }
}

void TScanIterator::WaitResult() {
    while (true) {
        TScanResult res = NextResult.ExtractValueSync();
        if (res.GetStatus() == EStatus::SUCCESS) {
            size_t bc = res.GetBuffersCount();

            for (size_t i = 0; i < bc; ++i) {
                TString block = res.GetBuffer(i);
                if (block.size() > 0)
                    Blocks.push_back(block);
            }

            RequestsDone = res.IsEos();
            std::tie(LastReadKey, LastReadKeyInclusive) = res.GetLastKey();

            if (!EndKey.empty()) {
                RequestsDone = RangeFinished(LastReadKey, EndKey, KeyColumnTypes);
            }

            // Reset backoffs after a successful attempt
            Retried = 0;

            return;
        }

        if (!IsRetriable(res.GetStatus()) || Retried > MaxRetries) {
            ythrow yexception() << res.GetStatus() << ": " << res.GetIssues().ToString();
        }

        TDuration delay = TDuration::MilliSeconds(50 * (1 << Retried));
        Sleep(Min(TDuration::Seconds(3), delay));
        ++Retried;

        MakeRequest();
    }
}


///////////////////////////////////////////////////////////////////////////
class TGetShardLocationsResult::TResultImpl {
public:
    TResultImpl(Ydb::ClickhouseInternal::GetShardLocationsResult&& result) {
        for (const auto& ti : result.tablets()) {
            ShardLocations[ti.tablet_id()] = { ti.host(), ti.port() };
        }
    }

    THashMap<ui64, std::pair<TString, ui16>> ShardLocations;
};

TGetShardLocationsResult::TGetShardLocationsResult(TResultImpl* impl, TStatus&& status)
    : TStatus(std::move(status))
    , ResultImpl(impl)
{}

TGetShardLocationsResult::TGetShardLocationsResult(TGetShardLocationsResult&& other)
    : TStatus(std::move(other))
    , ResultImpl(std::move(other.ResultImpl))
{}

TGetShardLocationsResult& TGetShardLocationsResult::operator = (TGetShardLocationsResult&& other) {
    (TStatus&)*this = std::move(other);
    ResultImpl = std::move(other.ResultImpl);
    return *this;
}

TGetShardLocationsResult::~TGetShardLocationsResult() {
}


std::pair<TString, ui16> TGetShardLocationsResult::GetLocation(ui64 tabletId) const {
    const auto* info = ResultImpl->ShardLocations.FindPtr(tabletId);
    return info ? *info : std::pair<TString, ui16>();
}

class TDescribeTableResult::TResultImpl {
public:
    TResultImpl(Ydb::ClickhouseInternal::DescribeTableResult&& result)
        : Result(std::move(result))
    {}

    Ydb::ClickhouseInternal::DescribeTableResult Result;
};


TDescribeTableResult::TDescribeTableResult(TResultImpl* impl, TStatus&& status)
    : TStatus(std::move(status))
    , ResultImpl(impl)
{}

TDescribeTableResult::TDescribeTableResult(TDescribeTableResult&& other)
    : TStatus(std::move(other))
    , ResultImpl(std::move(other.ResultImpl))
{}

TDescribeTableResult& TDescribeTableResult::operator = (TDescribeTableResult&& other) {
    (TStatus&)*this = std::move(other);
    ResultImpl = std::move(other.ResultImpl);
    return *this;
}

TDescribeTableResult::~TDescribeTableResult() {
}

const Ydb::ClickhouseInternal::DescribeTableResult& TDescribeTableResult::GetDescription() const {
    return ResultImpl->Result;
}

////////////////////////////////////////////////////////////////////////////////

class TMetaClient::TImpl : public TClientImplCommon<TMetaClient::TImpl> {
    friend class TSnapshotHandleLifecycle;

public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    TAsyncGetShardLocationsResult GetShardLocations(
            const TVector<ui64>& tabletIds,
            const TGetShardLocationsSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ClickhouseInternal::GetShardLocationsRequest>(settings);
        for (ui64 id : tabletIds) {
            request.add_tablet_ids(id);
        }

        auto promise = NThreading::NewPromise<TGetShardLocationsResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::ClickhouseInternal::GetShardLocationsResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TGetShardLocationsResult val(new TGetShardLocationsResult::TResultImpl(std::move(result)),
                   TStatus(std::move(status)));

                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::ClickhouseInternal::V1::ClickhouseInternalService, Ydb::ClickhouseInternal::GetShardLocationsRequest, Ydb::ClickhouseInternal::GetShardLocationsResponse>(
                    std::move(request),
                    extractor,
                    &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub::AsyncGetShardLocations,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncDescribeTableResult GetTableDescription(
            const TString& path,
            bool includePartitionsInfo,
            const TGetShardLocationsSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ClickhouseInternal::DescribeTableRequest>(settings);
        request.set_path(path);
        request.set_include_partitions_info(includePartitionsInfo);

        auto promise = NThreading::NewPromise<TDescribeTableResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::ClickhouseInternal::DescribeTableResult result;
                if (any) {
                    any->UnpackTo(&result);
                }

                TDescribeTableResult val(new TDescribeTableResult::TResultImpl(std::move(result)),
                   TStatus(std::move(status)));

                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::ClickhouseInternal::V1::ClickhouseInternalService, Ydb::ClickhouseInternal::DescribeTableRequest, Ydb::ClickhouseInternal::DescribeTableResponse>(
                    std::move(request),
                    extractor,
                    &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub::AsyncDescribeTable,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    template<class TProtoResult, class TResultWrapper>
    auto MakeResultExtractor(NThreading::TPromise<TResultWrapper> promise) {
        return [promise = std::move(promise)]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::unique_ptr<TProtoResult> result;
                if (any) {
                    result.reset(new TProtoResult);
                    any->UnpackTo(result.get());
                }

                promise.SetValue(
                    TResultWrapper(
                        TStatus(std::move(status)),
                        std::move(result)));
            };
    }

    TAsyncCreateSnapshotResult CreateSnapshot(
            const TVector<TString>& tables,
            const TSnapshotSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ClickhouseInternal::CreateSnapshotRequest>(settings);
        for (const auto& table : tables) {
            request.add_path(table);
        }
        if (settings.IgnoreSystemViews_) {
            request.set_ignore_system_views(true);
        }

        auto promise = NThreading::NewPromise<TCreateSnapshotResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            Ydb::ClickhouseInternal::CreateSnapshotResult,
            TCreateSnapshotResult>(std::move(promise));

        Connections_->RunDeferred<
            Ydb::ClickhouseInternal::V1::ClickhouseInternalService,
            Ydb::ClickhouseInternal::CreateSnapshotRequest,
            Ydb::ClickhouseInternal::CreateSnapshotResponse>(
                std::move(request),
                std::move(extractor),
                &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub::AsyncCreateSnapshot,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncRefreshSnapshotResult RefreshSnapshot(
            const TVector<TString>& tables,
            TString snapshotId,
            const TSnapshotSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ClickhouseInternal::RefreshSnapshotRequest>(settings);
        for (const auto& table : tables) {
            request.add_path(table);
        }
        request.set_snapshot_id(snapshotId);
        if (settings.IgnoreSystemViews_) {
            request.set_ignore_system_views(true);
        }

        auto promise = NThreading::NewPromise<TRefreshSnapshotResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            Ydb::ClickhouseInternal::RefreshSnapshotResult,
            TRefreshSnapshotResult>(std::move(promise));

        Connections_->RunDeferred<
            Ydb::ClickhouseInternal::V1::ClickhouseInternalService,
            Ydb::ClickhouseInternal::RefreshSnapshotRequest,
            Ydb::ClickhouseInternal::RefreshSnapshotResponse>(
                std::move(request),
                std::move(extractor),
                &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub::AsyncRefreshSnapshot,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

    TAsyncDiscardSnapshotResult DiscardSnapshot(
            const TVector<TString>& tables,
            TString snapshotId,
            const TSnapshotSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::ClickhouseInternal::DiscardSnapshotRequest>(settings);
        for (const auto& table : tables) {
            request.add_path(table);
        }
        request.set_snapshot_id(snapshotId);
        if (settings.IgnoreSystemViews_) {
            request.set_ignore_system_views(true);
        }

        auto promise = NThreading::NewPromise<TDiscardSnapshotResult>();
        auto future = promise.GetFuture();

        auto extractor = MakeResultExtractor<
            Ydb::ClickhouseInternal::DiscardSnapshotResult,
            TDiscardSnapshotResult>(std::move(promise));

        Connections_->RunDeferred<
            Ydb::ClickhouseInternal::V1::ClickhouseInternalService,
            Ydb::ClickhouseInternal::DiscardSnapshotRequest,
            Ydb::ClickhouseInternal::DiscardSnapshotResponse>(
                std::move(request),
                std::move(extractor),
                &Ydb::ClickhouseInternal::V1::ClickhouseInternalService::Stub::AsyncDiscardSnapshot,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return future;
    }

private:
    TGRpcConnectionsImpl* Connections() const {
        return Connections_.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotHandleLifecycle : public TThrRefBase {
    friend class TSnapshotHandle::TImpl;

    static constexpr size_t MaxRefreshRetries = 10;
    static constexpr size_t MaxDiscardRetries = 5;

public:
    using TPtr = TIntrusivePtr<TSnapshotHandleLifecycle>;

public:
    TSnapshotHandleLifecycle(
            std::shared_ptr<TMetaClient::TImpl> driver,
            const TVector<TString>& tables,
            const TSnapshotSettings& settings)
        : Driver(std::move(driver))
        , Tables(tables)
        , Settings(settings)
    { }

    NThreading::TFuture<TCreateSnapshotHandleResult> Start() {
        auto promise = NThreading::NewPromise<TCreateSnapshotHandleResult>();
        auto future = promise.GetFuture();

        auto handler = [self = TPtr(this), promise = std::move(promise)]
            (TAsyncCreateSnapshotResult createFuture) mutable {
                auto createResult = createFuture.ExtractValue();
                if (!createResult.IsSuccess()) {
                    // Convert result to an empty handle with error
                    promise.SetValue(
                        TCreateSnapshotHandleResult(
                            std::move(createResult),
                            TSnapshotHandle()));
                    return;
                }

                self->OnCreated(createResult.GetResult());

                promise.SetValue(
                    TCreateSnapshotHandleResult(
                        std::move(createResult),
                        TSnapshotHandle(self.Get())));
            };

        Driver->CreateSnapshot(Tables, Settings)
            .Subscribe(std::move(handler));

        return future;
    }

private:
    bool IsAlive() const {
        return AtomicGet(IsAliveFlag);
    }

    void MarkAsDead() {
        AtomicSet(IsAliveFlag, 0);
    }

    void OnCreated(const Ydb::ClickhouseInternal::CreateSnapshotResult& result) {
        SnapshotId = result.snapshot_id();
        SnapshotTimeout = TDuration::MilliSeconds(result.timeout_ms());
        RefreshContext = Driver->Connections()->CreateContext();
        if (!RefreshContext) {
            MarkAsDead();
            return;
        }
        OnRefreshed();
    }

    void ScheduleRefresh() {
        Driver->Connections()->ScheduleCallback(
            SnapshotTimeout / 2,
            [self = TPtr(this)] (bool ok) mutable {
                self->OnRefreshTimer(ok);
            },
            RefreshContext);
    }

    void OnRefreshTimer(bool ok) {
        if (!ok) {
            MarkAsDead();
            return; // snapshot discarded or driver stopped
        }

        SendRefreshRequest();
    }

    void SendRefreshRequest() {
        auto handler = [self = TPtr(this)]
            (TAsyncRefreshSnapshotResult refreshFuture) mutable {
                auto refreshResult = refreshFuture.ExtractValue();
                if (refreshResult.IsSuccess()) {
                    self->OnRefreshSuccess(refreshResult.GetResult());
                } else {
                    self->OnRefreshFailure(std::move(refreshResult));
                }
            };

        Driver->RefreshSnapshot(Tables, SnapshotId, Settings)
            .Subscribe(std::move(handler));
    }

    void OnRefreshSuccess(const Ydb::ClickhouseInternal::RefreshSnapshotResult& result) {
        Y_UNUSED(result);
        OnRefreshed();
    }

    void OnRefreshed() {
        ExpectedDeadline = TInstant::Now() + SnapshotTimeout;
        RefreshRetries = 0;
        ScheduleRefresh();
    }

    void OnRefreshFailure(TStatus&& status) {
        switch (status.GetStatus()) {
            case EStatus::UNAVAILABLE:
            case EStatus::OVERLOADED:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED:
            case EStatus::UNDETERMINED:
            case EStatus::TRANSPORT_UNAVAILABLE:
            case EStatus::TIMEOUT:
                if (ScheduleRefreshRetry()) {
                    return;
                }

                // Request cannot be retried
                break;

            default:
                // Other errors are not retriable
                break;
        }

        // TODO: maybe log the error, but how?
        MarkAsDead();
    }

    bool ScheduleRefreshRetry() {
        if (TInstant::Now() > ExpectedDeadline) {
            // Snapshot probably expired, stop retrying
            return false;
        }

        if (++RefreshRetries > MaxRefreshRetries) {
            // Stop trying to perform any retries
            return false;
        }

        TDuration delay = Max(
            TDuration::MilliSeconds(50 * (1 << (RefreshRetries - 1))),
            TDuration::Seconds(3));

        Driver->Connections()->ScheduleCallback(
            delay,
            [self = TPtr(this)] (bool ok) mutable {
                self->OnRefreshTimer(ok);
            },
            RefreshContext);

        return true;
    }

    void Discard() {
        // N.B. user cannot read it anymore, but just be nice
        MarkAsDead();

        // Stop trying to refresh the snapshot
        if (RefreshContext) {
            // FIXME: we want to free this context as soon as refresh cycle stops working
            RefreshContext->Cancel();
        }

        // Discard snapshot so it stops consuming resources
        // N.B. Discard is called exactly once on the last handle destruction
        SendDiscardRequest();
    }

    void SendDiscardRequest() {
        auto handler = [self = TPtr(this)]
            (TAsyncDiscardSnapshotResult discardFuture) mutable {
                auto discardResult = discardFuture.ExtractValue();
                if (discardResult.IsSuccess()) {
                    self->OnDiscardSuccess(discardResult.GetResult());
                } else {
                    self->OnDiscardFailure(std::move(discardResult));
                }
            };

        Driver->DiscardSnapshot(Tables, SnapshotId, Settings)
            .Subscribe(std::move(handler));
    }

    void OnDiscardSuccess(const Ydb::ClickhouseInternal::DiscardSnapshotResult& result) {
        Y_UNUSED(result);
    }

    void OnDiscardFailure(TStatus&& status) {
        switch (status.GetStatus()) {
            case EStatus::UNAVAILABLE:
            case EStatus::OVERLOADED:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED:
            case EStatus::UNDETERMINED:
            case EStatus::TRANSPORT_UNAVAILABLE:
                if (ScheduleDiscardRetry()) {
                    return;
                }

                // Request cannot be retried
                break;

            default:
                // Other errors are not retriable
                break;
        }

        // TODO: maybe log the error, but how?
    }

    bool ScheduleDiscardRetry() {
        if (++DiscardRetries > MaxDiscardRetries) {
            // Stop trying to perform any retries
            return false;
        }

        TDuration delay = Max(
            TDuration::MilliSeconds(50 * (1 << (DiscardRetries - 1))),
            TDuration::Seconds(3));

        // N.B. not using cancellation token here
        Driver->Connections()->ScheduleCallback(
            delay,
            [self = TPtr(this)] (bool ok) mutable {
                self->OnDiscardRetryTimer(ok);
            });

        return true;
    }

    void OnDiscardRetryTimer(bool ok) {
        if (!ok) {
            return; // driver stopped
        }

        SendDiscardRequest();
    }

private:
    const std::shared_ptr<TMetaClient::TImpl> Driver;
    const TVector<TString> Tables;
    const TSnapshotSettings Settings;

private:
    // Initialized on snapshot creation, constant afterwards
    TString SnapshotId;
    TDuration SnapshotTimeout;
    IQueueClientContextPtr RefreshContext;

private:
    // Set to 0 if snapshot cannot be refreshed
    TAtomic IsAliveFlag = 1;

private:
    // Refresh loop state
    TInstant ExpectedDeadline;
    size_t RefreshRetries = 0;

private:
    // Discard loop state
    size_t DiscardRetries = 0;
};

class TSnapshotHandle::TImpl {
public:
    TImpl(TSnapshotHandleLifecycle* lifecycle)
        : Lifecycle(lifecycle)
    { }

    ~TImpl() {
        Lifecycle->Discard();
    }

    TString GetSnapshotId() const {
        return Lifecycle->SnapshotId;
    }

    bool IsAlive() const {
        return Lifecycle->IsAlive();
    }

private:
    TSnapshotHandleLifecycle::TPtr Lifecycle;
};

TSnapshotHandle::TSnapshotHandle(TSnapshotHandleLifecycle* lifecycle)
    : Impl_(std::make_shared<TImpl>(lifecycle))
{ }

TSnapshotHandle::~TSnapshotHandle() {
    // nothing
}

TString TSnapshotHandle::GetSnapshotId() const {
    Y_ABORT_UNLESS(Impl_, "Empty handle");
    return Impl_->GetSnapshotId();
}

bool TSnapshotHandle::IsAlive() const {
    return Impl_ && Impl_->IsAlive();
}

////////////////////////////////////////////////////////////////////////////////

TMetaClient::TMetaClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TAsyncGetShardLocationsResult TMetaClient::GetShardLocations(
        const TVector<ui64> tabletIds,
        const TGetShardLocationsSettings& settings)
{
    return Impl_->GetShardLocations(tabletIds, settings);
}

TAsyncDescribeTableResult TMetaClient::GetTableDescription(
        const TString& path,
        bool includePartitionsInfo,
        const TGetShardLocationsSettings& settings)
{
    return Impl_->GetTableDescription(path, includePartitionsInfo, settings);
}

TAsyncCreateSnapshotResult TMetaClient::CreateSnapshot(
        const TVector<TString>& tables,
        const TSnapshotSettings& settings)
{
    return Impl_->CreateSnapshot(tables, settings);
}

TAsyncRefreshSnapshotResult TMetaClient::RefreshSnapshot(
        const TVector<TString>& tables,
        const TString& snapshotId,
        const TSnapshotSettings& settings)
{
    return Impl_->RefreshSnapshot(tables, snapshotId, settings);
}

TAsyncDiscardSnapshotResult TMetaClient::DiscardSnapshot(
        const TVector<TString>& tables,
        const TString& snapshotId,
        const TSnapshotSettings& settings)
{
    return Impl_->DiscardSnapshot(tables, snapshotId, settings);
}

TAsyncCreateSnapshotHandleResult TMetaClient::CreateSnapshotHandle(
        const TVector<TString>& tables,
        const TSnapshotSettings& settings)
{
    return MakeIntrusive<TSnapshotHandleLifecycle>(Impl_, tables, settings)->Start();
}

} // namespace NClickhouseInternal
} // namespace NYdb
