#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/grpc/draft/ydb_clickhouse_internal_v1.pb.h>

// TODO: Bad dependency???
#include <ydb/core/scheme/scheme_type_order.h>

#include <util/generic/deque.h>

namespace NYdb {
namespace NClickhouseInternal {

class TScanResult : public TStatus {
    friend class TScanClient;
    class TResultImpl;

private:
    TScanResult(TResultImpl* impl, TStatus&& status);

public:
    ~TScanResult();
    TScanResult(TScanResult&& other);
    TScanResult& operator = (TScanResult&& other);

    size_t GetBuffersCount() const;
    TString GetBuffer(size_t idx) const;
    bool IsEos() const;
    std::pair<TString, bool>GetLastKey() const;

private:
    std::unique_ptr<TResultImpl> ResultImpl;
};

using TAsyncScanResult = NThreading::TFuture<TScanResult>;


struct TScanSettings : public TOperationRequestSettings<TScanSettings> {
    FLUENT_SETTING(TString, SnapshotId);
    FLUENT_SETTING(TString, Endpoint);
};

class TScanClient {
    class TImpl;

public:
    TScanClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncScanResult Scan(
            const TString& path, const TVector<TString>& columns,
            ui64 maxRows, ui64 maxBytes,
            const TString& fromKey, bool fromKeyInclusive,
            const TScanSettings& settings = TScanSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

// Makes table range scan by doing Scan requests one by one and keeping track of
// the last returned key
class TScanIterator {
public:
    TScanIterator(const TDriver& driver, const TString &database, const TString &endpoint, const TString& token, bool ssl, const TString& path, const TVector<TString>& columns,
                         const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
                         ui64 maxRowsInRequest, ui64 maxBytesInRequest,
                         const TString& keyFrom = TString(), const TString& keyTo = TString(),
                         const TScanSettings& settings = TScanSettings());
    TScanIterator(const TDriver& driver, const TString &database, const TString& token, const TString& path, const TVector<TString>& columns,
                         const TVector<NKikimr::NScheme::TTypeInfo>& keyColumnTypes,
                         ui64 maxRowsInRequest, ui64 maxBytesInRequest,
                         const TString& keyFrom = TString(), const TString& keyTo = TString(),
                         const TScanSettings& settings = TScanSettings());
    TString GetBlocks();

private:
    void MakeRequest();
    void WaitResult();

private:
    const TString Path;
    const TVector<TString> Columns;
    const TVector<NKikimr::NScheme::TTypeInfo> KeyColumnTypes;
    const ui64 MaxRows;
    const ui64 MaxBytes;
    const TScanSettings Settings;
    TScanClient Connection;

    TDeque<TString> Blocks;
    TAsyncScanResult NextResult;
    TString LastReadKey;
    bool LastReadKeyInclusive;
    TString EndKey;
    bool RequestsDone;
    int MaxRetries;
    int Retried;
};


////////////////////////////////////////////////////////////////////////////////

class TGetShardLocationsResult : public TStatus {
    friend class TMetaClient;
    class TResultImpl;

private:
    TGetShardLocationsResult(TResultImpl* impl, TStatus&& status);

public:
    ~TGetShardLocationsResult();
    TGetShardLocationsResult(TGetShardLocationsResult&& other);
    TGetShardLocationsResult& operator = (TGetShardLocationsResult&& other);

    std::pair<TString, ui16> GetLocation(ui64 tabletId) const;

private:
    std::unique_ptr<TResultImpl> ResultImpl;
};

class TDescribeTableResult : public TStatus {
    friend class TMetaClient;
    class TResultImpl;

private:
    TDescribeTableResult(TResultImpl* impl, TStatus&& status);

public:
    ~TDescribeTableResult();
    TDescribeTableResult(TDescribeTableResult&& other);
    TDescribeTableResult& operator = (TDescribeTableResult&& other);

    const Ydb::ClickhouseInternal::DescribeTableResult& GetDescription() const;

private:
    std::unique_ptr<TResultImpl> ResultImpl;
};

template<class TProtoResult>
class TProtoResultWrapper : public TStatus {
    friend class TMetaClient;

private:
    TProtoResultWrapper(
            TStatus&& status,
            std::unique_ptr<TProtoResult> result)
        : TStatus(std::move(status))
        , Result(std::move(result))
    { }

public:
    const TProtoResult& GetResult() const {
        Y_ABORT_UNLESS(Result, "Uninitialized result");
        return *Result;
    }

private:
    std::unique_ptr<TProtoResult> Result;
};

using TCreateSnapshotResult = TProtoResultWrapper<Ydb::ClickhouseInternal::CreateSnapshotResult>;
using TRefreshSnapshotResult = TProtoResultWrapper<Ydb::ClickhouseInternal::RefreshSnapshotResult>;
using TDiscardSnapshotResult = TProtoResultWrapper<Ydb::ClickhouseInternal::DiscardSnapshotResult>;

/**
 * Internal class that manages snapshot lifecycle (automatic refresh and discard)
 */
class TSnapshotHandleLifecycle;

/**
 * Handle to an existing snapshot
 *
 * Snapshot is automatically discarded when the last reference is dropped
 */
class TSnapshotHandle {
    friend class TSnapshotHandleLifecycle;

    class TImpl;

public:
    TSnapshotHandle() = default;
    ~TSnapshotHandle();

    explicit operator bool() const {
        return IsAlive();
    }

    TString GetSnapshotId() const;

    bool IsAlive() const;

private:
    explicit TSnapshotHandle(TSnapshotHandleLifecycle* lifecycle);

private:
    std::shared_ptr<TImpl> Impl_;
};

class TCreateSnapshotHandleResult : public TStatus {
    friend class TSnapshotHandleLifecycle;

private:
    TCreateSnapshotHandleResult(TStatus&& status, TSnapshotHandle handle)
        : TStatus(std::move(status))
        , Handle(std::move(handle))
    { }

public:
    const TSnapshotHandle& GetResult() const {
        CheckStatusOk("TCreateSnapshotHandleResult::GetResult");
        return Handle;
    }

    TSnapshotHandle&& ExtractResult() {
        CheckStatusOk("TCreateSnapshotHandleResult::ExtractResult");
        return std::move(Handle);
    }

private:
    TSnapshotHandle Handle;
};

using TAsyncGetShardLocationsResult = NThreading::TFuture<TGetShardLocationsResult>;
using TAsyncDescribeTableResult = NThreading::TFuture<TDescribeTableResult>;
using TAsyncCreateSnapshotResult = NThreading::TFuture<TCreateSnapshotResult>;
using TAsyncRefreshSnapshotResult = NThreading::TFuture<TRefreshSnapshotResult>;
using TAsyncDiscardSnapshotResult = NThreading::TFuture<TDiscardSnapshotResult>;
using TAsyncCreateSnapshotHandleResult = NThreading::TFuture<TCreateSnapshotHandleResult>;


struct TGetShardLocationsSettings : public TOperationRequestSettings<TGetShardLocationsSettings> {};

struct TSnapshotSettings : public TOperationRequestSettings<TSnapshotSettings> {
    FLUENT_SETTING_FLAG(IgnoreSystemViews);
};

class TMetaClient {
    friend class TSnapshotHandleLifecycle;

    class TImpl;

public:
    TMetaClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncGetShardLocationsResult GetShardLocations(
            const TVector<ui64> tabletIds,
            const TGetShardLocationsSettings& settings = TGetShardLocationsSettings());

    TAsyncDescribeTableResult GetTableDescription(
            const TString& path,
            bool includePartitionsInfo,
            const TGetShardLocationsSettings& settings = TGetShardLocationsSettings());

    TAsyncCreateSnapshotResult CreateSnapshot(
            const TVector<TString>& tables,
            const TSnapshotSettings& settings = TSnapshotSettings());

    TAsyncRefreshSnapshotResult RefreshSnapshot(
            const TVector<TString>& tables,
            const TString& snapshotId,
            const TSnapshotSettings& settings = TSnapshotSettings());

    TAsyncDiscardSnapshotResult DiscardSnapshot(
            const TVector<TString>& tables,
            const TString& snapshotId,
            const TSnapshotSettings& settings = TSnapshotSettings());

    TAsyncCreateSnapshotHandleResult CreateSnapshotHandle(
            const TVector<TString>& tables,
            const TSnapshotSettings& settings = TSnapshotSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NClickhouseInternal
} // namespace NYdb
