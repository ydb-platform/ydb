#pragma once

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>
#include <ydb/library/yql/providers/yt/gateway/lib/transaction_cache.h>
#include <ydb/library/yql/utils/threading/async_queue.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <library/cpp/threading/future/future.h>

#include <util/folder/path.h>
#include <util/thread/pool.h>

namespace NYql {

template <class TDerived>
class TQueryCacheItemBase {
public:
    TQueryCacheItemBase(EQueryCacheMode mode)
        : Mode(mode)
    {
    }

    // returns true if cache was used
    bool Lookup(const TAsyncQueue::TPtr& queue) {
        return LookupAsync(queue).GetValueSync();
    }

    [[nodiscard]]
    NThreading::TFuture<bool> LookupAsync(const TAsyncQueue::TPtr& queue) {
        if (Mode == EQueryCacheMode::Disable || Mode == EQueryCacheMode::Refresh) {
            return NThreading::MakeFuture<bool>(false);
        }
        if (Found) {
            return *Found;
        }

        Found = static_cast<TDerived*>(this)->LookupImpl(queue);
        return *Found;

    }

    void Store() {
        if (Mode == EQueryCacheMode::Disable || Mode == EQueryCacheMode::Readonly) {
            return;
        }
        static_cast<TDerived*>(this)->StoreImpl();
    }

    bool Hit() const {
        return Found && Found->GetValueSync();
    }

protected:
    const EQueryCacheMode Mode;

private:
    TMaybe<NThreading::TFuture<bool>> Found;
};


class TFsQueryCacheItem: public TQueryCacheItemBase<TFsQueryCacheItem> {
public:
    TFsQueryCacheItem(const TYtSettings& config, const TString& cluster, const TString& tmpDir, const TString& hash,
        const TString& outputTablePath);
    TFsQueryCacheItem(const TYtSettings& config, const TString& cluster, const TString& tmpDir, const TString& hash,
        const TVector<TString>& outputTablePaths);

    // returns true if cache was used
    NThreading::TFuture<bool> LookupImpl(const TAsyncQueue::TPtr& queue);
    void StoreImpl();

private:
    const TVector<TString> OutputTablePaths;
    TVector<TFsPath> CachedPaths;
};

class TYtQueryCacheItem: public TQueryCacheItemBase<TYtQueryCacheItem> {
public:
    TYtQueryCacheItem(EQueryCacheMode mode, const TTransactionCache::TEntry::TPtr& entry, const TString& hash,
        const TVector<TString>& dstTables, const TVector<NYT::TNode>& dstSpecs, const TString& userName,
        const TString& tmpFolder, const NYT::TNode& mergeSpec,
        const NYT::TNode& tableAttrs, ui64 chunkLimit, bool useExpirationTimeout, bool useMultiSet,
        const std::pair<TString, TString>& logCtx);

    // returns true if cache was used
    NThreading::TFuture<bool> LookupImpl(const TAsyncQueue::TPtr& queue);
    void StoreImpl();

private:
    static TString GetCachePath(const TString& userName, const TString& tmpFolder);
    TString MakeCachedPath(const TString& hash);
    static void PrepareParentFolders(const TString& path, NYT::IClientBasePtr tx);
    void SetTableAttrs(const NYT::TNode& spec, const TString& cachedPath);

private:
    const TTransactionCache::TEntry::TPtr Entry;
    const TVector<TString> DstTables;
    const TVector<NYT::TNode> DstSpecs;
    const TString CachePath;
    TVector<TString> CachedPaths;
    const ui64 ChunkLimit;
    const bool UseExpirationTimeout;
    const bool UseMultiSet;
    const std::pair<TString, TString> LogCtx;
    TMap<TString, ui32> SortedCachedPaths;
    const NYT::TNode MergeSpec;
    NYT::TNode TableAttrs;
    NYT::ITransactionPtr LockTx;
};

} // NYql
