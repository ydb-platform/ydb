#include "query_cache.h"
#include "yt_helpers.h"

#include <ydb/library/yql/providers/yt/lib/hash/yql_hash_builder.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <util/generic/algorithm.h>
#include <util/string/hex.h>
#include <util/system/fs.h>
#include <util/system/error.h>

#include <errno.h>

namespace NYql {

using namespace NYT;

namespace {
    bool IsRace(const NYT::TYtError& e) {
        if (e.ContainsErrorCode(NClusterErrorCodes::NTransactionClient::NoSuchTransaction)) {
            return false;
        }

        return e.ContainsErrorCode(NClusterErrorCodes::NYTree::ResolveError) ||
            e.ContainsErrorCode(NClusterErrorCodes::NCypressClient::ConcurrentTransactionLockConflict);
    }
}

TFsQueryCacheItem::TFsQueryCacheItem(const TYtSettings& config, const TString& cluster, const TString& tmpDir,
    const TString& hash, const TString& outputTablePath)
    : TQueryCacheItemBase<TFsQueryCacheItem>(hash.empty() ? EQueryCacheMode::Disable : config.QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable))
    , OutputTablePaths(1, outputTablePath)
    , CachedPaths(1, TFsPath(tmpDir) / "query_cache" / cluster / (HexEncode(hash) + ".tmp"))
{
}

TFsQueryCacheItem::TFsQueryCacheItem(const TYtSettings& config, const TString& cluster, const TString& tmpDir,
    const TString& hash, const TVector<TString>& outputTablePaths)
    : TQueryCacheItemBase<TFsQueryCacheItem>(hash.empty() ? EQueryCacheMode::Disable : config.QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable))
    , OutputTablePaths(outputTablePaths)
{
    for (size_t i = 0; i < outputTablePaths.size(); ++i) {
        auto outputHash = (THashBuilder() << hash << i).Finish();
        CachedPaths.push_back(TFsPath(tmpDir) / "query_cache" / cluster / (HexEncode(outputHash) + ".tmp"));
    }
}

NThreading::TFuture<bool> TFsQueryCacheItem::LookupImpl(const TAsyncQueue::TPtr& /*queue*/) {
    for (auto cachePath: CachedPaths) {
        Cerr << "Check path: " << cachePath << "\n";
        if (!cachePath.Exists()) {
            return NThreading::MakeFuture<bool>(false);
        }
    }

    for (size_t i = 0; i < CachedPaths.size(); ++i) {
        Cerr << "Copy from: " << CachedPaths.at(i) << " to " << OutputTablePaths.at(i) << "\n";
        NFs::HardLinkOrCopy(CachedPaths.at(i), OutputTablePaths.at(i));
        NFs::HardLinkOrCopy(CachedPaths.at(i).GetPath() + ".attr", OutputTablePaths.at(i) + ".attr");
    }

    return NThreading::MakeFuture<bool>(true);
}

void TFsQueryCacheItem::StoreImpl() {
    for (size_t i = 0; i < CachedPaths.size(); ++i) {
        Cerr << "Make dir: " << CachedPaths.at(i).Parent() << "\n";
        CachedPaths.at(i).Parent().MkDirs();
        if (CachedPaths.at(i).Exists()) {
            if (Mode != EQueryCacheMode::Refresh) {
                continue;
            }

            Cerr << "Remove: " << CachedPaths.at(i) << "\n";
            if (!NFs::Remove(CachedPaths.at(i)) && LastSystemError() != ENOENT) {
                ythrow TSystemError();
            }

            if (!NFs::Remove(CachedPaths.at(i).GetPath() + ".attr") && LastSystemError() != ENOENT) {
                ythrow TSystemError();
            }
        }

        Cerr << "Copy from: " << OutputTablePaths.at(i) << " to " << CachedPaths.at(i) << "\n";
        if (!NFs::HardLink(OutputTablePaths.at(i), CachedPaths.at(i))) {
            if (LastSystemError() != EEXIST || Mode != EQueryCacheMode::Normal) {
                ythrow TSystemError();
            }
        }

        if (!NFs::HardLink(OutputTablePaths.at(i) + ".attr", CachedPaths.at(i).GetPath() + ".attr")) {
            if (LastSystemError() != EEXIST || Mode != EQueryCacheMode::Normal) {
                ythrow TSystemError();
            }
        }
    }
}

TYtQueryCacheItem::TYtQueryCacheItem(EQueryCacheMode mode, const TTransactionCache::TEntry::TPtr& entry, const TString& hash,
    const TVector<TString>& dstTables, const TVector<NYT::TNode>& dstSpecs,
    const TString& userName, const TString& tmpFolder, const NYT::TNode& mergeSpec,
    const NYT::TNode& tableAttrs, ui64 chunkLimit, bool useExpirationTimeout, bool useMultiSet,
    const std::pair<TString, TString>& logCtx)
    : TQueryCacheItemBase<TYtQueryCacheItem>(hash.empty() ? EQueryCacheMode::Disable : mode)
    , Entry(entry)
    , DstTables(dstTables)
    , DstSpecs(dstSpecs)
    , CachePath(GetCachePath(userName, tmpFolder))
    , ChunkLimit(chunkLimit)
    , UseExpirationTimeout(useExpirationTimeout)
    , UseMultiSet(useMultiSet)
    , LogCtx(logCtx)
    , MergeSpec(mergeSpec)
    , TableAttrs(tableAttrs)
{
    if (!hash.empty()) {
        for (size_t i = 0; i < dstTables.size(); ++i) {
            auto outputHash = (THashBuilder() << hash << i).Finish();
            auto path = MakeCachedPath(outputHash);
            CachedPaths.push_back(path);
            SortedCachedPaths[path] = i;
        }
    }
}

TString TYtQueryCacheItem::GetCachePath(const TString& userName, const TString& tmpFolder) {
    auto path = tmpFolder;
    if (path.empty()) {
        path = "tmp/yql/" + userName;
    }

    if (!path.EndsWith('/')) {
        path += "/";
    }

    return path + "query_cache";
}

TString TYtQueryCacheItem::MakeCachedPath(const TString& hash) {
    YQL_ENSURE(hash.size() == 32);

    auto hex = HexEncode(hash);
    return TString::Join(CachePath, "/", hex.substr(0, 2), "/", hex.substr(2, 2), "/", hex);
}

NThreading::TFuture<bool> TYtQueryCacheItem::LookupImpl(const TAsyncQueue::TPtr& queue) {
    if (Entry->CacheTxId) {
        return NThreading::MakeFuture<bool>(false);
    }

    Entry->CreateDefaultTmpFolder();
    CreateParents(DstTables, Entry->CacheTx);

    NThreading::TFuture futureLock = NThreading::MakeFuture();
    if (Mode == EQueryCacheMode::Refresh || Mode == EQueryCacheMode::Normal) {
        CreateParents(CachedPaths, Entry->CacheTx);
        // get a lock for all paths before return
        if (!LockTx) {
            LockTx = Entry->Tx->StartTransaction(TStartTransactionOptions().Attributes(Entry->TransactionSpec));
        }
        for (const auto& sortedEntry : SortedCachedPaths) {
            TString cachedPath = CachedPaths[sortedEntry.second];
            futureLock = futureLock.Apply([cachedPath, lockTx = LockTx, logCtx = LogCtx](const auto& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                if (f.HasException()) {
                    return f;
                }
                auto pos = cachedPath.rfind("/");
                auto dir = cachedPath.substr(0, pos);
                auto childKey = cachedPath.substr(pos + 1) + ".lock";
                YQL_CLOG(INFO, ProviderYt) << "Wait for " << cachedPath;
                for (ui32 retriesLeft = 10; retriesLeft != 0; --retriesLeft) {
                    try {
                        return lockTx->Lock(dir, NYT::ELockMode::LM_SHARED, NYT::TLockOptions()
                            .Waitable(true).ChildKey(childKey))->GetAcquiredFuture();
                    } catch (const TErrorResponse& e) {
                        if (!IsRace(e.GetError())) {
                            throw;
                        }
                    }
                    YQL_CLOG(INFO, ProviderYt) << "Retry, retriesLeft: " << retriesLeft;
                }
                YQL_CLOG(INFO, ProviderYt) << "Wait complete";
                return NThreading::MakeFuture();
            });
        }
    }

    return futureLock.Apply([queue, cachedPaths = CachedPaths, dstTables = DstTables, entry = Entry,
                             useExpirationTimeout = UseExpirationTimeout, logCtx = LogCtx](const auto& f) {
        f.GetValue();
        return queue->Async([=]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            bool hit = true;
            try {
                for (size_t i = 0; i < cachedPaths.size(); ++i) {
                    YQL_CLOG(INFO, ProviderYt) << "Check path: " << cachedPaths.at(i);
                    entry->Tx->Copy(cachedPaths.at(i), dstTables.at(i), TCopyOptions().Force(true));
                }
            } catch (const TErrorResponse& e) {
                // Yt returns NoSuchTransaction as inner issue for ResolveError
                if (!e.IsResolveError() || e.IsNoSuchTransaction()) {
                    throw;
                }

                YQL_CLOG(INFO, ProviderYt) << "Query cache miss";
                hit = false;
            }
            if (!hit) {
                return false;
            }

            YQL_CLOG(INFO, ProviderYt) << "Query cache hit";
            if (useExpirationTimeout) {
                return true;
            }
            for (const auto& cachedPath: cachedPaths) {
                try {
                    entry->CacheTx->Set(cachedPath + "/@touched", TNode(true));
                } catch (const TErrorResponse& e) {
                    if (!IsRace(e.GetError())) {
                        throw;
                    }
                }
                try {
                    entry->CacheTx->Set(cachedPath + "/@expiration_time",
                                        TNode((Now() + entry->CacheTtl).ToStringUpToSeconds()));
                } catch (const TErrorResponse& e) {
                    if (!IsRace(e.GetError())) {
                        throw;
                    }
                }
            }
            return true;
        });
    });
}

void TYtQueryCacheItem::StoreImpl() {
    if (Entry->CacheTxId) {
        return;
    }

    CreateParents(CachedPaths, Entry->CacheTx);
    try {
        Entry->CacheTx->Set(CachePath + "/@prune_empty_map_nodes", TNode(true));
    } catch (const TErrorResponse& e) {
        if (!IsRace(e.GetError())) {
            throw;
        }
    }

    NYT::ITransactionPtr nestedReadTx;
    NYT::ITransactionPtr nestedWriteTx;

    auto handle = [&](const NYT::TYtError& e) {
        if (nestedReadTx) {
            nestedReadTx->Abort();
        }
        if (nestedWriteTx) {
            nestedWriteTx->Abort();
        }

        if (!IsRace(e) || Mode == EQueryCacheMode::Refresh) {
            throw;
        }
    };

    try {
        nestedReadTx = Entry->Tx->StartTransaction(TStartTransactionOptions().Attributes(Entry->TransactionSpec));
        nestedWriteTx = Entry->CacheTx->StartTransaction(TStartTransactionOptions().Attributes(Entry->TransactionSpec));

        TVector<NThreading::TFuture<void>> futures;
        futures.reserve(CachedPaths.size());
        for (size_t i = 0; i < CachedPaths.size(); ++i) {
            YQL_CLOG(INFO, ProviderYt) << "Store to query cache, from " << DstTables.at(i) << " into " << CachedPaths.at(i);
            if (ChunkLimit) {
                NYT::TNode attrs = nestedReadTx->Get(DstTables.at(i) + "&/@", NYT::TGetOptions().AttributeFilter(
                    NYT::TAttributeFilter()
                        .AddAttribute("chunk_count")
                        .AddAttribute("compression_codec")
                        .AddAttribute("erasure_codec")
                        .AddAttribute("optimize_for")
                        .AddAttribute("media")
                        .AddAttribute("primary_medium")
                        .AddAttribute("schema")
                ));

                auto inputChunks = attrs["chunk_count"].AsInt64();
                if ((ui64)inputChunks <= ChunkLimit) {
                   YQL_CLOG(INFO, ProviderYt) << "Use Concatenate to store cache (chunks: " <<
                       inputChunks << ", limit: " << ChunkLimit << ")";

                   attrs.AsMap().erase("chunk_count");
                   nestedWriteTx->Create(CachedPaths.at(i), NYT::NT_TABLE, TCreateOptions().Force(true).Attributes(attrs));
                   nestedWriteTx->Concatenate(
                       { NYT::TRichYPath(DstTables.at(i)).TransactionId(nestedReadTx->GetId()) },
                       NYT::TRichYPath(CachedPaths.at(i)),
                       TConcatenateOptions()
                   );
                   continue;
                } else {
                   YQL_CLOG(INFO, ProviderYt) << "Use Merge to store cache (chunks: " <<
                       inputChunks << ", limit: " << ChunkLimit << ")";
                }
            }

            auto operation = nestedWriteTx->Merge(TMergeOperationSpec()
                .AddInput(NYT::TRichYPath(DstTables.at(i))
                    .TransactionId(nestedReadTx->GetId()))
                .Output(CachedPaths.at(i))
                .Mode(EMergeMode::MM_ORDERED),
                TOperationOptions().Spec(MergeSpec).Wait(false));
            futures.push_back(operation->Watch());
        }

        auto all = NThreading::WaitExceptionOrAll(futures);
        all.GetValueSync();

        nestedReadTx->Abort();
        nestedReadTx = {};

        nestedWriteTx->Commit();
        if (LockTx) {
            LockTx->Abort();
        }

        if (UseExpirationTimeout) {
            TableAttrs["expiration_timeout"] = TNode(Entry->CacheTtl.MilliSeconds());
        } else {
            TableAttrs["expiration_time"] = TNode((Now() + Entry->CacheTtl).ToStringUpToSeconds());
        }

        for (size_t i = 0; i < CachedPaths.size(); ++i) {
            SetTableAttrs(DstSpecs[i], CachedPaths[i]);
        }
    } catch (const TErrorResponse& e) {
        handle(e.GetError());
    } catch (const NYT::TOperationFailedError& e) {
        handle(e.GetError());
    }
}

void TYtQueryCacheItem::SetTableAttrs(const NYT::TNode& spec, const TString& cachedPath) {
    NYT::TNode attrs = spec;
    NYT::MergeNodes(attrs, TableAttrs);
    if (UseMultiSet) {
        try {
            Entry->CacheTx->MultisetAttributes(cachedPath + "/@", attrs.AsMap(), NYT::TMultisetAttributesOptions());
        } catch (const TErrorResponse& e) {
            if (!IsRace(e.GetError())) {
                throw;
            }
        }
    } else {
        auto batchSet = Entry->CacheTx->CreateBatchRequest();
        TVector<NThreading::TFuture<void>> batchSetRes;
        for (auto& attr : attrs.AsMap()) {
            batchSetRes.push_back(batchSet->Set(TStringBuilder() << cachedPath << "/@" << attr.first, attr.second));
        }

        batchSet->ExecuteBatch();
        for (auto& x : batchSetRes) {
            try {
                x.GetValueSync();
            } catch (const TErrorResponse& e) {
                if (!IsRace(e.GetError())) {
                    throw;
                }
            }
        }
    }
}

} // NYql
