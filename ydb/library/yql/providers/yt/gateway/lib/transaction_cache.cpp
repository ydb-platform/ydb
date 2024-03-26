#include "transaction_cache.h"
#include "yt_helpers.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/config.h>

#include <ydb/library/yql/utils/log/log.h>

#include <util/system/guard.h>
#include <util/generic/yexception.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>
#include <util/folder/path.h>

namespace NYql {

using namespace NYT;

void TTransactionCache::TEntry::DeleteAtFinalizeUnlocked(const TString& table, bool isInternal)
{
    auto inserted = TablesToDeleteAtFinalize.insert(table);
    if (!isInternal && inserted.second) {
        if (++ExternalTempTablesCount > InflightTempTablesLimit) {
            YQL_LOG_CTX_THROW yexception() << "Too many temporary tables registered - limit is " << InflightTempTablesLimit;
        }
    }
}

bool TTransactionCache::TEntry::CancelDeleteAtFinalizeUnlocked(const TString& table, bool isInternal)
{
    auto erased = TablesToDeleteAtFinalize.erase(table);
    if (!isInternal) {
        YQL_ENSURE(erased <= ExternalTempTablesCount);
        ExternalTempTablesCount -= erased;
    }
    return erased != 0;
}


void TTransactionCache::TEntry::RemoveInternal(const TString& table) {
    bool existed;
    with_lock(Lock_) {
        existed = CancelDeleteAtFinalizeUnlocked(table, true);
    }
    if (existed) {
        DoRemove(table);
    }
}

void TTransactionCache::TEntry::DoRemove(const TString& table) {
    if (!KeepTables) {
        YQL_CLOG(INFO, ProviderYt) << "Removing " << table.Quote() << " on " << Server;
        Tx->Remove(table, TRemoveOptions().Force(true));
    }
}

void TTransactionCache::TEntry::Finalize(const TString& clusterName) {
    NYT::ITransactionPtr binarySnapshotTx;
    decltype(SnapshotTxs) snapshotTxs;
    THashSet<TString> toDelete;
    decltype(CheckpointTxs) checkpointTxs;
    decltype(WriteTxs) writeTxs;
    with_lock(Lock_) {
        binarySnapshotTx.Swap(BinarySnapshotTx);
        snapshotTxs.swap(SnapshotTxs);
        LastSnapshotTx.Drop();
        toDelete.swap(TablesToDeleteAtFinalize);
        ExternalTempTablesCount = 0;
        checkpointTxs.swap(CheckpointTxs);
        writeTxs.swap(WriteTxs);
    }

    for (auto& item: writeTxs) {
        item.second->Abort();
    }

    for (auto& item: checkpointTxs) {
        item.second->Abort();
    }

    if (binarySnapshotTx) {
        binarySnapshotTx->Abort();
    }

    for (auto& item: snapshotTxs) {
        item.second->Abort();
    }

    for (auto i : toDelete) {
        DoRemove(i);
    }

    YQL_CLOG(INFO, ProviderYt) << "Committing tx " << GetGuidAsString(Tx->GetId())  << " on " << clusterName;
    Tx->Commit();
}

TMaybe<ui64> TTransactionCache::TEntry::GetColumnarStat(NYT::TRichYPath ytPath) const {
    YQL_ENSURE(ytPath.Columns_.Defined());
    TVector<TString> columns(std::move(ytPath.Columns_->Parts_));
    ytPath.Columns_.Clear();

    auto guard = Guard(Lock_);
    if (auto p = StatisticsCache.FindPtr(NYT::NodeToCanonicalYsonString(NYT::PathToNode(ytPath), NYT::NYson::EYsonFormat::Text))) {
        ui64 sum = p->LegacyChunksDataWeight;
        for (auto& column: columns) {
            if (auto c = p->ColumnDataWeight.FindPtr(column)) {
                sum += *c;
            } else {
                return Nothing();
            }
        }
        return sum;
    }
    return Nothing();
}

void TTransactionCache::TEntry::UpdateColumnarStat(NYT::TRichYPath ytPath, ui64 size) {
    YQL_ENSURE(ytPath.Columns_.Defined());
    TVector<TString> columns(std::move(ytPath.Columns_->Parts_));
    ytPath.Columns_.Clear();

    auto guard = Guard(Lock_);
    NYT::TTableColumnarStatistics& cacheColumnStat = StatisticsCache[NYT::NodeToCanonicalYsonString(NYT::PathToNode(ytPath), NYT::NYson::EYsonFormat::Text)];
    cacheColumnStat.LegacyChunksDataWeight = size;
    for (auto& c: cacheColumnStat.ColumnDataWeight) {
        c.second = 0;
    }
    for (auto& c: columns) {
        cacheColumnStat.ColumnDataWeight[c] = 0;
    }
}

void TTransactionCache::TEntry::UpdateColumnarStat(NYT::TRichYPath ytPath, const NYT::TTableColumnarStatistics& columnStat) {
    ytPath.Columns_.Clear();
    auto guard = Guard(Lock_);
    NYT::TTableColumnarStatistics& cacheColumnStat = StatisticsCache[NYT::NodeToCanonicalYsonString(NYT::PathToNode(ytPath), NYT::NYson::EYsonFormat::Text)];
    cacheColumnStat.LegacyChunksDataWeight = columnStat.LegacyChunksDataWeight;
    cacheColumnStat.TimestampTotalWeight = columnStat.TimestampTotalWeight;
    for (auto& c: columnStat.ColumnDataWeight) {
        cacheColumnStat.ColumnDataWeight[c.first] = c.second;
    }
}

ITransactionPtr TTransactionCache::TEntry::GetSnapshotTx(bool createTx) {
    auto guard = Guard(Lock_);
    if (createTx || !LastSnapshotTx) {
        LastSnapshotTx = Tx->StartTransaction(TStartTransactionOptions().Attributes(TransactionSpec).PingAncestors(true));
        SnapshotTxs.emplace(LastSnapshotTx->GetId(), LastSnapshotTx);
    }
    return LastSnapshotTx;
}

NYT::ITransactionPtr TTransactionCache::TEntry::GetSnapshotTx(const NYT::TTransactionId& id) const {
    auto guard = Guard(Lock_);
    auto p = SnapshotTxs.FindPtr(id);
    YQL_ENSURE(p, "Unknown snapshot transaction id=" << GetGuidAsString(id));
    return *p;
}

NYT::ITransactionPtr TTransactionCache::TEntry::GetCheckpointTx(const TString& tablePath) const {
    auto guard = Guard(Lock_);
    auto p = CheckpointTxs.FindPtr(tablePath);
    YQL_ENSURE(p, "No transaction found for checkpoint " << tablePath.Quote());
    return *p;
}

NYT::ITransactionPtr TTransactionCache::TEntry::GetOrCreateCheckpointTx(const TString& tablePath) {
    auto guard = Guard(Lock_);
    auto& tx = CheckpointTxs[tablePath];
    if (!tx) {
        tx = Client->StartTransaction(TStartTransactionOptions().Attributes(TransactionSpec));
        YQL_CLOG(INFO, ProviderYt) << "Started checkpoint tx " << GetGuidAsString(tx->GetId());
    }
    return tx;
}

void TTransactionCache::TEntry::CommitCheckpointTx(const TString& tablePath) {
    auto guard = Guard(Lock_);
    auto p = CheckpointTxs.FindPtr(tablePath);
    YQL_ENSURE(p, "No transaction found for checkpoint " << tablePath.Quote());
    YQL_CLOG(INFO, ProviderYt) << "Commiting checkpoint tx " << GetGuidAsString((*p)->GetId());
    (*p)->Commit();
    CheckpointTxs.erase(tablePath);
}

NYT::TTransactionId TTransactionCache::TEntry::AllocWriteTx() {
    auto guard = Guard(Lock_);
    auto writeTx = Tx->StartTransaction(TStartTransactionOptions().Attributes(TransactionSpec));
    WriteTxs.emplace(writeTx->GetId(), writeTx);
    YQL_CLOG(INFO, ProviderYt) << "Allocated write tx " << GetGuidAsString(writeTx->GetId());
    return writeTx->GetId();

}

void TTransactionCache::TEntry::CompleteWriteTx(const NYT::TTransactionId& id, bool abort) {
    auto guard = Guard(Lock_);
    auto p = WriteTxs.FindPtr(id);
    YQL_ENSURE(p, "No transaction found: " << GetGuidAsString(id));
    YQL_CLOG(INFO, ProviderYt) << (abort ? "Aborting" : "Commiting") << " write tx " << GetGuidAsString(id);
    if (abort) {
        (*p)->Abort();
    } else {
        (*p)->Commit();
    }
    WriteTxs.erase(id);
}

std::pair<TString, NYT::TTransactionId> TTransactionCache::TEntry::GetBinarySnapshot(TString remoteTmpFolder, const TString& md5, const TString& localPath, TDuration expirationInterval) {
    if (remoteTmpFolder.StartsWith(NYT::TConfig::Get()->Prefix)) {
        remoteTmpFolder = remoteTmpFolder.substr(NYT::TConfig::Get()->Prefix.size());
    }
    TString remotePath = TFsPath(remoteTmpFolder) / md5;

    ITransactionPtr snapshotTx;
    with_lock(Lock_) {
        if (!BinarySnapshotTx) {
            BinarySnapshotTx = Client->StartTransaction(TStartTransactionOptions().Attributes(TransactionSpec));
        }
        snapshotTx = BinarySnapshotTx;
        if (auto p = BinarySnapshots.FindPtr(remotePath)) {
            return std::make_pair(*p, snapshotTx->GetId());
        }
    }
    CreateParents({remotePath}, Client);

    NYT::ILockPtr fileLock;
    ITransactionPtr lockTx;
    NYT::ILockPtr waitLock;

    for (bool uploaded = false; ;) {
        try {
            YQL_CLOG(INFO, ProviderYt) << "Taking snapshot of " << remotePath;
            fileLock = snapshotTx->Lock(remotePath, NYT::ELockMode::LM_SNAPSHOT);
            break;
        } catch (const TErrorResponse& e) {
            // Yt returns NoSuchTransaction as inner issue for ResolveError
            if (!e.IsResolveError() || e.IsNoSuchTransaction()) {
                throw;
            }
        }
        YQL_ENSURE(!uploaded, "Fail to take snapshot");
        if (!lockTx) {
            auto pos = remotePath.rfind("/");
            auto dir = remotePath.substr(0, pos);
            auto childKey = remotePath.substr(pos + 1) + ".lock";

            lockTx = Client->StartTransaction(TStartTransactionOptions().Attributes(TransactionSpec));
            YQL_CLOG(INFO, ProviderYt) << "Waiting for " << dir << '/' << childKey;
            waitLock = lockTx->Lock(dir, NYT::ELockMode::LM_SHARED, TLockOptions().Waitable(true).ChildKey(childKey));
            waitLock->GetAcquiredFuture().GetValueSync();
            // Try to take snapshot again after waiting lock. Someone else may complete uploading the file at the moment
            continue;
        }
        // Lock is already taken and file still doesn't exist
        YQL_CLOG(INFO, ProviderYt) << "Start uploading " << localPath << " to " << remotePath;
        Y_SCOPE_EXIT(localPath, remotePath) {
            YQL_CLOG(INFO, ProviderYt) << "Complete uploading " << localPath << " to " << remotePath;
        };
        auto uploadTx = Client->StartTransaction(TStartTransactionOptions().Attributes(TransactionSpec));
        try {
            auto out = uploadTx->CreateFileWriter(TRichYPath(remotePath).Executable(true), TFileWriterOptions().CreateTransaction(false));
            TIFStream in(localPath);
            TransferData(&in, out.Get());
            out->Finish();
            uploadTx->Commit();
        } catch (...) {
            uploadTx->Abort();
            throw;
        }
        // Continue with taking snapshot lock after uploading
        uploaded = true;
    }

    TString snapshotPath = TStringBuilder() << '#' << GetGuidAsString(fileLock->GetLockedNodeId());
    YQL_CLOG(INFO, ProviderYt) << "Snapshot of " << remotePath << ": " << snapshotPath;
    with_lock(Lock_) {
        BinarySnapshots[remotePath] = snapshotPath;
    }

    if (expirationInterval) {
        TString expirationTime = (Now() + expirationInterval).ToStringUpToSeconds();
        try {
            YQL_CLOG(INFO, ProviderYt) << "Prolonging expiration time for " << remotePath << " up to " << expirationTime;
            Client->Set(remotePath + "/@expiration_time", expirationTime);
        } catch (...) {
            // log and ignore the error
            YQL_CLOG(ERROR, ProviderYt) << "Error setting expiration time for " << remotePath << ": " << CurrentExceptionMessage();
        }
    }

    return std::make_pair(snapshotPath, snapshotTx->GetId());
}

void TTransactionCache::TEntry::CreateDefaultTmpFolder() {
    if (DefaultTmpFolder) {
        Client->Create(DefaultTmpFolder, NYT::NT_MAP, NYT::TCreateOptions().Recursive(true).IgnoreExisting(true));
    }
}

TTransactionCache::TTransactionCache(const TString& userName)
   : UserName_(userName)
{}

TTransactionCache::TEntry::TPtr TTransactionCache::GetEntry(const TString& server) {
    auto res = TryGetEntry(server);
    if (!res) {
        YQL_LOG_CTX_THROW yexception() << "GetEntry() failed for " << server;
    }
    return res;
}

TTransactionCache::TEntry::TPtr TTransactionCache::TryGetEntry(const TString& server) {
    auto guard = Guard(Lock_);
    auto it = TxMap_.find(server);
    if (it != TxMap_.end()) {
        return it->second;
    }
    return {};
}

TTransactionCache::TEntry::TPtr TTransactionCache::GetOrCreateEntry(const TString& server, const TString& token,
    const TMaybe<TString>& impersonationUser, const TSpecProvider& specProvider, const TYtSettings::TConstPtr& config)
{
    TEntry::TPtr createdEntry = nullptr;
    NYT::TTransactionId externalTx = config->ExternalTx.Get().GetOrElse(TGUID());
    with_lock(Lock_) {
        auto it = TxMap_.find(server);
        if (it != TxMap_.end()) {
            return it->second;
        }

        TString tmpFolder = GetTablesTmpFolder(*config);

        createdEntry = MakeIntrusive<TEntry>();
        createdEntry->Server = server;
        auto createClientOptions = TCreateClientOptions().Token(token);
        if (impersonationUser) {
            createClientOptions = createClientOptions.ImpersonationUser(*impersonationUser);
        }
        createdEntry->Client = CreateClient(server, createClientOptions);
        createdEntry->TransactionSpec = specProvider();
        if (externalTx) {
            createdEntry->ExternalTx = createdEntry->Client->AttachTransaction(externalTx);
            createdEntry->Tx = createdEntry->ExternalTx->StartTransaction(TStartTransactionOptions().Attributes(createdEntry->TransactionSpec));
        } else {
            createdEntry->Tx = createdEntry->Client->StartTransaction(TStartTransactionOptions().Attributes(createdEntry->TransactionSpec));
        }
        createdEntry->CacheTx = createdEntry->Client;
        createdEntry->CacheTtl = config->QueryCacheTtl.Get().GetOrElse(TDuration::Days(7));
        if (!tmpFolder.empty()) {
            auto fullTmpFolder = AddPathPrefix(tmpFolder, NYT::TConfig::Get()->Prefix);
            bool existsGlobally = createdEntry->Client->Exists(fullTmpFolder);
            bool existsInTx = externalTx && createdEntry->ExternalTx->Exists(fullTmpFolder);
            if (!existsGlobally && existsInTx) {
                createdEntry->CacheTx = createdEntry->ExternalTx;
                createdEntry->CacheTxId = createdEntry->ExternalTx->GetId();
            }
        } else {
            createdEntry->DefaultTmpFolder = NYT::AddPathPrefix("tmp/yql/" + UserName_, NYT::TConfig::Get()->Prefix);
        }
        createdEntry->InflightTempTablesLimit = config->InflightTempTablesLimit.Get().GetOrElse(Max<ui32>());
        createdEntry->KeepTables = GetReleaseTempDataMode(*config) == EReleaseTempDataMode::Never;

        TxMap_.emplace(server, createdEntry);
    }
    if (externalTx) {
        YQL_CLOG(INFO, ProviderYt) << "Attached to external tx " << GetGuidAsString(externalTx);
    }
    YQL_CLOG(INFO, ProviderYt) << "Created tx " << GetGuidAsString(createdEntry->Tx->GetId()) << " on " << server;
    return createdEntry;
}

void TTransactionCache::Commit(const TString& server) {
    ITransactionPtr tx;
    THashSet<TString> tablesToDelete;
    with_lock(Lock_) {
        auto it = TxMap_.find(server);
        if (it != TxMap_.end()) {
            auto entry = it->second;
            tablesToDelete.swap(entry->TablesToDeleteAtCommit);
            if (!tablesToDelete.empty()) {
                tx = entry->Tx;
            }
        }
    }
    if (tx) {
        for (auto& table : tablesToDelete) {
            YQL_CLOG(INFO, ProviderYt) << "Removing " << table.Quote() << " on " << server;
            tx->Remove(table, TRemoveOptions().Force(true));
        }
    }
}

void TTransactionCache::Finalize() {
    THashMap<TString, TEntry::TPtr> txMap;
    with_lock(Lock_) {
        txMap.swap(TxMap_);
    }
    for (auto& item: txMap) {
        item.second->Finalize(item.first);
    }
}

void TTransactionCache::AbortAll() {
    THashMap<TString, TEntry::TPtr> txMap;
    with_lock(Lock_) {
        txMap.swap(TxMap_);
    }

    TString error;
    auto abortTx = [&] (const ITransactionPtr& tx) {
        try {
            tx->Abort();
        } catch (...) {
            YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();

            // Store first abort error.
            if (error.empty()) {
                error = "Failed to abort transaction " + GetGuidAsString(tx->GetId()) + ": " + CurrentExceptionMessage();
            }
        }
    };

    for (auto& item : txMap) {
        auto entry = item.second;

        for (auto& item: entry->SnapshotTxs) {
            YQL_CLOG(DEBUG, ProviderYt) << "AbortAll(): Aborting Snapshot tx " << GetGuidAsString(item.second->GetId());
            abortTx(item.second);
        }
        for (auto& item : entry->CheckpointTxs) {
            YQL_CLOG(DEBUG, ProviderYt) << "AbortAll(): Aborting Checkpoint tx " << GetGuidAsString(item.second->GetId());
            abortTx(item.second);
        }
        for (auto& item: entry->WriteTxs) {
            YQL_CLOG(DEBUG, ProviderYt) << "AbortAll(): Aborting Write tx " << GetGuidAsString(item.second->GetId());
            abortTx(item.second);
        }
        if (entry->BinarySnapshotTx) {
            YQL_CLOG(INFO, ProviderYt) << "AbortAll(): Aborting BinarySnapshot tx " << GetGuidAsString(entry->BinarySnapshotTx->GetId());
            abortTx(entry->BinarySnapshotTx);
        }
        if (entry->Tx) {
            YQL_CLOG(INFO, ProviderYt) << "Aborting tx " << GetGuidAsString(entry->Tx->GetId())  << " on " << item.first;
            abortTx(entry->Tx);
        }

        if (entry->Client) {
            YQL_CLOG(INFO, ProviderYt) << "Shutting down client";
            try {
                entry->Client->Shutdown();
            } catch (...) {
                if (!error) {
                    error = "Failed to shut down client: " + CurrentExceptionMessage();
                }
            }
        }
    }

    if (error) {
        ythrow yexception() << error;
    }
}

void TTransactionCache::DetachSnapshotTxs() {
    TString error;
    auto detachTx = [&] (const ITransactionPtr& tx) {
        try {
            tx->Detach();
        } catch (...) {
            YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();

            // Store first detach error.
            if (error.empty()) {
                error = "Failed to detach transaction " + GetGuidAsString(tx->GetId()) + ": " + CurrentExceptionMessage();
            }
        }
    };

    for (auto& item : TxMap_) {
        auto entry = item.second;

        for (auto& item : entry->SnapshotTxs) {
            YQL_CLOG(DEBUG, ProviderYt) << "DetachSnapshotTxs(): Detaching Snapshot tx " << GetGuidAsString(item.second->GetId());
            detachTx(item.second);
        }
        if (entry->Tx) {
            YQL_CLOG(INFO, ProviderYt) << "Detaching tx " << GetGuidAsString(entry->Tx->GetId())  << " on " << item.first;
            detachTx(entry->Tx);
        }
    }

    if (error) {
        ythrow yexception() << error;
    }
}

} // NYql
