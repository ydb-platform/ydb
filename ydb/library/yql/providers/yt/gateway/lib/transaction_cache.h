#pragma once

#include <ydb/library/yql/providers/yt/common/yql_yt_settings.h>

#include <ydb/library/yql/core/file_storage/storage.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/datetime/base.h>
#include <util/system/mutex.h>

#include <utility>
#include <functional>
#include <tuple>
#include <vector>
#include <exception>

namespace NYql {

class TTransactionCache {
public:
    using TSpecProvider = std::function<NYT::TNode()>;

    struct TEntry : public TThrRefBase {
        TString Server;
        NYT::IClientPtr Client;
        NYT::ITransactionPtr Tx;
        NYT::ITransactionPtr ExternalTx;
        NYT::IClientBasePtr CacheTx;
        NYT::TTransactionId CacheTxId;
        TDuration CacheTtl;
        THashMap<NYT::TTransactionId, NYT::ITransactionPtr> SnapshotTxs;
        THashMap<NYT::TTransactionId, NYT::ITransactionPtr> WriteTxs;
        NYT::ITransactionPtr LastSnapshotTx;
        THashSet<TString> TablesToDeleteAtFinalize;
        THashSet<TString> TablesToDeleteAtCommit;
        ui32 InflightTempTablesLimit = Max<ui32>();
        bool KeepTables = false;
        THashMap<std::pair<TString, ui32>, std::tuple<TString, NYT::TTransactionId, ui64>> Snapshots; // {tablepath, epoch} -> {table_id, transaction_id, revision}
        NYT::TNode TransactionSpec;
        THashMap<TString, NYT::TTableColumnarStatistics> StatisticsCache;
        THashMap<TString, TString> BinarySnapshots; // remote path -> snapshot path
        NYT::ITransactionPtr BinarySnapshotTx;
        THashMap<TString, NYT::ITransactionPtr> CheckpointTxs;
        TString DefaultTmpFolder;
        THashMap<std::tuple<TString, TString, TString>, std::vector<NYT::TRichYPath>> RangeCache;
        THashMap<TString, std::pair<std::vector<TString>, std::vector<std::exception_ptr>>> PartialRangeCache;

        using TFolderCache = THashMap<TString, std::vector<std::tuple<TString, TString, NYT::TNode>>>;
        TFolderCache FolderCache;

        THashMap<TString, TFileLinkPtr> FolderFilePtrCache;

        TMutex Lock_;

        inline void DeleteAtFinalize(const TString& table) {
            with_lock(Lock_) {
                DeleteAtFinalizeUnlocked(table, false);
            }
        }

        inline void DeleteAtFinalizeInternal(const TString& table) {
            with_lock(Lock_) {
                DeleteAtFinalizeUnlocked(table, true);
            }
        }

        inline void CancelDeleteAtFinalize(const TString& table) {
            with_lock(Lock_) {
                CancelDeleteAtFinalizeUnlocked(table, false);
            }
        }

        void RemoveInternal(const TString& table);
        void Finalize(const TString& clusterName);

        template<typename T>
        T CancelDeleteAtFinalize(const T& range) {
            T filteredRange;
            with_lock(Lock_) {
                for (const auto& i : range) {
                    if (CancelDeleteAtFinalizeUnlocked(i, false)) {
                        filteredRange.insert(filteredRange.end(), i);
                    }
                }
            }
            return filteredRange;
        }

        inline NYT::IClientBasePtr GetRoot() const {
            if (ExternalTx) {
                return ExternalTx;
            }
            return Client;
        }

        NYT::ITransactionPtr GetSnapshotTx(bool createTx);
        NYT::ITransactionPtr GetSnapshotTx(const NYT::TTransactionId& id) const;

        NYT::ITransactionPtr GetCheckpointTx(const TString& tablePath) const;
        NYT::ITransactionPtr GetOrCreateCheckpointTx(const TString& tablePath);
        void CommitCheckpointTx(const TString& tablePath);

        NYT::TTransactionId AllocWriteTx();
        void CompleteWriteTx(const NYT::TTransactionId& id, bool abort);

        TMaybe<ui64> GetColumnarStat(NYT::TRichYPath ytPath) const;
        void UpdateColumnarStat(NYT::TRichYPath ytPath, ui64 size);
        void UpdateColumnarStat(NYT::TRichYPath ytPath, const NYT::TTableColumnarStatistics& columnStat);

        std::pair<TString, NYT::TTransactionId> GetBinarySnapshot(TString remoteTmpFolder, const TString& md5, const TString& localPath, TDuration expirationInterval);

        void CreateDefaultTmpFolder();

        using TPtr = TIntrusivePtr<TEntry>;

    private:
        void DeleteAtFinalizeUnlocked(const TString& table, bool isInternal);
        bool CancelDeleteAtFinalizeUnlocked(const TString& table, bool isInternal);
        void DoRemove(const TString& table);

        size_t ExternalTempTablesCount = 0;
    };

    TTransactionCache(const TString& userName);

    TEntry::TPtr GetEntry(const TString& server);
    TEntry::TPtr GetOrCreateEntry(const TString& server, const TString& token, const TMaybe<TString>& impersonationUser, const TSpecProvider& specProvider, const TYtSettings::TConstPtr& config);
    TEntry::TPtr TryGetEntry(const TString& server);

    void Commit(const TString& server);
    void Finalize();
    void AbortAll();
    void DetachSnapshotTxs();

private:
    TMutex Lock_;
    THashMap<TString, TEntry::TPtr> TxMap_;
    const TString UserName_;
};

} // NYql
