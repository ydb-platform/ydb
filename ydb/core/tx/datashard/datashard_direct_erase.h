#pragma once

#include "change_collector.h"
#include "datashard_direct_transaction.h"

namespace NKikimr {
namespace NDataShard {

class TDirectTxErase : public IDirectTx {
    TEvDataShard::TEvEraseRowsRequest::TPtr Ev;
    THolder<TEvDataShard::TEvEraseRowsResponse> Result;
    THolder<IDataShardChangeCollector> ChangeCollector;

    enum class EStatus {
        Success,
        Error,
        PageFault,
    };

    struct TExecuteParams {
        TDirectTxErase* const Tx;
        TTransactionContext* const Txc;
        const TRowVersion MvccVersion;
        const ui64 GlobalTxId;
        absl::flat_hash_set<ui64>* const VolatileReadDependencies;

    private:
        explicit TExecuteParams(TDirectTxErase* tx, TTransactionContext* txc,
                const TRowVersion& mvccVersion, ui64 globalTxId,
                absl::flat_hash_set<ui64>* volatileReadDependencies)
            : Tx(tx)
            , Txc(txc)
            , MvccVersion(mvccVersion)
            , GlobalTxId(globalTxId)
            , VolatileReadDependencies(volatileReadDependencies)
        {
        }

    public:
        static TExecuteParams ForCheck() {
            return TExecuteParams(nullptr, nullptr, TRowVersion(), 0, nullptr);
        }

        template <typename... Args>
        static TExecuteParams ForExecute(Args&&... args) {
            return TExecuteParams(std::forward<Args>(args)...);
        }

        explicit operator bool() const {
            if (!Tx || !Txc) {
                Y_ENSURE(!Tx && !Txc);
                return false;
            }

            return true;
        }

        IDataShardChangeCollector* GetChangeCollector() const {
            return Tx ? Tx->ChangeCollector.Get() : nullptr;
        }
    };

    static EStatus CheckedExecute(
        TDataShard* self, const TExecuteParams& params,
        const NKikimrTxDataShard::TEvEraseRowsRequest& request,
        NKikimrTxDataShard::TEvEraseRowsResponse::EStatus& status, TString& error);

public:
    explicit TDirectTxErase(TEvDataShard::TEvEraseRowsRequest::TPtr& ev);

    static bool CheckRequest(TDataShard* self, const NKikimrTxDataShard::TEvEraseRowsRequest& request,
        NKikimrTxDataShard::TEvEraseRowsResponse::EStatus& status, TString& error);

    bool Execute(TDataShard* self, TTransactionContext& txc,
        const TRowVersion& mvccVersion, ui64 globalTxId,
        absl::flat_hash_set<ui64>& volatileReadDependencies) override;
    TDirectTxResult GetResult(TDataShard* self) override;
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const override;
};

} // NDataShard
} // NKikimr
