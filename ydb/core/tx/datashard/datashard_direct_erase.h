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
        const TRowVersion ReadVersion;
        const TRowVersion WriteVersion;

    private:
        explicit TExecuteParams(TDirectTxErase* tx, TTransactionContext* txc,
                const TRowVersion& readVersion, const TRowVersion& writeVersion)
            : Tx(tx)
            , Txc(txc)
            , ReadVersion(readVersion)
            , WriteVersion(writeVersion)
        {
        }

    public:
        static TExecuteParams ForCheck() {
            return TExecuteParams(nullptr, nullptr, TRowVersion(), TRowVersion());
        }

        template <typename... Args>
        static TExecuteParams ForExecute(Args&&... args) {
            return TExecuteParams(std::forward<Args>(args)...);
        }

        explicit operator bool() const {
            if (!Tx || !Txc) {
                Y_VERIFY(!Tx && !Txc);
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

    bool Execute(TDataShard* self, TTransactionContext& txc, const TRowVersion& readVersion, const TRowVersion& writeVersion) override;
    TDirectTxResult GetResult(TDataShard* self) override;
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const override;
};

} // NDataShard
} // NKikimr
