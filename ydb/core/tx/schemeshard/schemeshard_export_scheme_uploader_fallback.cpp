#include "schemeshard_export_scheme_uploader.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeUploaderFallback: public TActorBootstrapped<TSchemeUploaderFallback> {
public:
    explicit TSchemeUploaderFallback(TSchemeShard* schemeShard, TTxId txId)
        : SchemeShard(schemeShard)
        , TxId(txId)
    {
    }

    void Bootstrap() {
        Send(SchemeShard->SelfId(), new TEvSchemeShard::TEvModifySchemeTransactionResult(
            TEvSchemeShard::EStatus::StatusPreconditionFailed,
            ui64(TxId),
            ui64(SchemeShard->SelfTabletId()),
            "Exports to S3 are disabled"
        ));
        PassAway();
    }

private:
    TSchemeShard* const SchemeShard;
    TTxId TxId;

};

IActor* CreateSchemeUploader(TSchemeShard* schemeShard, TExportInfo::TPtr exportInfo, ui32 itemIdx, TTxId txId, const NKikimrSchemeOp::TBackupTask& task) {
    return new TSchemeUploaderFallback(schemeShard, txId);
}

} // NSchemeShard
} // NKikimr
