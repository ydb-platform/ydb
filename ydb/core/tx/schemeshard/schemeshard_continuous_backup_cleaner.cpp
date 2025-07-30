#include "schemeshard_continuous_backup_cleaner.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/tx_allocator_client/actor_client.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NSchemeShard {

class TContinuousBackupCleaner : public TActorBootstrapped<TContinuousBackupCleaner> {
public:
    TContinuousBackupCleaner(TActorId txAllocatorClient,
                             TActorId schemeShard,
                             const TString& workingDir,
                             const TString& tableName,
                             const TString& streamName)
        : TxAllocatorClient(txAllocatorClient)
        , SchemeShard(schemeShard)
        , WorkingDir(workingDir)
        , TableName(tableName)
        , StreamName(streamName)
    {}

    void Bootstrap() {
        AllocateTxId();
        Become(&TContinuousBackupCleaner::StateWork);
    }

    void AllocateTxId() const {
        Send(TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate());
    }

    void Handle(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
        TxId = TTxId(ev->Get()->TxIds.front());
        Send(SchemeShard, DropPropose());
    }

    THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose() const {
        auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>();
        propose->Record.SetTxId(ui64(TxId));

        auto& modifyScheme = *propose->Record.AddTransaction();
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropCdcStream);
        modifyScheme.SetInternal(true);
        modifyScheme.SetWorkingDir(WorkingDir);

        auto& drop = *modifyScheme.MutableDropCdcStream();
        drop.SetTableName(TableName);
        drop.AddStreamName(StreamName);

        return propose;
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto status = record.GetStatus();

        if (status == NKikimrScheme::StatusAccepted) {
            return ReplyAndDie();
        }

        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            return ReplyAndDie();
        }

        if (status == NKikimrScheme::StatusMultipleModifications) {
            return Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
        }

        ReplyAndDie(false, record.GetReason());
    }

    void ReplyAndDie(bool success = true, const TString& error = "") {
        Send(SchemeShard, new TEvPrivate::TEvContinuousBackupCleanerResult(success, error));
        PassAway();
    }

    void Retry() const {
        Send(SchemeShard, DropPropose());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxAllocatorClient::TEvAllocateResult, Handle);
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            cFunc(TEvents::TEvWakeup::EventType, Retry);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    TActorId TxAllocatorClient;
    TActorId SchemeShard;
    TString WorkingDir;
    TString TableName;
    TString StreamName;
    TTxId TxId;

}; // TContinuousBackupCleaner

IActor* CreateContinuousBackupCleaner(TActorId txAllocatorClient,
                                      TActorId schemeShard,
                                      const TString& workingDir,
                                      const TString& tableName,
                                      const TString& streamName)
{
    return new TContinuousBackupCleaner(txAllocatorClient, schemeShard, workingDir, tableName, streamName);
}

} // namespace NKikimr::NSchemeShard
