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
                             ui64 backupId,
                             TPathId item,
                             const TString& workingDir,
                             const TString& tableName,
                             const TString& streamName)
        : TxAllocatorClient(txAllocatorClient)
        , SchemeShard(schemeShard)
        , BackupId(backupId)
        , Item(item)
        , WorkingDir(workingDir)
        , TableName(tableName)
        , StreamName(streamName)
    {}

    void Bootstrap() {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Starting continuous backup cleaner:"
            << " workingDir# " << WorkingDir
            << " table# " << TableName
            << " stream# " << StreamName);

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
            return SubscribeTx(TxId);
        }

        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            return ReplyAndDie();
        }

        if (status == NKikimrScheme::StatusMultipleModifications) {
            return Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup);
        }

        ReplyAndDie(false, record.GetReason());
    }


    void SubscribeTx(TTxId txId) {
        Send(SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(ui64(txId)));
    }

    void ReplyAndDie(bool success = true, const TString& error = "") {
        Send(SchemeShard, new TEvPrivate::TEvContinuousBackupCleanerResult(BackupId, Item, success, error));
        PassAway();
    }

    void Retry() const {
        Send(SchemeShard, DropPropose());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxAllocatorClient::TEvAllocateResult, Handle);
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
            cFunc(TEvSchemeShard::TEvNotifyTxCompletionResult::EventType, ReplyAndDie);
            cFunc(TEvents::TEvWakeup::EventType, Retry);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    TActorId TxAllocatorClient;
    TActorId SchemeShard;
    ui64 BackupId;
    TPathId Item;
    TString WorkingDir;
    TString TableName;
    TString StreamName;

    TTxId TxId;

}; // TContinuousBackupCleaner

IActor* CreateContinuousBackupCleaner(TActorId txAllocatorClient,
                                      TActorId schemeShard,
                                      ui64 backupId,
                                      TPathId item,
                                      const TString& workingDir,
                                      const TString& tableName,
                                      const TString& streamName)
{
    return new TContinuousBackupCleaner(txAllocatorClient, schemeShard, backupId, item, workingDir, tableName, streamName);
}

} // namespace NKikimr::NSchemeShard
