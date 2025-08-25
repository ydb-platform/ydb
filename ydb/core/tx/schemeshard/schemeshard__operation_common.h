#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include "schemeshard_private.h"


namespace NKikimr::TEvHive {
    struct TEvCreateTablet;
    struct TEvAdoptTablet;
}

namespace NKikimr::NSchemeShard {

class TSchemeShard;

template<typename T>
struct TEvSchemaChangedTraits;

template<>
struct TEvSchemaChangedTraits<TEvDataShard::TEvSchemaChanged::TPtr> {
    static TActorId GetSource(const TEvDataShard::TEvSchemaChanged::TPtr& ev) {
        return TActorId{ev->Get()->GetSource()};
    }
    static std::optional<ui32> GetGeneration(const TEvDataShard::TEvSchemaChanged::TPtr& ev) {
        return {ev->Get()->GetGeneration()};
    }
    static bool HasOpResult(const TEvDataShard::TEvSchemaChanged::TPtr& ev) {
        return ev->Get()->Record.HasOpResult();
    }
    static TString GetName() {
        return "TEvDataShard::TEvSchemaChanged";
    }
};

template<>
struct TEvSchemaChangedTraits<TEvColumnShard::TEvNotifyTxCompletionResult::TPtr> {
    static TActorId GetSource(const TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& ev) {
        return TActorId{ev->Sender};
    }
    static std::optional<ui32> GetGeneration(const TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& /* ev */) {
        return std::nullopt; //TODO consider to add generation to TEvColumnShard::TEvNotifyTxCompletionResult
    }
    static bool HasOpResult(const TEvColumnShard::TEvNotifyTxCompletionResult::TPtr& /* ev */) {
        return false;
    }
    static TString GetName() {
        return "TEvColumnShard::TEvNotifyTxCompletionResult";
    }
};

TSet<ui32> AllIncomingEvents();

void IncParentDirAlterVersionWithRepublishSafeWithUndo(const TOperationId& opId, const TPath& path, TSchemeShard* ss, TSideEffects& onComplete);
void IncParentDirAlterVersionWithRepublish(const TOperationId& opId, const TPath& path, TOperationContext& context);

void IncAliveChildrenSafeWithUndo(const TOperationId& opId, const TPath& parentPath, TOperationContext& context, bool isBackup = false);
void IncAliveChildrenDirect(const TOperationId& opId, const TPath& parentPath, TOperationContext& context, bool isBackup = false);
void DecAliveChildrenDirect(const TOperationId& opId, TPathElement::TPtr parentPath, TOperationContext& context, bool isBackup = false);


NKikimrSchemeOp::TModifyScheme MoveTableTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst);
NKikimrSchemeOp::TModifyScheme MoveTableIndexTask(NKikimr::NSchemeShard::TPath& src, NKikimr::NSchemeShard::TPath& dst);

THolder<TEvHive::TEvCreateTablet> CreateEvCreateTablet(TPathElement::TPtr targetPath, TShardIdx shardIdx, TOperationContext& context);

void AbortUnsafeDropOperation(const TOperationId& operationId, const TTxId& txId, TOperationContext& context);

namespace NTableState {

bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvDataShard::TEvProposeTransactionResult__HandlePtr& ev, TOperationContext& context);
bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvColumnShard::TEvProposeTransactionResult__HandlePtr& ev, TOperationContext& context);
bool CollectSchemaChanged(const TOperationId& operationId, const TEvDataShard::TEvSchemaChanged__HandlePtr& ev, TOperationContext& context);
bool CollectSchemaChanged(const TOperationId& operationId, const TEvColumnShard::TEvNotifyTxCompletionResult__HandlePtr& ev, TOperationContext& context);

void SendSchemaChangedNotificationAck(const TOperationId& operationId, TActorId ackTo, TShardIdx shardIdx, TOperationContext& context);
void AckAllSchemaChanges(const TOperationId& operationId, TTxState& txState, TOperationContext& context);

bool CheckPartitioningChangedForTableModification(TTxState& txState, TOperationContext& context);
void UpdatePartitioningForTableModification(TOperationId txId, TTxState& txState, TOperationContext& context);

TVector<TTableShardInfo> ApplyPartitioningCopyTable(const TShardInfo& templateDatashardInfo, TTableInfo::TPtr srcTableInfo, TTxState& txState, TSchemeShard* ss);

bool SourceTablePartitioningChangedForCopyTable(const TTxState& txState, TOperationContext& context);
void UpdatePartitioningForCopyTable(TOperationId operationId, TTxState& txState, TOperationContext& context);

class TProposedWaitParts: public TSubOperationState {
private:
    TOperationId OperationId;
    const TTxState::ETxState NextState;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NTableState::TProposedWaitParts"
                << " operationId# " << OperationId;
    }
    template<typename TEvent>
    bool HandleReplyImpl(const TEvent& ev, TOperationContext& context);

public:
    TProposedWaitParts(TOperationId id, TTxState::ETxState nextState = TTxState::Done);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvDataShard::TEvSchemaChanged__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvColumnShard::TEvNotifyTxCompletionResult__HandlePtr& ev, TOperationContext& context) override;
};

} // namespace NTableState

class TCreateParts: public TSubOperationState {
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TCreateParts"
            << " opId# " << OperationId;
    }

    THolder<TEvHive::TEvAdoptTablet> AdoptRequest(TShardIdx shardIdx, TOperationContext& context);

public:
    explicit TCreateParts(const TOperationId& id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvHive::TEvCreateTabletReply__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvHive::TEvAdoptTabletReply__HandlePtr& ev, TOperationContext& context) override;
};

class TDeleteParts: public TSubOperationState {
protected:
    const TOperationId OperationId;
    const TTxState::ETxState NextState;

    TString DebugHint() const override {
        return TStringBuilder() << "TDeleteParts"
            << " opId# " << OperationId << " ";
    }

    void DeleteShards(TOperationContext& context);

public:
    explicit TDeleteParts(const TOperationId& id, TTxState::ETxState nextState = TTxState::Propose);

    bool ProgressState(TOperationContext& context) override;
};

class TDeletePartsAndDone: public TDeleteParts {
public:
    explicit TDeletePartsAndDone(const TOperationId& id);

    bool ProgressState(TOperationContext& context) override;
};

class TDone: public TSubOperationState {
protected:
    const TOperationId OperationId;
    const TMaybe<TPathElement::EPathState> TargetState;

    TString DebugHint() const override {
        return TStringBuilder() << "TDone"
            << " opId# " << OperationId;
    }

    bool Process(TOperationContext& context);

public:
    explicit TDone(const TOperationId& id);

    TDone(const TOperationId& id, TPathElement::EPathState targetState);

    bool ProgressState(TOperationContext& context) override;
};

namespace NPQState {

bool CollectProposeTransactionResults(const TOperationId& operationId, const TEvPersQueue::TEvProposeTransactionResult__HandlePtr& ev, TOperationContext& context);

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NPQState::TConfigureParts"
                << " operationId# " << OperationId;
    }

public:
    TConfigureParts(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvPersQueue::TEvProposeTransactionResult__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvPersQueue::TEvUpdateConfigResponse__HandlePtr& ev, TOperationContext& context) override;
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NPQState::TPropose"
                << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvPersQueue::TEvProposeTransactionResult__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvDataShard::TEvProposeTransactionAttachResult__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvPrivate::TEvOperationPlan__HandlePtr& ev, TOperationContext& context) override;

private:
    bool CanPersistState(const TTxState& txState,
                         TOperationContext& context);
    void PersistState(const TTxState& txState,
                      TOperationContext& context) const;
    bool TryPersistState(TOperationContext& context);
    void SendEvProposeTransactionAttach(TShardIdx shard, TTabletId tablet,
                                        TOperationContext& context);

    void PrepareShards(TTxState& txState, TSet<TTabletId>& shardSet, TOperationContext& context);

    TPathId PathId;
    TPathElement::TPtr Path;
};

} // NPQState

namespace NBSVState {

class TConfigureParts: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
            << "NBSVState::TConfigureParts"
            << " operationId: " << OperationId;
    }

public:
    TConfigureParts(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvBlockStore::TEvUpdateVolumeConfigResponse__HandlePtr& ev, TOperationContext& context) override;
};

class TPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder()
                << "NBSVState::TPropose"
                << " operationId# " << OperationId;
    }

public:
    TPropose(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvPrivate::TEvOperationPlan__HandlePtr& ev, TOperationContext& context) override;
};

} // NBSVState

namespace NCdcStreamState {

class TConfigurePartsAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NCdcStreamState::TConfigurePartsAtTable"
            << " operationId: " << OperationId;
    }

protected:
    virtual void FillNotice(const TPathId& pathId, NKikimrTxDataShard::TFlatSchemeTransaction& tx, TOperationContext& context) const = 0;

public:
    explicit TConfigurePartsAtTable(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvDataShard::TEvProposeTransactionResult__HandlePtr& ev, TOperationContext& context) override;

protected:
    const TOperationId OperationId;
}; // TConfigurePartsAtTable

class TProposeAtTable: public TSubOperationState {
    TString DebugHint() const override {
        return TStringBuilder()
            << "NCdcStreamState::TProposeAtTable"
            << " operationId: " << OperationId;
    }

public:
    explicit TProposeAtTable(TOperationId id);

    bool ProgressState(TOperationContext& context) override;
    bool HandleReply(TEvPrivate::TEvOperationPlan__HandlePtr& ev, TOperationContext& context) override;
    bool HandleReply(TEvDataShard::TEvSchemaChanged__HandlePtr& ev, TOperationContext& context) override;

protected:
    const TOperationId OperationId;
}; // TProposeAtTable

class TProposeAtTableDropSnapshot: public TProposeAtTable {
public:
    using TProposeAtTable::TProposeAtTable;

    bool HandleReply(TEvPrivate::TEvOperationPlan__HandlePtr& ev, TOperationContext& context) override;
}; // TProposeAtTableDropSnapshot

} // NCdcStreamState

namespace NForceDrop {

void ValidateNoTransactionOnPaths(TOperationId operationId, const THashSet<TPathId>& paths, TOperationContext& context);
void CollectShards(const THashSet<TPathId>& paths, TOperationId operationId, TTxState* txState, TOperationContext& context);

} // namespace NForceDrop

} // namespace NKikimr::NSchemeShard
