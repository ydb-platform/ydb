#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_affected_paths.h"
#include "schemeshard_operation_plan.h"
#include "schemeshard_tx_infly.h"

#include <util/generic/set.h>

#include <optional>

namespace NKikimr::NSchemeShard {

// Pilot: the CreateTable planner, defined beside the operation it plans
// (schemeshard__operation_create_table.cpp). Declared here because IgniteOperation builds the
// plan before any part exists.
TConclusion<TLogicalOperationPlan> PlanCreateTableEffects(
    const TTxTransaction& tx, TOperationContext& context);

struct TOperation: TSimpleRefCount<TOperation> {
    using TPtr = TIntrusivePtr<TOperation>;

    const TTxId TxId;
    ui32 PreparedParts = 0;
    TVector<ISubOperation::TPtr> Parts;

    // Indexed 1:1 with the post-rewrite, pre-split transaction list. nullopt means the
    // operation type is exempt rather than that it affects nothing -- the two must stay
    // distinguishable or an unmigrated op reads as "touches no paths".
    TVector<std::optional<TAffectedPaths>> DeclaredAffectedPaths;

    // Pilot (see .omc/plans/pilot-create-table.md): the new-model plan, built once for the
    // whole operation before any part is constructed or proposed. Deliberately not per part --
    // a top-level CreateTable carrying CopyFromTable is re-dispatched to TCopyTable, so a
    // planner hung off the sub-operation class never runs for that shape. Planning is a
    // property of the operation type, which is exactly why it belongs here.
    // Indexed 1:1 with the post-split transaction list, the same shape as
    // DeclaredAffectedPaths. One plan per *operation* is wrong: an operation carries many
    // transactions -- a consistent copy issues one CreateTable per table -- and parts are
    // constructed per transaction, so a single plan bound part 0 of every batch to the last
    // transaction's effects. Caught by TSchemeShardCheckProposeSize::CopyTables.
    TVector<std::optional<TLogicalOperationPlan>> PilotPlans;

    // The declaration of each constructed part, keyed by its SubTxId.
    //
    // DeclaredAffectedPaths above is indexed by *transaction*, so it covers what the request
    // asked for and not the parts an operation fans out into. admitParts already computes a
    // TAffectedPaths per part and, before this existed, folded it into DeclaredPathSet and
    // dropped the structure -- which left a part unable to find its own plan entry even
    // though the plan contained it.
    //
    // Keyed by SubTxId because that is fixed when the part is constructed
    // (ISubOperation::GetOperationId) and admitParts runs before those parts propose, so a
    // part can look itself up during its own Propose.
    //
    // Nothing consumes this yet: it is the enabling step for having parts resolve *from* the
    // plan rather than alongside it. Kept deliberately, since the alternative is recomputing
    // at the point of use and reintroducing the second answer this whole mechanism exists to
    // eliminate.
    THashMap<TSubTxId, TAffectedPaths> DeclaredPartPaths;

    // The above flattened to absolute paths, for ObservePathTouched to compare against.
    // Lives on the operation rather than on the propose stack because path rows are written
    // well after IgniteOperation returns -- TStorageChanges is applied later, and plan and
    // progress write in transactions of their own.
    THashSet<TString> DeclaredPathSet;

    // The subset of the above that actually got a path-row write, accumulated across every
    // phase of the operation -- propose, plan and progress each write, and a declaration is
    // only fulfilled by the union of them. Read once, at successful completion, against the
    // MustWrite entries of DeclaredAffectedPaths.
    THashSet<TString> ObservedPathSet;

    // Cleared once anything -- a requested transaction or a constructed part -- reports an
    // Incomplete declaration, and never set again for this operation. A plain clear of
    // DeclaredPathSet is not enough: parts are admitted in a loop, so a later part would
    // repopulate the set, and TDeclaredPathsGuard would then re-arm at plan and progress
    // time against a subset that is known to be partial -- reporting everything outside it.
    bool DeclaredPathsUsable = true;

    THashSet<TActorId> Subscribers;
    THashSet<TTxId> DependentOperations;
    THashSet<TTxId> WaitOperations;

    struct TPreSerializedMessage {
        ui32 Type;
        TIntrusivePtr<TEventSerializedData> Data;
        TOperationId OpId;

        TPreSerializedMessage() = default;

        TPreSerializedMessage(ui32 type, TIntrusivePtr<TEventSerializedData> data, TOperationId opId)
            : Type(type)
            , Data(std::move(data))
            , OpId(opId)
        { }
    };

    THashMap<TTabletId, TMap<TPipeMessageId, TPreSerializedMessage>> PipeBindedMessages; // std::pair<ui64, ui64> it's a cookie

    THashMap<TTabletId, TSubTxId> RelationsByTabletId;
    THashMap<TShardIdx, TSubTxId> RelationsByShardIdx;

    using TProposeRec = std::tuple<TSubTxId, TPathId, TStepId>;
    TDeque<TProposeRec> Proposes;

    using TProposeShards = std::tuple<TSubTxId, TTabletId>;
    TDeque<TProposeShards> ShardsProposes;

    using TPublishPath = std::pair<TPathId, ui64>;
    TSet<TPublishPath> Publications;

    TSet<TSubTxId> ReadyToProposeParts;
    THashSet<TSubTxId> ReadyToNotifyParts;
    THashSet<TSubTxId> DoneParts;
    THashMap<TPathId, NKikimrSchemeOp::EPathState> ReleasePathAtDone;

    THashMap<TShardIdx, THashSet<TSubTxId>> WaitingShardCreatedByShard;
    THashMap<TSubTxId, THashSet<TShardIdx>> WaitingShardCreatedByPart;

    TMap<TSubTxId, TSet<TPublishPath>> WaitingPublicationsByPart;
    TMap<TPublishPath, TSet<TSubTxId>> WaitingPublicationsByPath;

    TMap<TString, TSet<TSubTxId>> Barriers;

    struct TConsumeQuotaResult {
        NKikimrScheme::EStatus Status = NKikimrScheme::StatusSuccess;
        TString Reason;
    };

    struct TSplitTransactionsResult {
        NKikimrScheme::EStatus Status = NKikimrScheme::StatusSuccess;
        TString Reason;
        TVector<TTxTransaction> Transactions;
        std::optional<TTxTransaction> Transaction;
    };

    TOperation(TTxId txId)
        : TxId(txId)
    {}
    ~TOperation() = default;

    TTxId GetTxId() const { return TxId; }

    static TConsumeQuotaResult ConsumeQuota(const TTxTransaction& tx, TOperationContext& context);
    static TSplitTransactionsResult SplitIntoTransactions(const TTxTransaction& tx, const TOperationContext& context);

    ISubOperation::TPtr RestorePart(TTxState::ETxType opType, TTxState::ETxState opState, TOperationContext& context) const;
    TVector<ISubOperation::TPtr> ConstructParts(const TTxTransaction& tx, TOperationContext& context) const;
    void AddPart(ISubOperation::TPtr part);

    bool AddPublishingPath(TPathId pathId, ui64 version);
    bool IsPublished() const;

    void ReadyToNotifyPart(TSubTxId partId);
    bool IsReadyToNotify(const TActorContext& ctx) const;
    bool IsReadyToNotify() const;
    void AddNotifySubscriber(const TActorId& actorId);
    void DoNotify(TSchemeShard* ss, TSideEffects& sideEffects, const TActorContext& ctx);

    bool IsReadyToDone(const TActorContext& ctx) const;

    // propose operation to coordinator
    bool IsReadyToPropose(const TActorContext& ctx) const;
    bool IsReadyToPropose() const;
    void ProposePart(TSubTxId partId, TPathId pathId, TStepId minStep);
    void ProposePart(TSubTxId partId, TTabletId tableId);
    void DoPropose(TSchemeShard* ss, TSideEffects& sideEffects, const TActorContext& ctx) const;

    // route incoming messages to suboperations (parts)
    void RegisterRelationByTabletId(TSubTxId partId, TTabletId tablet, const TActorContext& ctx);
    void RegisterRelationByShardIdx(TSubTxId partId, TShardIdx shardIdx, const TActorContext& ctx);
    TSubTxId FindRelatedPartByTabletId(TTabletId tablet, const TActorContext& ctx) const;
    TSubTxId FindRelatedPartByShardIdx(TShardIdx shardIdx, const TActorContext& ctx) const;

    void WaitShardCreated(TShardIdx shardIdx, TSubTxId partId);
    TVector<TSubTxId> ActivateShardCreated(TShardIdx shardIdx);

    void RegisterWaitPublication(TSubTxId partId, TPathId pathId, ui64 pathVersion);
    TSet<TOperationId> ActivatePartsWaitPublication(TPathId pathId, ui64 pathVersion);
    ui64 CountWaitPublication(TOperationId opId) const;

    void RegisterBarrier(TSubTxId partId, const TString& name) {
        Barriers[name].insert(partId);
        Y_ABORT_UNLESS(Barriers.size() == 1);
    }

    bool HasBarrier() const {
        Y_ABORT_UNLESS(Barriers.size() <= 1);
        return Barriers.size() == 1;
    }

    bool IsDoneBarrier() const {
        Y_ABORT_UNLESS(Barriers.size() <= 1);

        for (const auto& [_, subTxIds] : Barriers) {
            for (const auto blocked : subTxIds) {
                Y_VERIFY_S(!DoneParts.contains(blocked), "part is blocked and done: " << blocked);
            }
            return subTxIds.size() + DoneParts.size() == Parts.size();
        }

        return false;
    }

    void DropBarrier(const TString& name) {
        Y_ABORT_UNLESS(IsDoneBarrier());
        Y_ABORT_UNLESS(Barriers.begin()->first == name);
        Barriers.erase(name);
    }

    void ForceClearBarriers() {
        Barriers.clear();
    }

    TOperationId NextPartId() const {
        return TOperationId(TxId, TSubTxId(PreparedParts));
    }
};

inline TOperationId NextPartId(const TOperationId& opId, const TVector<ISubOperation::TPtr>& parts) {
    return TOperationId(opId.GetTxId(), opId.GetSubTxId() + parts.size());
}

}
