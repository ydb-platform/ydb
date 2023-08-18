#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard::TIndexBuilder::TTxBase: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    TSideEffects SideEffects;

    using TChangeStateRec = std::tuple<TIndexBuildId, TIndexBuildInfo::EState>;
    TDeque<TChangeStateRec> StateChanges;
    using TBillingEventSchedule = std::tuple<TIndexBuildId, TDuration>;
    TDeque<TBillingEventSchedule> ToScheduleBilling;
    using TToBill = std::tuple<TIndexBuildId, TInstant, TInstant>;
    TDeque<TToBill> ToBill;

    void ApplyState(NTabletFlatExecutor::TTransactionContext& txc);
    void ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);
    void ApplyOnComplete(const TActorContext& ctx);
    void ApplySchedule(const TActorContext& ctx);
    ui64 RequestUnits(const TBillingStats& stats);
    void RoundPeriod(TInstant& start, TInstant& end);
    void ApplyBill(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx);

protected:
    void Send(TActorId dst, THolder<IEventBase> message, ui32 flags = 0, ui64 cookie = 0);
    void ChangeState(TIndexBuildId id, TIndexBuildInfo::EState state);
    void Progress(TIndexBuildId id);
    void Fill(NKikimrIndexBuilder::TIndexBuild& index, const TIndexBuildInfo::TPtr indexInfo);
    void Fill(NKikimrIndexBuilder::TIndexBuildSettings& settings, const TIndexBuildInfo::TPtr indexInfo);
    void AddIssue(::google::protobuf::RepeatedPtrField< ::Ydb::Issue::IssueMessage>* issues,
                  const TString& message,
                  NYql::TSeverityIds::ESeverityId severity = NYql::TSeverityIds::S_ERROR);
    void SendNotificationsIfFinished(TIndexBuildInfo::TPtr indexInfo);
    void EraseBuildInfo(const TIndexBuildInfo::TPtr indexBuildInfo);
    Ydb::StatusIds::StatusCode TranslateStatusCode(NKikimrScheme::EStatus status);
    void Bill(const TIndexBuildInfo::TPtr& indexBuildInfo, TInstant startPeriod = TInstant::Zero(), TInstant endPeriod = TInstant::Zero());
    void AskToScheduleBilling(const TIndexBuildInfo::TPtr& indexBuildInfo);
    bool GotScheduledBilling(const TIndexBuildInfo::TPtr& indexBuildInfo);

public:
    explicit TTxBase(TSelf* self)
        : TBase(self)
    { }

    virtual ~TTxBase() = default;

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual void DoComplete(const TActorContext& ctx) = 0;

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
};

} // NSchemeShard
} // NKikimr
