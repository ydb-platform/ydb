#pragma once

#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard::TIndexBuilder::TTxBase: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    TSideEffects SideEffects;
    const NKikimr::NSchemeShard::ETxTypes TxType;
public:
    const TString LogPrefix;
private:
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
    void Fill(NKikimrIndexBuilder::TIndexBuild& index, const TIndexBuildInfo& indexInfo);
    void Fill(NKikimrIndexBuilder::TIndexBuildSettings& settings, const TIndexBuildInfo& indexInfo);
    void AddIssue(::google::protobuf::RepeatedPtrField< ::Ydb::Issue::IssueMessage>* issues,
                  const TString& message,
                  NYql::TSeverityIds::ESeverityId severity = NYql::TSeverityIds::S_ERROR);
    void SendNotificationsIfFinished(TIndexBuildInfo& indexInfo);
    void EraseBuildInfo(const TIndexBuildInfo& indexBuildInfo);
    Ydb::StatusIds::StatusCode TranslateStatusCode(NKikimrScheme::EStatus status);
    void Bill(const TIndexBuildInfo& indexBuildInfo, TInstant startPeriod = TInstant::Zero(), TInstant endPeriod = TInstant::Zero());
    void AskToScheduleBilling(TIndexBuildInfo& indexBuildInfo);
    bool GotScheduledBilling(TIndexBuildInfo& indexBuildInfo);

public:
    explicit TTxBase(TSelf* self, NKikimr::NSchemeShard::ETxTypes txType)
        : TBase(self)
        , TxType(txType)
        , LogPrefix(TStringBuilder() << "TIndexBuilder::" << NKikimr::NSchemeShard::ETxTypes_Name(txType) << ": ")
    { }

    TTxType GetTxType() const override { return TxType; }

    virtual ~TTxBase() = default;

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual void DoComplete(const TActorContext& ctx) = 0;

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
};

template<typename TRequest, typename TResponse>
class TSchemeShard::TIndexBuilder::TTxSimple : public TSchemeShard::TIndexBuilder::TTxBase {
public:
    typename TRequest::TPtr Request;
    THolder<TResponse> Response;
    const bool IsMutableOperation;

    explicit TTxSimple(TSelf* self, typename TRequest::TPtr& ev, NKikimr::NSchemeShard::ETxTypes txType, bool isMutableOperation = true)
        : TTxBase(self, txType)
        , Request(ev)
        , IsMutableOperation(isMutableOperation)
    { }

    bool Reply(const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = TString())
    {
        Y_ABORT_UNLESS(Response);
        auto& record = Response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            AddIssue(record.MutableIssues(), errorMessage);
        }

        if (IsMutableOperation) {
            LOG_N("Reply " << Response->Record.ShortDebugString());
        } else {
            LOG_D("Reply " << Response->Record.ShortDebugString());
        }

        Send(Request->Sender, std::move(Response), 0, Request->Cookie);
        return true;
    }

    bool Reply(const NKikimrScheme::EStatus status, const TString& errorMessage)
    {
        Y_ABORT_UNLESS(Response);
        Response->Record.SetSchemeStatus(status);
        return Reply(TranslateStatusCode(status), errorMessage);
    }
};

} // NSchemeShard
} // NKikimr
