#pragma once
#include "session.h"
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

class TStatusChannel: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrSchemeShardTxBackgroundProto::TTaskResharding, NOlap::NBackground::IStatusChannel> {
private:
    TString TaskClassName;
    TString TaskIdentifier;
    ui64 SSTabletId;
public:
    TStatusChannel(const TString taskClassName, const TString& taskId, const ui64 ssTabletId)
        : TaskClassName(taskClassName)
        , TaskIdentifier(taskId)
        , SSTabletId(ssTabletId)
    {
        AFL_VERIFY(!!TaskClassName);
        AFL_VERIFY(!!TaskIdentifier);
        AFL_VERIFY(SSTabletId);
    }
    virtual void DoOnFail(const TString& errorMessage) const override {

    }
    virtual void DoOnAdded() const override {

    }
    virtual void DoOnFinished() const override {

    }
    virtual TProtoStorage DoSerializeToProto() const override {
        TProtoStorage result;
        result.SetTaskClassName(TaskClassName);
        result.SetTaskIdentifier(TaskIdentifier);
        result.SetSSTabletId(SSTabletId);
        return result;
    }
    virtual TConclusionStatus DoDeserializeFromProto(const TProtoStorage& proto) override {
        TaskClassName = proto.GetTaskClassName();
        TaskIdentifier = result.GetTaskIdentifier();
        SSTabletId = result.GetSSTabletId();
        return TConclusionStatus::Success();
    }
};

class TTransactionsActor: public NOlap::NBackground::TSessionActor {
private:
    std::shared_ptr<TSessionResharding> SessionLogic;
protected:
    virtual void OnTxCompleted(const ui64 txInternalId) override {

    }
    virtual void OnSessionProgressSaved() override {

    }
    virtual void OnSessionStateSaved() override {
        if (SessionLogic->GetStepForExecute() < SessionLogic->GetTransactions().size()) {
            SendTransactionForExecute(SessionLogic->GetTransactions()[SessionLogic->GetStepForExecute()]);
        }
    }
    virtual void OnBootstrap(const TActorContext& ctx) override {
        Become(&TTransactionsActor::StateInProgress);
        SessionLogic = Session.GetLogicAsVerifiedPtr<TSessionResharding>();
        AFL_VERIFY(SessionLogic->GetStepForExecute() < SessionLogic->GetTransactions().size());
        SendTransactionForExecute(SessionLogic->GetTransactions()[SessionLogic->GetStepForExecute()]);
    }

    void SendTransactionForExecute(const NKikimrSchemeOp::TModifyScheme& modification) const {
        auto ev = std::make_unique<NKikimr::NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>(txId, SSTabletId);
        *ev->Record.AddTransaction() = modification;
        NActors::TActivationContext::AsActorContext().Send(MakePipePeNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.release(), SSTabletId, true), IEventHandle::FlagTrackDelivery, SessionLogic->GetStepForExecute());
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
        SessionLogic->NextStep();
        SaveSessionProgress();
    }
public:
    STATEFN(StateInProgress) {
        const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_BACKGROUND)("SelfId", SelfId())("TabletId", TabletId);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);
        default:
            TBase::StateInProgress(ev);
        }
    }

};

class TSessionTransactions: public NOlap::NBackground::TSessionProtoAdapter<
    NKikimrSchemeShardTxBackgroundProto::TSessionTransactionsLogic, 
    NKikimrSchemeShardTxBackgroundProto::TSessionTransactionsProgress,
    NKikimrSchemeShardTxBackgroundProto::TSessionTransactionsState> {
public:
    static TString GetStaticClassName() {
        return "SS::BG::TRANSACTIONS";
    }
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<TProtoStorage, NOlap::NBackground::ISessionLogic>;
    YDB_READONLY_DEF(TString, TablePath);
    YDB_READONLY_DEF(std::vector<NKikimrSchemeOp::TModifyScheme>, Transactions);
    YDB_READONLY(ui32, StepForExecute, 0);
protected:
    virtual TConclusionStatus DoDeserializeFromProto(const TProtoLogic& proto) override {
        TablePath = proto.GetTablePath();
        for (auto&& i : proto.GetModifications()) {
            Transactions.emplace_back(i.GetTransaction());
        }
        return TConclusionStatus::Success();
    }
    virtual TProtoLogic DoSerializeToProto() const override {
        TProtoLogic result;
        result.SetTablePath(TablePath);
        for (auto&& i : Transactions) {
            *result.AddModification()->MutableTransaction() = i;
        }
        return result;
    }
    virtual TConclusionStatus DoDeserializeProgressFromProto(const TProtoProgress& proto) override {
        StepForExecute = proto.GetStepForExecute();
        return TConclusionStatus::Success();
    }
    virtual TProtoProgress DoSerializeProgressToProto() const override {
        TProtoProgress proto;
        proto.SetStepForExecute(StepForExecute);
        return proto;
    }
    virtual TConclusionStatus DoDeserializeStateFromProto(const TProtoState& proto) override {
        return TConclusionStatus::Success();
    }
    virtual TProtoState DoSerializeStateToProto() const override {

    }
public:
    virtual TString GetClassName() const override {
        return GetStaticClassName();
    }
    virtual bool IsReadyForStart() const override {
        return true;
    }
    virtual bool IsFinished() const override {
        AFL_VERIFY(StepForExecute <= Transactions.size())
        return StepForExecute == Transactions.size();
    }
    virtual bool IsReadyForRemove() const override {
        return true;
    }
};

class TTaskResharding: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrSchemeShardTxBackgroundProto::TTaskResharding, NOlap::NBackground::ITaskDescription> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<TProtoStorage, NOlap::NBackground::ITaskDescription>;
public:
    static TString GetStaticClassName() {
        return "SS::OLAP::RESHARDING";
    }
private:
    virtual TConclusionStatus DoDeserializeFromProto(const TProtoStorage& proto) override {
        TablePath = proto.GetTablePath();
    }
    virtual TProtoStorage DoSerializeToProto() const {
        TProtoStorage result;
        result.SetTablePath(TablePath);
        return result;
    }
    virtual std::shared_ptr<ISessionLogic> DoBuildSession() const override {
        return std::make_shared<TSessionResharding>(TablePath);
    }
    static const inline TFactory::TRegistrator<TTaskResharding> Registrator = TFactory::TRegistrator<TTaskResharding>(GetStaticClassName());
public:
    virtual TString GetClassName() const override {
        return GetStaticClassName();
    }
};

}