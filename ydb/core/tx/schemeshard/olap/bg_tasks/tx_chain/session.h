#pragma once
#include "common.h"
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/protos/data.pb.h>

#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

class TTxChainSession: public NKikimr::NOlap::NBackground::TSessionProtoAdapter<
    NKikimrSchemeShardTxBackgroundProto::TTxChainSessionLogic, 
    NKikimrSchemeShardTxBackgroundProto::TTxChainSessionProgress,
    NKikimrSchemeShardTxBackgroundProto::TTxChainSessionState> {
public:
    static TString GetStaticClassName() {
        return "SS::BG::TX_CHAIN";
    }
private:
    using TBase = NKikimr::NOlap::NBackground::TSessionProtoAdapter<TProtoStorage, TProtoProgress, TProtoState>;
    YDB_READONLY_DEF(TTxChainData, TxData);
    YDB_READONLY(ui32, StepForExecute, 0);
    std::optional<ui64> CurrentTxId;
protected:
    virtual TConclusion<std::unique_ptr<NActors::IActor>> DoCreateActor(const NKikimr::NOlap::NBackground::TStartContext& context) const override;
    virtual TConclusionStatus DoDeserializeFromProto(const TProtoLogic& proto) override {
        return TxData.DeserializeFromProto(proto.GetCommonData());
    }
    virtual TProtoLogic DoSerializeToProto() const override {
        TProtoLogic result;
        *result.MutableCommonData() = TxData.SerializeToProto();
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
    virtual TConclusionStatus DoDeserializeStateFromProto(const TProtoState& /*proto*/) override {
        return TConclusionStatus::Success();
    }
    virtual TProtoState DoSerializeStateToProto() const override {
        TProtoState result;
        return result;
    }
public:
    TTxChainSession(const TTxChainData& data)
        : TxData(data)
    {

    }

    ui64 GetCurrentTxIdVerified() const {
        AFL_VERIFY(!!CurrentTxId);
        return *CurrentTxId;
    }

    void SetCurrentTxId(const ui64 value) {
        AFL_VERIFY(!CurrentTxId);
        CurrentTxId = value;
    }

    void ResetCurrentTxId() {
        AFL_VERIFY(!!CurrentTxId);
        CurrentTxId.reset();
    }

    bool HasCurrentTxId() const {
        return !!CurrentTxId;
    }

    void NextStep() {
        AFL_VERIFY(++StepForExecute <= GetTxData().GetTransactions().size());
    }

    virtual TString GetClassName() const override {
        return GetStaticClassName();
    }
    virtual bool IsReadyForStart() const override {
        return true;
    }
    virtual bool IsFinished() const override {
        AFL_VERIFY(StepForExecute <= GetTxData().GetTransactions().size());
        return StepForExecute == GetTxData().GetTransactions().size();
    }
    virtual bool IsReadyForRemoveOnFinished() const override {
        return true;
    }
};

}