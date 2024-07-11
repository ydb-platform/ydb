#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NDataSharing {

class TTestInitiatorController: public IInitiatorController {
public:
    static TString GetClassNameStatic() {
        return "TEST";
    }
private:
    static inline TFactory::TRegistrator<TTestInitiatorController> Registrator = TFactory::TRegistrator<TTestInitiatorController>(GetClassNameStatic());
protected:
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) override {
        return TConclusionStatus::Success();
    }
    virtual void DoSerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TController& /*proto*/) const override {

    }

    virtual void DoStatus(const TStatusContainer& /*status*/) const override {

    }
    virtual void DoProposeError(const TString& /*sessionId*/, const TString& /*message*/) const override {
    }
    virtual void DoProposeSuccess(const TString& /*sessionId*/) const override {
    }
    virtual void DoConfirmSuccess(const TString& /*sessionId*/) const override {
    }
    virtual void DoFinished(const TString& /*sessionId*/) const override {
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}