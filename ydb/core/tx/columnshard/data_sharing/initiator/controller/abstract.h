#pragma once
#include <ydb/core/tx/columnshard/data_sharing/initiator/status/abstract.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/initiator.pb.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NOlap::NDataSharing {

class IInitiatorController {
protected:
    virtual void DoProposeError(const TString& sessionId, const TString& message) const = 0;
    virtual void DoProposeSuccess(const TString& sessionId) const = 0;
    virtual void DoConfirmSuccess(const TString& sessionId) const = 0;
    virtual void DoFinished(const TString& sessionId) const = 0;
    virtual void DoStatus(const TStatusContainer& status) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TController& proto) = 0;
    virtual void DoSerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TController& proto) const = 0;
public:
    using TProto = NKikimrColumnShardDataSharingProto::TInitiator::TController;
    using TFactory = NObjectFactory::TObjectFactory<IInitiatorController, TString>;

    virtual ~IInitiatorController() = default;

    void SerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TController& proto) const {
        return DoSerializeToProto(proto);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TController& proto) {
        return DoDeserializeFromProto(proto);
    }

    void Status(const TStatusContainer& status) const {
        DoStatus(status);
    }
    void ProposeError(const TString& sessionId, const TString& message) const {
        DoProposeError(sessionId, message);
    }
    void ConfirmSuccess(const TString& sessionId) const {
        DoConfirmSuccess(sessionId);
    }
    void ProposeSuccess(const TString& sessionId) const {
        DoProposeSuccess(sessionId);
    }
    void Finished(const TString& sessionId) const {
        DoFinished(sessionId);
    }
    virtual TString GetClassName() const = 0;
};

class TInitiatorControllerContainer: public NBackgroundTasks::TInterfaceProtoContainer<IInitiatorController> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IInitiatorController>;
public:
    using TBase::TBase;

    void Status(const TStatusContainer& status) const {
        TBase::GetObjectPtrVerified()->Status(status);
    }
    void ProposeSuccess(const TString& sessionId) const {
        TBase::GetObjectPtrVerified()->ProposeSuccess(sessionId);
    }
    void ConfirmSuccess(const TString& sessionId) const {
        TBase::GetObjectPtrVerified()->ConfirmSuccess(sessionId);
    }
    void ProposeError(const TString& sessionId, const TString& message) const {
        TBase::GetObjectPtrVerified()->ProposeError(sessionId, message);
    }
    void Finished(const TString& sessionId) const {
        TBase::GetObjectPtrVerified()->Finished(sessionId);
    }
};

}