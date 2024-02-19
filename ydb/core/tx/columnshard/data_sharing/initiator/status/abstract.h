#pragma once
#include <ydb/core/tx/columnshard/data_sharing/protos/initiator.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NOlap::NDataSharing {

enum class EStatus {
    Undefined,
    NotFound,
    StartFailed,
    InProgress
};

class IStatus {
private:
    YDB_READONLY(EStatus, Status, EStatus::Undefined);
    YDB_READONLY_DEF(TString, SessionId);
protected:
    virtual NJson::TJsonValue DoDebugJson() const {
        return NJson::JSON_NULL;
    }
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TStatus& proto) = 0;
    virtual void DoSerializeFromProto(NKikimrColumnShardDataSharingProto::TInitiator::TStatus& proto) const = 0;
public:
    using TProto = NKikimrColumnShardDataSharingProto::TInitiator::TStatus;
    using TFactory = NObjectFactory::TObjectFactory<IStatus, TString>;
    IStatus(const EStatus status, const TString& sessionId)
        : Status(status)
        , SessionId(sessionId)
    {
        AFL_VERIFY(SessionId);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TInitiator::TStatus& proto) {
        if (!TryFromString(proto.GetClassName(), Status)) {
            return TConclusionStatus::Fail("cannot parse class name as status: " + proto.GetClassName());
        }
        SessionId = proto.GetSessionId();
        return DoDeserializeFromProto(proto);
    }
    void SerializeToProto(NKikimrColumnShardDataSharingProto::TInitiator::TStatus& proto) const {
        *proto.MutableSessionId() = SessionId;
        return DoSerializeFromProto(proto);
    }

    TString GetClassName() const {
        return ::ToString(Status);
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("class_name", GetClassName());
        result.InsertValue("session_id", SessionId);
        auto detailsJson = DoDebugJson();
        if (!detailsJson.IsNull()) {
            result.InsertValue("details", std::move(detailsJson));
        }
        return result;
    }
};

class TStatusContainer: public NBackgroundTasks::TInterfaceProtoContainer<IStatus> {

};
}