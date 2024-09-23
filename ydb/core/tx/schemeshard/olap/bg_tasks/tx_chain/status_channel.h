#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/status_channel.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/protos/data.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard::NOlap::NBackground {

class TStatusChannel: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrSchemeShardTxBackgroundProto::TStatusChannel, NKikimr::NOlap::NBackground::IStatusChannel> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<NKikimrSchemeShardTxBackgroundProto::TStatusChannel, NKikimr::NOlap::NBackground::IStatusChannel>;
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
    virtual void DoOnFail(const TString& /*errorMessage*/) const override {

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
        TaskIdentifier = proto.GetTaskIdentifier();
        SSTabletId = proto.GetSSTabletId();
        return TConclusionStatus::Success();
    }
};

}