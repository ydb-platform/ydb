#pragma once
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

namespace NKikimr::NOlap::NExport {

class TConfirmSessionControl: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl>;
public:
    static TString GetClassNameStatic() {
        return "CS::EXPORT::CONFIRM";
    }
private:
    virtual TConclusionStatus DoApply(const std::shared_ptr<NBackground::ISessionLogic>& session) const override;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) override {
        return TConclusionStatus::Success();
    }
    virtual NKikimrColumnShardExportProto::TSessionControlContainer DoSerializeToProto() const override {
        NKikimrColumnShardExportProto::TSessionControlContainer result;
        return result;
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
    static const inline TFactory::TRegistrator<TConfirmSessionControl> Registrator = TFactory::TRegistrator<TConfirmSessionControl>(GetClassNameStatic());
public:
    using TBase::TBase;
};

class TAbortSessionControl: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl>;
public:
    static TString GetClassNameStatic() {
        return "CS::EXPORT::ABORT";
    }
private:
    virtual TConclusionStatus DoApply(const std::shared_ptr<NBackground::ISessionLogic>& session) const override;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) override {
        return TConclusionStatus::Success();
    }
    virtual NKikimrColumnShardExportProto::TSessionControlContainer DoSerializeToProto() const override {
        NKikimrColumnShardExportProto::TSessionControlContainer result;
        return result;
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
    static const inline TFactory::TRegistrator<TAbortSessionControl> Registrator = TFactory::TRegistrator<TAbortSessionControl>(GetClassNameStatic());
public:
    using TBase::TBase;
};
}
