#pragma once
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

namespace NKikimr::NOlap::NImport {

class TConfirmSessionControl: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl>;
public:
  static TString GetClassNameStatic();

private:
    virtual TConclusionStatus DoApply(const std::shared_ptr<NBackground::ISessionLogic>& session) const override;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) override;
    virtual NKikimrColumnShardExportProto::TSessionControlContainer DoSerializeToProto() const override;
    virtual TString GetClassName() const override;
    static const inline TFactory::TRegistrator<TConfirmSessionControl> Registrator = TFactory::TRegistrator<TConfirmSessionControl>(GetClassNameStatic());
public:
    using TBase::TBase;
};

class TAbortSessionControl: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardExportProto::TSessionControlContainer, NBackground::ISessionLogicControl>;
public:
  static TString GetClassNameStatic();

private:
    virtual TConclusionStatus DoApply(const std::shared_ptr<NBackground::ISessionLogic>& session) const override;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TSessionControlContainer& /*proto*/) override;
    virtual NKikimrColumnShardExportProto::TSessionControlContainer DoSerializeToProto() const override;
    virtual TString GetClassName() const override;
    static const inline TFactory::TRegistrator<TAbortSessionControl> Registrator = TFactory::TRegistrator<TAbortSessionControl>(GetClassNameStatic());
public:
    using TBase::TBase;
};
}
