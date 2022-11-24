#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/bg_tasks/abstract/activity.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NColumnShard::NTiers {

class TTaskCleanerActivity: public NBackgroundTasks::IProtoStringSerializable<NKikimrSchemeOp::TTaskCleaner, NBackgroundTasks::ITaskActivity> {
private:
    YDB_READONLY_DEF(TString, TieringId);
    YDB_READONLY(ui64, PathId, 0);
    static TFactory::TRegistrator<TTaskCleanerActivity> Registrator;
protected:
    virtual NKikimrSchemeOp::TTaskCleaner DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TTaskCleaner& protoData) override;

    virtual void DoExecute(NBackgroundTasks::ITaskExecutorController::TPtr controller,
        const NBackgroundTasks::TTaskStateContainer& /*state*/) override;
public:
    TTaskCleanerActivity() = default;

    TTaskCleanerActivity(const TString& tieringId, const ui64 pathId)
        : TieringId(tieringId)
        , PathId(pathId)
    {

    }

    static TString GetClassNameStatic() {
        return "sso_path_tiers_cleaner";
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}
