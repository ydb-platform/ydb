#pragma once
#include <ydb/core/tx/columnshard/backup/import/protos/task.pb.h>

#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/task.h>
#include <ydb/core/tx/columnshard/export/common/identifier.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

#include <ydb/library/conclusion/status.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NImport {

class TImportTask: public NBackgroundTasks::TInterfaceProtoAdapter<NKikimrColumnShardImportProto::TImportTask, NBackground::ITaskDescription> {
public:
    static TString GetClassNameStatic();
    static const inline TFactory::TRegistrator<TImportTask> Registrator = TFactory::TRegistrator<TImportTask>(GetClassNameStatic());

private:
    TInternalPathId InternalPathId;
    YDB_READONLY_DEF(std::optional<ui64>, TxId);

private:
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardImportProto::TImportTask& proto) override;
    virtual NKikimrColumnShardImportProto::TImportTask DoSerializeToProto() const override;

    virtual std::shared_ptr<NBackground::ISessionLogic> DoBuildSession() const override;

public:
    virtual TString GetClassName() const override;

    NBackground::TSessionControlContainer BuildConfirmControl() const;
    NBackground::TSessionControlContainer BuildAbortControl() const;

    const TInternalPathId GetInternalPathId() const;

    TImportTask() = default;

    TImportTask(const TInternalPathId &internalPathId, const std::optional<ui64> txId = {});

    TString DebugString() const;
};
}
