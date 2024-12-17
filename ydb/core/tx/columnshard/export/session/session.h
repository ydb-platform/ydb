#pragma once
#include "cursor.h"
#include "task.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/export/protos/cursor.pb.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NTabletFlatExecutor {
class ITransaction;
}

namespace NKikimr::NOlap {
class IStoragesManager;
}

namespace NKikimr::NOlap::NExport {
class TSession: public NBackground::TSessionProtoAdapter<NKikimrColumnShardExportProto::TExportSessionLogic,
    NKikimrColumnShardExportProto::TCursor, NKikimrColumnShardExportProto::TExportSessionState> {
public:
    static TString GetClassNameStatic() {
        return "CS::EXPORT";
    }
    enum class EStatus: ui64 {
        Draft = 0 /*"draft"*/,
        Confirmed = 1 /*"confirmed"*/,
        Started = 2 /*"started"*/,
        Finished = 3 /*"finished"*/,
        Aborted = 4 /*"aborted"*/
    };

private:
    std::shared_ptr<TExportTask> Task;
    mutable EStatus Status = EStatus::Draft;
    TCursor Cursor;

    virtual TConclusion<std::unique_ptr<NActors::IActor>> DoCreateActor(const NBackground::TStartContext& context) const override;

    virtual TConclusionStatus DoDeserializeProgressFromProto(const TProtoProgress& proto) override {
        auto cursorConclusion = TCursor::BuildFromProto(proto);
        if (cursorConclusion.IsFail()) {
            return cursorConclusion;
        }
        Cursor = cursorConclusion.DetachResult();
        return TConclusionStatus::Success();
    }
    virtual TProtoProgress DoSerializeProgressToProto() const override {
        return Cursor.SerializeToProto();
    }
    virtual TConclusionStatus DoDeserializeStateFromProto(const TProtoState& proto) override {
        if (!TryFromString(proto.GetStatus(), Status)) {
            return TConclusionStatus::Fail("cannot read status from proto: " + proto.GetStatus());
        }
        return TConclusionStatus::Success();
    }
    virtual TProtoState DoSerializeStateToProto() const override {
        TProtoState result;
        if (Status == EStatus::Started) {
            result.SetStatus(::ToString(EStatus::Confirmed));
        } else {
            result.SetStatus(::ToString(Status));
        }
        return result;
    }
    virtual TConclusionStatus DoDeserializeFromProto(const TProtoLogic& proto) override {
        Task = std::make_shared<TExportTask>();
        return Task->DeserializeFromProto(proto.GetTask());
    }
    virtual TProtoLogic DoSerializeToProto() const override {
        TProtoLogic result;
        *result.MutableTask() = Task->SerializeToProto();
        return result;
    }
    static const inline TFactory::TRegistrator<TSession> Registrator = TFactory::TRegistrator<TSession>(GetClassNameStatic());

public:
    std::optional<ui64> GetTxId() const {
        return Task->GetTxId();
    }
    virtual bool IsReadyForStart() const override {
        return Status == EStatus::Confirmed;
    }
    virtual bool IsFinished() const override {
        return Status == EStatus::Finished;
    }
    virtual bool IsReadyForRemoveOnFinished() const override {
        return Status == EStatus::Aborted;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TSession() = default;

    TSession(const std::shared_ptr<TExportTask>& task)
        : Task(task) {
        AFL_VERIFY(Task);
    }

    bool IsConfirmed() const {
        return Status == EStatus::Confirmed;
    }

    const TCursor& GetCursor() const {
        return Cursor;
    }

    TCursor& MutableCursor() {
        return Cursor;
    }

    TString DebugString() const {
        return TStringBuilder() << "task=" << Task->DebugString() << ";status=" << Status;
    }

    bool IsDraft() const {
        return Status == EStatus::Draft;
    }

    void Confirm() {
        AFL_VERIFY(IsDraft());
        Status = EStatus::Confirmed;
    }

    void Abort() {
        AFL_VERIFY(Status != EStatus::Finished && Status != EStatus::Aborted);
        Status = EStatus::Aborted;
    }

    bool IsStarted() const {
        return Status == EStatus::Started;
    }

    const TExportTask& GetTask() const {
        return *Task;
    }

    const TIdentifier& GetIdentifier() const {
        return Task->GetIdentifier();
    }

    void Finish() {
        AFL_VERIFY(Status == EStatus::Started);
        Status = EStatus::Finished;
    }
};
}   // namespace NKikimr::NOlap::NExport
