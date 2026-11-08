#pragma once
#include "task.h"

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/backup/import/protos/task.pb.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>
#include <ydb/core/tx/datashard/datashard_s3_download.h>

#include <contrib/libs/protobuf/src/google/protobuf/empty.pb.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NTabletFlatExecutor {
class ITransaction;
}

namespace NKikimr::NOlap {
class IStoragesManager;
}

namespace NKikimr::NOlap::NImport {

// Saved with the written rows, so that an import restarts from them
struct TImportProgress {
    bool ManifestLoaded = false;
    // 0: a single data_NN file
    ui32 FilesCount = 0;
    ui32 FileIdx = 0;
    std::optional<NDataShard::TS3Download> Download;

    NKikimrColumnShardImportProto::TImportProgress SerializeToProto() const;
    void DeserializeFromProto(const NKikimrColumnShardImportProto::TImportProgress &proto);
};

class TSession: public NBackground::TSessionProtoAdapter<NKikimrColumnShardImportProto::TImportSessionLogic,
                    NKikimrColumnShardImportProto::TImportProgress, NKikimrColumnShardImportProto::TImportSessionState> {
public:
    static TString GetClassNameStatic();

    enum class EStatus: ui64 {
        Draft = 0 /*"draft"*/,
        Confirmed = 1 /*"confirmed"*/,
        Started = 2 /*"started"*/,
        Finished = 3 /*"finished"*/,
        Aborted = 4 /*"aborted"*/
    };

private:
    std::shared_ptr<TImportTask> Task;
    TString ErrorMessage;
    mutable EStatus Status = EStatus::Draft;
    TImportProgress Progress;

    virtual TConclusion<std::unique_ptr<NActors::IActor>> DoCreateActor(const NBackground::TStartContext &context) const override;

    virtual TConclusionStatus DoDeserializeProgressFromProto(const TProtoProgress & /* proto */) override;

    virtual TProtoProgress DoSerializeProgressToProto() const override;

    virtual TConclusionStatus DoDeserializeStateFromProto(const TProtoState &proto) override;

    virtual TProtoState DoSerializeStateToProto() const override;

    virtual TConclusionStatus DoDeserializeFromProto(const TProtoLogic &proto) override;

    virtual TProtoLogic DoSerializeToProto() const override;

    static const inline TFactory::TRegistrator<TSession> Registrator = TFactory::TRegistrator<TSession>(GetClassNameStatic());

public:
    std::optional<ui64> GetTxId() const;

    virtual bool IsReadyForStart() const override;

    virtual bool IsFinished() const override;

    virtual bool IsReadyForRemoveOnFinished() const override;

    virtual TStatus GetStatus() const override;

    virtual TString GetClassName() const override;

    TSession() = default;

    TSession(const std::shared_ptr<TImportTask> &task);

    bool IsConfirmed() const;

    TString DebugString() const;

    bool IsDraft() const;

    void Confirm();

    void Abort(const TString &errorMessage);

    bool IsStarted() const;

    const TImportTask &GetTask() const;

    const TImportProgress &GetProgress() const {
        return Progress;
    }

    TImportProgress &MutableProgress() {
        return Progress;
    }

    const NColumnShard::TSchemeShardLocalPathId GetSchemeShardLocalPathId() const;

    void Finish();
};

}   // namespace NKikimr::NOlap::NImport
