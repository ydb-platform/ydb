#include "session.h"

#include <ydb/core/tx/columnshard/backup/import/import_actor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NImport {

NKikimr::TConclusion<std::unique_ptr<NActors::IActor>> TSession::DoCreateActor(const NBackground::TStartContext &context) const {
    AFL_VERIFY(IsConfirmed());
    Status = EStatus::Started;
    return std::make_unique<TImportActor>(context.GetSessionSelfPtr(), context.GetAdapter());
}

void TSession::Finish() {
    AFL_VERIFY(Status == EStatus::Started);
    Status = EStatus::Finished;
}

const NColumnShard::TSchemeShardLocalPathId TSession::GetSchemeShardLocalPathId() const {
    return Task->GetSchemeShardLocalPathId();
}

const TImportTask &TSession::GetTask() const {
    return *Task;
}

bool TSession::IsStarted() const {
    return Status == EStatus::Started;
}

void TSession::Abort(const TString &errorMessage) {
    if (Status == EStatus::Finished || Status == EStatus::Aborted) {
        YDB_LOG_DEBUG("",
            {"event", "import_session_abort_ignored"},
            {"status", (ui64)Status},
            {"message", "import session is already in terminal state"});
        return;
    }
    Status = EStatus::Aborted;
    ErrorMessage = errorMessage;
}

void TSession::Confirm() {
    AFL_VERIFY(IsDraft());
    Status = EStatus::Confirmed;
}

bool TSession::IsDraft() const {
    return Status == EStatus::Draft;
}

TString TSession::DebugString() const {
    return TStringBuilder() << "task=" << Task->DebugString() << ";status=" << Status;
}

bool TSession::IsConfirmed() const {
    return Status == EStatus::Confirmed;
}

TSession::TSession(const std::shared_ptr<TImportTask> &task)
    : Task(task)
{
    AFL_VERIFY(Task);
}

TString TSession::GetClassName() const {
    return GetClassNameStatic();
}

bool TSession::IsReadyForRemoveOnFinished() const {
    return Status == EStatus::Aborted;
}

TSession::TStatus TSession::GetStatus() const {
    return TStatus{ Status == EStatus::Finished, ErrorMessage };
}

bool TSession::IsFinished() const {
    return Status == EStatus::Finished;
}

bool TSession::IsReadyForStart() const {
    return Status == EStatus::Confirmed;
}

std::optional<ui64> TSession::GetTxId() const {
    return Task->GetTxId();
}

TSession::TProtoLogic TSession::DoSerializeToProto() const {
    TProtoLogic result;
    *result.MutableTask() = Task->SerializeToProto();
    return result;
}

TConclusionStatus TSession::DoDeserializeFromProto(const TProtoLogic &proto) {
    Task = std::make_shared<TImportTask>();
    return Task->DeserializeFromProto(proto.GetTask());
}

TSession::TProtoState TSession::DoSerializeStateToProto() const {
    TProtoState result;
    result.SetStatus(::ToString(Status));
    return result;
}

TConclusionStatus TSession::DoDeserializeStateFromProto(const TProtoState &proto) {
    if (!TryFromString(proto.GetStatus(), Status)) {
        return TConclusionStatus::Fail("cannot read status from proto: " + proto.GetStatus());
    }
    return TConclusionStatus::Success();
}

NKikimrColumnShardImportProto::TImportProgress TImportProgress::SerializeToProto() const {
    NKikimrColumnShardImportProto::TImportProgress result;
    result.SetManifestLoaded(ManifestLoaded);
    result.SetFilesCount(FilesCount);
    result.SetFileIdx(FileIdx);
    if (Download) {
        auto &download = *result.MutableDownload();
        if (Download->DataETag) {
            download.SetDataETag(*Download->DataETag);
        }
        download.SetProcessedBytes(Download->ProcessedBytes);
        download.SetWrittenBytes(Download->WrittenBytes);
        download.SetWrittenRows(Download->WrittenRows);
        *download.MutableProcessedChecksumState() = Download->ProcessedChecksumState;
        *download.MutableDownloadState() = Download->DownloadState;
    }
    return result;
}

void TImportProgress::DeserializeFromProto(const NKikimrColumnShardImportProto::TImportProgress &proto) {
    ManifestLoaded = proto.GetManifestLoaded();
    FilesCount = proto.GetFilesCount();
    FileIdx = proto.GetFileIdx();
    Download.reset();
    if (proto.HasDownload()) {
        const auto &download = proto.GetDownload();
        auto &info = Download.emplace();
        if (download.HasDataETag()) {
            info.DataETag = download.GetDataETag();
        }
        info.ProcessedBytes = download.GetProcessedBytes();
        info.WrittenBytes = download.GetWrittenBytes();
        info.WrittenRows = download.GetWrittenRows();
        info.ProcessedChecksumState = download.GetProcessedChecksumState();
        info.DownloadState = download.GetDownloadState();
    }
}

TSession::TProtoProgress TSession::DoSerializeProgressToProto() const {
    return Progress.SerializeToProto();
}

TConclusionStatus TSession::DoDeserializeProgressFromProto(const TProtoProgress &proto) {
    Progress.DeserializeFromProto(proto);
    return TConclusionStatus::Success();
}

TString TSession::GetClassNameStatic() {
    return "CS::IMPORT";
}

}   // namespace NKikimr::NOlap::NImport
