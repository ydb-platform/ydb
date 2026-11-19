#include "events.h"
#include "writer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/tablet_flat/flat_executor_backup.h>
#include <ydb/core/tablet_flat/flat_executor_backup_common.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/util.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <deque>
#include <memory>
#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard::NLocalDbBackup {

using namespace NTabletFlatExecutor::NBackup;
using namespace NWrappers;

namespace {

TString JoinKey(const TString& prefix, TStringBuf suffix) {
    if (prefix.empty()) {
        return TString(suffix);
    }
    if (prefix.EndsWith('/')) {
        return prefix + suffix;
    }
    return TStringBuilder() << prefix << '/' << suffix;
}

class TS3SnapshotWriter: public NActors::TActorBootstrapped<TS3SnapshotWriter> {
    enum class EPhase {
        WaitingStart,   // TEvBackupStarted has not arrived yet, chunks are queued
        Schema,
        Tables,
        Manifest,
        ManifestChecksum,
    };

    struct TTableState {
        TString Name;
        TString Key;
        TSha256Hasher Sha256;
        TBuffer Buffer;   // data not yet uploaded
        TString UploadId;   // empty while multipart upload is not started
        TVector<TString> ETags;
        bool Done = false;
    };

    struct TChunk {
        NActors::TActorId Scan;
        TTableId TableId = 0;
        TBuffer Data;
        EScanStatus Status = EScanStatus::InProgress;
    };

public:
    TS3SnapshotWriter(const NActors::TActorId& owner, ui64 tabletId, TS3SnapshotWriterParams&& params)
        : Owner(owner)
        , TabletId(tabletId)
        , Params(std::move(params))
    {
    }

    void Bootstrap() {
        try {
            auto config = IExternalStorageConfig::Construct(AppData()->AwsClientConfig, Params.S3Settings);
            Client = Register(CreateStorageWrapper(config->ConstructStorageOperator()));
        } catch (const std::exception& e) {
            return Fail(TStringBuilder() << "Failed to initialize S3 client: " << e.what());
        }

        Become(&TThis::StateWork);
        Schedule(Params.StartTimeout, new NActors::TEvents::TEvWakeup());
    }

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBackupStarted, Handle);
            hFunc(TEvWriteSnapshot, Handle);
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle);
            hFunc(TEvExternalStorage::TEvCreateMultipartUploadResponse, Handle);
            hFunc(TEvExternalStorage::TEvUploadPartResponse, Handle);
            hFunc(TEvExternalStorage::TEvCompleteMultipartUploadResponse, Handle);
            sFunc(NActors::TEvents::TEvWakeup, HandleStartTimeout);
            sFunc(NActors::TEvents::TEvPoison, HandlePoison);
        }
    }

    // --- start: the scans are queued, we know what we are writing ---

    void Handle(TEvBackupStarted::TPtr& ev) {
        if (Phase != EPhase::WaitingStart) {
            return Fail("Got TEvBackupStarted twice");
        }
        auto* msg = ev->Get();
        Generation = msg->Generation;
        Step = msg->Step;
        SnapshotPath = JoinKey(msg->BackupPath, "snapshot");

        Send(Owner, TEvBackupProgress::MakeInit(msg->Tables));
        for (const auto& [tableId, name] : msg->Tables) {
            TTableState state;
            state.Name = name;
            state.Key = JoinKey(SnapshotPath, name + ".json");
            Tables.emplace(tableId, std::move(state));
        }

        YDB_LOG_NOTICE("LocalDbBackup: starting snapshot", {"tabletId", TabletId}, {"path", SnapshotPath}, {"tables", Tables.size()},
            {"generation", Generation}, {"step", Step});

        TStringStream schema;
        try {
            NProtobufJson::Proto2Json(*msg->Schema, schema,
                {
                    .EnumMode = NProtobufJson::TProto2JsonConfig::EnumName,
                    .FieldNameMode = NProtobufJson::TProto2JsonConfig::FieldNameSnakeCaseDense,
                    .MapAsObject = true,
                });
        } catch (const std::exception& e) {
            return Fail(TStringBuilder() << "Failed to serialize schema: " << e.what());
        }
        SchemaSha256.Update(schema.Data(), schema.Size());
        WrittenBytes += schema.Size();
        Send(Owner, TEvBackupProgress::MakeBytes(schema.Size()));
        Send(Owner, TEvBackupProgress::MakeStage("Uploading schema"));

        Phase = EPhase::Schema;
        PutObject(JoinKey(SnapshotPath, "schema.json"), TString(schema.Str()));
    }

    void HandleStartTimeout() {
        if (Phase == EPhase::WaitingStart) {
            Fail(TStringBuilder() << "The tablet did not start the local DB scans within " << Params.StartTimeout);
        }
    }

    // --- S3 requests ---

    void PutObject(const TString& key, TString&& body) {
        RequestInFlight = true;
        Send(
            Client, std::make_unique<TEvExternalStorage::TEvPutObjectRequest>(Aws::S3::Model::PutObjectRequest().WithKey(key), std::move(body)));
    }

    void UploadBufferedPart(TTableState& table) {
        RequestInFlight = true;
        auto request =
            Aws::S3::Model::UploadPartRequest().WithKey(table.Key).WithUploadId(table.UploadId).WithPartNumber(table.ETags.size() + 1);
        Send(Client, std::make_unique<TEvExternalStorage::TEvUploadPartRequest>(request, TString(table.Buffer.Data(), table.Buffer.Size())));
        table.Buffer.Clear();
    }

    template <class TResponse>
    bool CheckResult(const TResponse& response, TStringBuf operation, const TString& key) {
        if (response.IsSuccess()) {
            return true;
        }
        const auto& error = response.GetError();
        Fail(TStringBuilder() << operation << " failed for key " << key << ": " << TString(error.GetExceptionName().c_str()) << ": "
                              << TString(error.GetMessage().c_str()));
        return false;
    }

    // --- table chunks produced by NBackup::CreateSnapshotScan ---

    void Handle(TEvWriteSnapshot::TPtr& ev) {
        auto* msg = ev->Get();
        YDB_LOG_DEBUG("LocalDbBackup: got snapshot chunk", {"tabletId", TabletId}, {"tableId", msg->TableId},
            {"bytes", msg->SnapshotData.Size()}, {"status", (ui32)msg->ScanStatus});
        Queue.push_back(TChunk{ ev->Sender, msg->TableId, std::move(msg->SnapshotData), msg->ScanStatus });
        ProcessQueue();
    }

    void ProcessQueue() {
        while (Phase == EPhase::Tables && !RequestInFlight && !Current && !Queue.empty()) {
            Current = std::move(Queue.front());
            Queue.pop_front();

            auto it = Tables.find(Current->TableId);
            if (it == Tables.end()) {
                return Fail(TStringBuilder() << "Got snapshot chunk for unknown table " << Current->TableId);
            }
            auto& table = it->second;
            if (table.Done) {
                return Fail(TStringBuilder() << "Got snapshot chunk for already finished table " << table.Name);
            }

            table.Sha256.Update(Current->Data.Data(), Current->Data.Size());
            table.Buffer.Append(Current->Data.Data(), Current->Data.Size());
            WrittenBytes += Current->Data.Size();
            Send(Owner, TEvBackupProgress::MakeChunk(Current->TableId, Current->Data.Size()));

            switch (Current->Status) {
                case EScanStatus::InProgress:
                    if (table.Buffer.Size() < Params.UploadPartBytes) {
                        // Just buffered, let the scan continue right away.
                        Send(Current->Scan, std::make_unique<TEvWriteSnapshotAck>());
                        Current.reset();
                        continue;
                    }
                    if (table.UploadId.empty()) {
                        CreateMultipartUpload(table);
                    } else {
                        UploadBufferedPart(table);
                    }
                    return;
                case EScanStatus::Done:
                    if (table.UploadId.empty()) {
                        // The whole table fits into a single object.
                        PutObject(table.Key, TString(table.Buffer.Data(), table.Buffer.Size()));
                        table.Buffer.Clear();
                    } else if (table.Buffer.Size() == 0) {
                        // Finish() sends an empty tail after the last part was already uploaded.
                        // S3 rejects a 0-byte UploadPart.
                        CompleteMultipartUpload(table);
                    } else {
                        UploadBufferedPart(table);
                    }
                    return;
                case EScanStatus::Lost:
                    return ScanFailed(table.Name, "Owner entity is lost");
                case EScanStatus::Term:
                    return ScanFailed(table.Name, "Explicit process termination by owner");
                case EScanStatus::StorageError:
                    return ScanFailed(table.Name, "Some blob has been failed to load");
                case EScanStatus::Exception:
                    return ScanFailed(table.Name, "Unhandled exception has happened");
            }
        }
    }

    void CreateMultipartUpload(TTableState& table) {
        RequestInFlight = true;
        Send(Client, std::make_unique<TEvExternalStorage::TEvCreateMultipartUploadRequest>(
                         Aws::S3::Model::CreateMultipartUploadRequest().WithKey(table.Key)));
    }

    void CompleteMultipartUpload(TTableState& table) {
        RequestInFlight = true;
        TVector<Aws::S3::Model::CompletedPart> parts(Reserve(table.ETags.size()));
        for (ui32 i = 0; i < table.ETags.size(); ++i) {
            parts.emplace_back(Aws::S3::Model::CompletedPart().WithPartNumber(i + 1).WithETag(table.ETags[i]));
        }
        auto request = Aws::S3::Model::CompleteMultipartUploadRequest()
                           .WithKey(table.Key)
                           .WithUploadId(table.UploadId)
                           .WithMultipartUpload(Aws::S3::Model::CompletedMultipartUpload().WithParts(std::move(parts)));
        Send(Client, std::make_unique<TEvExternalStorage::TEvCompleteMultipartUploadRequest>(request));
    }

    void ScanFailed(const TString& tableName, const TString& error) {
        Fail(TStringBuilder() << "Snapshot scan for " << tableName << " failed: " << error);
    }

    TTableState& CurrentTable() {
        Y_ABORT_UNLESS(Current);
        return Tables.at(Current->TableId);
    }

    void Handle(TEvExternalStorage::TEvCreateMultipartUploadResponse::TPtr& ev) {
        auto& table = CurrentTable();
        if (!CheckResult(*ev->Get(), "CreateMultipartUpload", table.Key)) {
            return;
        }
        table.UploadId = ev->Get()->GetResult().GetUploadId().c_str();
        UploadBufferedPart(table);
    }

    void Handle(TEvExternalStorage::TEvUploadPartResponse::TPtr& ev) {
        auto& table = CurrentTable();
        if (!CheckResult(*ev->Get(), "UploadPart", table.Key)) {
            return;
        }
        table.ETags.emplace_back(ev->Get()->GetResult().GetETag().c_str());

        if (Current->Status == EScanStatus::InProgress) {
            Send(Current->Scan, std::make_unique<TEvWriteSnapshotAck>());
            Current.reset();
            RequestInFlight = false;
            return ProcessQueue();
        }

        CompleteMultipartUpload(table);
    }

    void Handle(TEvExternalStorage::TEvCompleteMultipartUploadResponse::TPtr& ev) {
        auto& table = CurrentTable();
        if (!CheckResult(*ev->Get(), "CompleteMultipartUpload", table.Key)) {
            return;
        }
        table.UploadId.clear();
        TableDone(table);
    }

    void TableDone(TTableState& table) {
        table.Done = true;
        ++DoneTables;
        Send(Owner, TEvBackupProgress::MakeTableDone(Current->TableId));
        Current.reset();
        RequestInFlight = false;
        YDB_LOG_DEBUG("LocalDbBackup: table snapshot done", {"tabletId", TabletId}, {"table", table.Name},
            {"done", DoneTables}, {"total", Tables.size()});
        if (DoneTables == Tables.size()) {
            return Finalize();
        }
        ProcessQueue();
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const TString key = ev->Get()->Key.value_or(TString());
        if (!CheckResult(*ev->Get(), "PutObject", key)) {
            return;
        }
        RequestInFlight = false;

        switch (Phase) {
            case EPhase::WaitingStart:
                return Fail("Got a PutObject response before the backup was started");
            case EPhase::Schema:
                Phase = EPhase::Tables;
                Send(Owner, TEvBackupProgress::MakeStage("Uploading tables"));
                if (Tables.empty()) {
                    return Finalize();
                }
                return ProcessQueue();
            case EPhase::Tables:
                return TableDone(CurrentTable());
            case EPhase::Manifest:
                Phase = EPhase::ManifestChecksum;
                WrittenBytes += ManifestChecksum.size();
                return PutObject(JoinKey(SnapshotPath, "manifest.json.sha256"), TString(ManifestChecksum));
            case EPhase::ManifestChecksum:
                return FinishSuccess();
        }
    }

    static NJson::TJsonValue MakeFileEntry(const TString& name, TSha256Hasher& hasher) {
        NJson::TJsonValue entry;
        entry["name"] = name;
        entry["sha256"] = hasher.Final();
        return entry;
    }

    // Same manifest as in NTabletFlatExecutor::NBackup::TSnapshotWriter::Finalize.
    void Finalize() {
        Phase = EPhase::Manifest;
        Send(Owner, TEvBackupProgress::MakeStage("Writing manifest"));

        NJson::TJsonValue manifest;
        TString tabletTypeName = TTabletTypes::EType_Name(TTabletTypes::ColumnShard);
        NProtobufJson::ToSnakeCaseDense(&tabletTypeName);
        manifest["tablet_type"] = tabletTypeName;
        manifest["tablet_id"] = TabletId;
        manifest["generation"] = Generation;
        manifest["step"] = Step;

        auto& files = manifest["files"];
        files.SetType(NJson::JSON_ARRAY);
        files.AppendValue(MakeFileEntry("schema.json", SchemaSha256));
        for (auto& [_, table] : Tables) {
            files.AppendValue(MakeFileEntry(table.Name + ".json", table.Sha256));
        }

        TString manifestStr = NJson::WriteJson(manifest, /*formatOutput=*/false);
        ManifestChecksum = TSha256Hasher::Hash(manifestStr);
        WrittenBytes += manifestStr.size();
        Send(Owner, TEvBackupProgress::MakeBytes(manifestStr.size() + ManifestChecksum.size()));

        PutObject(JoinKey(SnapshotPath, "manifest.json"), std::move(manifestStr));
    }

    // --- completion ---

    void FinishSuccess() {
        YDB_LOG_NOTICE("LocalDbBackup: snapshot finalized", {"tabletId", TabletId}, {"path", SnapshotPath}, {"bytes", WrittenBytes});
        Send(Owner, TEvBackupProgress::MakeStage("Completed"));
        Send(Owner, std::make_unique<TEvSnapshotCompleted>(WrittenBytes));
        PassAway();
    }

    void Fail(const TString& error) {
        YDB_LOG_ERROR("LocalDbBackup: snapshot failed", {"tabletId", TabletId}, {"path", SnapshotPath}, {"error", error});
        Send(Owner, TEvBackupProgress::MakeStage("Failed"));
        AbortUploads();
        Send(Owner, std::make_unique<TEvSnapshotCompleted>(error));
        PassAway();
    }

    void HandlePoison() {
        AbortUploads();
        PassAway();
    }

    void AbortUploads() {
        if (!Client) {
            return;
        }
        for (auto& [_, table] : Tables) {
            if (!table.UploadId.empty() && !table.Done) {
                Send(Client, std::make_unique<TEvExternalStorage::TEvAbortMultipartUploadRequest>(
                                 Aws::S3::Model::AbortMultipartUploadRequest().WithKey(table.Key).WithUploadId(table.UploadId)));
            }
        }
    }

    void PassAway() override {
        if (Client) {
            Send(Client, std::make_unique<NActors::TEvents::TEvPoison>());
        }
        NActors::TActorBootstrapped<TS3SnapshotWriter>::PassAway();
    }

private:
    const NActors::TActorId Owner;
    const ui64 TabletId;
    TS3SnapshotWriterParams Params;
    NActors::TActorId Client;

    // Known since TEvBackupStarted.
    TGeneration Generation = 0;
    TStep Step = 0;
    TString SnapshotPath;

    EPhase Phase = EPhase::WaitingStart;
    bool RequestInFlight = false;

    TMap<TTableId, TTableState> Tables;
    size_t DoneTables = 0;
    std::deque<TChunk> Queue;
    std::optional<TChunk> Current;

    TSha256Hasher SchemaSha256;
    TString ManifestChecksum;
    ui64 WrittenBytes = 0;
};

}   // namespace

// Same naming as NTabletFlatExecutor::NBackup::CreateBackupPath, but relative to the S3 key prefix.
TString MakeBackupPath(const TString& objectKeyPattern, ui64 tabletId, TGeneration generation, TStep step, TInstant now) {
    TString tabletTypeName = TTabletTypes::EType_Name(TTabletTypes::ColumnShard);
    NProtobufJson::ToSnakeCaseDense(&tabletTypeName);
    const TString backupDir = TStringBuilder() << "backup_" << now.FormatGmTime("%Y%m%d%H%M%SZ") << "_g" << generation << "_s" << step;
    return JoinKey(JoinKey(JoinKey(objectKeyPattern, tabletTypeName), ToString(tabletId)), backupDir);
}

std::unique_ptr<NActors::IActor> CreateS3SnapshotWriter(const NActors::TActorId& owner, ui64 tabletId, TS3SnapshotWriterParams&& params) {
    return std::make_unique<TS3SnapshotWriter>(owner, tabletId, std::move(params));
}

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
