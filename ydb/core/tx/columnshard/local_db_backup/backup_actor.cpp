#include "backup_actor.h"
#include "events.h"
#include "page.h"
#include "status.h"
#include "writer.h"

#include <ydb/core/tablet_flat/flat_executor_backup.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/services/services.pb.h>

#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard::NLocalDbBackup {

using NTabletFlatExecutor::NBackup::TEvSnapshotCompleted;

namespace {

class TLocalDbBackupActor: public NActors::TActorBootstrapped<TLocalDbBackupActor> {
    // A POST that started a backup and is waiting for TEvBackupStarted to be answered with the backup path.
    struct TPendingReply {
        NActors::TActorId ReplyTo;
        NKikimrSchemeOp::TS3Settings S3;
    };

public:
    TLocalDbBackupActor(const NActors::TActorId& tablet, ui64 tabletId)
        : Tablet(tablet)
        , TabletId(tabletId)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            hFunc(TEvBackupStarted, Handle);
            hFunc(TEvSnapshotCompleted, Handle);
            sFunc(NActors::TEvents::TEvPoison, HandlePoison);
        }
    }

    // --- App page ---

    void Handle(NMon::TEvRemoteHttpInfo::TPtr& ev) {
        const auto* msg = ev->Get();
        const TInstant now = TActivationContext::Now();

        if (msg->Cgi().Get("format") == "json") {
            Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(RenderJson(Status, now)));
            return;
        }

        if (msg->GetMethod() != HTTP_METHOD_POST || !msg->ExtendedQuery) {
            ReplyPage(ev->Sender, {}, false, {});
            return;
        }

        NKikimrSchemeOp::TS3Settings s3;
        const TString formError = ParseS3Form(*msg->ExtendedQuery, s3);
        if (Status.IsActive()) {
            ReplyPage(ev->Sender, "Backup is already in progress", true, s3);
            return;
        }
        if (!formError.empty()) {
            ReplyPage(ev->Sender, formError, true, s3);
            return;
        }

        StartBackup(ev->Sender, std::move(s3));
    }

    void ReplyPage(const NActors::TActorId& to, const TString& message, bool isError, NKikimrSchemeOp::TS3Settings prefill) {
        prefill.ClearSecretKey();
        Send(to, new NMon::TEvRemoteHttpInfoRes(RenderPage(TabletId, Status, message, isError, prefill, TActivationContext::Now())));
    }

    void StartBackup(const NActors::TActorId& replyTo, NKikimrSchemeOp::TS3Settings&& s3) {
        TS3SnapshotWriterParams params;
        params.S3Settings = s3;
        params.Progress = std::make_shared<TProgress>();
        params.Progress->SetStage("Starting");

        // The writer must exist before the scans are queued: they stream rows straight to it.
        const auto progress = params.Progress;
        const NActors::TActorId writer = Register(CreateS3SnapshotWriter(SelfId(), TabletId, std::move(params)).release());
        Status.OnStarted(writer, /*backupPath=*/{}, progress, TActivationContext::Now());
        Pending = TPendingReply{ replyTo, std::move(s3) };

        // The very same IScan the system tablet backup uses: it serializes rows to JSON lines and
        // streams them to the writer with TEvWriteSnapshot / TEvWriteSnapshotAck flow control.
        Send(Tablet, new TEvStartBackup(writer));
    }

    void Handle(TEvBackupStarted::TPtr& ev) {
        if (!Status.IsActive() || !Pending) {
            YDB_LOG_WARN("LocalDbBackup: ignoring TEvBackupStarted of a finished backup", {"tabletId", TabletId});
            return;
        }
        auto pending = std::move(*Pending);
        Pending.reset();

        auto* msg = ev->Get();
        msg->BackupPath = MakeBackupPath(pending.S3.GetObjectKeyPattern(), TabletId, msg->Generation, msg->Step, TActivationContext::Now());
        Status.CurrentBackupPath = msg->BackupPath;

        YDB_LOG_NOTICE("LocalDbBackup: snapshot started", {"tabletId", TabletId}, {"path", msg->BackupPath}, {"tables", msg->Tables.size()},
            {"generation", msg->Generation}, {"step", msg->Step});

        ReplyPage(pending.ReplyTo, "Backup started: " + msg->BackupPath, false, pending.S3);
        Send(ev->Forward(Status.WriterActorId));
    }

    // --- completion ---

    void Handle(TEvSnapshotCompleted::TPtr& ev) {
        const auto* msg = ev->Get();
        if (ev->Sender != Status.WriterActorId) {
            YDB_LOG_WARN("LocalDbBackup: ignoring result of a stale snapshot writer", {"tabletId", TabletId});
            return;
        }
        if (Pending) {
            // failed before the tablet answered TEvStartBackup (e.g. S3 client initialization or the start timeout)
            auto pending = std::move(*Pending);
            Pending.reset();
            Status.OnFinished(msg->Success, msg->Error, msg->WrittenBytes, TActivationContext::Now());
            Send(Tablet, new TEvCancelScans());
            ReplyPage(pending.ReplyTo, "Backup failed: " + msg->Error, true, pending.S3);
            return;
        }

        if (msg->Success) {
            YDB_LOG_NOTICE("LocalDbBackup: snapshot finished", {"tabletId", TabletId}, {"path", Status.CurrentBackupPath},
                {"bytes", msg->WrittenBytes});
        } else {
            YDB_LOG_ERROR(
                "LocalDbBackup: snapshot failed", {"tabletId", TabletId}, {"path", Status.CurrentBackupPath}, {"error", msg->Error});
            // the writer is gone, stop the scans that are still running
            Send(Tablet, new TEvCancelScans());
        }
        Status.OnFinished(msg->Success, msg->Error, msg->WrittenBytes, TActivationContext::Now());
    }

    void HandlePoison() {
        if (Status.IsActive()) {
            Send(Status.WriterActorId, new NActors::TEvents::TEvPoison());
        }
        PassAway();
    }

private:
    const NActors::TActorId Tablet;
    const ui64 TabletId;
    TStatus Status;
    std::optional<TPendingReply> Pending;
};

}   // namespace

std::unique_ptr<NActors::IActor> CreateLocalDbBackupActor(const NActors::TActorId& tablet, ui64 tabletId) {
    return std::make_unique<TLocalDbBackupActor>(tablet, tabletId);
}

}   // namespace NKikimr::NColumnShard::NLocalDbBackup
