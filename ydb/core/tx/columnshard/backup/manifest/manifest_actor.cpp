#include "manifest_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#ifndef KIKIMR_DISABLE_S3_OPS
#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/tx/datashard/extstorage_usage_config.h>
#include <ydb/core/wrappers/retry_policy.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#endif

#include <util/string/builder.h>
#include <util/string/strip.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard::NBackup {

#ifndef KIKIMR_DISABLE_S3_OPS

namespace {

using TEvExternalStorage = NWrappers::TEvExternalStorage;

template <typename TDerived>
class TManifestActorBase: public NActors::TActorBootstrapped<TDerived> {
    using TBase = NActors::TActorBootstrapped<TDerived>;

protected:
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    const NActors::TActorId SubscriberActorId;
    const NKikimrSchemeOp::TS3Settings S3Settings;
    const NDataShard::TStorageSettings Settings;
    const ui32 Retries;
    ui32 Attempt = 0;
    TDuration Delay = TDuration::Seconds(1);
    NActors::TActorId Client;
    TString CurrentKey;

    TManifestActorBase(const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TS3Settings& s3Settings,
        NDataShard::TStorageSettings&& settings, ui32 retries)
        : SubscriberActorId(subscriberActorId)
        , S3Settings(s3Settings)
        , Settings(std::move(settings))
        , Retries(retries)
    {
    }

    TString GetManifestKey() const {
        return Settings.GetObjectKeyPattern() + '/' + DataManifestKeySuffix(Settings.Shard);
    }

    void CreateClient() {
        if (Client) {
            this->Send(Client, new NActors::TEvents::TEvPoisonPill());
        }
        auto config = NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig, S3Settings);
        Client = this->RegisterWithSameMailbox(NWrappers::CreateStorageWrapper(config->ConstructStorageOperator()));
    }

    // Returns false when the request has to be retried later or the actor has failed
    template <typename TResult>
    bool CheckResult(const TResult& result, TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }
        const auto& error = result.GetError();
        YDB_LOG_WARN("",
            {"event", "data_manifest_request_failed"},
            {"marker", marker},
            {"key", CurrentKey},
            {"attempt", Attempt},
            {"error", result});
        if (Attempt < Retries && NWrappers::ShouldRetry(error)) {
            Delay = Min(Delay * ++Attempt, MaxDelay);
            this->Schedule(Delay, new NActors::TEvents::TEvWakeup());
        } else {
            static_cast<TDerived*>(this)->Fail(TStringBuilder() << marker << " " << CurrentKey << " failed: " << error.GetMessage());
        }
        return false;
    }

    void PassAway() override {
        if (Client) {
            this->Send(Client, new NActors::TEvents::TEvPoisonPill());
        }
        TBase::PassAway();
    }
};

class TManifestWriter: public TManifestActorBase<TManifestWriter> {
    using TBase = TManifestActorBase<TManifestWriter>;

    const TString Data;
    const bool EnableChecksums;
    bool DataUploaded = false;

public:
    TManifestWriter(const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& task, const TDataManifest& manifest)
        : TBase(subscriberActorId, task.GetS3Settings(), NDataShard::TStorageSettings::FromBackupTask<NKikimrSchemeOp::TS3Settings>(task),
              task.GetNumberOfRetries())
        , Data(manifest.Serialize())
        , EnableChecksums(task.GetEnableChecksums())
    {
    }

    void Bootstrap() {
        Upload();
        Become(&TManifestWriter::StateMain);
    }

    void Fail(const TString& error) {
        Send(SubscriberActorId, new TEvPrivate::TEvBackupManifestWriteResult(error));
        PassAway();
    }

private:
    STRICT_STFUNC(StateMain,
        hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle)
        cFunc(NActors::TEvents::TEvWakeup::EventType, Upload)
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway))

    void Upload() {
        CreateClient();
        const TString manifestKey = GetManifestKey();
        TString data = Data;
        if (DataUploaded) {
            // make checksum verifiable using sha256sum CLI
            data = NKikimr::NBackup::ComputeChecksum(Data) + ' ' + DataManifestKeySuffix(Settings.Shard);
            CurrentKey = NKikimr::NBackup::ChecksumKey(manifestKey);
        } else {
            CurrentKey = manifestKey;
        }
        auto request = Aws::S3::Model::PutObjectRequest().WithKey(CurrentKey);
        Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(data)));
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        if (!CheckResult(ev->Get()->Result, "PutObject")) {
            return;
        }
        if (!DataUploaded && EnableChecksums) {
            DataUploaded = true;
            Attempt = 0;
            return Upload();
        }
        Send(SubscriberActorId, new TEvPrivate::TEvBackupManifestWriteResult());
        PassAway();
    }
};

class TManifestReader: public TManifestActorBase<TManifestReader> {
    using TBase = TManifestActorBase<TManifestReader>;

    const bool ValidateChecksums;
    TString ManifestData;
    bool ReadingChecksum = false;

public:
    TManifestReader(const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TRestoreTask& task)
        : TBase(subscriberActorId, task.GetS3Settings(), NDataShard::TStorageSettings::FromRestoreTask<NKikimrSchemeOp::TS3Settings>(task),
              task.GetNumberOfRetries())
        , ValidateChecksums(task.GetValidateChecksums())
    {
    }

    void Bootstrap() {
        Head();
        Become(&TManifestReader::StateMain);
    }

    void Fail(const TString& error) {
        auto result = std::make_unique<TEvPrivate::TEvBackupManifestReadResult>();
        result->Error = error;
        Send(SubscriberActorId, result.release());
        PassAway();
    }

private:
    STRICT_STFUNC(StateMain,
        hFunc(TEvExternalStorage::TEvHeadObjectResponse, Handle)
        hFunc(TEvExternalStorage::TEvGetObjectResponse, Handle)
        cFunc(NActors::TEvents::TEvWakeup::EventType, Head)
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway))

    void Head() {
        CreateClient();
        const TString manifestKey = GetManifestKey();
        CurrentKey = ReadingChecksum ? NKikimr::NBackup::ChecksumKey(manifestKey) : manifestKey;
        Send(Client, new TEvExternalStorage::TEvHeadObjectRequest(Aws::S3::Model::HeadObjectRequest().WithKey(CurrentKey)));
    }

    static bool IsNoSuchKey(const Aws::S3::S3Error& error) {
        return error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY || error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND ||
               error.GetExceptionName() == "NoSuchKey";
    }

    void Handle(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess() && IsNoSuchKey(result.GetError())) {
            if (ReadingChecksum) {
                return Fail(TStringBuilder() << "checksum " << CurrentKey << " of the data manifest is missing");
            }
            // A backup with a single data file per shard
            Send(SubscriberActorId, new TEvPrivate::TEvBackupManifestReadResult());
            return PassAway();
        }
        if (!CheckResult(result, "HeadObject")) {
            return;
        }
        const ui64 contentLength = result.GetResult().GetContentLength();
        if (!contentLength) {
            return Fail(TStringBuilder() << CurrentKey << " is empty");
        }
        auto request = Aws::S3::Model::GetObjectRequest().WithKey(CurrentKey).WithRange(TStringBuilder() << "bytes=0-" << contentLength - 1);
        Send(Client, new TEvExternalStorage::TEvGetObjectRequest(request));
    }

    void Handle(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        if (!CheckResult(ev->Get()->Result, "GetObject")) {
            return;
        }
        if (ReadingChecksum) {
            const TString expected = ToString(StripString(TStringBuf(ev->Get()->Body).Before(' ')));
            const TString actual = NKikimr::NBackup::ComputeChecksum(ManifestData);
            if (expected != actual) {
                return Fail(TStringBuilder() << "checksum mismatch for the data manifest " << GetManifestKey() << ": expected " << expected
                                             << ", got " << actual);
            }
            return Reply();
        }
        ManifestData = ev->Get()->Body;
        if (ValidateChecksums) {
            ReadingChecksum = true;
            Attempt = 0;
            return Head();
        }
        Reply();
    }

    void Reply() {
        auto manifest = TDataManifest::Parse(ManifestData);
        if (manifest.IsFail()) {
            return Fail(TStringBuilder() << GetManifestKey() << ": " << manifest.GetErrorMessage());
        }
        auto result = std::make_unique<TEvPrivate::TEvBackupManifestReadResult>();
        result->FilesCount = manifest->FilesCount;
        Send(SubscriberActorId, result.release());
        PassAway();
    }
};

}   // namespace

std::unique_ptr<NActors::IActor> CreateDataManifestWriter(
    const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask, const TDataManifest& manifest) {
    AFL_VERIFY(backupTask.HasS3Settings());
    return std::make_unique<TManifestWriter>(subscriberActorId, backupTask, manifest);
}

std::unique_ptr<NActors::IActor> CreateDataManifestReader(
    const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TRestoreTask& restoreTask) {
    AFL_VERIFY(restoreTask.HasS3Settings());
    return std::make_unique<TManifestReader>(subscriberActorId, restoreTask);
}

#else

namespace {

template <typename TResult>
class TS3DisabledActor: public NActors::TActorBootstrapped<TS3DisabledActor<TResult> > {
    const NActors::TActorId SubscriberActorId;

public:
    explicit TS3DisabledActor(const NActors::TActorId& subscriberActorId)
        : SubscriberActorId(subscriberActorId)
    {
    }

    void Bootstrap() {
        auto result = std::make_unique<TResult>();
        result->Error = "Exports and imports with S3 are disabled";
        this->Send(SubscriberActorId, result.release());
        this->PassAway();
    }
};

}   // namespace

std::unique_ptr<NActors::IActor> CreateDataManifestWriter(
    const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask&, const TDataManifest&) {
    return std::make_unique<TS3DisabledActor<TEvPrivate::TEvBackupManifestWriteResult> >(subscriberActorId);
}

std::unique_ptr<NActors::IActor> CreateDataManifestReader(const NActors::TActorId& subscriberActorId, const NKikimrSchemeOp::TRestoreTask&) {
    return std::make_unique<TS3DisabledActor<TEvPrivate::TEvBackupManifestReadResult> >(subscriberActorId);
}

#endif

}   // namespace NKikimr::NColumnShard::NBackup
