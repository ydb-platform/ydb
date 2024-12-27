#include "schemeshard.h"
#include "schemeshard_export_scheme_uploader.h"
#include "schemeshard_impl.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/tx/datashard/export_common.h>
#include <ydb/core/tx/schemeshard/schemeshard_export_helpers.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>

namespace NKikimr::NSchemeShard {

class TSchemeUploader: public TActorBootstrapped<TSchemeUploader> {

    using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;

    void UploadScheme() {
        Y_ABORT_UNLESS(!SchemeUploaded);

        if (!Scheme) {
            return Finish(false, "Cannot infer scheme");
        }

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Sprintf("%s/create_view.sql", DestinationFolder.c_str()));
        this->Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, std::exchange(Scheme, "")));

        this->Become(&TThis::StateUploadScheme);
    }

    void UploadPermissions() {
        Y_ABORT_UNLESS(!PermissionsUploaded);

        if (!Permissions) {
            return Finish(false, "Cannot infer permissions");
        }

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Sprintf("%s/permissions.pb", DestinationFolder.c_str()));
        this->Send(StorageOperator, new TEvExternalStorage::TEvPutObjectRequest(request, std::exchange(Permissions, "")));

        this->Become(&TThis::StateUploadPermissions);
    }

    void HandleScheme(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandleScheme TEvExternalStorage::TEvPutObjectResponse"
            << ", self: " << this->SelfId()
            << ", result: " << result
        );

        if (!CheckResult(result, TStringBuf("PutObject (scheme)"))) {
            return;
        }

        SchemeUploaded = true;

        UploadPermissions();
    }

    void HandlePermissions(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        LOG_D("HandlePermissions TEvExternalStorage::TEvPutObjectResponse"
            << ", self: " << this->SelfId()
            << ", result: " << result
        );

        if (!CheckResult(result, TStringBuf("PutObject (permissions)"))) {
            return;
        }

        PermissionsUploaded = true;

        Finish();
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        LOG_E("Error at '" << marker << "'"
            << ", self: " << this->SelfId()
            << ", error: " << result
        );

        RetryOrFinish(result.GetError());
        return false;
    }

    static bool ShouldRetry(const Aws::S3::S3Error& error) {
        if (error.ShouldRetry()) {
            return true;
        }
        return error.GetExceptionName() == "TooManyRequests";
    }

    bool CanRetry(const Aws::S3::S3Error& error) const {
        return Attempt < Retries && ShouldRetry(error);
    }

    void Retry() {
        Delay = Min(Delay * ++Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        this->Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    void RetryOrFinish(const Aws::S3::S3Error& error) {
        if (CanRetry(error)) {
            Retry();
        } else {
            Finish(false, TStringBuilder() << "S3 error: " << error.GetMessage());
        }
    }

    void Finish(bool success = true, const TString& error = TString()) {
        LOG_I("Finish"
            << ", self: " << this->SelfId()
            << ", success: " << success
            << ", error: " << error
        );

        auto& item = ExportInfo->Items[ItemIdx];

        if (!success) {
            item.Issue = error;
        }
        Send(SchemeShard->SelfId(), new TEvPrivate::TEvExportSchemeUploadResult(ExportInfo->Id, ItemIdx, success, error));

        PassAway();
    }

    void PassAway() override {
        this->Send(StorageOperator, new TEvents::TEvPoisonPill());
        IActor::PassAway();
    }

    void Restart() {
        if (Attempt) {
            this->Send(std::exchange(StorageOperator, TActorId()), new TEvents::TEvPoisonPill());
        }

        StorageOperator = this->RegisterWithSameMailbox(
            NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator())
        );

        if (!SchemeUploaded) {
            UploadScheme();
        } else if (!PermissionsUploaded) {
            UploadPermissions();
        }
    }

public:

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
    }

    TSchemeUploader(
        TSchemeShard* schemeShard,
        TExportInfo::TPtr exportInfo,
        ui32 itemIdx,
        TTxId txId,
        const NKikimrSchemeOp::TBackupTask& task
    )
        : ExternalStorageConfig(new TS3ExternalStorageConfig(task.GetS3Settings()))
        , DestinationFolder(task.GetS3Settings().GetObjectKeyPattern())
        , Retries(task.GetNumberOfRetries())
        , SchemeShard(schemeShard)
        , ExportInfo(exportInfo)
        , ItemIdx(itemIdx)
        , TxId(txId)
    {
    }

    void Bootstrap() {
        auto* uploadingResult = new TEvPrivate::TEvExportSchemeUploadResult(ExportInfo->Id, ItemIdx, true, "");

        auto describeResult = DescribePath(SchemeShard, ActorContext(), ExportInfo->Items[ItemIdx].SourcePathId)->GetRecord();
        if (describeResult.GetStatus() != TEvSchemeShard::EStatus::StatusSuccess) {
            uploadingResult->SetError(describeResult.GetReason());
            Send(SchemeShard->SelfId(), uploadingResult);
            return;
        }

        NYql::TIssues issues;
        const auto& viewDescription = describeResult.GetPathDescription().GetViewDescription();
        const auto backupRoot = TStringBuilder() << '/' << JoinSeq('/', SchemeShard->RootPathElements);
        Scheme = NYdb::NDump::BuildCreateViewQuery(viewDescription.GetName(), describeResult.GetPath(), viewDescription.GetQueryText(), backupRoot, issues);
        if (Scheme.empty()) {
            uploadingResult->SetError(issues.ToString());
            Send(SchemeShard->SelfId(), uploadingResult);
            return;
        }

        if (auto permissions = NDataShard::GenYdbPermissions(describeResult.GetPathDescription()); !permissions.Defined()) {
            uploadingResult->SetError("cannot infer permissions");
            Send(SchemeShard->SelfId(), uploadingResult);
            return;
        } else {
            google::protobuf::TextFormat::PrintToString(permissions.GetRef(), &Permissions);
        }

        Restart();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateUploadScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleScheme);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadPermissions) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandlePermissions);
        default:
            return StateBase(ev);
        }
    }

private:

    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TString DestinationFolder;

    TActorId StorageOperator;

    TString Scheme;
    bool SchemeUploaded = false;

    TString Permissions;
    bool PermissionsUploaded = false;

    ui32 Attempt = 0;
    const ui32 Retries;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TSchemeShard* const SchemeShard;
    TExportInfo::TPtr ExportInfo;
    const ui32 ItemIdx;

    TTxId TxId;

}; // TSchemeUploader

IActor* CreateSchemeUploader(TSchemeShard* schemeShard, TExportInfo::TPtr exportInfo, ui32 itemIdx, TTxId txId, const NKikimrSchemeOp::TBackupTask& task) {
    return new TSchemeUploader(schemeShard, exportInfo, itemIdx, txId, task);
}

} // NSchemeShard::NKikimr
