#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_common.h"
#include "datashard.h"
#include "export_common.h"
#include "export_s3.h"
#include "extstorage_usage_config.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <google/protobuf/text_format.h>

#include <ydb/core/protos/config.pb.h>

namespace NKikimr {
namespace NDataShard {

class TS3Uploader: public TActorBootstrapped<TS3Uploader> {
    using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
    using THttpResolverConfig = NKikimrConfig::TS3ProxyResolverConfig::THttpResolverConfig;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;
    using TEvBuffer = TEvExportScan::TEvBuffer<TBuffer>;

    static TMaybe<THttpResolverConfig> GetHttpResolverConfig(TStringBuf endpoint) {
        for (const auto& entry : AppData()->S3ProxyResolverConfig.GetEndpoints()) {
            if (entry.GetEndpoint() == endpoint && entry.HasHttpResolver()) {
                return entry.GetHttpResolver();
            }
        }

        return Nothing();
    }

    static TStringBuf NormalizeEndpoint(TStringBuf endpoint) {
        Y_UNUSED(endpoint.SkipPrefix("http://") || endpoint.SkipPrefix("https://"));
        Y_UNUSED(endpoint.ChopSuffix(":80") || endpoint.ChopSuffix(":443"));
        return endpoint;
    }

    static TMaybe<THttpResolverConfig> GetHttpResolverConfig(const TS3ExternalStorageConfig& settings) {
        return GetHttpResolverConfig(NormalizeEndpoint(settings.GetConfig().endpointOverride));
    }

    std::shared_ptr<TS3ExternalStorageConfig> GetS3StorageConfig() const {
        return std::dynamic_pointer_cast<TS3ExternalStorageConfig>(ExternalStorageConfig);
    }

    TString GetResolveProxyUrl(const TS3ExternalStorageConfig& settings) const {
        Y_ABORT_UNLESS(HttpResolverConfig);

        TStringBuilder url;
        switch (settings.GetConfig().scheme) {
        case Aws::Http::Scheme::HTTP:
            url << "http://";
            break;
        case Aws::Http::Scheme::HTTPS:
            url << "https://";
            break;
        }

        url << HttpResolverConfig->GetResolveUrl();
        return url;
    }

    void ApplyProxy(TS3ExternalStorageConfig& settings, const TString& proxyHost) const {
        Y_ABORT_UNLESS(HttpResolverConfig);

        settings.ConfigRef().proxyScheme = settings.GetConfig().scheme;
        settings.ConfigRef().proxyHost = proxyHost;
        settings.ConfigRef().proxyCaPath = settings.GetConfig().caPath;

        switch (settings.GetConfig().proxyScheme) {
        case Aws::Http::Scheme::HTTP:
            settings.ConfigRef().proxyPort = HttpResolverConfig->GetHttpPort();
            break;
        case Aws::Http::Scheme::HTTPS:
            settings.ConfigRef().proxyPort = HttpResolverConfig->GetHttpsPort();
            break;
        }
    }

    void ResolveProxy() {
        if (!HttpProxy) {
            HttpProxy = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
        }

        Send(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(
            NHttp::THttpOutgoingRequest::CreateRequestGet(GetResolveProxyUrl(*GetS3StorageConfig())),
            TDuration::Seconds(10)
        ));

        Become(&TThis::StateResolveProxy);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        const auto& msg = *ev->Get();

        EXPORT_LOG_D("Handle NHttp::TEvHttpProxy::TEvHttpIncomingResponse"
            << ": self# " << SelfId()
            << ", status# " << (msg.Response ? msg.Response->Status : "null")
            << ", body# " << (msg.Response ? msg.Response->Body : "null"));

        if (!msg.Response || !msg.Response->Status.StartsWith("200")) {
            EXPORT_LOG_E("Error at 'GetProxy'"
                << ": self# " << SelfId()
                << ", error# " << msg.GetError());
            return RetryOrFinish(Aws::S3::S3Error({Aws::S3::S3Errors::SERVICE_UNAVAILABLE, true}));
        }

        if (msg.Response->Body.find('<') != TStringBuf::npos) {
            EXPORT_LOG_E("Error at 'GetProxy'"
                << ": self# " << SelfId()
                << ", error# " << "invalid body"
                << ", body# " << msg.Response->Body);
            return RetryOrFinish(Aws::S3::S3Error({Aws::S3::S3Errors::SERVICE_UNAVAILABLE, true}));
        }

        ApplyProxy(*GetS3StorageConfig(), TString(msg.Response->Body));
        ProxyResolved = true;

        const auto& cfg = GetS3StorageConfig()->GetConfig();
        EXPORT_LOG_N("Using proxy: "
            << (cfg.proxyScheme == Aws::Http::Scheme::HTTPS ? "https://" : "http://")
            << cfg.proxyHost << ":" << cfg.proxyPort);

        Restart();
    }

    void Restart() {
        Y_ABORT_UNLESS(ProxyResolved);

        MultiPart = false;
        Last = false;
        Parts.clear();

        if (Attempt) {
            this->Send(std::exchange(Client, TActorId()), new TEvents::TEvPoisonPill());
        }

        Client = this->RegisterWithSameMailbox(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));

        if (!MetadataUploaded) {
            UploadMetadata();
        } else if (!PermissionsUploaded) {
            UploadPermissions();
        } else if (!SchemeUploaded) {
            UploadScheme();
        } else {
            this->Become(&TThis::StateUploadData);

            if (Attempt) {
                this->Send(std::exchange(Scanner, TActorId()), new TEvExportScan::TEvReset());
            } else if (Scanner) {
                this->Send(Scanner, new TEvExportScan::TEvFeed());
            }
        }
    }

    void UploadScheme() {
        Y_ABORT_UNLESS(!SchemeUploaded);

        if (!Scheme) {
            return Finish(false, "Cannot infer scheme");
        }

        google::protobuf::TextFormat::PrintToString(Scheme.GetRef(), &Buffer);

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Settings.GetSchemeKey())
            .WithStorageClass(Settings.GetStorageClass());
        this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));

        this->Become(&TThis::StateUploadScheme);
    }

    void UploadPermissions() {
        Y_ABORT_UNLESS(!PermissionsUploaded);

        if (!Permissions) {
            return Finish(false, "Cannot infer permissions");
        }

        google::protobuf::TextFormat::PrintToString(Permissions.GetRef(), &Buffer);

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Settings.GetPermissionsKey())
            .WithStorageClass(Settings.GetStorageClass());
        this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));

        this->Become(&TThis::StateUploadPermissions);
    }

    void UploadMetadata() {
        Y_ABORT_UNLESS(!MetadataUploaded);

        Buffer = std::move(Metadata);

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(Settings.GetMetadataKey())
            .WithStorageClass(Settings.GetStorageClass());
        this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));

        this->Become(&TThis::StateUploadMetadata);
    }

    void HandleScheme(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("HandleScheme TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("PutObject (scheme)"))) {
            return;
        }

        SchemeUploaded = true;

        if (Scanner) {
            this->Send(Scanner, new TEvExportScan::TEvFeed());
        }

        this->Become(&TThis::StateUploadData);
    }

    void HandlePermissions(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("HandleMetadata TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("PutObject (permissions)"))) {
            return;
        }

        PermissionsUploaded = true;

        UploadScheme();
    }

    void HandleMetadata(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("HandleMetadata TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("PutObject (metadata)"))) {
            return;
        }

        MetadataUploaded = true;

        UploadPermissions();
    }

    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvReady"
            << ": self# " << this->SelfId()
            << ", sender# " << ev->Sender);

        Scanner = ev->Sender;

        if (Error) {
            return PassAway();
        }

        if (ProxyResolved && SchemeUploaded && MetadataUploaded && PermissionsUploaded) {
            this->Send(Scanner, new TEvExportScan::TEvFeed());
        }
    }

    void Handle(TEvBuffer::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvBuffer"
            << ": self# " << this->SelfId()
            << ", sender# " << ev->Sender
            << ", msg# " << ev->Get()->ToString());

        if (ev->Sender != Scanner) {
            EXPORT_LOG_W("Received buffer from unknown scanner"
                << ": self# " << this->SelfId()
                << ", sender# " << ev->Sender
                << ", scanner# " << Scanner);
            return;
        }

        Last = ev->Get()->Last;
        MultiPart = MultiPart || !Last;
        ev->Get()->Buffer.AsString(Buffer);

        UploadData();
    }

    void UploadData() {
        if (!MultiPart) {
            auto request = Aws::S3::Model::PutObjectRequest()
                .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                .WithStorageClass(Settings.GetStorageClass());
            this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));
        } else {
            if (!UploadId) {
                this->Send(DataShard, new TEvDataShard::TEvGetS3Upload(this->SelfId(), TxId));
                return;
            }

            auto request = Aws::S3::Model::UploadPartRequest()
                .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                .WithUploadId(*UploadId)
                .WithPartNumber(Parts.size() + 1);
            this->Send(Client, new TEvExternalStorage::TEvUploadPartRequest(request, std::move(Buffer)));
        }
    }

    void HandleData(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("HandleData TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("PutObject (data)"))) {
            return;
        }

        Finish();
    }

    void Handle(TEvDataShard::TEvS3Upload::TPtr& ev) {
        auto& upload = ev->Get()->Upload;

        EXPORT_LOG_D("Handle TEvDataShard::TEvS3Upload"
            << ": self# " << this->SelfId()
            << ", upload# " << upload);

        if (!upload) {
            auto request = Aws::S3::Model::CreateMultipartUploadRequest()
                .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                .WithStorageClass(Settings.GetStorageClass());
            this->Send(Client, new TEvExternalStorage::TEvCreateMultipartUploadRequest(request));
        } else {
            UploadId = upload->Id;

            switch (upload->Status) {
                case TS3Upload::EStatus::UploadParts:
                    return UploadData();

                case TS3Upload::EStatus::Complete: {
                    Parts = std::move(upload->Parts);

                    TVector<Aws::S3::Model::CompletedPart> parts(Reserve(Parts.size()));
                    for (ui32 partIndex = 0; partIndex < Parts.size(); ++partIndex) {
                        parts.emplace_back(Aws::S3::Model::CompletedPart()
                            .WithPartNumber(partIndex + 1)
                            .WithETag(Parts.at(partIndex)));
                    }

                    auto request = Aws::S3::Model::CompleteMultipartUploadRequest()
                        .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                        .WithUploadId(*UploadId)
                        .WithMultipartUpload(Aws::S3::Model::CompletedMultipartUpload().WithParts(std::move(parts)));
                    this->Send(Client, new TEvExternalStorage::TEvCompleteMultipartUploadRequest(request));
                    break;
                }

                case TS3Upload::EStatus::Abort: {
                    Error = std::move(upload->Error);
                    if (!Error) {
                        Error = "<empty>";
                    }

                    auto request = Aws::S3::Model::AbortMultipartUploadRequest()
                        .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                        .WithUploadId(*UploadId);
                    this->Send(Client, new TEvExternalStorage::TEvAbortMultipartUploadRequest(request));
                    break;
                }
            }
        }
    }

    void Handle(TEvExternalStorage::TEvCreateMultipartUploadResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvExternalStorage::TEvCreateMultipartUploadResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("CreateMultipartUpload"))) {
            return;
        }

        this->Send(DataShard, new TEvDataShard::TEvStoreS3UploadId(this->SelfId(), TxId, result.GetResult().GetUploadId().c_str()));
    }

    void Handle(TEvExternalStorage::TEvUploadPartResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvExternalStorage::TEvUploadPartResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("UploadPart"))) {
            return;
        }

        Parts.push_back(result.GetResult().GetETag().c_str());

        if (Last) {
            return Finish();
        }

        this->Send(Scanner, new TEvExportScan::TEvFeed());
    }

    void Handle(TEvExternalStorage::TEvCompleteMultipartUploadResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvExternalStorage::TEvCompleteMultipartUploadResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (result.IsSuccess()) {
            return PassAway();
        }

        const auto& error = result.GetError();
        if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD) {
            return PassAway();
        }

        if (CanRetry(error)) {
            UploadId.Clear(); // force getting info after restart
            Retry();
        } else {
            Error = error.GetMessage().c_str();
            PassAway();
        }
    }

    void Handle(TEvExternalStorage::TEvAbortMultipartUploadResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_D("Handle TEvExternalStorage::TEvAbortMultipartUploadResponse"
            << ": self# " << this->SelfId()
            << ", result# " << result);

        if (result.IsSuccess()) {
            return PassAway();
        }

        const auto& error = result.GetError();
        if (CanRetry(error)) {
            UploadId.Clear(); // force getting info after restart
            Retry();
        } else {
            Y_ABORT_UNLESS(Error);
            Error = TStringBuilder() << *Error << " Additionally, 'AbortMultipartUpload' has failed: "
                << error.GetMessage();
            PassAway();
        }
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        EXPORT_LOG_E("Error at '" << marker << "'"
            << ": self# " << this->SelfId()
            << ", error# " << result);
        RetryOrFinish(result.GetError());

        return false;
    }

    static bool ShouldRetry(const Aws::S3::S3Error& error) {
        if (error.ShouldRetry()) {
            return true;
        }

        if ("TooManyRequests" == error.GetExceptionName()) {
            return true;
        }

        return false;
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
            Finish(false, TStringBuilder() << "S3 error: " << error.GetMessage().c_str());
        }
    }

    void Finish(bool success = true, const TString& error = TString()) {
        EXPORT_LOG_I("Finish"
            << ": self# " << this->SelfId()
            << ", success# " << success
            << ", error# " << error
            << ", multipart# " << MultiPart
            << ", uploadId# " << UploadId);

        if (!success) {
            Error = error;
        }

        if (!MultiPart || !UploadId) {
            if (!Scanner) {
                return;
            }

            PassAway();
        } else {
            if (success) {
                this->Send(DataShard, new TEvDataShard::TEvChangeS3UploadStatus(this->SelfId(), TxId,
                    TS3Upload::EStatus::Complete, std::move(Parts)));
            } else {
                this->Send(DataShard, new TEvDataShard::TEvChangeS3UploadStatus(this->SelfId(), TxId,
                    TS3Upload::EStatus::Abort, *Error));
            }
        }
    }

    void PassAway() override {
        if (HttpProxy) {
            Send(HttpProxy, new TEvents::TEvPoisonPill());
        }

        if (Scanner) {
            this->Send(Scanner, new TEvExportScan::TEvFinish(Error.Empty(), Error.GetOrElse(TString())));
        }

        this->Send(Client, new TEvents::TEvPoisonPill());

        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "s3"sv;
    }

    explicit TS3Uploader(
            const TActorId& dataShard, ui64 txId,
            const NKikimrSchemeOp::TBackupTask& task,
            TMaybe<Ydb::Table::CreateTableRequest>&& scheme,
            TMaybe<Ydb::Scheme::ModifyPermissionsRequest>&& permissions,
            TString&& metadata)
        : ExternalStorageConfig(new TS3ExternalStorageConfig(task.GetS3Settings()))
        , Settings(TS3Settings::FromBackupTask(task))
        , DataFormat(NBackupRestoreTraits::EDataFormat::Csv)
        , CompressionCodec(NBackupRestoreTraits::CodecFromTask(task))
        , HttpResolverConfig(GetHttpResolverConfig(*GetS3StorageConfig()))
        , DataShard(dataShard)
        , TxId(txId)
        , Scheme(std::move(scheme))
        , Metadata(std::move(metadata))
        , Permissions(std::move(permissions))
        , Retries(task.GetNumberOfRetries())
        , Attempt(0)
        , Delay(TDuration::Minutes(1))
        , SchemeUploaded(task.GetShardNum() == 0 ? false : true)
        , MetadataUploaded(task.GetShardNum() == 0 ? false : true)
        , PermissionsUploaded(task.GetShardNum() == 0 ? false : true)
    {
    }

    void Bootstrap() {
        EXPORT_LOG_D("Bootstrap"
            << ": self# " << this->SelfId()
            << ", attempt# " << Attempt);

        ProxyResolved = !HttpResolverConfig.Defined();
        if (!ProxyResolved) {
            ResolveProxy();
        } else {
            Restart();
        }
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReady, Handle);

            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateResolveProxy) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
        default:
            return StateBase(ev);
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

    STATEFN(StateUploadMetadata) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleMetadata);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadData) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBuffer, Handle);
            hFunc(TEvDataShard::TEvS3Upload, Handle);

            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleData);
            hFunc(TEvExternalStorage::TEvCreateMultipartUploadResponse, Handle);
            hFunc(TEvExternalStorage::TEvUploadPartResponse, Handle);
            hFunc(TEvExternalStorage::TEvCompleteMultipartUploadResponse, Handle);
            hFunc(TEvExternalStorage::TEvAbortMultipartUploadResponse, Handle);
        default:
            return StateBase(ev);
        }
    }

private:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TS3Settings Settings;
    const NBackupRestoreTraits::EDataFormat DataFormat;
    const NBackupRestoreTraits::ECompressionCodec CompressionCodec;
    bool ProxyResolved;

    TMaybe<THttpResolverConfig> HttpResolverConfig;
    TActorId HttpProxy;

    const TActorId DataShard;
    const ui64 TxId;
    const TMaybe<Ydb::Table::CreateTableRequest> Scheme;
    const TString Metadata;
    const TMaybe<Ydb::Scheme::ModifyPermissionsRequest> Permissions;

    const ui32 Retries;
    ui32 Attempt;

    TDuration Delay;
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TActorId Client;
    bool SchemeUploaded;
    bool MetadataUploaded;
    bool PermissionsUploaded;
    bool MultiPart;
    bool Last;

    TActorId Scanner;
    TString Buffer;

    TMaybe<TString> UploadId;
    TVector<TString> Parts;
    TMaybe<TString> Error;

}; // TS3Uploader

IActor* TS3Export::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();

    auto permissions = (Task.GetShardNum() == 0)
        ? GenYdbPermissions(Task.GetTable())
        : Nothing();

    NBackupRestore::TMetadata metadata;

    NBackupRestore::TFullBackupMetadata::TPtr backup = new NBackupRestore::TFullBackupMetadata{
        .SnapshotVts = NBackupRestore::TVirtualTimestamp(
            Task.GetSnapshotStep(),
            Task.GetSnapshotTxId())
    };
    metadata.AddFullBackup(backup);

    return new TS3Uploader(
        dataShard, txId, Task, std::move(scheme), std::move(permissions), metadata.Serialize());
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
