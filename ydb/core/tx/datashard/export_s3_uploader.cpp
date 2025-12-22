#ifndef KIKIMR_DISABLE_S3_OPS

#include "datashard.h"
#include "export_common.h"
#include "export_s3.h"
#include "extstorage_usage_config.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/wrappers/retry_policy.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/topic_description.h>
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

using namespace NBackup;
using namespace NBackupRestoreTraits;

struct TChangefeedExportDescriptions {
    const Ydb::Table::ChangefeedDescription ChangefeedDescription;
    const Ydb::Topic::DescribeTopicResult Topic;
    TString Name;
    TString Prefix;
};

template <typename TSettings>
class TS3Uploader: public TActorBootstrapped<TS3Uploader<TSettings>> {
    using TThis = TS3Uploader;
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
        if (std::is_same<TSettings, NKikimrSchemeOp::TFSSettings>::value) {
            return Nothing();
        }
        return GetHttpResolverConfig(NormalizeEndpoint(settings.GetConfig().endpointOverride));
    }

    std::shared_ptr<TS3ExternalStorageConfig> GetS3StorageConfig() const {
        return std::dynamic_pointer_cast<TS3ExternalStorageConfig>(ExternalStorageConfig);
    }

    TString GetResolveProxyUrl(const TS3ExternalStorageConfig& settings) const {
        Y_ENSURE(HttpResolverConfig);

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
        Y_ENSURE(HttpResolverConfig);

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
            HttpProxy = this->Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
        }

        this->Send(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(
            NHttp::THttpOutgoingRequest::CreateRequestGet(GetResolveProxyUrl(*GetS3StorageConfig())),
            TDuration::Seconds(10)
        ));

        this->Become(&TThis::StateResolveProxy);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        const auto& msg = *ev->Get();

        EXPORT_LOG_D("Handle NHttp::TEvHttpProxy::TEvHttpIncomingResponse"
            << ": self# " << this->SelfId()
            << ", status# " << (msg.Response ? msg.Response->Status : "null")
            << ", body# " << (msg.Response ? msg.Response->Body : "null"));

        if (!msg.Response || !msg.Response->Status.StartsWith("200")) {
            EXPORT_LOG_E("Error at 'GetProxy'"
                << ": self# " << this->SelfId()
                << ", error# " << msg.GetError());
            return RetryOrFinish(Aws::S3::S3Error({Aws::S3::S3Errors::SERVICE_UNAVAILABLE, true}));
        }

        if (msg.Response->Body.find('<') != TStringBuf::npos) {
            EXPORT_LOG_E("Error at 'GetProxy'"
                << ": self# " << this->SelfId()
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
        Y_ENSURE(ProxyResolved);

        EXPORT_LOG_I("Restart: creating storage wrapper, Attempt# " << Attempt);

        MultiPart = false;
        Last = false;
        Parts.clear();

        if (Attempt) {
            this->Send(std::exchange(Client, TActorId()), new TEvents::TEvPoisonPill());
        }

        EXPORT_LOG_I("Restart: calling ConstructStorageOperator");
        auto storageOperator = ExternalStorageConfig->ConstructStorageOperator();
        EXPORT_LOG_I("Restart: storageOperator constructed, initializing ReplyAdapter");
        storageOperator->InitReplyAdapter(nullptr);
        EXPORT_LOG_I("Restart: ReplyAdapter initialized, creating S3 wrapper");
        Client = this->RegisterWithSameMailbox(NWrappers::CreateS3Wrapper(std::move(storageOperator)));
        EXPORT_LOG_I("Restart: Client registered, ClientId# " << Client);

        if (!MetadataUploaded) {
            EXPORT_LOG_I("Restart: uploading metadata");
            UploadMetadata();
        } else if (EnablePermissions && !PermissionsUploaded) {
            EXPORT_LOG_I("Restart: uploading permissions");
            UploadPermissions();
        } else if (!SchemeUploaded) {
            EXPORT_LOG_I("Restart: uploading scheme");
            UploadScheme();
        } else if (!ChangefeedsUploaded) {
            EXPORT_LOG_I("Restart: uploading changefeed");
            UploadChangefeed();
        } else {
            EXPORT_LOG_I("Restart: switching to StateUploadData");
            this->Become(&TThis::StateUploadData);

            if (Attempt) {
                this->Send(std::exchange(Scanner, TActorId()), new TEvExportScan::TEvReset());
            } else if (Scanner) {
                this->Send(Scanner, new TEvExportScan::TEvFeed());
            }
        }
    }

    template <typename T>
    void PutData(TString&& data, const TString& key, T stateFunc) {
        EXPORT_LOG_I("PutData: key# " << key << ", size# " << data.size() << ", Client# " << Client);
        auto request = Aws::S3::Model::PutObjectRequest().WithKey(key);
        this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(data)));
        this->Become(stateFunc);
    }

    template <typename T>
    void PutDataWithChecksum(TString&& data, const TString& key, TString& checksum, T stateFunc, TMaybe<TEncryptionIV> iv) {
        EXPORT_LOG_I("PutDataWithChecksum: key# " << key 
            << ", dataSize# " << data.size()
            << ", EnableChecksums# " << EnableChecksums
            << ", hasIV# " << iv.Defined());
        
        if (EnableChecksums) {
            checksum = ComputeChecksum(data);
            EXPORT_LOG_I("PutDataWithChecksum: checksum computed");
        }
        if (iv) {
            EXPORT_LOG_I("PutDataWithChecksum: encrypting data");
            try {
                TBuffer encryptedData = TEncryptedFileSerializer::EncryptFullFile(Settings.EncryptionSettings.EncryptionAlgorithm, *Settings.EncryptionSettings.Key, *iv, data);
                data = TString(encryptedData.Data(), encryptedData.Size());
                EXPORT_LOG_I("PutDataWithChecksum: data encrypted, new size# " << data.size());
            } catch (const std::exception& ex) {
                EXPORT_LOG_E("PutDataWithChecksum: encryption failed: " << ex.what());
                Finish(false, TStringBuilder() << "Failed to encrypt " << key << ": " << ex.what());
                return;
            }
        }
        EXPORT_LOG_I("PutDataWithChecksum: calling PutData");
        PutData(std::move(data), key, stateFunc);
    }

    template <typename T>
    void PutMessage(const google::protobuf::Message& message, const TString& key, TString& checksum, T stateFunc, TMaybe<TEncryptionIV> iv) {
        google::protobuf::TextFormat::PrintToString(message, &Buffer);
        PutDataWithChecksum(std::move(Buffer), key, checksum, stateFunc, iv);
    }

    void PutScheme(const Ydb::Table::CreateTableRequest& scheme) {
        PutMessage(scheme, Settings.GetSchemeKey(), SchemeChecksum, &TThis::StateUploadScheme, Settings.EncryptionSettings.GetSchemeIV());
    }

    void UploadScheme() {
        EXPORT_LOG_I("UploadScheme START"
            << ": self# " << this->SelfId()
            << ", SchemeUploaded# " << SchemeUploaded
            << ", hasScheme# " << Scheme.Defined());
        
        Y_ENSURE(!SchemeUploaded);

        if (!Scheme) {
            EXPORT_LOG_E("UploadScheme: No scheme defined, finishing");
            return Finish(false, "Cannot infer scheme");
        }
        
        EXPORT_LOG_I("UploadScheme: calling PutScheme");
        PutScheme(Scheme.GetRef());
    }

    void PutPermissions(const Ydb::Scheme::ModifyPermissionsRequest& permissions) {
        PutMessage(permissions, Settings.GetPermissionsKey(), PermissionsChecksum, &TThis::StateUploadPermissions, Settings.EncryptionSettings.GetPermissionsIV());
    }

    void UploadPermissions() {
        EXPORT_LOG_I("UploadPermissions START"
            << ": self# " << this->SelfId()
            << ", EnablePermissions# " << EnablePermissions
            << ", PermissionsUploaded# " << PermissionsUploaded
            << ", hasPermissions# " << Permissions.Defined());
        
        Y_ENSURE(EnablePermissions && !PermissionsUploaded);

        if (!Permissions) {
            EXPORT_LOG_E("UploadPermissions: No permissions defined, finishing");
            return Finish(false, "Cannot infer permissions");
        }
        
        EXPORT_LOG_I("UploadPermissions: calling PutPermissions");
        PutPermissions(Permissions.GetRef());
    }

    void PutChangefeedDescription(ui64 changefeedIndex) {
        const auto& desc = Changefeeds[changefeedIndex];
        PutMessage(
            desc.ChangefeedDescription,
            Settings.GetChangefeedKey(desc.Prefix),
            ChangefeedChecksum,
            &TThis::StateUploadChangefeed,
            Settings.EncryptionSettings.GetChangefeedIV(static_cast<ui32>(changefeedIndex))
        );
    }

    void UploadChangefeed() {
        EXPORT_LOG_I("UploadChangefeed START"
            << ": self# " << this->SelfId()
            << ", ChangefeedsUploaded# " << ChangefeedsUploaded
            << ", IndexExportedChangefeed# " << IndexExportedChangefeed
            << ", totalChangefeeds# " << Changefeeds.size());
        
        Y_ENSURE(!ChangefeedsUploaded);
        if (IndexExportedChangefeed == Changefeeds.size()) {
            EXPORT_LOG_I("UploadChangefeed: All changefeeds uploaded, switching to StateUploadData");
            ChangefeedsUploaded = true;
            if (Scanner) {
                this->Send(Scanner, new TEvExportScan::TEvFeed());
            }
            this->Become(&TThis::StateUploadData);
            return;
        }
        EXPORT_LOG_I("UploadChangefeed: calling PutChangefeedDescription for index# " << IndexExportedChangefeed);
        PutChangefeedDescription(IndexExportedChangefeed);
    }

    void PutTopicDescription(ui64 changefeedIndex) {
        const auto& desc = Changefeeds[changefeedIndex];
        PutMessage(
            desc.Topic,
            Settings.GetTopicKey(desc.Prefix),
            TopicChecksum,
            &TThis::StateUploadTopic,
            Settings.EncryptionSettings.GetChangefeedTopicIV(static_cast<ui32>(changefeedIndex))
        );
    }

    void UploadTopic() {
        PutTopicDescription(IndexExportedChangefeed);
    }

    void UploadMetadata() {
        EXPORT_LOG_I("UploadMetadata START"
            << ": self# " << this->SelfId()
            << ", MetadataUploaded# " << MetadataUploaded
            << ", metadataSize# " << Metadata.size());
        
        Y_ENSURE(!MetadataUploaded);

        Buffer = std::move(Metadata);
        EXPORT_LOG_I("UploadMetadata: calling PutDataWithChecksum, key# " << Settings.GetMetadataKey());
        PutDataWithChecksum(std::move(Buffer), Settings.GetMetadataKey(), MetadataChecksum, &TThis::StateUploadMetadata, Settings.EncryptionSettings.GetMetadataIV());
    }

    void UploadChecksum(TString&& checksum, const TString& checksumKey, const TString& objectKeySuffix,
        std::function<void()> checksumUploadedCallback)
    {
        // make checksum verifiable using sha256sum CLI
        checksum += ' ' + objectKeySuffix;
        PutData(std::move(checksum), checksumKey, &TThis::StateUploadChecksum);
        ChecksumUploadedCallback = checksumUploadedCallback;
    }

    void HandleScheme(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandleScheme TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", EnableChecksums# " << EnableChecksums);

        if (!CheckResult(result, TStringBuf("PutObject (scheme)"))) {
            return;
        }

        auto nextStep = [this]() {
            EXPORT_LOG_I("HandleScheme: nextStep, SchemeUploaded=true, calling UploadChangefeed");
            SchemeUploaded = true;
            UploadChangefeed();
        };

        if (EnableChecksums) {
            EXPORT_LOG_I("HandleScheme: uploading checksum");
            TString checksumKey = ChecksumKey(Settings.GetSchemeKey());
            UploadChecksum(std::move(SchemeChecksum), checksumKey, SchemeKeySuffix(false), nextStep);
        } else {
            EXPORT_LOG_I("HandleScheme: no checksums, calling nextStep");
            nextStep();
        }
    }

    void HandlePermissions(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandlePermissions TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", EnableChecksums# " << EnableChecksums);

        if (!CheckResult(result, TStringBuf("PutObject (permissions)"))) {
            return;
        }

        auto nextStep = [this]() {
            EXPORT_LOG_I("HandlePermissions: nextStep, PermissionsUploaded=true, calling UploadScheme");
            PermissionsUploaded = true;
            UploadScheme();
        };

        if (EnableChecksums) {
            EXPORT_LOG_I("HandlePermissions: uploading checksum");
            TString checksumKey = ChecksumKey(Settings.GetPermissionsKey());
            UploadChecksum(std::move(PermissionsChecksum), checksumKey, PermissionsKeySuffix(false), nextStep);
        } else {
            EXPORT_LOG_I("HandlePermissions: no checksums, calling nextStep");
            nextStep();
        }
    }

    void HandleChangefeed(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandleChangefeed TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", IndexExportedChangefeed# " << IndexExportedChangefeed);

        if (!CheckResult(result, TStringBuf("PutObject (changefeed)"))) {
            return;
        }

        auto nextStep = [this]() {
            EXPORT_LOG_I("HandleChangefeed: nextStep, calling UploadTopic");
            UploadTopic();
        };
        if (EnableChecksums) {
            EXPORT_LOG_I("HandleChangefeed: uploading checksum");
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            TString checksumKey = ChecksumKey(Settings.GetChangefeedKey(desc.Prefix));
            UploadChecksum(std::move(ChangefeedChecksum), checksumKey, ChangefeedKeySuffix(false), nextStep);
        } else {
            EXPORT_LOG_I("HandleChangefeed: no checksums, calling nextStep");
            nextStep();
        }
    }

    void HandleTopic(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandleTopic TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", IndexExportedChangefeed# " << IndexExportedChangefeed);

        if (!CheckResult(result, TStringBuf("PutObject (topic)"))) {
            return;
        }

        auto nextStep = [this]() {
            EXPORT_LOG_I("HandleTopic: nextStep, incrementing IndexExportedChangefeed and calling UploadChangefeed");
            ++IndexExportedChangefeed;
            UploadChangefeed();
        };
        if (EnableChecksums) {
            EXPORT_LOG_I("HandleTopic: uploading checksum");
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            TString checksumKey = ChecksumKey(Settings.GetTopicKey(desc.Prefix));
            UploadChecksum(std::move(TopicChecksum), checksumKey, TopicKeySuffix(false), nextStep);
        } else {
            EXPORT_LOG_I("HandleTopic: no checksums, calling nextStep");
            nextStep();
        }
    }

    void HandleMetadata(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandleMetadata TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", EnableChecksums# " << EnableChecksums
            << ", EnablePermissions# " << EnablePermissions);

        if (!CheckResult(result, TStringBuf("PutObject (metadata)"))) {
            return;
        }

        auto nextStep = [this]() {
            EXPORT_LOG_I("HandleMetadata: nextStep, MetadataUploaded=true");
            MetadataUploaded = true;
            if (EnablePermissions) {
                EXPORT_LOG_I("HandleMetadata: calling UploadPermissions");
                UploadPermissions();
            } else {
                EXPORT_LOG_I("HandleMetadata: calling UploadScheme");
                UploadScheme();
            }
        };

        if (EnableChecksums) {
            EXPORT_LOG_I("HandleMetadata: uploading checksum");
            TString checksumKey = ChecksumKey(Settings.GetMetadataKey());
            UploadChecksum(std::move(MetadataChecksum), checksumKey, MetadataKeySuffix(false), nextStep);
        } else {
            EXPORT_LOG_I("HandleMetadata: no checksums, calling nextStep");
            nextStep();
        }
    }

    void HandleChecksum(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandleChecksum TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess());

        if (!CheckResult(result, TStringBuf("PutObject (checksum)"))) {
            return;
        }

        EXPORT_LOG_I("HandleChecksum: calling ChecksumUploadedCallback");
        ChecksumUploadedCallback();
    }

    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        EXPORT_LOG_I("Handle TEvExportScan::TEvReady"
            << ": self# " << this->SelfId()
            << ", sender# " << ev->Sender
            << ", hasError# " << Error.Defined());

        Scanner = ev->Sender;

        if (Error) {
            EXPORT_LOG_E("Handle TEvReady: Error present, calling PassAway");
            return PassAway();
        }

        const bool permissionsDone = !EnablePermissions || PermissionsUploaded;
        EXPORT_LOG_I("Handle TEvReady: ProxyResolved# " << ProxyResolved
            << ", SchemeUploaded# " << SchemeUploaded
            << ", MetadataUploaded# " << MetadataUploaded
            << ", permissionsDone# " << permissionsDone
            << ", ChangefeedsUploaded# " << ChangefeedsUploaded);
        
        if (ProxyResolved && SchemeUploaded && MetadataUploaded && permissionsDone && ChangefeedsUploaded) {
            EXPORT_LOG_I("Handle TEvReady: All uploads done, sending TEvFeed to Scanner");
            this->Send(Scanner, new TEvExportScan::TEvFeed());
        } else {
            EXPORT_LOG_I("Handle TEvReady: Not all uploads done yet, waiting");
        }
    }

    void Handle(TEvBuffer::TPtr& ev) {
        EXPORT_LOG_I("Handle TEvExportScan::TEvBuffer"
            << ": self# " << this->SelfId()
            << ", sender# " << ev->Sender
            << ", isLast# " << ev->Get()->Last
            << ", bufferSize# " << ev->Get()->Buffer.Size());

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
        DataChecksum = std::move(ev->Get()->Checksum);

        EXPORT_LOG_I("Handle TEvBuffer: Last# " << Last << ", MultiPart# " << MultiPart << ", calling UploadData");
        UploadData();
    }

    void UploadData() {
        EXPORT_LOG_I("UploadData START"
            << ": self# " << this->SelfId()
            << ", MultiPart# " << MultiPart
            << ", Last# " << Last
            << ", bufferSize# " << Buffer.size()
            << ", hasUploadId# " << UploadId.Defined()
            << ", partsCount# " << Parts.size());
        
        if (!MultiPart) {
            EXPORT_LOG_I("UploadData: Single-part upload, sending PutObjectRequest");
            auto request = Aws::S3::Model::PutObjectRequest()
                .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec));
            this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Buffer)));
        } else {
            if (!UploadId) {
                EXPORT_LOG_I("UploadData: Multi-part upload, no UploadId yet, requesting from DataShard");
                this->Send(DataShard, new TEvDataShard::TEvGetS3Upload(this->SelfId(), TxId));
                return;
            }

            EXPORT_LOG_I("UploadData: Multi-part upload, sending UploadPartRequest, partNumber# " << (Parts.size() + 1));
            auto request = Aws::S3::Model::UploadPartRequest()
                .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                .WithUploadId(*UploadId)
                .WithPartNumber(Parts.size() + 1);
            this->Send(Client, new TEvExternalStorage::TEvUploadPartRequest(request, std::move(Buffer)));
        }
    }

    void HandleData(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("HandleData TEvExternalStorage::TEvPutObjectResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", EnableChecksums# " << EnableChecksums);

        if (!CheckResult(result, TStringBuf("PutObject (data)"))) {
            return;
        }

        auto nextStep = [this]() {
            EXPORT_LOG_I("HandleData: nextStep, calling Finish");
            Finish();
        };

        if (EnableChecksums) {
            EXPORT_LOG_I("HandleData: uploading checksum");
            // checksum is always calculated before compression
            TString checksumKey = ChecksumKey(Settings.GetDataKey(DataFormat, ECompressionCodec::None));
            TString dataKeySuffix = DataKeySuffix(ShardNum, DataFormat, ECompressionCodec::None, false);
            UploadChecksum(std::move(DataChecksum), checksumKey, dataKeySuffix, nextStep);
        } else {
            EXPORT_LOG_I("HandleData: no checksums, calling nextStep");
            nextStep();
        }
    }

    void Handle(TEvDataShard::TEvS3Upload::TPtr& ev) {
        auto& upload = ev->Get()->Upload;

        EXPORT_LOG_I("Handle TEvDataShard::TEvS3Upload"
            << ": self# " << this->SelfId()
            << ", hasUpload# " << (upload ? "true" : "false"));

        if (!upload) {
            EXPORT_LOG_I("Handle TEvS3Upload: No upload info, creating multipart upload");
            auto request = Aws::S3::Model::CreateMultipartUploadRequest()
                .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec));
            this->Send(Client, new TEvExternalStorage::TEvCreateMultipartUploadRequest(request));
        } else {
            UploadId = upload->Id;
            EXPORT_LOG_I("Handle TEvS3Upload: uploadId# " << *UploadId << ", status# " << (ui32)upload->Status);

            switch (upload->Status) {
                case TS3Upload::EStatus::UploadParts:
                    EXPORT_LOG_I("Handle TEvS3Upload: status UploadParts, calling UploadData");
                    return UploadData();

                case TS3Upload::EStatus::Complete: {
                    EXPORT_LOG_I("Handle TEvS3Upload: status Complete, partsCount# " << upload->Parts.size());
                    Parts = std::move(upload->Parts);

                    TVector<Aws::S3::Model::CompletedPart> parts(Reserve(Parts.size()));
                    for (ui32 partIndex = 0; partIndex < Parts.size(); ++partIndex) {
                        parts.emplace_back(Aws::S3::Model::CompletedPart()
                            .WithPartNumber(partIndex + 1)
                            .WithETag(Parts.at(partIndex)));
                    }

                    EXPORT_LOG_I("Handle TEvS3Upload: sending CompleteMultipartUploadRequest");
                    auto request = Aws::S3::Model::CompleteMultipartUploadRequest()
                        .WithKey(Settings.GetDataKey(DataFormat, CompressionCodec))
                        .WithUploadId(*UploadId)
                        .WithMultipartUpload(Aws::S3::Model::CompletedMultipartUpload().WithParts(std::move(parts)));
                    this->Send(Client, new TEvExternalStorage::TEvCompleteMultipartUploadRequest(request));
                    break;
                }

                case TS3Upload::EStatus::Abort: {
                    EXPORT_LOG_I("Handle TEvS3Upload: status Abort");
                    Error = std::move(upload->Error);
                    if (!Error) {
                        Error = "<empty>";
                    }

                    EXPORT_LOG_I("Handle TEvS3Upload: sending AbortMultipartUploadRequest");
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

        EXPORT_LOG_I("Handle TEvExternalStorage::TEvCreateMultipartUploadResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess());

        if (!CheckResult(result, TStringBuf("CreateMultipartUpload"))) {
            return;
        }

        const auto uploadId = result.GetResult().GetUploadId().c_str();
        EXPORT_LOG_I("Handle TEvCreateMultipartUploadResponse: uploadId# " << uploadId << ", sending to DataShard");
        this->Send(DataShard, new TEvDataShard::TEvStoreS3UploadId(this->SelfId(), TxId, uploadId));
    }

    void Handle(TEvExternalStorage::TEvUploadPartResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("Handle TEvExternalStorage::TEvUploadPartResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess()
            << ", Last# " << Last
            << ", partsCount# " << Parts.size());

        if (!CheckResult(result, TStringBuf("UploadPart"))) {
            return;
        }

        const auto etag = result.GetResult().GetETag().c_str();
        EXPORT_LOG_I("Handle TEvUploadPartResponse: ETag# " << etag);
        Parts.push_back(etag);

        if (Last) {
            EXPORT_LOG_I("Handle TEvUploadPartResponse: Last part uploaded, total parts# " << Parts.size());
            auto nextStep = [this]() {
                EXPORT_LOG_I("Handle TEvUploadPartResponse: nextStep, calling Finish");
                Finish();
            };

            if (EnableChecksums) {
                EXPORT_LOG_I("Handle TEvUploadPartResponse: uploading checksum");
                // checksum is always calculated before compression
                TString checksumKey = ChecksumKey(Settings.GetDataKey(DataFormat, ECompressionCodec::None));
                TString dataKeySuffix = DataKeySuffix(ShardNum, DataFormat, ECompressionCodec::None, false);
                return UploadChecksum(std::move(DataChecksum), checksumKey, dataKeySuffix, nextStep);
            } else {
                EXPORT_LOG_I("Handle TEvUploadPartResponse: no checksums, calling nextStep");
                return nextStep();
            }
        }

        EXPORT_LOG_I("Handle TEvUploadPartResponse: Not last part, sending TEvFeed to Scanner");
        this->Send(Scanner, new TEvExportScan::TEvFeed());
    }

    void Handle(TEvExternalStorage::TEvCompleteMultipartUploadResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("Handle TEvExternalStorage::TEvCompleteMultipartUploadResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess());

        if (result.IsSuccess()) {
            EXPORT_LOG_I("Handle TEvCompleteMultipartUploadResponse: Success, calling PassAway");
            return PassAway();
        }

        const auto& error = result.GetError();
        EXPORT_LOG_E("Handle TEvCompleteMultipartUploadResponse: Error"
            << ", errorType# " << (ui32)error.GetErrorType()
            << ", message# " << error.GetMessage().c_str());
        
        if (error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_UPLOAD) {
            EXPORT_LOG_I("Handle TEvCompleteMultipartUploadResponse: NO_SUCH_UPLOAD, calling PassAway");
            return PassAway();
        }

        if (CanRetry(error)) {
            EXPORT_LOG_I("Handle TEvCompleteMultipartUploadResponse: Can retry, clearing UploadId and calling Retry");
            UploadId.Clear(); // force getting info after restart
            Retry();
        } else {
            EXPORT_LOG_E("Handle TEvCompleteMultipartUploadResponse: Cannot retry, setting Error and calling PassAway");
            Error = error.GetMessage().c_str();
            this->PassAway();
        }
    }

    void Handle(TEvExternalStorage::TEvAbortMultipartUploadResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        EXPORT_LOG_I("Handle TEvExternalStorage::TEvAbortMultipartUploadResponse"
            << ": self# " << this->SelfId()
            << ", isSuccess# " << result.IsSuccess());

        if (result.IsSuccess()) {
            EXPORT_LOG_I("Handle TEvAbortMultipartUploadResponse: Success, calling PassAway");
            return PassAway();
        }

        const auto& error = result.GetError();
        EXPORT_LOG_E("Handle TEvAbortMultipartUploadResponse: Error"
            << ", errorType# " << (ui32)error.GetErrorType()
            << ", message# " << error.GetMessage().c_str());
        
        if (CanRetry(error)) {
            EXPORT_LOG_I("Handle TEvAbortMultipartUploadResponse: Can retry, clearing UploadId and calling Retry");
            UploadId.Clear(); // force getting info after restart
            Retry();
        } else {
            Y_ENSURE(Error);
            EXPORT_LOG_E("Handle TEvAbortMultipartUploadResponse: Cannot retry, appending error and calling PassAway");
            Error = TStringBuilder() << *Error << " Additionally, 'AbortMultipartUpload' has failed: "
                << error.GetMessage();
            this->PassAway();
        }
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            EXPORT_LOG_I("CheckResult: Success for '" << marker << "'");
            return true;
        }

        EXPORT_LOG_E("CheckResult: Error at '" << marker << "'"
            << ": self# " << this->SelfId()
            << ", attempt# " << Attempt
            << ", retries# " << Retries
            << ", error# " << result);
        RetryOrFinish(result.GetError());

        return false;
    }

    bool CanRetry(const Aws::S3::S3Error& error) const {
        return Attempt < Retries && NWrappers::ShouldRetry(error);
    }

    void Retry() {
        Delay = Min(Delay * ++Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        EXPORT_LOG_I("Retry: scheduling wakeup"
            << ": self# " << this->SelfId()
            << ", attempt# " << Attempt
            << ", delay# " << Delay
            << ", random# " << random);
        this->Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    void RetryOrFinish(const Aws::S3::S3Error& error) {
        const bool canRetry = CanRetry(error);
        EXPORT_LOG_I("RetryOrFinish"
            << ": self# " << this->SelfId()
            << ", canRetry# " << canRetry
            << ", attempt# " << Attempt
            << ", retries# " << Retries
            << ", errorType# " << (ui32)error.GetErrorType()
            << ", message# " << error.GetMessage().c_str());
        
        if (canRetry) {
            EXPORT_LOG_I("RetryOrFinish: calling Retry");
            Retry();
        } else {
            EXPORT_LOG_E("RetryOrFinish: calling Finish with error");
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

            this->PassAway();
        } else {
            if (success) {
                this->Send(DataShard, new TEvDataShard::TEvChangeS3UploadStatus(this->SelfId(), TxId,
                    TS3Upload::EStatus::Complete, std::move(Parts)));
            } else {
                this->Send(DataShard, new TEvDataShard::TEvChangeS3UploadStatus(this->SelfId(), TxId,
                    TS3Upload::EStatus::Abort, *Error));
            }
            this->Become(&TThis::StateUploadData);
        }
    }

    void PassAway() override {
        EXPORT_LOG_I("PassAway START"
            << ": self# " << this->SelfId()
            << ", hasError# " << !Error.Empty()
            << ", error# " << Error.GetOrElse(TString())
            << ", hasScanner# " << (Scanner ? "true" : "false")
            << ", hasClient# " << (Client ? "true" : "false"));

        if (HttpProxy) {
            EXPORT_LOG_I("PassAway: sending PoisonPill to HttpProxy");
            this->Send(HttpProxy, new TEvents::TEvPoisonPill());
        }

        if (Scanner) {
            EXPORT_LOG_I("PassAway: sending TEvFinish to Scanner, success# " << Error.Empty());
            this->Send(Scanner, new TEvExportScan::TEvFinish(Error.Empty(), Error.GetOrElse(TString())));
        }

        EXPORT_LOG_I("PassAway: sending PoisonPill to Client");
        this->Send(Client, new TEvents::TEvPoisonPill());

        EXPORT_LOG_I("PassAway: calling IActor::PassAway");
        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "s3"sv;
    }

    static TSettings GetSettings(const NKikimrSchemeOp::TBackupTask& task);

    explicit TS3Uploader(
            const TActorId& dataShard, ui64 txId,
            const NKikimrSchemeOp::TBackupTask& task,
            TMaybe<Ydb::Table::CreateTableRequest>&& scheme,
            TVector<TChangefeedExportDescriptions> changefeeds,
            TMaybe<Ydb::Scheme::ModifyPermissionsRequest>&& permissions,
            TString&& metadata)
        : ExternalStorageConfig(NWrappers::IExternalStorageConfig::Construct(GetSettings(task)))
        , Settings(TStorageSettings::FromBackupTask<TSettings>(task))
        , DataFormat(EDataFormat::Csv)
        , CompressionCodec(CodecFromTask(task))
        , ShardNum(task.GetShardNum())
        , HttpResolverConfig(GetHttpResolverConfig(*GetS3StorageConfig()))
        , DataShard(dataShard)
        , TxId(txId)
        , Scheme(std::move(scheme))
        , Changefeeds(std::move(changefeeds))
        , Metadata(std::move(metadata))
        , Permissions(std::move(permissions))
        , Retries(task.GetNumberOfRetries())
        , Attempt(0)
        , Delay(TDuration::Minutes(1))
        , SchemeUploaded(ShardNum == 0 ? false : true)
        , ChangefeedsUploaded(ShardNum == 0 ? false : true)
        , MetadataUploaded(ShardNum == 0 ? false : true)
        , PermissionsUploaded(ShardNum == 0 ? false : true)
        , EnableChecksums(task.GetEnableChecksums())
        , EnablePermissions(task.GetEnablePermissions())
    {
        EXPORT_LOG_I("TS3Uploader constructor"
            << ": DataShard# " << dataShard
            << ", TxId# " << txId
            << ", ShardNum# " << ShardNum
            << ", hasScheme# " << Scheme.Defined()
            << ", changefeedsCount# " << Changefeeds.size()
            << ", hasPermissions# " << Permissions.Defined()
            << ", metadataSize# " << Metadata.size()
            << ", Retries# " << Retries
            << ", EnableChecksums# " << EnableChecksums
            << ", EnablePermissions# " << EnablePermissions);
    }

    void Bootstrap() {
        EXPORT_LOG_I("Bootstrap START"
            << ": self# " << this->SelfId()
            << ", attempt# " << Attempt
            << ", shardNum# " << ShardNum
            << ", retries# " << Retries
            << ", enableChecksums# " << EnableChecksums
            << ", enablePermissions# " << EnablePermissions);

        ProxyResolved = !HttpResolverConfig.Defined() || std::is_same<TSettings, NKikimrSchemeOp::TFSSettings>::value;
        EXPORT_LOG_I("Bootstrap: ProxyResolved# " << ProxyResolved);
        
        if (!ProxyResolved) {
            EXPORT_LOG_I("Bootstrap: calling ResolveProxy");
            ResolveProxy();
        } else {
            EXPORT_LOG_I("Bootstrap: calling Restart");
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

    STATEFN(StateUploadChangefeed) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleChangefeed);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadTopic) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleTopic);
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

    STATEFN(StateUploadChecksum) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, HandleChecksum);
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
    TStorageSettings Settings;
    const EDataFormat DataFormat;
    const ECompressionCodec CompressionCodec;
    const ui32 ShardNum;
    bool ProxyResolved;

    TMaybe<THttpResolverConfig> HttpResolverConfig;
    TActorId HttpProxy;

    const TActorId DataShard;
    const ui64 TxId;
    const TMaybe<Ydb::Table::CreateTableRequest> Scheme;
    const TVector<TChangefeedExportDescriptions> Changefeeds;
    const TString Metadata;
    const TMaybe<Ydb::Scheme::ModifyPermissionsRequest> Permissions;

    const ui32 Retries;
    ui32 Attempt;
    ui64 IndexExportedChangefeed = 0;

    TDuration Delay;
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TActorId Client;
    bool SchemeUploaded;
    bool ChangefeedsUploaded;
    bool MetadataUploaded;
    bool PermissionsUploaded;
    bool MultiPart;
    bool Last;

    TActorId Scanner;
    TString Buffer;

    TMaybe<TString> UploadId;
    TVector<TString> Parts;
    TMaybe<TString> Error;

    bool EnableChecksums;
    bool EnablePermissions;

    TString DataChecksum;
    TString MetadataChecksum;
    TString ChangefeedChecksum;
    TString TopicChecksum;
    TString SchemeChecksum;
    TString PermissionsChecksum;
    std::function<void()> ChecksumUploadedCallback;

}; // TS3Uploader

template <>
NKikimrSchemeOp::TS3Settings TS3Uploader<NKikimrSchemeOp::TS3Settings>::GetSettings(
    const NKikimrSchemeOp::TBackupTask& task) 
{
    return task.GetS3Settings();
}

template <>
NKikimrSchemeOp::TFSSettings TS3Uploader<NKikimrSchemeOp::TFSSettings>::GetSettings(
    const NKikimrSchemeOp::TBackupTask& task) 
{
    return task.GetFSSettings();
}

IActor* CreateUploaderBySettingsType(
    const TActorId& dataShard,
    ui64 txId,
    const NKikimrSchemeOp::TBackupTask& task,
    TMaybe<Ydb::Table::CreateTableRequest>&& scheme,
    TVector<TChangefeedExportDescriptions>&& changefeeds,
    TMaybe<Ydb::Scheme::ModifyPermissionsRequest>&& permissions,
    TString&& metadata)
{
    if (task.HasS3Settings()) {
        return new TS3Uploader<NKikimrSchemeOp::TS3Settings>(
            dataShard, txId, task,
            std::move(scheme), std::move(changefeeds),
            std::move(permissions), std::move(metadata));
    }

    if (task.HasFSSettings()) {
        return new TS3Uploader<NKikimrSchemeOp::TFSSettings>(
            dataShard, txId, task,
            std::move(scheme), std::move(changefeeds),
            std::move(permissions), std::move(metadata));
    }

    Y_ABORT("Unsupported storage type in backup task");
}

IActor* TS3Export::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();

    const bool encrypted = Task.HasEncryptionSettings();

    TMetadata metadata;
    metadata.SetVersion(Task.GetEnableChecksums() ? 1 : 0);
    metadata.SetEnablePermissions(Task.GetEnablePermissions());

    TVector<TChangefeedExportDescriptions> changefeeds;
    if (AppData()->FeatureFlags.GetEnableChangefeedsExport()) {
        const auto& persQueues = Task.GetChangefeedUnderlyingTopics();
        const auto& cdcStreams = Task.GetTable().GetTable().GetCdcStreams();
        Y_ASSERT(persQueues.size() == cdcStreams.size());

        const int changefeedsCount = cdcStreams.size();
        changefeeds.reserve(changefeedsCount);

        for (int i = 0; i < changefeedsCount; ++i) {
            Ydb::Table::ChangefeedDescription changefeed;
            const auto& cdcStream = cdcStreams.at(i);
            FillChangefeedDescription(changefeed, cdcStream);

            Ydb::Topic::DescribeTopicResult topic;
            const auto& pq = persQueues.at(i);
            Ydb::StatusIds::StatusCode status;
            TString error;
            FillTopicDescription(topic, pq.GetPersQueueGroup(), pq.GetSelf(), cdcStream.GetName(), status, error);
            // Unnecessary fields
            topic.clear_self();
            topic.clear_topic_stats();

            auto& descr = changefeeds.emplace_back(changefeed, topic);
            descr.Name = descr.ChangefeedDescription.name();
            if (encrypted) {
                // Anonymize changefeed name in export
                std::stringstream prefix;
                prefix << std::setfill('0') << std::setw(3) << std::right << (i + 1);
                descr.Prefix = prefix.str();
            } else {
                descr.Prefix = descr.Name;
            }

            metadata.AddChangefeed(TChangefeedMetadata{
                .ExportPrefix = descr.Prefix,
                .Name = descr.Name,
            });
        }
    }

    if (scheme) {
        int idx = changefeeds.size() + 1;
        for (const auto& index : scheme->indexes()) {
            const auto indexType = NTableIndex::ConvertIndexType(index.type_case());
            const TVector<TString> indexColumns(index.index_columns().begin(), index.index_columns().end());
            std::optional<Ydb::Table::FulltextIndexSettings::Layout> layout;
            if (indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltext) {
                const auto& settings = index.global_fulltext_index().fulltext_settings();
                layout = settings.has_layout() ? settings.layout() : Ydb::Table::FulltextIndexSettings::LAYOUT_UNSPECIFIED;
            }

            for (const auto& implTable : NTableIndex::GetImplTables(indexType, indexColumns, layout)) {
                const TString implTablePrefix = TStringBuilder() << index.name() << "/" << implTable;
                TString exportPrefix;
                if (encrypted) {
                    std::stringstream prefix;
                    prefix << std::setfill('0') << std::setw(3) << std::right << idx++;
                    exportPrefix = prefix.str();
                } else {
                    exportPrefix = implTablePrefix;
                }

                metadata.AddIndex(TIndexMetadata{
                    .ExportPrefix = exportPrefix,
                    .ImplTablePrefix = implTablePrefix,
                });
            }
        }
    }

    auto permissions = (Task.GetEnablePermissions() && Task.GetShardNum() == 0)
        ? GenYdbPermissions(Task.GetTable())
        : Nothing();
    TFullBackupMetadata::TPtr backup = new TFullBackupMetadata{
        .SnapshotVts = TVirtualTimestamp(
            Task.GetSnapshotStep(),
            Task.GetSnapshotTxId())
    };
    metadata.AddFullBackup(backup);

    return CreateUploaderBySettingsType(
        dataShard, txId, Task,
        std::move(scheme), std::move(changefeeds),
        std::move(permissions), metadata.Serialize());
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
