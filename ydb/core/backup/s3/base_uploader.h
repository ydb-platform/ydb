#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "extstorage_usage_config.h"

#include <ydb/core/backup/common/backup_restore_traits.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/services/services.pb.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#define EXPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, LogService(), "[Export] [" << LogPrefix() << "] " << stream)

namespace NKikimr::NBackup::NS3 {

using namespace NCommon;

class TS3UploaderBase: public TActorBootstrapped<TS3UploaderBase> {
public:
    using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
    using THttpResolverConfig = NKikimrConfig::TS3ProxyResolverConfig::THttpResolverConfig;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;

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
        // TODO(pixcc): move to impl

        if (Attempt) {
            this->Send(std::exchange(Client, TActorId()), new TEvents::TEvPoisonPill());
        }

        Client = this->RegisterWithSameMailbox(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));

        Start();
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

    void PassAway() override {
        if (HttpProxy) {
            Send(HttpProxy, new TEvents::TEvPoisonPill());
        }
        // TODO(pixcc): move to impl

        this->Send(Client, new TEvents::TEvPoisonPill());

        IActor::PassAway();
    }

protected:
    virtual NKikimrServices::EServiceKikimr LogService() const = 0;
    virtual void Start() = 0;
    virtual void Finish(bool success = true, const TString& error = {}) = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "s3"sv;
    }

    explicit TS3UploaderBase(const NKikimrSchemeOp::TBackupTask& task)
        : ExternalStorageConfig(new TS3ExternalStorageConfig(task.GetS3Settings()))
        , Settings(TS3Settings::FromBackupTask(task))
        , HttpResolverConfig(GetHttpResolverConfig(*GetS3StorageConfig()))
        , Retries(task.GetNumberOfRetries())
        , Attempt(0)
        , Delay(TDuration::Minutes(1))
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

protected:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TS3Settings Settings;

    bool ProxyResolved;
    TMaybe<THttpResolverConfig> HttpResolverConfig;
    TActorId HttpProxy;

    const ui32 Retries;
    ui32 Attempt;

    TDuration Delay;
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TActorId Client;
    TMaybe<TString> Error;

}; // TS3UploaderBase

} // namespace NKikimr::NBackup::NS3

#endif // KIKIMR_DISABLE_S3_OPS
