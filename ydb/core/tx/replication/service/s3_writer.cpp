#include "json_change_record.h"
#include "logging.h"
#include "s3_writer.h"
#include "worker.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

#define CB_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define CB_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define CB_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define CB_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define CB_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() << stream)
#define CB_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CONTINUOUS_BACKUP, GetLogPrefix() <<  stream)

namespace {

TString GetPartKey(ui64 firstOffset, const TString& writerName) {
    return Sprintf("part.%ld.%s.jsonl", firstOffset, writerName.c_str());
}

TString GetIdentityKey(const TString& writerName) {
    return Sprintf("writer.%s.json", writerName.c_str());
}

} // anonymous namespace

namespace NKikimr::NReplication::NService {

using TS3ExternalStorageConfig = NWrappers::NExternalStorage::TS3ExternalStorageConfig;
using TEvExternalStorage = NWrappers::TEvExternalStorage;

struct TS3Request {
    TS3Request(Aws::S3::Model::PutObjectRequest&& request, TString&& buffer)
        : Request(std::move(request))
        , Buffer(std::move(buffer))
    {}

    Aws::S3::Model::PutObjectRequest Request;
    TString Buffer;
};

// TODO try to batch
// TODO add external way to configure retries
// TODO add sensors
class TS3Writer
    : public TActor<TS3Writer>
{
    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[S3Writer]"
                << TableName
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        CB_LOG_E("Error at '" << marker << "'"
            << ", error# " << result);
        RetryOrLeave(result.GetError());

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

    void RetryOrLeave(const Aws::S3::S3Error& error) {
        if (CanRetry(error)) {
            Retry();
        } else {
            Leave(TStringBuilder() << "S3 error: " << error.GetMessage().c_str());
        }
    }

    void Leave(const TString& error) {
        CB_LOG_I("Leave"
            << ": error# " << error);

        // TODO support different error kinds
        Send(Worker, new TEvWorker::TEvGone(TEvWorker::TEvGone::S3_ERROR));

        PassAway();
    }

    void SendS3Request() {
        Y_VERIFY(RequestInFlight);
        Send(S3Client, new TEvExternalStorage::TEvPutObjectRequest(RequestInFlight->Request, TString(RequestInFlight->Buffer)));
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        CB_LOG_D("Handshake"
            << ": worker# " << Worker);

        S3Client = RegisterWithSameMailbox(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));

        WriteIdentity();
    }

    void WriteIdentity() {
        const TString key = GetIdentityKey(WriterName);

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(key);

        auto identity = NJson::TJsonMap{
            {"finished", Finished},
            {"table_name", TableName},
            {"writer_name", WriterName},
        };

        TString buffer = NJson::WriteJson(identity, false);

        RequestInFlight = std::make_unique<TS3Request>(std::move(request), std::move(buffer));
        SendS3Request();
    }

    void PassAway() override {
        Send(S3Client, new TEvents::TEvPoison());
        TActor::PassAway();
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        CB_LOG_D("Handle " << ev->Get()->ToString());

        if (!ev->Get()->Records) {
            Finished = true;
            WriteIdentity();
            return;
        }

        const TString key = GetPartKey(ev->Get()->Records[0].Offset, WriterName);

        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(key);

        TStringBuilder buffer;

        for (auto& rec : ev->Get()->Records) {
            buffer << rec.Data << '\n';
        }

        RequestInFlight = std::make_unique<TS3Request>(std::move(request), std::move(buffer));
        SendS3Request();
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        CB_LOG_D("Handle " << ev->Get()->ToString());

        if (!CheckResult(result, TStringBuf("PutObject"))) {
            return;
        } else {
            RequestInFlight = nullptr;
        }

        if (!IdentityWritten) {
            IdentityWritten = true;
            Send(Worker, new TEvWorker::TEvHandshake());
        } else if (!Finished) {
            Send(Worker, new TEvWorker::TEvPoll());
        } else {
            Send(Worker, new TEvWorker::TEvGone(TEvWorker::TEvGone::DONE));
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_S3_WRITER;
    }

    explicit TS3Writer(
        NWrappers::IExternalStorageConfig::TPtr&& s3Settings,
        const TString& tableName,
        const TString& writerName)
        : TActor(&TThis::StateWork)
        , ExternalStorageConfig(std::move(s3Settings))
        , TableName(tableName)
        , WriterName(writerName)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle);
            sFunc(TEvents::TEvWakeup, SendS3Request);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    mutable TMaybe<TString> LogPrefix;
    const TString TableName;
    const TString WriterName;
    TActorId Worker;
    TActorId S3Client;
    bool IdentityWritten = false;
    bool Finished = false;

    std::unique_ptr<TS3Request> RequestInFlight = nullptr;

    const ui32 Retries = 3;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);
}; // TS3Writer

IActor* CreateS3Writer(NWrappers::IExternalStorageConfig::TPtr&& s3Settings, const TString& tableName, const TString& writerName) {
    return new TS3Writer(std::move(s3Settings), tableName, writerName);
}

}
