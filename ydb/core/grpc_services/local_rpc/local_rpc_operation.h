#pragma once

#include "local_rpc.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

namespace NKikimr::NRpcService {

using TLocalRpcOperationResult = std::pair<NYdb::TStatus, ::google::protobuf::Any>;
using TLocalRpcOperationRequestCreator = std::function<void(std::unique_ptr<NGRpcService::IRequestOpCtx>, const NKikimr::NGRpcService::IFacilityProvider&)>;

template <typename TRpc, typename TOperationSettings>
class TOperationRequestExecuter final : public TActorBootstrapped<TOperationRequestExecuter<TRpc, TOperationSettings>>, public NGRpcService::IFacilityProvider {
    using TBase = TActorBootstrapped<TOperationRequestExecuter<TRpc, TOperationSettings>>;
    using TCbWrapper = TPromiseWrapper<typename TRpc::TResponse>;
    using TCtx = TLocalRpcCtx<TRpc, TCbWrapper>;

public:
    struct TSettings {
        const ui64 ChannelBufferSize = 0;
        const NYdb::TOperationRequestSettings<TOperationSettings>& OperationSettings;
        TLocalRpcOperationRequestCreator RequestCreator;
        TString Database;
        TMaybe<TString> Token;
        NThreading::TPromise<TLocalRpcOperationResult> Promise;
        TString OperationName = "local_rpc_operation";
    };

    TOperationRequestExecuter(typename TRpc::TRequest&& proto, const TSettings& settings)
        : ChannelBufferSize(settings.ChannelBufferSize)
        , RequestCreator(settings.RequestCreator)
        , ClientLostStatus(std::make_shared<std::atomic_bool>(false))
        , Promise(settings.Promise)
        , Ctx(CreateCtx(std::move(proto), settings, ClientLostStatus))
    {
        Y_VALIDATE(ChannelBufferSize, "ChannelBufferSize is not set");
        Y_VALIDATE(RequestCreator, "RequestCreator is not set");
    }

    void Bootstrap() {
        TBase::Become(&TOperationRequestExecuter::StateFunc);

        Promise.GetFuture().Subscribe([actorSystem = TBase::ActorContext().ActorSystem(), selfId = TBase::SelfId()](const auto&) {
            actorSystem->Send(selfId, new TEvents::TEvPoison());
        });

        RequestCreator(std::move(Ctx), *this);
    }

    STRICT_STFUNC(StateFunc,
        sFunc(TEvents::TEvPoison, Finish);
    );

    ui64 GetChannelBufferSize() const final {
        return ChannelBufferSize;
    }

    TActorId RegisterActor(IActor* actor) const final {
        return TBase::RegisterWithSameMailbox(actor);
    }

private:
    static std::unique_ptr<TCtx> CreateCtx(typename TRpc::TRequest&& proto, const TSettings& settings, std::shared_ptr<std::atomic_bool> clientLostStatus) {
        const auto& operationSettings = settings.OperationSettings;
        FillOperationParams(*proto.mutable_operation_params(), operationSettings);

        auto responsePromise = NThreading::NewPromise<typename TRpc::TResponse>();
        auto req = std::make_unique<TCtx>(std::move(proto), TCbWrapper(responsePromise), typename TCtx::TSettings{
            .DatabaseName = settings.Database,
            .Token = settings.Token,
            .RequestType = operationSettings.RequestType_.empty() ? Nothing() : TMaybe<TString>(operationSettings.RequestType_),
            .PeerName = TStringBuilder() << "localhost/" << settings.OperationName,
            .TraceId = TString(operationSettings.TraceId_),
            .Deadline = TInstant::MicroSeconds(std::chrono::duration_cast<std::chrono::microseconds>(operationSettings.Deadline_.GetTimePoint().time_since_epoch()).count()),
            .ClientLostStatus = clientLostStatus,
        });

        if (!operationSettings.TraceParent_.empty()) {
            req->PutPeerMeta(NYdb::OTEL_TRACE_HEADER, TString(operationSettings.TraceParent_));
        }

        for (const auto& [key, value] : operationSettings.Header_) {
            req->PutPeerMeta(TString(key), TString(value));
        }

        responsePromise.GetFuture().Apply([promise = settings.Promise](const NThreading::TFuture<typename TRpc::TResponse>& f) mutable {
            const auto& response = f.GetValue().operation();
            Y_VALIDATE(response.ready(), "Operation is not ready in SYNC mode");

            NYdb::NIssue::TIssues opIssues;
            NYdb::NIssue::IssuesFromMessage(response.issues(), opIssues);
            promise.SetValue(std::make_pair(NYdb::TStatus(static_cast<NYdb::EStatus>(response.status()), std::move(opIssues)), response.result()));
        });

        return req;
    }

    static void FillOperationParams(Ydb::Operations::OperationParams& params, const NYdb::TOperationRequestSettings<TOperationSettings>& settings) {
        params.set_operation_mode(Ydb::Operations::OperationParams::SYNC);

        if (settings.CancelAfter_) {
            SetDuration(*params.mutable_cancel_after(), settings.CancelAfter_);
        }

        if (settings.ForgetAfter_) {
            SetDuration(*params.mutable_forget_after(), settings.ForgetAfter_);
        }

        if (settings.OperationTimeout_) {
            SetDuration(*params.mutable_operation_timeout(), settings.OperationTimeout_);
        } else if (settings.ClientTimeout_ && settings.ClientTimeout_ != TDuration::Max() && settings.UseClientTimeoutForOperation_) {
            SetDuration(*params.mutable_operation_timeout(), settings.ClientTimeout_);
        }

        if (settings.ReportCostInfo_) {
            params.set_report_cost_info(Ydb::FeatureFlag::ENABLED);
        }
    }

    static void SetDuration(google::protobuf::Duration& protoValue, const TDuration& duration) {
        protoValue.set_seconds(duration.Seconds());
        protoValue.set_nanos(duration.NanoSecondsOfSecond());
    }

    void Finish() {
        if (ClientLostStatus) {
            ClientLostStatus->store(true);
            ClientLostStatus = nullptr;
        }
        TBase::PassAway();
    }

    const ui64 ChannelBufferSize = 0;
    const TLocalRpcOperationRequestCreator RequestCreator;
    std::shared_ptr<std::atomic_bool> ClientLostStatus;
    NThreading::TPromise<TLocalRpcOperationResult> Promise;
    std::unique_ptr<TCtx> Ctx;
};

} // namespace NKikimr::NRpcService
