#pragma once

#include "cli_command.h"

#include <ydb/public/lib/deprecated/kicli/kicli.h>

namespace NKikimr {
namespace NDriverClient {

template <typename ResultType>
int HandleResponse(const NThreading::TFuture<ResultType>& future, std::function<int(const ResultType&)> callback) {
    static char animSprites[] = "\\|/-";
    size_t animSprite = 0;
    NHPTimer::STime startTime;
    if (TClientCommand::TIME_REQUESTS) {
        NHPTimer::GetTime(&startTime);
    }
    while (!future.Wait(TInstant::MilliSeconds(200))) {
        if (TClientCommand::PROGRESS_REQUESTS) {
            Cerr << '\r' << animSprites[animSprite++];
            if (animSprite > 3)
                animSprite = 0;
            if (TClientCommand::TIME_REQUESTS) {
                NHPTimer::STime tempTime = startTime;
                TDuration timePassed = TDuration::Seconds(NHPTimer::GetTimePassed(&tempTime));
                Cerr << ' ' << timePassed;
            }
        }
    }
    if (TClientCommand::PROGRESS_REQUESTS) {
        Cerr << '\r';
    }
    if (TClientCommand::TIME_REQUESTS) {
        TDuration timePassed = TDuration::Seconds(NHPTimer::GetTimePassed(&startTime));
        Cerr << timePassed << "    " << Endl;
    }
    const ResultType& result(future.GetValue());
    NClient::TError error(result.GetError());
    if (!error.Success()) {
        Cerr << error.GetCode() << ' ' << error.GetMessage() << Endl;
        return 1;
    }
    return callback(result);
}

int InvokeThroughKikimr(TClientCommand::TConfig& config, std::function<int(NClient::TKikimr&)> handler);

template <typename RequestType>
void PrepareRequest(TClientCommand::TConfig&, TAutoPtr<RequestType>&) {}

void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusRequest>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusSchemeInitRoot>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusSchemeOperation>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusSchemeDescribe>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusCmsRequest>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusConsoleRequest>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusTabletLocalMKQL>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusTabletLocalSchemeTx>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusFillNode>& request);
void PrepareRequest(TClientCommand::TConfig& config, TAutoPtr<NMsgBusProxy::TBusDrainNode>& request);

template <typename ResponseType>
int OnMessageBus(const TClientCommand::TConfig& config, const ResponseType& response);

template <typename RequestType, typename ResponseType>
int MessageBusCall(TClientCommand::TConfig& config, TAutoPtr<RequestType> request, std::function<int(const ResponseType&)> callback) {
    PrepareRequest(config, request);
    auto handler = [&](NClient::TKikimr& kikimr) {
        NThreading::TFuture<NClient::TResult> future(kikimr.ExecuteRequest(request.Release()));
        return HandleResponse<NClient::TResult>(future, [&](const NClient::TResult& result) -> int {
            if (!result.HaveResponse<ResponseType>()) {
                Cerr << "Unknown response received: " << result.GetType() << Endl;
                return 1;
            }
            return callback(result.GetResponse<ResponseType>());
        });
    };
    return InvokeThroughKikimr(config, std::move(handler));
}

template <typename RequestType, typename ResponseType = NMsgBusProxy::TBusResponse>
int MessageBusCall(TClientCommand::TConfig& config, TAutoPtr<RequestType> request) {
    return MessageBusCall<RequestType, ResponseType>(config, request, [&config](const ResponseType& response) -> int {
        return OnMessageBus(config, response);
    });
}

}
}
