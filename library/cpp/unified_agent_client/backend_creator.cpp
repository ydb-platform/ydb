#include "backend_creator.h"
#include <library/cpp/logger/global/global.h>

namespace NUnifiedAgent {


    TLogBackendCreator::TLogBackendCreator()
        : TLogBackendCreatorBase("unified_agent")
    {}

    bool TLogBackendCreator::Init(const IInitContext& ctx) {
        if(TString socket = ctx.GetOrElse("Uri", TString())) {
            ClientParams = MakeHolder<TClientParameters>(socket);
        } else {
            Cdbg << "Uri not set for unified_agent log backend" << Endl;
            return false;
        }
        TString secretKey;
        ctx.GetValue("SharedSecretKey", secretKey);
        if (secretKey) {
            ClientParams->SharedSecretKey = secretKey;
        }
        ctx.GetValue("MaxInflightBytes", ClientParams->MaxInflightBytes);
        ctx.GetValue("GrpcSendDelay", ClientParams->GrpcSendDelay);
        size_t rateLimit;
        if (ctx.GetValue("LogRateLimit", rateLimit)) {
            ClientParams->LogRateLimitBytes = rateLimit;
        }
        ctx.GetValue("GrpcReconnectDelay", ClientParams->GrpcReconnectDelay);
        ctx.GetValue("GrpcMaxMessageSize", ClientParams->GrpcMaxMessageSize);
        const auto ownLogger = ctx.GetChildren("OwnLogger");
        if (!ownLogger.empty() && ownLogger.front()->GetOrElse("LoggerType", TString()) != "global") {
            OwnLogger = ILogBackendCreator::Create(*ownLogger.front());
            TLog log;
            log.ResetBackend(OwnLogger->CreateLogBackend());
            ClientParams->SetLog(log);
        }
        return true;
    }


    void TLogBackendCreator::DoToJson(NJson::TJsonValue& value) const {
        value["Uri"] = ClientParams->Uri;
        if (ClientParams->SharedSecretKey) {
            value["SharedSecretKey"] = *ClientParams->SharedSecretKey;
        }
        value["MaxInflightBytes"] = ClientParams->MaxInflightBytes;
        value["GrpcSendDelay"] = ClientParams->GrpcSendDelay.ToString();
        if (ClientParams->LogRateLimitBytes) {
            value["LogRateLimit"] = *ClientParams->LogRateLimitBytes;
        }
        value["GrpcReconnectDelay"] = ClientParams->GrpcReconnectDelay.ToString();
        value["GrpcMaxMessageSize"] = ClientParams->GrpcMaxMessageSize;
        if (OwnLogger) {
            OwnLogger->ToJson(value["OwnLogger"].AppendValue(NJson::JSON_MAP));
        }
    }

    THolder<TLogBackend> TLogBackendCreator::DoCreateLogBackend() const {
        return MakeLogBackend(*ClientParams);
    }

}
