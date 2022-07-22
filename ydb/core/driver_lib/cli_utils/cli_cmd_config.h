#pragma once
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <util/generic/variant.h>

namespace NKikimr {
namespace NDriverClient {
    struct TCliCmdConfig : TNonCopyable {
        TMaybe<std::variant<NMsgBusProxy::TMsgBusClientConfig, NGRpcProxy::TGRpcClientConfig>> ClientConfig;
        NMsgBusProxy::TMsgBusClientConfig MsgBusClientConfig;
        TString Address;

        void ConfigureBaseLastGetopt(NLastGetopt::TOpts &opts);
        void ConfigureMsgBusLastGetopt(const NLastGetopt::TOptsParseResult& res, int argc, char** argv);

        template<typename TRequest>
        NBus::EMessageStatus SyncCall(TAutoPtr<TRequest> request, TAutoPtr<NBus::TBusMessage>& response) const {
            auto visitor = [&](const auto& config) {
                NClient::TKikimr kikimr(config);
                auto future = kikimr.ExecuteRequest(request.Release());
                auto data = future.GetValue(TDuration::Max());
                if (data.GetTransportStatus() == NBus::MESSAGE_OK) {
                    switch (data.GetType()) {
                        case NMsgBusProxy::MTYPE_CLIENT_RESPONSE: {
                            TAutoPtr<NMsgBusProxy::TBusResponse> x(new NMsgBusProxy::TBusResponse);
                            x->Record = data.template GetResult<NKikimrClient::TResponse>();
                            response = x.Release();
                            break;
                        }

                        case NMsgBusProxy::MTYPE_CLIENT_LOAD_RESPONSE: {
                            TAutoPtr<NMsgBusProxy::TBusBsTestLoadResponse> x(new NMsgBusProxy::TBusBsTestLoadResponse);
                            x->Record = data.template GetResult<NKikimrClient::TBsTestLoadResponse>();
                            response = x.Release();
                            break;
                        }

                        case NMsgBusProxy::MTYPE_CLIENT_DS_LOAD_RESPONSE: {
                            TAutoPtr<NMsgBusProxy::TBusDsTestLoadResponse> x(new NMsgBusProxy::TBusDsTestLoadResponse);
                            x->Record = data.template GetResult<NKikimrClient::TDsTestLoadResponse>();
                            response = x.Release();
                            break;
                        }

                        default:
                            Y_FAIL("unexpected reply message type");
                    }
                }
                return data.GetTransportStatus();
            };
            if (const auto& conf = ClientConfig) {
                return std::visit(std::move(visitor), *conf);
            } else {
                Y_FAIL("Client configuration is not provided");
            }
        }
    };

}
}
