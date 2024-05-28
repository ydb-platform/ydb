#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include "viewer.h"
#include "json_pipe_req.h"

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

namespace NKikimr {
namespace NViewer {

struct TEvLocalRpcPrivate {
    enum EEv {
        EvGrpcRequestResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 100,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    template<class TProtoResult>
    struct TEvGrpcRequestResult : NActors::TEventLocal<TEvGrpcRequestResult<TProtoResult>, EvGrpcRequestResult> {
        THolder<TProtoResult> Message;
        THolder<NYdb::TStatus> Status;

        TEvGrpcRequestResult()
        {}
    };
};

using namespace NActors;
using NSchemeShard::TEvSchemeShard;

template <class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoService, class TRpcEv>
class TJsonLocalRpc : public TActorBootstrapped<TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>> {
    using TThis = TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>;
    using TBase = TActorBootstrapped<TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>>;

    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;

    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>> DescribeResult;

    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    TString Database;

    NThreading::TFuture<TProtoResponse> RpcFuture;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonLocalRpc(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    TProtoRequest Params2Proto(const TCgiParameters& params) {
        TProtoRequest request;
        using google::protobuf::Descriptor;
        using google::protobuf::Reflection;
        using google::protobuf::FieldDescriptor;
        using google::protobuf::EnumDescriptor;
        using google::protobuf::EnumValueDescriptor;
        const Descriptor& descriptor = *request.GetDescriptor();
        const Reflection& reflection = *request.GetReflection();
        for (int idx = 0; idx < descriptor.field_count(); ++idx) {
            const FieldDescriptor* field = descriptor.field(idx);
            TString name;
            name = field->name();
            TString value = params.Get(name);
            if (!value.empty()) {
                FieldDescriptor::CppType type = field->cpp_type();
                switch (type) {
#define CASE(BT, ST, CT) case FieldDescriptor::CPPTYPE_##BT: {\
                ST res = {};\
                if (TryFromString(value, res)) {\
                    reflection.Set##CT(&request, field, res);\
                }\
                break;\
                }

                CASE(INT32, i32, Int32);
                CASE(INT64, i64, Int64);
                CASE(UINT32, ui32, UInt32);
                CASE(UINT64, ui64, UInt64);
                CASE(FLOAT, float, Float);
                CASE(DOUBLE, double, Double);
                CASE(BOOL, bool, Bool);
                CASE(STRING, string, String);
#undef CASE
                case FieldDescriptor::CPPTYPE_ENUM: {
                    const EnumDescriptor* enumDescriptor = field->enum_type();
                    const EnumValueDescriptor* enumValueDescriptor = enumDescriptor->FindValueByName(value);
                    int number = 0;
                    if (enumValueDescriptor == nullptr && TryFromString(value, number)) {
                        enumValueDescriptor = enumDescriptor->FindValueByNumber(number);
                    }
                    if (enumValueDescriptor != nullptr) {
                        reflection.SetEnum(&request, field, enumValueDescriptor);
                    }
                    break;
                }
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    break;
                }
            }
        }
        return request;
    }

    TProtoRequest Params2Proto() {
        TProtoRequest request;
        NProtobufJson::TJson2ProtoConfig json2ProtoConfig;
        auto postData = Event->Get()->Request.GetPostContent();
        if (!postData.empty()) {
            try {
                NProtobufJson::Json2Proto(postData, request, json2ProtoConfig);
            }
            catch (const yexception& e) {
                Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                PassAway();
            }
        } else {
            const auto& params(Event->Get()->Request.GetParams());
            return Params2Proto(params);
        }
        return request;
    }

    void SendGrpcRequest() {
        TProtoRequest request = Params2Proto();

        RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), Database,
                                        Event->Get()->UserToken, TlsActivationContext->ActorSystem());
        RpcFuture.Subscribe([actorId = TBase::SelfId(), actorSystem = TlsActivationContext->ActorSystem()]
                            (const NThreading::TFuture<TProtoResponse>& future) {
            auto& response = future.GetValueSync();
            auto result = MakeHolder<TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>>();
            Y_ABORT_UNLESS(response.operation().ready());
            if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                TProtoResult rs;
                response.operation().result().UnpackTo(&rs);
                result->Message = MakeHolder<TProtoResult>(rs);
            }
            NYql::TIssues issues;
            NYql::IssuesFromMessage(response.operation().issues(), issues);
            result->Status = MakeHolder<NYdb::TStatus>(NYdb::EStatus(response.operation().status()),
                                                           std::move(issues));

            actorSystem->Send(actorId, result.Release());
        });
    }


    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Database = params.Get("database_path");

        SendGrpcRequest();

        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Handle(typename TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>::TPtr& ev) {
        DescribeResult = ev->Release();
        ReplyAndPassAway();
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() {
        TStringStream json;
        TString headers = Viewer->GetHTTPOKJSON(Event->Get());
        if (DescribeResult) {
            if (!DescribeResult->Status->IsSuccess()) {
                headers = Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request");
                if (DescribeResult->Status->GetStatus() == NYdb::EStatus::UNAUTHORIZED) {
                    headers = Viewer->GetHTTPFORBIDDEN(Event->Get());
                }
            } else {
                TProtoToJson::ProtoToJson(json, *(DescribeResult->Message), JsonSettings);
            }
        } else {
            json << "null";
        }

        Send(Event->Sender, new NMon::TEvHttpInfoRes(headers + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};


using TJsonDescribeTopic = TJsonLocalRpc<Ydb::Topic::DescribeTopicRequest,
                                         Ydb::Topic::DescribeTopicResponse,
                                         Ydb::Topic::DescribeTopicResult,
                                         Ydb::Topic::V1::TopicService,
                                         NKikimr::NGRpcService::TEvDescribeTopicRequest>;

using TJsonDescribeConsumer = TJsonLocalRpc<Ydb::Topic::DescribeConsumerRequest,
                                         Ydb::Topic::DescribeConsumerResponse,
                                         Ydb::Topic::DescribeConsumerResult,
                                         Ydb::Topic::V1::TopicService,
                                         NKikimr::NGRpcService::TEvDescribeConsumerRequest>;

template <>
struct TJsonRequestParameters<TJsonDescribeTopic> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: path
              in: query
              description: schema path
              required: false
              type: string
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            - name: database_path
              in: query
              description: database path
              required: false
              type: string
            - name: include_stats
              in: query
              description: include stat flag
              required: false
              type: bool
        )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonDescribeTopic> {
    static TString GetSummary() {
        return "Topic schema detailed information";
    }
};

template <>
struct TJsonRequestDescription<TJsonDescribeTopic> {
    static TString GetDescription() {
        return "Returns detailed information about topic";
    }
};


template <>
struct TJsonRequestParameters<TJsonDescribeConsumer> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: path
              in: query
              description: schema path
              required: false
              type: string
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            - name: database_path
              in: query
              description: database path
              required: false
              type: string
            - name: consumer
              in: query
              description: consumer name
              required: false
              type: string
            - name: include_stats
              in: query
              description: include stat flag
              required: false
              type: bool
        )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonDescribeConsumer> {
    static TString GetSummary() {
        return "Topic's consumer detailed information";
    }
};

template <>
struct TJsonRequestDescription<TJsonDescribeConsumer> {
    static TString GetDescription() {
        return "Returns detailed information about topic's consumer";
    }
};


}
}
