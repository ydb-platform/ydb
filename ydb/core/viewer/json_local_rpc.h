#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <library/cpp/json/json_writer.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr {
namespace NViewer {

struct TEvLocalRpcPrivate {
    enum EEv {
        EvGrpcRequestResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 100,
        EvError,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    template<class TProtoResult>
    struct TEvGrpcRequestResult : NActors::TEventLocal<TEvGrpcRequestResult<TProtoResult>, EvGrpcRequestResult> {
        TProtoResult Message;
        std::optional<NYdb::TStatus> Status;

        TEvGrpcRequestResult()
        {}
    };
};

using namespace NActors;
using NSchemeShard::TEvSchemeShard;

template <class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoService, class TRpcEv>
class TJsonLocalRpc : public TActorBootstrapped<TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>> {
    using TThis = TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>;
    using TBase = TActorBootstrapped<TThis>;

    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;

protected:
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TProtoRequest Request;
    TAutoPtr<TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>> Result;

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
                CASE(STRING, TString, String);
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
                ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", e.what()));
            }
        } else {
            const auto& params(Event->Get()->Request.GetParams());
            return Params2Proto(params);
        }
        return request;
    }

    bool PostToRequest() {
        auto postData = Event->Get()->Request.GetPostContent();
        if (!postData.empty()) {
            try {
                NProtobufJson::Json2Proto(postData, Request, {});
                return true;
            }
            catch (const yexception& e) {
                ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", e.what()));
                return false;
            }
        }
        return true;
    }

    void SendGrpcRequest() {
        RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(Request), Database, Event->Get()->UserToken, TlsActivationContext->ActorSystem());
        RpcFuture.Subscribe([actorId = TBase::SelfId(), actorSystem = TlsActivationContext->ActorSystem()]
                            (const NThreading::TFuture<TProtoResponse>& future) {
            auto& response = future.GetValueSync();
            auto result = MakeHolder<TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>>();
            if constexpr (TRpcEv::IsOp) {
                if (response.operation().ready() && response.operation().status() == Ydb::StatusIds::SUCCESS) {
                    TProtoResult rs;
                    response.operation().result().UnpackTo(&rs);
                    result->Message = std::move(rs);
                }
                NYql::TIssues issues;
                NYql::IssuesFromMessage(response.operation().issues(), issues);
                result->Status = NYdb::TStatus(NYdb::EStatus(response.operation().status()), std::move(issues));
            } else {
                result->Message = response;
                NYql::TIssues issues;
                NYql::IssuesFromMessage(response.issues(), issues);
                result->Status = NYdb::TStatus(NYdb::EStatus(response.status()), std::move(issues));
            }

            actorSystem->Send(actorId, result.Release());
        });
    }

    virtual void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), true);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);

        SendGrpcRequest();

        Become(&TThis::StateRequested, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Handle(typename TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>::TPtr& ev) {
        Result = ev->Release();
        ReplyAndPassAway();
    }

    STATEFN(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() {
        if (Result && Result->Status) {
            if (!Result->Status->IsSuccess()) {
                NJson::TJsonValue json;
                TString message;
                MakeErrorReply(json, message, Result->Status.value());
                TStringStream stream;
                NJson::WriteJson(&stream, &json);
                if (Result->Status->GetStatus() == NYdb::EStatus::UNAUTHORIZED) {
                    return ReplyAndPassAway(Viewer->GetHTTPFORBIDDEN(Event->Get(), "application/json", stream.Str()));
                } else {
                    return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "application/json", stream.Str()));
                }
            } else {
                TStringStream json;
                TProtoToJson::ProtoToJson(json, Result->Message, JsonSettings);
                return ReplyAndPassAway(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()));
            }
        } else {
            return ReplyAndPassAway(Viewer->GetHTTPINTERNALERROR(Event->Get()));
        }
    }


    void HandleTimeout() {
        ReplyAndPassAway(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()));
    }

    void ReplyAndPassAway(TString data) {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};


}
}
