#pragma once
#include "json_pipe_req.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NViewer {

struct TEvLocalRpcPrivate {
    enum EEv {
        EvGrpcRequestResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 100,
        EvError,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
};

template <class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoService, class TRpcEv>
class TJsonLocalRpc : public TViewerPipeClient {
    using TThis = TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>;
    using TBase = TViewerPipeClient;

    struct TEvGrpcRequestResult : NActors::TEventLocal<TEvGrpcRequestResult, TEvLocalRpcPrivate::EvGrpcRequestResult> {
        TProtoResponse Response;
        //std::optional<NYdb::TStatus> Status;

        TEvGrpcRequestResult(TProtoResponse response)
            : Response(std::move(response))
        {}
    };

protected:
    using TBase::ReplyAndPassAway;
    using TRequestProtoType = TProtoRequest;
    std::vector<HTTP_METHOD> AllowedMethods = {};
    std::unique_ptr<TEvGrpcRequestResult> Result;
    NThreading::TFuture<TProtoResponse> RpcFuture;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonLocalRpc(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev, TRequestProtoType::descriptor()->name())
    {}

    void Params2Proto(const TCgiParameters& params, TRequestProtoType& request) {
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
    }

    virtual bool ValidateRequest(TRequestProtoType& request) {
        using google::protobuf::Descriptor;
        using google::protobuf::Reflection;
        using google::protobuf::FieldDescriptor;
        const Descriptor& descriptor = *TRequestProtoType::GetDescriptor();
        const Reflection& reflection = *TRequestProtoType::GetReflection();
        for (int idx = 0; idx < descriptor.field_count(); ++idx) {
            const FieldDescriptor* field = descriptor.field(idx);
            const auto& options(field->options());
            if (options.HasExtension(Ydb::required)) {
                if (options.GetExtension(Ydb::required)) {
                    if (!reflection.HasField(request, field)) {
                        ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", TStringBuilder() << "field '" << field->name() << "' is required"));
                        return false;
                    }
                }
            }
        }
        return true;
    }

    bool Params2Proto(TRequestProtoType& request) {
        auto postData = Event->Get()->Request.GetPostContent();
        if (!postData.empty()) {
            try {
                NProtobufJson::Json2Proto(postData, request);
            }
            catch (const yexception& e) {
                ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", e.what()));
                return false;
            }
        }
        Params2Proto(Params, request);
        if (!ValidateRequest(request)) {
            return false;
        }
        return true;
    }

    void SendGrpcRequest(TRequestProtoType&& request) {
        // TODO(xenoxeno): pass trace id
        RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), Database, Event->Get()->UserToken, TActivationContext::ActorSystem());
        RpcFuture.Subscribe([actorId = TBase::SelfId(), actorSystem = TActivationContext::ActorSystem()]
                            (const NThreading::TFuture<TProtoResponse>& future) {
            actorSystem->Send(actorId, new TEvGrpcRequestResult(future.GetValueSync()));
        });
    }

    virtual void Bootstrap() {
        if (!AllowedMethods.empty() && std::find(AllowedMethods.begin(), AllowedMethods.end(), Event->Get()->Request.GetMethod()) == AllowedMethods.end()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Method is not allowed"));
        }
        if (Database.empty()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'database' is required"));
        }
        if (TBase::NeedToRedirect()) {
            return;
        }
        TRequestProtoType request;
        if (!Params2Proto(request)) {
            return;
        }
        SendGrpcRequest(std::move(request));
        Become(&TThis::StateRequested, Timeout, new TEvents::TEvWakeup());
    }

    void Handle(TEvGrpcRequestResult::TPtr& ev) {
        Result.reset(ev->Release().Release());
        ReplyAndPassAway();
    }

    STATEFN(StateRequested) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvGrpcRequestResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ReplyAndPassAway() {
        if (Result) {
            auto& response = Result->Response;
            std::optional<NYdb::TStatus> status;
            NJson::TJsonValue body;
            if constexpr (TRpcEv::IsOp) {
                if constexpr (!std::is_same_v<TProtoResult, Ydb::Operations::Operation>) {
                    NYql::TIssues issues;
                    NYql::IssuesFromMessage(response.operation().issues(), issues);
                    status = NYdb::TStatus(NYdb::EStatus(response.operation().status()), NYdb::NAdapters::ToSdkIssues(std::move(issues)));
                }
                if (response.operation().ready() && response.operation().has_result()) {
                    TProtoResult rs;
                    response.operation().result().UnpackTo(&rs);
                    Proto2Json(rs, body);
                } else {
                    Proto2Json(response.operation(), body);
                }
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(response.issues(), issues);
                status = NYdb::TStatus(NYdb::EStatus(response.status()), NYdb::NAdapters::ToSdkIssues(std::move(issues)));
                Proto2Json(response, body);
            }

            if (!status || status->IsSuccess()) {
                return ReplyAndPassAway(GetHTTPOKJSON(body));
            } else {
                if (status) {
                    NJson::TJsonValue json;
                    TString message;
                    MakeJsonErrorReply(json, message, status.value());
                    TStringStream stream;
                    NJson::WriteJson(&stream, &json);
                    if (status->GetStatus() == NYdb::EStatus::UNAUTHORIZED) {
                        return ReplyAndPassAway(GETHTTPACCESSDENIED("application/json", stream.Str()), message);
                    } else {
                        return ReplyAndPassAway(GetHTTPBADREQUEST("application/json", stream.Str()), message);
                    }
                } else {
                    return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No status"), "internal error");
                }
            }
        } else {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No result"), "internal error");
        }
    }
};

}  // namespace NKikimr::NViewer
