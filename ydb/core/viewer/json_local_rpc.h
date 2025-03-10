#pragma once
#include "json_pipe_req.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NViewer {

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

template <class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoService, class TRpcEv>
class TJsonLocalRpc : public TViewerPipeClient {
    using TThis = TJsonLocalRpc<TProtoRequest, TProtoResponse, TProtoResult, TProtoService, TRpcEv>;
    using TBase = TViewerPipeClient;

protected:
    using TBase::ReplyAndPassAway;
    using TRequestProtoType = TProtoRequest;
    std::vector<HTTP_METHOD> AllowedMethods = {};
    TAutoPtr<TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>> Result;
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
        const auto& params(Event->Get()->Request.GetParams());
        Params2Proto(params, request);
        if (!ValidateRequest(request)) {
            return false;
        }
        return true;
    }

    void SendGrpcRequest(TRequestProtoType&& request) {
        // TODO(xenoxeno): pass trace id
        RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(request), Database, Event->Get()->UserToken, TlsActivationContext->ActorSystem());
        RpcFuture.Subscribe([actorId = TBase::SelfId(), actorSystem = TlsActivationContext->ActorSystem()]
                            (const NThreading::TFuture<TProtoResponse>& future) {
            auto& response = future.GetValueSync();
            auto result = MakeHolder<TEvLocalRpcPrivate::TEvGrpcRequestResult<TProtoResult>>();
            if constexpr (TRpcEv::IsOp) {
                if (response.operation().ready() && response.operation().status() == Ydb::StatusIds::SUCCESS) {
                    if (response.operation().has_result()) {
                        TProtoResult rs;
                        response.operation().result().UnpackTo(&rs);
                        result->Message = std::move(rs);
                    } else if constexpr (std::is_same_v<TProtoResult, Ydb::Operations::Operation>) {
                        result->Message = std::move(response.operation());
                    }
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
            if (Result->Status->IsSuccess()) {
                return ReplyAndPassAway(GetHTTPOKJSON(Result->Message));
            } else {
                NJson::TJsonValue json;
                TString message;
                MakeJsonErrorReply(json, message, Result->Status.value());
                TStringStream stream;
                NJson::WriteJson(&stream, &json);
                if (Result->Status->GetStatus() == NYdb::EStatus::UNAUTHORIZED) {
                    return ReplyAndPassAway(GetHTTPFORBIDDEN("application/json", stream.Str()), message);
                } else {
                    return ReplyAndPassAway(GetHTTPBADREQUEST("application/json", stream.Str()), message);
                }
            }
        } else {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "no Result or Status"), "internal error");
        }
    }
};

}  // namespace NKikimr::NViewer
