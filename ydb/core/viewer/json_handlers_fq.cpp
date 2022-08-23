#include "grpc_request_context_wrapper.h"
#include "http_router.h"
#include "json_handlers.h"
#include "viewer.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/service_yq.h>
#include <ydb/core/viewer/protos/fq.pb.h>
#include <ydb/core/yq/libs/result_formatter/result_formatter.h>

namespace NKikimr {
namespace NViewer {

using namespace NActors;

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Reflection;
using ::google::protobuf::RepeatedField;
using ::google::protobuf::RepeatedPtrField;

#define SIMPLE_COPY_FIELD(field) dst.set_##field(src.field())
#define SIMPLE_COPY_RENAME_FIELD(srcField, dstField) dst.set_##dstField(src.srcField())

#define SIMPLE_COPY_MUTABLE_FIELD(field) *dst.mutable_##field() = src.field()
#define SIMPLE_COPY_MUTABLE_RENAME_FIELD(srcField, dstField) *dst.mutable_##dstField() = src.srcField()

#define SIMPLE_COPY_REPEATABLE_FIELD(field) FqConvert(src.field(), *dst.mutable_##field())
#define SIMPLE_COPY_REPEATABLE_RENAME_FIELD(srcField, dstField) FqConvert(src.srcField(), *dst.mutable_##dstField())

#define FQ_CONVERT_FIELD(field) FqConvert(src.field(), *dst.mutable_##field())
#define FQ_CONVERT_RENAME_FIELD(srcField, dstField) FqConvert(src.srcField(), *dst.mutable_##dstField())

template <typename T>
void FqConvert(const T& src, T& dst) {
    dst.CopyFrom(src);
}

template <typename T>
void FqConvert(const T& src, ::google::protobuf::Empty& dst) {
    Y_UNUSED(src);
    Y_UNUSED(dst);
}

template <typename T, typename U>
void FqConvert(const RepeatedPtrField<T>& src, RepeatedPtrField<U>& dst) {
    dst.Reserve(src.size());
    for (auto& v : src) {
        FqConvert(v, *dst.Add());
    }
}

void FqConvert(const Ydb::Operations::Operation& src, FederatedQueryHttp::Error& dst) {
    SIMPLE_COPY_FIELD(status);
    SIMPLE_COPY_MUTABLE_FIELD(issues);
}

#define FQ_CONVERT_QUERY_CONTENT(srcType, dstType) \
void FqConvert(const srcType& src, dstType& dst) { \
    SIMPLE_COPY_FIELD(type); \
    SIMPLE_COPY_FIELD(name); \
    SIMPLE_COPY_FIELD(text); \
    SIMPLE_COPY_FIELD(description); \
}

FQ_CONVERT_QUERY_CONTENT(FederatedQueryHttp::CreateQueryRequest, YandexQuery::QueryContent);
FQ_CONVERT_QUERY_CONTENT(YandexQuery::QueryContent, FederatedQueryHttp::GetQueryResult);

void FqConvert(const FederatedQueryHttp::CreateQueryRequest& src, YandexQuery::CreateQueryRequest& dst) {
    FqConvert(src, *dst.mutable_content());

    dst.set_execute_mode(YandexQuery::RUN);

    auto& content = *dst.mutable_content();
    if (content.type() == YandexQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
        content.set_type(YandexQuery::QueryContent::ANALYTICS);
    }

    if (content.acl().visibility() == YandexQuery::Acl::VISIBILITY_UNSPECIFIED) {
        content.mutable_acl()->set_visibility(YandexQuery::Acl::PRIVATE);
    }

    content.set_automatic(true);
}

void FqConvert(const YandexQuery::CreateQueryResult& src, FederatedQueryHttp::CreateQueryResult& dst) {
    SIMPLE_COPY_RENAME_FIELD(query_id, id);
}

void FqConvert(const YandexQuery::CommonMeta& src, FederatedQueryHttp::QueryMeta& dst) {
    SIMPLE_COPY_MUTABLE_FIELD(created_at);
}

void FqConvert(const YandexQuery::QueryMeta& src, FederatedQueryHttp::QueryMeta& dst) {
    SIMPLE_COPY_MUTABLE_FIELD(submitted_at);
    SIMPLE_COPY_MUTABLE_FIELD(finished_at);
    FqConvert(src.common(), dst);
}

FederatedQueryHttp::GetQueryResult::ComputeStatus RemapQueryStatus(YandexQuery::QueryMeta::ComputeStatus status) {
    switch (status) {
    case YandexQuery::QueryMeta::COMPLETED:
        return FederatedQueryHttp::GetQueryResult::COMPLETED;

    case YandexQuery::QueryMeta::ABORTED_BY_USER:
        [[fallthrough]];
    case YandexQuery::QueryMeta::ABORTING_BY_SYSTEM:
        [[fallthrough]];
    case YandexQuery::QueryMeta::FAILED:
        return FederatedQueryHttp::GetQueryResult::FAILED;

    default:
        return FederatedQueryHttp::GetQueryResult::RUNNING;
    }
}

void FqConvert(const YandexQuery::ResultSetMeta& src, FederatedQueryHttp::ResultSetMeta& dst) {
    SIMPLE_COPY_FIELD(rows_count);
    SIMPLE_COPY_FIELD(truncated);
}

void FqConvert(const YandexQuery::Query& src, FederatedQueryHttp::GetQueryResult& dst) {
    FQ_CONVERT_FIELD(meta);

    FqConvert(src.content(), dst);
    dst.set_id(src.meta().common().id());
    dst.set_status(RemapQueryStatus(src.meta().status()));

    for (const auto& result_meta : src.result_set_meta()) {
        FqConvert(result_meta, *dst.mutable_result_sets()->Add());
    }

    SIMPLE_COPY_MUTABLE_RENAME_FIELD(issue, issues);
    dst.mutable_issues()->MergeFrom(src.transient_issue());
}

void FqConvert(const FederatedQueryHttp::GetQueryRequest& src, YandexQuery::DescribeQueryRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
}

void FqConvert(const YandexQuery::DescribeQueryResult& src, FederatedQueryHttp::GetQueryResult& dst) {
    FqConvert(src.query(), dst);
}

void FqConvert(const FederatedQueryHttp::DeleteQueryRequest& src, YandexQuery::DeleteQueryRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
}

void FqConvert(const FederatedQueryHttp::GetQueryStatusRequest& src, YandexQuery::GetQueryStatusRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
}

void FqConvert(const YandexQuery::GetQueryStatusResult& src, FederatedQueryHttp::GetQueryStatusResult& dst) {
    dst.set_status(RemapQueryStatus(src.status()));
}

void FqConvert(const FederatedQueryHttp::StopQueryRequest& src, YandexQuery::ControlQueryRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
    dst.set_action(YandexQuery::ABORT);
}

void FqConvert(const FederatedQueryHttp::GetResultDataRequest& src, YandexQuery::GetResultDataRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
    SIMPLE_COPY_FIELD(result_set_index);
    SIMPLE_COPY_FIELD(offset);
    SIMPLE_COPY_FIELD(limit);

    if (!dst.limit()) {
        dst.set_limit(100);
    }
}

void FqConvert(const YandexQuery::GetResultDataResult& src, FederatedQueryHttp::GetResultDataResult& dst) {
    SIMPLE_COPY_MUTABLE_FIELD(result_set);
}

template <typename T>
void FqPackToJson(TStringStream& json, const T& httpResult, const TJsonSettings& jsonSettings) {
    TProtoToJson::ProtoToJson(json, httpResult, jsonSettings);
}

void FqPackToJson(TStringStream& json, const FederatedQueryHttp::GetResultDataResult& httpResult, const TJsonSettings&) {
    auto resultSet = NYdb::TResultSet(httpResult.result_set());
    NJson::TJsonValue v;
    NYq::FormatResultSet(v, resultSet, true);
    NJson::TJsonWriterConfig jsonWriterConfig;
    jsonWriterConfig.WriteNanAsString = true;
    NJson::WriteJson(&json, &v, jsonWriterConfig);
}

template <typename GrpcProtoRequestType, typename HttpProtoRequestType, typename GrpcProtoResultType, typename HttpProtoResultType, typename GrpcProtoResponseType>
class TGrpcCallWrapper : public TActorBootstrapped<TGrpcCallWrapper<GrpcProtoRequestType, HttpProtoRequestType, GrpcProtoResultType, HttpProtoResultType, GrpcProtoResponseType>> {
    IViewer* const Viewer;
    const TRequest Request;

    typedef std::function<std::unique_ptr<NGRpcService::TEvProxyRuntimeEvent>(TIntrusivePtr<NGrpc::IRequestContextBase> ctx)> TGrpcProxyEventFactory;
    TGrpcProxyEventFactory EventFactory;

    NProtobufJson::TJson2ProtoConfig Json2ProtoConfig;

public:
    typedef GrpcProtoRequestType TGrpcProtoRequestType;
    typedef HttpProtoRequestType THttpProtoRequestType;
    typedef GrpcProtoResultType TGrpcProtoResultType;
    typedef HttpProtoResultType THttpProtoResultType;
    typedef GrpcProtoResponseType TGrpcProtoResponseType;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TGrpcCallWrapper(IViewer* viewer, const TRequest& request, TGrpcProxyEventFactory eventFactory)
        : Viewer(viewer)
        , Request(request)
        , EventFactory(eventFactory)
    {
        Json2ProtoConfig = NProtobufJson::TJson2ProtoConfig()
            .SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameCamelCase)
            .SetMapAsObject(true);
    }

    const NMon::TEvHttpInfo::TPtr& GetEvent() const {
        return Request.Event;
    }

    void Bootstrap(const TActorContext& ctx) {
        auto grpcRequest = std::make_unique<TGrpcProtoRequestType>();
        if (Parse(ctx, *grpcRequest)) {
            TIntrusivePtr<TGrpcRequestContextWrapper> requestContext = new TGrpcRequestContextWrapper(ctx.ActorSystem(), Viewer, GetEvent(), std::move(grpcRequest), &SendReply);
            ctx.Send(NGRpcService::CreateGRpcRequestProxyId(), EventFactory(requestContext).release());
        }

        this->Die(ctx);
    }

    bool Parse(const TActorContext& ctx, TGrpcProtoRequestType& grpcRequest) {
        try {
            THttpProtoRequestType request;
            const auto& httpRequest = GetEvent()->Get()->Request;
            // todo: check Headers to copy idempotency-key into protobuf
            if (httpRequest.GetMethod() == HTTP_METHOD_POST && httpRequest.GetHeader("content-type") == "application/json"sv) {
                // todo: fix Duration + Timestamp parsing
                // todo: PostContent is empty for PUT in monlib
                NProtobufJson::Json2Proto(httpRequest.GetPostContent(), request, Json2ProtoConfig);
            }

            const auto& params = httpRequest.GetParams();
            for (const auto& [name, value] : params) {
                SetProtoMessageField(request, name, value);
            }

            // path params should overwrite query params in case of conflict
            for (const auto& [name, value] : Request.PathParams) {
                SetProtoMessageField(request, name, value);
            }
            FqConvert(request, grpcRequest);
            return true;
        } catch (const std::exception& e) {
            ReplyError(ctx, TStringBuilder() << "Error in parsing: " << e.what() << ", original text: " << GetEvent()->Get()->Request.GetPostContent());
            return false;
        }
    }

    static void SetProtoMessageField(THttpProtoRequestType& request, const TString& name, const TString& value) {
        const Reflection* reflection = request.GetReflection();
        const Descriptor* descriptor = request.GetDescriptor();
        auto field = descriptor->FindFieldByLowercaseName(name);
        if (!field) {
            return;
        }

        switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            return reflection->SetInt32(&request, field, FromString<i32>(value));
        case FieldDescriptor::CPPTYPE_INT64:
            return reflection->SetInt64(&request, field, FromString<i64>(value));
        case FieldDescriptor::CPPTYPE_UINT32:
            return reflection->SetUInt32(&request, field, FromString<ui32>(value));
        case FieldDescriptor::CPPTYPE_UINT64:
            return reflection->SetUInt64(&request, field, FromString<ui64>(value));
        case FieldDescriptor::CPPTYPE_STRING:
            return reflection->SetString(&request, field, value);
        default:
            break;
        }
    }

    void ReplyError(const TActorContext& ctx, const TString& error) {
        ctx.Send(GetEvent()->Sender, new NMon::TEvHttpInfoRes(TStringBuilder() << HTTPBADREQUESTJSON << error, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    static void SendReply(TActorSystem* actorSystem, IViewer* viewer, const NMon::TEvHttpInfo::TPtr& event, const TJsonSettings& jsonSettings, NProtoBuf::Message* resp, ui32 status) {
        Y_VERIFY(resp);
        Y_VERIFY(resp->GetArena());
        Y_UNUSED(status);
        auto* typedResponse = static_cast<TGrpcProtoResponseType*>(resp);
        if (!typedResponse->operation().result().template Is<TGrpcProtoResultType>()) {
            TStringStream json;
            auto* httpResult = google::protobuf::Arena::CreateMessage<FederatedQueryHttp::Error>(resp->GetArena());
            FqConvert(typedResponse->operation(), *httpResult);
            FqPackToJson(json, *httpResult, jsonSettings);

            // todo: remap ydb status to http code
            actorSystem->Send(event->Sender, new NMon::TEvHttpInfoRes(TStringBuilder() << HTTPBADREQUESTJSON << json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }

        auto* grpcResult = google::protobuf::Arena::CreateMessage<TGrpcProtoResultType>(resp->GetArena());
        typedResponse->operation().result().UnpackTo(grpcResult);

        if (THttpProtoResultType::descriptor()->full_name() == google::protobuf::Empty::descriptor()->full_name()) {
            actorSystem->Send(event->Sender, new NMon::TEvHttpInfoRes(
                "HTTP/1.1 204 No Content\r\n"
                "Connection: Keep-Alive\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }

        TStringStream json;
        auto* httpResult = google::protobuf::Arena::CreateMessage<THttpProtoResultType>(resp->GetArena());
        FqConvert(*grpcResult, *httpResult);
        FqPackToJson(json, *httpResult, jsonSettings);

        actorSystem->Send(event->Sender, new NMon::TEvHttpInfoRes(viewer->GetHTTPOKJSON(event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }
};

template <typename ProtoType>
void ProtoToPublicJsonSchema(IOutputStream& to) {
    TJsonSettings settings;
    settings.EnumAsNumbers = false;
    settings.EmptyRepeated = false;
    settings.EnumValueFilter = [](const TString& value) {
        return !value.EndsWith("UNSPECIFIED");
    };

    TProtoToJson::ProtoToJsonSchema<ProtoType>(to, settings);
}


#define DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, t1, t2, t3, t4, t5) \
class TJson##action : public TGrpcCallWrapper<t1, t2, t3, t4, t5> { \
    typedef TGrpcCallWrapper<t1, t2, t3, t4, t5> TBase;             \
public:                                                                                                                                   \
    TJson##action(IViewer* viewer, const TRequest& request)                       \
      :  TBase(viewer, request, &NGRpcService::Create##internalAction##RequestOperationCall) {}                                            \
}

#define DECLARE_YQ_GRPC_ACTOR(action, internalAction) DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, YandexQuery::internalAction##Request, FederatedQueryHttp::action##Request, YandexQuery::internalAction##Result, FederatedQueryHttp::action##Result, YandexQuery::internalAction##Response)
#define DECLARE_YQ_GRPC_ACTOR_WIHT_EMPTY_RESULT(action, internalAction) DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, YandexQuery::internalAction##Request, FederatedQueryHttp::action##Request, YandexQuery::internalAction##Result, ::google::protobuf::Empty, YandexQuery::internalAction##Response)

//#define DECLARE_YQ_GRPC_ACTOR(action) DECLARE_YQ_GRPC_ACTOR_IMPL(action, action, YandexQuery::action##Request, YandexQuery::action##Request, YandexQuery::action##Result, YandexQuery::action##Result, YandexQuery::action##Response)
//#define DECLARE_YQ_GRPC_ACTOR_WITH_REMAPPING(action, internalAction) DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, YandexQuery::internalAction##Request, FederatedQueryHttp::action##Request, YandexQuery::internalAction##Result, FederatedQueryHttp::action##Result, YandexQuery::internalAction##Response)
//#define DECLARE_YQ_GRPC_ACTOR_WITH_REMAPPING_RESULT(action, internalAction, resultType) DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, YandexQuery::internalAction##Request, FederatedQueryHttp::action##Request, YandexQuery::internalAction##Result, resultType, YandexQuery::internalAction##Response)

// create queries
DECLARE_YQ_GRPC_ACTOR(CreateQuery, CreateQuery);

template <>
struct TJsonRequestSchema<TJsonCreateQuery> {
    static TString GetSchema() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::CreateQueryResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonCreateQuery> {
    static TString GetParameters() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::CreateQueryRequest>(stream);
        auto bodyScheme = stream.Str();

        return TStringBuilder() << R"___([
                      {"name":"folder_id","in":"path","description":"folder id","required":true,"type":"string"},
                      {"name":"body","in":"body","required":true, "schema": )___" << bodyScheme << "}]";
    }
};

template <>
struct TJsonRequestSummary<TJsonCreateQuery> {
    static TString GetSummary() {
        return "\"Create new query\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonCreateQuery> {
    static TString GetDescription() {
        return "\"Create new query and optionally run it\"";
    }
};

// describe query
DECLARE_YQ_GRPC_ACTOR(GetQuery, DescribeQuery);

template <>
struct TJsonRequestSchema<TJsonGetQuery> {
    static TString GetSchema() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::GetQueryResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonGetQuery> {
    static TString GetParameters() {
        return R"___([
                      {"name":"folder_id","in":"path","description":"folder id","required":true,"type":"string"},
                      {"name":"query_id","in":"path","description":"query id", "required": true, "type":"string"}
                      ])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonGetQuery> {
    static TString GetSummary() {
        return "\"Get information about query\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonGetQuery> {
    static TString GetDescription() {
        return "\"Get detailed information about specified query\"";
    }
};

// delete query
DECLARE_YQ_GRPC_ACTOR_WIHT_EMPTY_RESULT(DeleteQuery, DeleteQuery);

template <>
struct TJsonRequestSchema<TJsonDeleteQuery> {
    static TString GetSchema() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::DeleteQueryResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonDeleteQuery> {
    static TString GetParameters() {
        return R"___([
                      {"name":"folder_id","in":"path","description":"folder id","required":true,"type":"string"},
                      {"name":"idempotency-key","in":"header","description":"idempotency key", "required": false, "type":"string"},
                      {"name":"query_id","in":"path","description":"query id", "required": true, "type":"string"}
                      ])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonDeleteQuery> {
    static TString GetSummary() {
        return "\"Delete query\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonDeleteQuery> {
    static TString GetDescription() {
        return "\"Delete existing query by id\"";
    }
};

// get query status
DECLARE_YQ_GRPC_ACTOR(GetQueryStatus, GetQueryStatus);

template <>
struct TJsonRequestSchema<TJsonGetQueryStatus> {
    static TString GetSchema() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::GetQueryStatusResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonGetQueryStatus> {
    static TString GetParameters() {
        return R"___([
                      {"name":"folder_id","in":"path","description":"folder id","required":true,"type":"string"},
                      {"name":"idempotency-key","in":"header","description":"idempotency key", "required": false, "type":"string"},
                      {"name":"query_id","in":"path","description":"query id", "required": true, "type":"string"}
                      ])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonGetQueryStatus> {
    static TString GetSummary() {
        return "\"Get query status\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonGetQueryStatus> {
    static TString GetDescription() {
        return "\"Get status of the query\"";
    }
};

// stop query
DECLARE_YQ_GRPC_ACTOR_WIHT_EMPTY_RESULT(StopQuery, ControlQuery);

template <>
struct TJsonRequestSchema<TJsonStopQuery> {
    static TString GetSchema() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::StopQueryResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonStopQuery> {
    static TString GetParameters() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::StopQueryRequest>(stream);
        auto bodyScheme = stream.Str();

        return TStringBuilder() << R"___([
                      {"name":"folder_id","in":"path","description":"folder id","required":true,"type":"string"},
                      {"name":"idempotency-key","in":"header","description":"idempotency key", "required": false, "type":"string"},
                      {"name":"query_id","in":"path","description":"query id", "required": true, "type":"string"},
                      {"name":"body","in":"body","required":false, "schema": )___" << bodyScheme << "}]";
    }
};

template <>
struct TJsonRequestSummary<TJsonStopQuery> {
    static TString GetSummary() {
        return "\"Stop query\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonStopQuery> {
    static TString GetDescription() {
        return "\"Stop running query\"";
    }
};

// get result data
DECLARE_YQ_GRPC_ACTOR(GetResultData, GetResultData);

template <>
struct TJsonRequestSchema<TJsonGetResultData> {
    static TString GetSchema() {
        TStringStream stream;
        ProtoToPublicJsonSchema<FederatedQueryHttp::GetResultDataResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonGetResultData> {
    static TString GetParameters() {
        return R"___([
                      {"name":"folder_id","in":"path","description":"folder id","required":true,"type":"string"},
                      {"name":"idempotency-key","in":"header","description":"idempotency key", "required": false, "type":"string"},
                      {"name":"query_id","in":"path","description":"query id", "required": true, "type":"string"},
                      {"name":"result_set_index","in":"query","description":"result set index","required": true, "type":"integer"},
                      {"name":"offset","in":"query","description":"row offset","default":0, "type":"integer"},
                      {"name":"limit","in":"query","description":"row limit","default":100, "type":"integer"}
                      ])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonGetResultData> {
    static TString GetSummary() {
        return "\"Get query results\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonGetResultData> {
    static TString GetDescription() {
        return "\"Get query results\"";
    }
};

template <>
void TFQJsonHandlers::Init() {
    Router.RegisterHandler(HTTP_METHOD_POST,   "/fq/json/queries", std::make_shared<TJsonHandler<TJsonCreateQuery>>());
    Router.RegisterHandler(HTTP_METHOD_GET,    "/fq/json/queries/{query_id}", std::make_shared<TJsonHandler<TJsonGetQuery>>());
    Router.RegisterHandler(HTTP_METHOD_DELETE, "/fq/json/queries/{query_id}", std::make_shared<TJsonHandler<TJsonDeleteQuery>>());
    Router.RegisterHandler(HTTP_METHOD_GET,    "/fq/json/queries/{query_id}/status", std::make_shared<TJsonHandler<TJsonGetQueryStatus>>());
    Router.RegisterHandler(HTTP_METHOD_GET,    "/fq/json/queries/{query_id}/results", std::make_shared<TJsonHandler<TJsonGetResultData>>());
    Router.RegisterHandler(HTTP_METHOD_POST,   "/fq/json/queries/{query_id}/stop", std::make_shared<TJsonHandler<TJsonStopQuery>>());
}

}
}
