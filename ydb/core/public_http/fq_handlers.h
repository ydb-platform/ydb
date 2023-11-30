#pragma once

#include "grpc_request_context_wrapper.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <ydb/core/fq/libs/result_formatter/result_formatter.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/public_http/protos/fq.pb.h>

namespace NKikimr::NPublicHttp {

using namespace NActors;

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Reflection;
using ::google::protobuf::RepeatedField;
using ::google::protobuf::RepeatedPtrField;

#define SIMPLE_COPY_FIELD(field) dst.set_##field(src.field())
#define SIMPLE_COPY_RENAME_FIELD(srcField, dstField) dst.set_##dstField(src.srcField())

#define SIMPLE_COPY_MUTABLE_OPTIONAL_FIELD(field) if (src.has_##field()) { *dst.mutable_##field() = src.field(); }

#define SIMPLE_COPY_MUTABLE_FIELD(field) *dst.mutable_##field() = src.field()
#define SIMPLE_COPY_MUTABLE_RENAME_FIELD(srcField, dstField) *dst.mutable_##dstField() = src.srcField()

#define SIMPLE_COPY_MUTABLE_WRAPPER_FIELD(field) dst.mutable_##field()->set_value(src.field())
#define SIMPLE_COPY_MUTABLE_RENAME_WRAPPER_FIELD(srcField, dstField) dst.mutable_##dstField()->set_value(src.srcField())

#define SIMPLE_COPY_REPEATABLE_FIELD(field) FqConvert(src.field(), *dst.mutable_##field())
#define SIMPLE_COPY_REPEATABLE_RENAME_FIELD(srcField, dstField) FqConvert(src.srcField(), *dst.mutable_##dstField())

#define FQ_CONVERT_FIELD(field) FqConvert(src.field(), *dst.mutable_##field())
#define FQ_CONVERT_RENAME_FIELD(srcField, dstField) FqConvert(src.srcField(), *dst.mutable_##dstField())

static constexpr auto APPLICATION_JSON = "application/json"sv;

template <typename T>
void FqConvert(const T& src, T& dst) {
    dst.CopyFrom(src);
}

template <typename T>
void FqConvert(const T& src, ::google::protobuf::Empty& dst) {
    Y_UNUSED(src);
    Y_UNUSED(dst);
}

TString RemapSeverity(int severity) {
    // values from ydb/library/yql/public/issue/protos/issue_severity.proto
    switch (severity) {
    case 0:
        return "FATAL";
    case 1:
        return "ERROR";
    case 2:
        return "WARNING";
    case 3:
        return "INFO";
    default:
        return "UNKNOWN";
    }
}

template <typename T, typename U>
void FqConvert(const RepeatedPtrField<T>& src, RepeatedPtrField<U>& dst);

void FqConvert(const Ydb::Issue::IssueMessage& src, FQHttp::IssueMessage& dst) {
    SIMPLE_COPY_MUTABLE_OPTIONAL_FIELD(position);
    SIMPLE_COPY_FIELD(message);
    SIMPLE_COPY_MUTABLE_OPTIONAL_FIELD(end_position);
    SIMPLE_COPY_FIELD(issue_code);
    SIMPLE_COPY_REPEATABLE_FIELD(issues);
    dst.set_severity(RemapSeverity(src.severity()));
}

template <typename T, typename U>
void FqConvert(const RepeatedPtrField<T>& src, RepeatedPtrField<U>& dst) {
    // append
    dst.Reserve(dst.size() + src.size());
    for (auto& v : src) {
        FqConvert(v, *dst.Add());
    }
}

void FqConvert(const Ydb::Operations::Operation& src, FQHttp::Error& dst) {
    dst.set_status(static_cast<int>(src.status()));
    dst.set_message(Ydb::StatusIds_StatusCode_Name(src.status()));
    SIMPLE_COPY_REPEATABLE_RENAME_FIELD(issues, details);
}

void FqConvert(const FQHttp::CreateQueryRequest& src, FederatedQuery::QueryContent& dst) {
    SIMPLE_COPY_FIELD(type);
    SIMPLE_COPY_FIELD(name);
    SIMPLE_COPY_FIELD(text);
    SIMPLE_COPY_FIELD(description);
}

void FqConvert(const FederatedQuery::QueryContent& src, FQHttp::GetQueryResult& dst) {
    SIMPLE_COPY_FIELD(type);
    SIMPLE_COPY_MUTABLE_WRAPPER_FIELD(name);
    SIMPLE_COPY_MUTABLE_WRAPPER_FIELD(text);
    SIMPLE_COPY_MUTABLE_WRAPPER_FIELD(description);
}

void FqConvert(const FQHttp::CreateQueryRequest& src, FederatedQuery::CreateQueryRequest& dst) {
    FqConvert(src, *dst.mutable_content());

    dst.set_execute_mode(FederatedQuery::RUN);

    auto& content = *dst.mutable_content();
    if (content.type() == FederatedQuery::QueryContent::QUERY_TYPE_UNSPECIFIED) {
        content.set_type(FederatedQuery::QueryContent::ANALYTICS);
    }

    if (content.acl().visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        content.mutable_acl()->set_visibility(FederatedQuery::Acl::PRIVATE);
    }

    content.set_automatic(true);
}

void FqConvert(const FederatedQuery::CreateQueryResult& src, FQHttp::CreateQueryResult& dst) {
    SIMPLE_COPY_RENAME_FIELD(query_id, id);
}

void FqConvert(const FederatedQuery::CommonMeta& src, FQHttp::QueryMeta& dst) {
    SIMPLE_COPY_MUTABLE_RENAME_FIELD(created_at, started_at);
}

void FqConvert(const FederatedQuery::QueryMeta& src, FQHttp::QueryMeta& dst) {
    SIMPLE_COPY_MUTABLE_FIELD(finished_at);
    FqConvert(src.common(), dst);
}

FQHttp::GetQueryResult::ComputeStatus RemapQueryStatus(FederatedQuery::QueryMeta::ComputeStatus status) {
    switch (status) {
    case FederatedQuery::QueryMeta::COMPLETED:
        return FQHttp::GetQueryResult::COMPLETED;

    case FederatedQuery::QueryMeta::ABORTED_BY_USER:
        [[fallthrough]];
    case FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM:
        [[fallthrough]];
    case FederatedQuery::QueryMeta::FAILED:
        return FQHttp::GetQueryResult::FAILED;

    default:
        return FQHttp::GetQueryResult::RUNNING;
    }
}

void FqConvert(const FederatedQuery::ResultSetMeta& src, FQHttp::ResultSetMeta& dst) {
    SIMPLE_COPY_MUTABLE_WRAPPER_FIELD(rows_count);
    SIMPLE_COPY_MUTABLE_WRAPPER_FIELD(truncated);
}

void FqConvert(const FederatedQuery::Query& src, FQHttp::GetQueryResult& dst) {
    FQ_CONVERT_FIELD(meta);

    FqConvert(src.content(), dst);
    dst.mutable_id()->set_value(src.meta().common().id());
    dst.set_status(RemapQueryStatus(src.meta().status()));

    for (const auto& result_meta : src.result_set_meta()) {
        FqConvert(result_meta, *dst.mutable_result_sets()->Add());
    }

    RepeatedPtrField<Ydb::Issue::IssueMessage> mergedIssues(src.issue());
    mergedIssues.MergeFrom(src.transient_issue());

    if (!mergedIssues.empty()) {
        FqConvert(mergedIssues, *dst.mutable_issues()->mutable_details());
        NYql::TIssues issues;
        NYql::IssuesFromMessage(mergedIssues, issues);
        dst.mutable_issues()->set_message(issues.ToOneLineString());
    }
}

void FqConvert(const FQHttp::GetQueryRequest& src, FederatedQuery::DescribeQueryRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
}

void FqConvert(const FederatedQuery::DescribeQueryResult& src, FQHttp::GetQueryResult& dst) {
    FqConvert(src.query(), dst);
}

void FqConvert(const FQHttp::GetQueryStatusRequest& src, FederatedQuery::GetQueryStatusRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
}

void FqConvert(const FederatedQuery::GetQueryStatusResult& src, FQHttp::GetQueryStatusResult& dst) {
    dst.set_status(RemapQueryStatus(src.status()));
}

void FqConvert(const FQHttp::StopQueryRequest& src, FederatedQuery::ControlQueryRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
    dst.set_action(FederatedQuery::ABORT);
}

void FqConvert(const FQHttp::GetResultDataRequest& src, FederatedQuery::GetResultDataRequest& dst) {
    SIMPLE_COPY_FIELD(query_id);
    SIMPLE_COPY_FIELD(result_set_index);
    SIMPLE_COPY_FIELD(offset);
    SIMPLE_COPY_FIELD(limit);

    if (!dst.limit()) {
        dst.set_limit(100);
    }
}

void FqConvert(const FederatedQuery::GetResultDataResult& src, FQHttp::GetResultDataResult& dst) {
    SIMPLE_COPY_MUTABLE_FIELD(result_set);
}

template <typename T>
void FqPackToJson(TStringStream& json, const T& httpResult, const TJsonSettings& jsonSettings) {
    TProtoToJson::ProtoToJson(json, httpResult, jsonSettings);
}

void FqPackToJson(TStringStream& json, const FQHttp::GetResultDataResult& httpResult, const TJsonSettings&) {
    auto resultSet = NYdb::TResultSet(httpResult.result_set());
    NJson::TJsonValue v;
    NFq::FormatResultSet(v, resultSet, true, true);
    // rename key data -> rows
    v["rows"] = v["data"];
    v.EraseValue("data");
    NJson::TJsonWriterConfig jsonWriterConfig;
    jsonWriterConfig.WriteNanAsString = true;
    NJson::WriteJson(&json, &v, jsonWriterConfig);
}

template <typename T>
void SetIdempotencyKey(T& dst, const TString& key) {
    Y_UNUSED(dst);
    Y_UNUSED(key);

    if constexpr (
        std::is_same<T, FederatedQuery::CreateQueryRequest>::value ||
        std::is_same<T, FederatedQuery::ControlQueryRequest>::value)
    {
        dst.set_idempotency_key(key);
    }
}

template <typename GrpcProtoRequestType, typename HttpProtoRequestType, typename GrpcProtoResultType, typename HttpProtoResultType, typename GrpcProtoResponseType>
class TGrpcCallWrapper : public TActorBootstrapped<TGrpcCallWrapper<GrpcProtoRequestType, HttpProtoRequestType, GrpcProtoResultType, HttpProtoResultType, GrpcProtoResponseType>> {
    THttpRequestContext RequestContext;

    typedef std::function<std::unique_ptr<NGRpcService::TEvProxyRuntimeEvent>(TIntrusivePtr<NYdbGrpc::IRequestContextBase> ctx)> TGrpcProxyEventFactory;
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

    TGrpcCallWrapper(const THttpRequestContext& requestContext, TGrpcProxyEventFactory eventFactory)
        : RequestContext(requestContext)
        , EventFactory(eventFactory)
    {
        Json2ProtoConfig = NProtobufJson::TJson2ProtoConfig()
            .SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameCamelCase)
            .SetMapAsObject(true);
    }

    void Bootstrap(const TActorContext& ctx) {
        auto grpcRequest = std::make_unique<TGrpcProtoRequestType>();
        if (Parse(*grpcRequest)) {
            TIntrusivePtr<TGrpcRequestContextWrapper> requestContext = new TGrpcRequestContextWrapper(RequestContext, std::move(grpcRequest), &SendReply);
            ctx.Send(NGRpcService::CreateGRpcRequestProxyId(), EventFactory(requestContext).release());
        }

        this->Die(ctx);
    }

    bool Parse(TGrpcProtoRequestType& grpcRequest) {
        const auto& httpRequest = *RequestContext.GetHttpRequest();
        try {
            THttpProtoRequestType request;
            if (httpRequest.Method == "POST"sv && RequestContext.GetContentType() == APPLICATION_JSON) {
                NProtobufJson::Json2Proto(httpRequest.Body, request, Json2ProtoConfig);
            }

            NHttp::TUrlParameters params(httpRequest.URL);
            for (const auto& [name, value] : params.Parameters) {
                SetProtoMessageField(request, name, value);
            }
            RequestContext.SetDb(TString(params.Get("db")));
            RequestContext.SetProject(TString(params.Get("project")));

            // path params should overwrite query params in case of conflict
            for (const auto& [name, value] : RequestContext.GetPathParams()) {
                SetProtoMessageField(request, name, value);
            }
            FqConvert(request, grpcRequest);
            SetIdempotencyKey(grpcRequest, RequestContext.GetIdempotencyKey());

            return true;
        } catch (const std::exception& e) {
            ReplyError(TStringBuilder() << "Error in parsing: " << e.what());
            return false;
        }
    }

    static void SetProtoMessageField(THttpProtoRequestType& request, TStringBuf name, TStringBuf value) {
        const Reflection* reflection = request.GetReflection();
        const Descriptor* descriptor = request.GetDescriptor();
        auto field = descriptor->FindFieldByLowercaseName(TString(name));
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
            return reflection->SetString(&request, field, TString(value));
        default:
            break;
        }
    }

    void ReplyError(const TString& error) {
        RequestContext.ResponseBadRequest(Ydb::StatusIds::BAD_REQUEST, error);
    }

    static void SendReply(const THttpRequestContext& requestContext, const TJsonSettings& jsonSettings, NProtoBuf::Message* resp, ui32 status) {
        Y_ABORT_UNLESS(resp);
        Y_ABORT_UNLESS(resp->GetArena());
        Y_UNUSED(status);
        auto* typedResponse = static_cast<TGrpcProtoResponseType*>(resp);
        if (!typedResponse->operation().result().template Is<TGrpcProtoResultType>()) {
            TStringStream json;
            auto* httpResult = google::protobuf::Arena::CreateMessage<FQHttp::Error>(resp->GetArena());
            FqConvert(typedResponse->operation(), *httpResult);
            FqPackToJson(json, *httpResult, jsonSettings);

            requestContext.ResponseBadRequestJson(typedResponse->operation().status(), json.Str());
            return;
        }

        auto* grpcResult = google::protobuf::Arena::CreateMessage<TGrpcProtoResultType>(resp->GetArena());
        typedResponse->operation().result().UnpackTo(grpcResult);

        if (THttpProtoResultType::descriptor()->full_name() == google::protobuf::Empty::descriptor()->full_name()) {
            requestContext.ResponseNoContent();
            return;
        }

        try {
            TStringStream json;
            auto* httpResult = google::protobuf::Arena::CreateMessage<THttpProtoResultType>(resp->GetArena());
            FqConvert(*grpcResult, *httpResult);
            FqPackToJson(json, *httpResult, jsonSettings);
            requestContext.ResponseOKJson(json.Str());
        } catch (const std::exception& e) {
            requestContext.ResponseInternalServerError(TStringBuilder() << "Error during formatting response: " << e.what());
        }
    }
};

#define DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, t1, t2, t3, t4, t5)           \
class TJson##action : public TGrpcCallWrapper<t1, t2, t3, t4, t5> {                      \
    typedef TGrpcCallWrapper<t1, t2, t3, t4, t5> TBase;                                  \
public:                                                                                  \
    explicit TJson##action(const THttpRequestContext& request)                           \
      :  TBase(request, &NGRpcService::CreateFederatedQuery##internalAction##RequestOperationCall) {}  \
}

#define DECLARE_YQ_GRPC_ACTOR(action, internalAction) DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, FederatedQuery::internalAction##Request, FQHttp::action##Request, FederatedQuery::internalAction##Result, FQHttp::action##Result, FederatedQuery::internalAction##Response)
#define DECLARE_YQ_GRPC_ACTOR_WIHT_EMPTY_RESULT(action, internalAction) DECLARE_YQ_GRPC_ACTOR_IMPL(action, internalAction, FederatedQuery::internalAction##Request, FQHttp::action##Request, FederatedQuery::internalAction##Result, ::google::protobuf::Empty, FederatedQuery::internalAction##Response)

DECLARE_YQ_GRPC_ACTOR(CreateQuery, CreateQuery);
DECLARE_YQ_GRPC_ACTOR(GetQuery, DescribeQuery);
DECLARE_YQ_GRPC_ACTOR(GetQueryStatus, GetQueryStatus);
DECLARE_YQ_GRPC_ACTOR_WIHT_EMPTY_RESULT(StopQuery, ControlQuery);
DECLARE_YQ_GRPC_ACTOR(GetResultData, GetResultData);

} // namespace NKikimr::NPublicHttp
