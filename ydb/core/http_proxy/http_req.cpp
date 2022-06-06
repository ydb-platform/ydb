#include "events.h"
#include "http_req.h"
#include "auth_factory.h"

#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/core/grpc_caching/cached_grpc_request_actor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/datastreams_proxy.h>

#include <ydb/core/viewer/json/json.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/naming_conventions/naming_conventions.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/services/lib/sharding/sharding.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/json_output_create.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/protobuf/json/proto2json_printer.h>

#include <library/cpp/uri/uri.h>

#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/vector.h>

namespace NKikimr::NHttpProxy {

    using namespace google::protobuf;
    using namespace Ydb::DataStreams::V1;
    using namespace NYdb::NDataStreams::V1;

    TString StatusToErrorType(NYdb::EStatus status) {
        switch(status) {
        case NYdb::EStatus::BAD_REQUEST:
            return "InvalidParameterValueException"; //TODO: bring here issues and parse from them
        case NYdb::EStatus::UNAUTHORIZED:
            return "AccessDeniedException";
        case NYdb::EStatus::INTERNAL_ERROR:
            return "InternalFailureException";
        case NYdb::EStatus::ABORTED:
            return "RequestExpiredException"; //TODO: find better code
        case NYdb::EStatus::UNAVAILABLE:
            return "ServiceUnavailableException";
        case NYdb::EStatus::OVERLOADED:
            return "ThrottlingException";
        case NYdb::EStatus::SCHEME_ERROR:
            return "ResourceNotFoundException";
        case NYdb::EStatus::GENERIC_ERROR:
            return "InternalFailureException"; //TODO: find better code
        case NYdb::EStatus::TIMEOUT:
            return "RequestTimeoutException";
        case NYdb::EStatus::BAD_SESSION:
            return "AccessDeniedException";
        case NYdb::EStatus::PRECONDITION_FAILED:
        case NYdb::EStatus::ALREADY_EXISTS:
            return "ValidationErrorException"; //TODO: find better code
        case NYdb::EStatus::NOT_FOUND:
            return "ResourceNotFoundException";
        case NYdb::EStatus::SESSION_EXPIRED:
            return "AccessDeniedException";
        case NYdb::EStatus::UNSUPPORTED:
            return "InvalidActionException";
        default:
            return "InternalFailureException";
        }

    }

    HttpCodes StatusToHttpCode(NYdb::EStatus status) {
        switch(status) {
        case NYdb::EStatus::UNSUPPORTED:
        case NYdb::EStatus::BAD_REQUEST:
            return HTTP_BAD_REQUEST;
        case NYdb::EStatus::UNAUTHORIZED:
            return HTTP_FORBIDDEN;
        case NYdb::EStatus::INTERNAL_ERROR:
            return HTTP_INTERNAL_SERVER_ERROR;
        case NYdb::EStatus::ABORTED:
            return HTTP_CONFLICT;
        case NYdb::EStatus::UNAVAILABLE:
            return HTTP_SERVICE_UNAVAILABLE;
        case NYdb::EStatus::OVERLOADED:
            return HTTP_BAD_REQUEST;
        case NYdb::EStatus::SCHEME_ERROR:
            return HTTP_NOT_FOUND;
        case NYdb::EStatus::GENERIC_ERROR:
            return HTTP_BAD_REQUEST;
        case NYdb::EStatus::TIMEOUT:
            return HTTP_GATEWAY_TIME_OUT;
        case NYdb::EStatus::BAD_SESSION:
            return HTTP_UNAUTHORIZED;
        case NYdb::EStatus::PRECONDITION_FAILED:
            return HTTP_PRECONDITION_FAILED;
        case NYdb::EStatus::ALREADY_EXISTS:
            return HTTP_CONFLICT;
        case NYdb::EStatus::NOT_FOUND:
            return HTTP_NOT_FOUND;
        case NYdb::EStatus::SESSION_EXPIRED:
            return HTTP_UNAUTHORIZED;
        default:
            return HTTP_INTERNAL_SERVER_ERROR;
        }
    }



    template<class TProto>
    TString ExtractStreamNameWithoutProtoField(const TProto& req)
    {
        using namespace NKikimr::NDataStreams::V1;
        if constexpr (std::is_same<TProto, GetRecordsRequest>::value) {
            return TShardIterator(req.shard_iterator()).GetStreamName();
        }
        if constexpr (std::is_same<TProto, GetRecordsResult>::value) {
            return TShardIterator(req.next_shard_iterator()).GetStreamName();
        }
        if constexpr (std::is_same<TProto, ListStreamConsumersRequest>::value ||
                      std::is_same<TProto, ListStreamConsumersResult>::value) {
            TNextToken tkn(req.next_token());
            return tkn.IsValid() ? tkn.GetStreamName() : req.stream_arn();
        }
        if constexpr (std::is_same<TProto, ListShardsRequest>::value ||
                      std::is_same<TProto, ListShardsResult>::value) {
            TNextToken tkn(req.next_token());
            return tkn.IsValid() ? tkn.GetStreamName() : req.stream_name();
        }
        return "";
    }

    template<class TProto>
    TString ExtractStreamName(const TProto& req)
    {
        constexpr bool has_stream_name = requires(const TProto& t) {
            t.stream_name();
        };

        if constexpr (has_stream_name) {
            return req.stream_name();
        } else {
            return ExtractStreamNameWithoutProtoField(req);
        }
    }

    template<class TProto>
    TString TruncateStreamName(const TProto& req, const TString& database)
    {
        constexpr bool has_stream_name = requires(const TProto& t) {
            t.stream_name();
        };

        if constexpr (has_stream_name) {
            Y_VERIFY(req.stream_name().StartsWith(database));
            return req.stream_name().substr(database.size(), -1);
        }
        return ExtractStreamNameWithoutProtoField<TProto>(req).substr(database.size(), -1);
    }

    constexpr TStringBuf IAM_HEADER = "x-yacloud-subjecttoken";
    constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
    constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";
    constexpr TStringBuf REQUEST_DATE_HEADER = "x-amz-date";
    constexpr TStringBuf REQUEST_FORWARDED_FOR = "x-forwarded-for";
    constexpr TStringBuf REQUEST_TARGET_HEADER = "x-amz-target";
    static const TString CREDENTIAL_PARAM = "credential";

    namespace {
        class TYdsProtoToJsonPrinter : public NProtobufJson::TProto2JsonPrinter {
        public:
            TYdsProtoToJsonPrinter(const google::protobuf::Reflection* reflection, const NProtobufJson::TProto2JsonConfig& config)
                : NProtobufJson::TProto2JsonPrinter(config)
                , ProtoReflection(reflection)
            {}

        protected:
            template <bool InMapContext>
            void PrintDoubleValue(const TStringBuf& key, double value,
                                  NProtobufJson::IJsonOutput& json) {
                if constexpr(InMapContext) {
                    json.WriteKey(key).Write(value);
                } else {
                    json.Write(value);
                }
            }

            void PrintField(const NProtoBuf::Message& proto, const NProtoBuf::FieldDescriptor& field,
                            NProtobufJson::IJsonOutput& json, TStringBuf key = {}) override
            {
                if (field.options().HasExtension(FieldTransformer)) {
                    if (field.options().GetExtension(FieldTransformer) == TRANSFORM_BASE64) {
                        Y_ENSURE(field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                                 "Base64 is only supported for strings");

                        if (!key) {
                            key = MakeKey(field);
                        }

                        if (field.is_repeated()) {
                            for (int i = 0, endI = ProtoReflection->FieldSize(proto, &field); i < endI; ++i) {
                                PrintStringValue<false>(field, TStringBuf(), Base64Encode(proto.GetReflection()->GetRepeatedString(proto, &field, i)), json);
                            }
                        } else {
                            PrintStringValue<true>(field, key, Base64Encode(proto.GetReflection()->GetString(proto, &field)), json);
                        }
                        return;
                    }

                    if (field.options().GetExtension(FieldTransformer) == TRANSFORM_DOUBLE_S_TO_INT_MS) {
                        Y_ENSURE(field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64,
                                 "Double S to Int MS is only supported for int64 timestamps");

                        if (!key) {
                            key = MakeKey(field);
                        }

                        if (field.is_repeated()) {
                            for (int i = 0, endI = ProtoReflection->FieldSize(proto, &field); i < endI; ++i) {
                                double value = proto.GetReflection()->GetRepeatedInt64(proto, &field, i) / 1000.0;
                                PrintDoubleValue<false>(TStringBuf(), value, json);
                            }
                        } else {
                            double value = proto.GetReflection()->GetInt64(proto, &field) / 1000.0;
                            PrintDoubleValue<true>(key, value, json);
                        }
                        return;
                    }

                    if (field.options().GetExtension(FieldTransformer) == TRANSFORM_EMPTY_TO_NOTHING) {
                        Y_ENSURE(field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                                 "Empty to nothing is only supported for strings");

                        if (!key) {
                            key = MakeKey(field);
                        }

                        if (field.is_repeated()) {
                            for (int i = 0, endI = ProtoReflection->FieldSize(proto, &field); i < endI; ++i) {
                                auto value = proto.GetReflection()->GetRepeatedString(proto, &field, i);
                                if (!value.empty()) {
                                    PrintStringValue<false>(field, TStringBuf(), proto.GetReflection()->GetRepeatedString(proto, &field, i), json);
                                }
                            }
                        } else {
                            auto value = proto.GetReflection()->GetString(proto, &field);
                            if (!value.empty()) {
                                PrintStringValue<true>(field, key, proto.GetReflection()->GetString(proto, &field), json);
                            }
                        }
                        return;
                    }
                } else {
                    return NProtobufJson::TProto2JsonPrinter::PrintField(proto, field, json, key);
                }
            }

        private:
            const google::protobuf::Reflection* ProtoReflection = nullptr;
        };

        TString ProxyFieldNameConverter(const google::protobuf::FieldDescriptor& descriptor) {
            return NNaming::SnakeToCamelCase(descriptor.name());
        }

        void ProtoToJson(const Message& resp, NJson::TJsonValue& value) {
            auto config = NProtobufJson::TProto2JsonConfig()
                    .SetFormatOutput(false)
                    .SetMissingSingleKeyMode(NProtobufJson::TProto2JsonConfig::MissingKeyDefault)
                    .SetNameGenerator(ProxyFieldNameConverter)
                    .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
            TYdsProtoToJsonPrinter printer(resp.GetReflection(), config);
            printer.Print(resp, *NProtobufJson::CreateJsonMapOutput(value));
        }

        void JsonToProto(Message* message, const NJson::TJsonValue& jsonValue, ui32 depth = 0) {
            Y_ENSURE(depth < 100, "Too deep map");
            Y_ENSURE(jsonValue.IsMap(), "Top level of json value is not a map");
            auto* desc = message->GetDescriptor();
            auto* reflection = message->GetReflection();
            for (const auto& [key, value] : jsonValue.GetMap()) {
                auto* fieldDescriptor = desc->FindFieldByName(NNaming::CamelToSnakeCase(key));
                Y_ENSURE(fieldDescriptor, "Unexpected json key: " + key);
                auto transformer = Ydb::DataStreams::V1::TRANSFORM_NONE;
                if (fieldDescriptor->options().HasExtension(FieldTransformer)) {
                    transformer = fieldDescriptor->options().GetExtension(FieldTransformer);
                }

                if (value.IsArray()) {
                    Y_ENSURE(fieldDescriptor->is_repeated());
                    for (auto& elem : value.GetArray()) {
                        switch (transformer) {
                            case Ydb::DataStreams::V1::TRANSFORM_BASE64: {
                                Y_ENSURE(fieldDescriptor->cpp_type() ==
                                         google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                                         "Base64 transformer is only applicable to strings");
                                reflection->AddString(message, fieldDescriptor, Base64Decode(value.GetString()));
                                break;
                            }
                            case Ydb::DataStreams::V1::TRANSFORM_DOUBLE_S_TO_INT_MS: {
                                reflection->SetInt64(message, fieldDescriptor, value.GetDouble() * 1000);
                                break;
                            }
                            case Ydb::DataStreams::V1::TRANSFORM_EMPTY_TO_NOTHING:
                            case Ydb::DataStreams::V1::TRANSFORM_NONE: {
                                switch (fieldDescriptor->cpp_type()) {
                                    case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                                        reflection->AddInt32(message, fieldDescriptor, value.GetInteger());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                                        reflection->AddInt64(message, fieldDescriptor, value.GetInteger());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                                        reflection->AddUInt32(message, fieldDescriptor, value.GetUInteger());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                                        reflection->AddUInt64(message, fieldDescriptor, value.GetUInteger());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                                        reflection->AddDouble(message, fieldDescriptor, value.GetDouble());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                                        reflection->AddFloat(message, fieldDescriptor, value.GetDouble());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                                        reflection->AddFloat(message, fieldDescriptor, value.GetBoolean());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                                        {
                                            const EnumValueDescriptor* enumValueDescriptor = fieldDescriptor->enum_type()->FindValueByName(value.GetString());
                                            i32 number{0};
                                            if (enumValueDescriptor == nullptr && TryFromString(value.GetString(), number)) {
                                                enumValueDescriptor = fieldDescriptor->enum_type()->FindValueByNumber(number);
                                            }
                                            if (enumValueDescriptor != nullptr) {
                                                reflection->AddEnum(message, fieldDescriptor, enumValueDescriptor);
                                            }
                                        }
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                                        reflection->AddString(message, fieldDescriptor, value.GetString());
                                        break;
                                    case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                                        Message *msg = reflection->AddMessage(message, fieldDescriptor);
                                        JsonToProto(msg, elem, depth + 1);
                                        break;
                                    }
                                    default:
                                        Y_ENSURE(false, "Unexpected type");
                                }
                                break;
                            }
                            default:
                                Y_ENSURE(false, "Unknown transformer type");
                        }
                    }
                } else {
                    switch (transformer) {
                        case Ydb::DataStreams::V1::TRANSFORM_BASE64: {
                            Y_ENSURE(fieldDescriptor->cpp_type() ==
                                     google::protobuf::FieldDescriptor::CPPTYPE_STRING,
                                     "Base64 transformer is applicable only to strings");
                            reflection->SetString(message, fieldDescriptor, Base64Decode(value.GetString()));
                            break;
                        }
                        case Ydb::DataStreams::V1::TRANSFORM_DOUBLE_S_TO_INT_MS: {
                            reflection->SetInt64(message, fieldDescriptor, value.GetDouble() * 1000);
                            break;
                        }
                        case Ydb::DataStreams::V1::TRANSFORM_EMPTY_TO_NOTHING:
                        case Ydb::DataStreams::V1::TRANSFORM_NONE: {
                            switch (fieldDescriptor->cpp_type()) {
                                case google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                                    reflection->SetInt32(message, fieldDescriptor, value.GetInteger());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                                    reflection->SetInt64(message, fieldDescriptor, value.GetInteger());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                                    reflection->SetUInt32(message, fieldDescriptor, value.GetUInteger());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                                    reflection->SetUInt64(message, fieldDescriptor, value.GetUInteger());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                                    reflection->SetDouble(message, fieldDescriptor, value.GetDouble());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                                    reflection->SetFloat(message, fieldDescriptor, value.GetDouble());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                                    reflection->SetBool(message, fieldDescriptor, value.GetBoolean());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                                    {
                                        const EnumValueDescriptor* enumValueDescriptor = fieldDescriptor->enum_type()->FindValueByName(value.GetString());
                                        i32 number{0};
                                        if (enumValueDescriptor == nullptr && TryFromString(value.GetString(), number)) {
                                            enumValueDescriptor = fieldDescriptor->enum_type()->FindValueByNumber(number);
                                        }
                                        if (enumValueDescriptor != nullptr) {
                                            reflection->SetEnum(message, fieldDescriptor, enumValueDescriptor);
                                        }
                                    }
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                                    reflection->SetString(message, fieldDescriptor, value.GetString());
                                    break;
                                case google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
                                    auto *msg = reflection->MutableMessage(message, fieldDescriptor);
                                    JsonToProto(msg, value, depth + 1);
                                    break;
                                }
                                default:
                                    Y_ENSURE(false, "Unexpected type");
                            }
                            break;
                        }
                        default:
                            Y_ENSURE(false, "Unexpected transformer");
                    }
                }
            }
        }

    }



    template<class TProtoRequest>
    void FillInputCustomMetrics(const TProtoRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
            Y_UNUSED(request, httpContext, ctx);
    }
    template<class TProtoResult>
    void FillOutputCustomMetrics(const TProtoResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
            Y_UNUSED(result, httpContext, ctx);
    }

    TVector<std::pair<TString, TString>> BuildLabels(const TString& method, const THttpRequestContext& httpContext, const TString& name) {
        if (method.empty()) {
            return {{"cloud", httpContext.CloudId}, {"folder", httpContext.FolderId},
                    {"database", httpContext.DatabaseId}, {"stream", httpContext.StreamName},
                    {"name", name}};

        }
        return {{"method", method}, {"cloud", httpContext.CloudId}, {"folder", httpContext.FolderId},
                {"database", httpContext.DatabaseId}, {"stream", httpContext.StreamName},
                {"name", name}};
    }

    template <>
    void FillInputCustomMetrics<PutRecordsRequest>(const PutRecordsRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{request.records_size(), true, true, BuildLabels("", httpContext, "stream.incoming_records_per_second")
                                            });

        i64 bytes = 0;
        for (auto& rec : request.records()) {
            bytes += rec.data().size() +  rec.partition_key().size() + rec.explicit_hash_key().size();
        }

        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{bytes, true, true, BuildLabels("", httpContext, "stream.incoming_bytes_per_second")
                                            });

        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{bytes, true, true, BuildLabels("", httpContext, "stream.put_records.bytes_per_second")
                                            });
    }

    template <>
    void FillInputCustomMetrics<PutRecordRequest>(const PutRecordRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{1, true, true, BuildLabels("", httpContext, "stream.incoming_records_per_second")
                                            });
        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{1, true, true, BuildLabels("", httpContext, "stream.put_record.records_per_second")
                                            });

        i64 bytes = request.data().size() +  request.partition_key().size() + request.explicit_hash_key().size();

        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{bytes, true, true, BuildLabels("", httpContext, "stream.incoming_bytes_per_second")
                                            });

        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{bytes, true, true, BuildLabels("", httpContext, "stream.put_record.bytes_per_second")
                                            });
    }


    template <>
    void FillOutputCustomMetrics<PutRecordResult>(const PutRecordResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        Y_UNUSED(result);
        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{1, true, true, BuildLabels("", httpContext, "stream.put_record.success_per_second")
                                            });
    }


    template <>
    void FillOutputCustomMetrics<PutRecordsResult>(const PutRecordsResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        i64 failed = result.failed_record_count();
        i64 success = result.records_size() - failed;
        if (success > 0) {
            ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{1, true, true, BuildLabels("", httpContext, "stream.put_records.success_per_second")
                });
            ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{success, true, true, BuildLabels("", httpContext, "stream.put_records.successfull_records_per_second")
                });
        }

        ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{result.records_size(), true, true, BuildLabels("", httpContext, "stream.put_records.total_records_per_second")
            });
        if (failed > 0) {
            ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{failed, true, true,
BuildLabels("", httpContext, "stream.put_records.failed_records_per_second")
                });
        }
    }

    template <>
    void FillOutputCustomMetrics<GetRecordsResult>(const GetRecordsResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        auto records_n = result.records().size();
        auto bytes = std::accumulate(result.records().begin(), result.records().end(), 0l,
                                     [](i64 sum, decltype(*result.records().begin()) &r) {
                                         return sum + r.data().size() +
                                             r.partition_key().size() +
                                             r.sequence_number().size() +
                                             sizeof(r.timestamp()) +
                                             sizeof(r.encryption())
                                             ;
                                     });

        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{1, true, true,
                     BuildLabels("", httpContext, "stream.get_records.success_per_second")}
                 );
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{records_n, true, true,
                     BuildLabels("", httpContext, "stream.get_records.records_per_second")}
                 );
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{bytes, true, true,
                     BuildLabels("", httpContext, "stream.get_records.bytes_per_second")}
                 );
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{records_n, true, true,
                     BuildLabels("", httpContext, "stream.outgoing_records_per_second")}
                 );
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{bytes, true, true,
                     BuildLabels("", httpContext, "stream.outgoing_bytes_per_second")}
                 );
    }

    template<class TProtoService, class TProtoRequest, class TProtoResponse, class TProtoResult, class TProtoCall, class TRpcEv>
    class THttpRequestProcessor : public IHttpRequestProcessor {
    public:
        enum TRequestState {
            StateIdle,
            StateAuthentication,
            StateAuthorization,
            StateListEndpoints,
            StateGrpcRequest,
            StateFinished
        };

        enum TEv {
            EvRequest,
            EvResponse,
            EvResult
        };

    public:
        THttpRequestProcessor(TString method, TProtoCall protoCall)
            : Method(method)
            , ProtoCall(protoCall)
        {
        }

        const TString& Name() const override {
            return Method;
        }

        void Execute(THttpRequestContext&& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature, const TActorContext& ctx) override {
            ctx.Register(new THttpRequestActor(
                    std::move(context),
                    std::move(signature),
                    ProtoCall, Method));
        }

    private:


        class THttpRequestActor : public NActors::TActorBootstrapped<THttpRequestActor> {
        public:
            using TBase = NActors::TActorBootstrapped<THttpRequestActor>;

            THttpRequestActor(THttpRequestContext&& httpContext,
                              THolder<NKikimr::NSQS::TAwsRequestSignV4>&& signature,
                              TProtoCall protoCall, const TString& method)
                : HttpContext(std::move(httpContext))
                , Signature(std::move(signature))
                , ProtoCall(protoCall)
                , Method(method)
            {
            }

            TStringBuilder LogPrefix() const {
                return HttpContext.LogPrefix();
            }

        private:
            STFUNC(StateWork)
            {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvServerlessProxy::TEvGrpcRequestResult, HandleGrpcResponse);
                    HFunc(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult, Handle);
                    HFunc(TEvents::TEvWakeup, HandleTimeout);
                    HFunc(TEvServerlessProxy::TEvToken, HandleToken);
                    HFunc(TEvServerlessProxy::TEvError, HandleError);
                    HFunc(TEvServerlessProxy::TEvClientReady, HandleClientReady);
                    default:
                        HandleUnexpectedEvent(ev, ctx);
                        break;
                }
            }

            void SendYdbDriverRequest(const TActorContext& ctx) {
                Y_VERIFY(HttpContext.Driver);

                RequestState = StateAuthorization;

                auto request = MakeHolder<TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest>();
                request->DatabasePath = HttpContext.DatabaseName;

                ctx.Send(MakeTenantDiscoveryID(), std::move(request));
            }

            void CreateClient(const TActorContext& ctx) {
                RequestState = StateListEndpoints;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "create client to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabaseName <<
                              "' iam token size: " << HttpContext.IamToken.size());

                auto clientSettings = NYdb::TCommonClientSettings()
                        .DiscoveryEndpoint(HttpContext.DiscoveryEndpoint)
                        .Database(HttpContext.DatabaseName)
                        .AuthToken(HttpContext.IamToken)
                        .DiscoveryMode(NYdb::EDiscoveryMode::Async);

                if (!HttpContext.DatabaseName.empty()) {
                    if (!HttpContext.ServiceConfig.GetTestMode()) {
                        clientSettings.Database(HttpContext.DatabaseName);
                    }
                }
                Y_VERIFY(!Client);
                Client.Reset(new TDataStreamsClient(*HttpContext.Driver, clientSettings));
                DiscoveryFuture = MakeHolder<NThreading::TFuture<void>>(Client->DiscoveryCompleted());
                DiscoveryFuture->Subscribe([actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()](const NThreading::TFuture<void>&) {
                    actorSystem->Send(actorId, new TEvServerlessProxy::TEvClientReady());
                });
            }

            void HandleClientReady(TEvServerlessProxy::TEvClientReady::TPtr&, const TActorContext& ctx){
                SendGrpcRequest(ctx);
            }


            void SendGrpcRequest(const TActorContext& ctx) {
                RequestState = StateGrpcRequest;
                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "sending grpc request to '" << HttpContext.DiscoveryEndpoint <<
                              "' database: '" << HttpContext.DatabaseName <<
                              "' iam token size: " << HttpContext.IamToken.size());
                if (!HttpContext.Driver) {
                    RpcFuture = NRpcService::DoLocalRpc<TRpcEv>(std::move(Request), HttpContext.DatabaseName, HttpContext.SerializedUserToken, ctx.ActorSystem());
                    RpcFuture.Subscribe([actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()](const NThreading::TFuture<TProtoResponse>& future) {
                        auto& response = future.GetValueSync();
                        auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                        Y_VERIFY(response.operation().ready());
                        if (response.operation().status() == Ydb::StatusIds::SUCCESS) {
                            TProtoResult rs;
                            response.operation().result().UnpackTo(&rs);
                            result->Message = MakeHolder<TProtoResult>(rs);
                        }
                        NYql::TIssues issues;
                        NYql::IssuesFromMessage(response.operation().issues(), issues);
                        result->Status = MakeHolder<NYdb::TStatus>(NYdb::EStatus(response.operation().status()), std::move(issues));
                        actorSystem->Send(actorId, result.Release());
                    });
                    return;
                }
                Y_VERIFY(Client);
                Y_VERIFY(DiscoveryFuture->HasValue());

                TProtoResponse response;

                LOG_SP_DEBUG_S(ctx, NKikimrServices::HTTP_PROXY, "sending grpc request " << Request.DebugString());

                Future = MakeHolder<NThreading::TFuture<TProtoResultWrapper<TProtoResult>>>(Client->template DoProtoRequest<TProtoRequest, TProtoResponse, TProtoResult, TProtoCall>(std::move(Request), ProtoCall));
                Future->Subscribe([actorId = ctx.SelfID, actorSystem = ctx.ActorSystem()](const NThreading::TFuture<TProtoResultWrapper<TProtoResult>>& future) {
                    auto& response = future.GetValueSync();
                    auto result = MakeHolder<TEvServerlessProxy::TEvGrpcRequestResult>();
                    if (response.IsSuccess()) {
                        result->Message = MakeHolder<TProtoResult>(response.GetResult());
                    }
                    result->Status = MakeHolder<NYdb::TStatus>(response);
                    actorSystem->Send(actorId, result.Release());
                });

            }

            void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev, const TActorContext& ctx) {
                Y_UNUSED(ev, ctx);
            }

            void HandleToken(TEvServerlessProxy::TEvToken::TPtr& ev, const TActorContext& ctx) {
                HttpContext.ServiceAccountId = ev->Get()->ServiceAccountId;
                HttpContext.IamToken = ev->Get()->IamToken;
                HttpContext.SerializedUserToken = ev->Get()->SerializedUserToken;

                if (HttpContext.Driver) {
                    SendYdbDriverRequest(ctx);
                } else {
                    SendGrpcRequest(ctx);
                }
            }

            void HandleError(TEvServerlessProxy::TEvError::TPtr& ev, const TActorContext& ctx) {
                ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Response);
            }

            void ReplyWithError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText) {
                ctx.Send(MakeMetricsServiceID(),
                         new TEvServerlessProxy::TEvCounter{
                             1, true, true,
                             {{"method", Method},
                              {"cloud", HttpContext.CloudId},
                              {"folder", HttpContext.FolderId},
                              {"database", HttpContext.DatabaseId},
                              {"stream", HttpContext.StreamName},
                              {"code", TStringBuilder() << (int)StatusToHttpCode(status)},
                              {"name", "api.http.errors_per_second"}}
                         });

                HttpContext.ResponseData.Status = status;
                HttpContext.ResponseData.ErrorText = errorText;
                HttpContext.DoReply(ctx);

                ctx.Send(AuthActor, new TEvents::TEvPoisonPill());

                TBase::Die(ctx);
            }

            void Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult::TPtr ev, const TActorContext& ctx) {
                if (ev->Get()->DatabaseInfo) {
                    auto& db = ev->Get()->DatabaseInfo;
                    HttpContext.FolderId = db->FolderId;
                    HttpContext.CloudId = db->CloudId;
                    HttpContext.DatabaseId = db->Id;
                    HttpContext.DiscoveryEndpoint = db->Endpoint;
                    HttpContext.DatabaseName = db->Path;

                    if (ExtractStreamName<TProtoRequest>(Request).StartsWith(HttpContext.DatabaseName + "/")) {
                        HttpContext.StreamName =
                            TruncateStreamName<TProtoRequest>(Request, HttpContext.DatabaseName + "/");
                    } else {
                        HttpContext.StreamName = ExtractStreamName<TProtoRequest>(Request);
                    }

                    FillInputCustomMetrics<TProtoRequest>(Request, HttpContext, ctx);
                    ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvCounter{1, true, true, BuildLabels(Method, HttpContext, "api.http.requests_per_second")
                                            });
                    CreateClient(ctx);
                    return;
                }

                return ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Message);
            }

            void ReportLatencyCounters(const TActorContext& ctx) {
                TDuration dur = ctx.Now() - StartTime;
                ctx.Send(MakeMetricsServiceID(), new TEvServerlessProxy::TEvHistCounter{static_cast<i64>(dur.MilliSeconds()), 1,
                                    BuildLabels(Method, HttpContext, "api.http.requests_duration_milliseconds")
                        });
            }

            void HandleGrpcResponse(TEvServerlessProxy::TEvGrpcRequestResult::TPtr ev,
                                    const TActorContext& ctx) {
                // convert grpc result to protobuf
                // return http response;
                if (ev->Get()->Status->IsSuccess()) {
                    ProtoToJson(*ev->Get()->Message, HttpContext.ResponseData.ResponseBody);
                    FillOutputCustomMetrics<TProtoResult>(*(dynamic_cast<TProtoResult*>(ev->Get()->Message.Get())), HttpContext, ctx);
                    ReportLatencyCounters(ctx);

                    ctx.Send(MakeMetricsServiceID(),
                             new TEvServerlessProxy::TEvCounter{1, true, true,
                                 BuildLabels(Method, HttpContext, "api.http.success_per_second")
                             });

                    HttpContext.DoReply(ctx);
                } else {
                    auto retryClass =
                        NYdb::NPersQueue::GetRetryErrorClass(ev->Get()->Status->GetStatus());

                    switch (retryClass) {
                    case ERetryErrorClass::ShortRetry:
                    case ERetryErrorClass::LongRetry:
                        RetryCounter.Click();
                        if (RetryCounter.HasAttemps()) {
                            return SendGrpcRequest(ctx);
                        }
                    case ERetryErrorClass::NoRetry: {
                        TString errorText;
                        TStringOutput stringOutput(errorText);
                        ev->Get()->Status->GetIssues().PrintTo(stringOutput);
                        RetryCounter.Void();
                        return ReplyWithError(ctx, ev->Get()->Status->GetStatus(), errorText);
                        }
                    }
                }
                TBase::Die(ctx);
            }

            void HandleTimeout(TEvents::TEvWakeup::TPtr ev, const TActorContext& ctx) {
                Y_UNUSED(ev);
                return ReplyWithError(ctx, NYdb::EStatus::TIMEOUT, "Request hasn't been completed by deadline");
            }

        public:
            void Bootstrap(const TActorContext& ctx) {
                StartTime = ctx.Now();
                try {
                    JsonToProto(&Request, HttpContext.RequestBody);
                } catch (std::exception& e) {
                    LOG_SP_WARN_S(ctx, NKikimrServices::HTTP_PROXY,
                                  "got new request with incorrect json from [" << SourceAddress << "] " <<
                                  "database '" << HttpContext.DatabaseName << "'");

                    return ReplyWithError(ctx, NYdb::EStatus::BAD_REQUEST, e.what());
                }

                if (HttpContext.DatabaseName.empty()) {
                    HttpContext.DatabaseName = ExtractStreamName<TProtoRequest>(Request);
                }

                LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
                              "got new request from [" << SourceAddress << "] " <<
                              "database '" << HttpContext.DatabaseName << "' " <<
                              "stream '" << ExtractStreamName<TProtoRequest>(Request) << "'");

                if (HttpContext.IamToken.empty() || !HttpContext.Driver) { //use Signature or no sdk mode - then need to auth anyway
                    if (HttpContext.IamToken.empty() && !Signature) { //Test mode - no driver and no creds
                        SendGrpcRequest(ctx);
                    } else {
                        AuthActor = ctx.Register(AppData(ctx)->DataStreamsAuthFactory->CreateAuthActor(ctx.SelfID, HttpContext, std::move(Signature)));
                    }
                } else {
                    SendYdbDriverRequest(ctx);
                }
                ctx.Schedule(RequestTimeout, new TEvents::TEvWakeup());

                TBase::Become(&THttpRequestActor::StateWork);
            }

        private:
            TInstant StartTime;
            TRequestState RequestState = StateIdle;
            TProtoRequest Request;
            TDuration RequestTimeout = TDuration::Seconds(60);
            ui32 PoolId;
            THttpRequestContext HttpContext;
            THolder<NKikimr::NSQS::TAwsRequestSignV4> Signature;
            THolder<NThreading::TFuture<TProtoResultWrapper<TProtoResult>>> Future;
            NThreading::TFuture<TProtoResponse> RpcFuture;
            THolder<NThreading::TFuture<void>> DiscoveryFuture;
            TProtoCall ProtoCall;
            TString Method;
            TString SourceAddress;
            TRetryCounter RetryCounter;

            THolder<TDataStreamsClient> Client;

            TActorId AuthActor;
        };





    private:
        TString Method;

        struct TAccessKeySignature {
            TString AccessKeyId;
            TString SignedString;
            TString Signature;
            TString Region;
            TInstant SignedAt;
        };

        TProtoCall ProtoCall;
    };


    void THttpRequestProcessors::Initialize() {
        #define DECLARE_PROCESSOR(name) Name2Processor[#name] = MakeHolder<THttpRequestProcessor<DataStreamsService, name##Request, name##Response, name##Result,\
                    decltype(&Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name), NKikimr::NGRpcService::TEvDataStreams##name##Request>> \
                    (#name, &Ydb::DataStreams::V1::DataStreamsService::Stub::Async##name);
        DECLARE_PROCESSOR(PutRecords);
        DECLARE_PROCESSOR(CreateStream);
        DECLARE_PROCESSOR(ListStreams);
        DECLARE_PROCESSOR(DeleteStream);
        DECLARE_PROCESSOR(DescribeStream);
        DECLARE_PROCESSOR(ListShards);
        DECLARE_PROCESSOR(PutRecord);
        DECLARE_PROCESSOR(GetRecords);
        DECLARE_PROCESSOR(GetShardIterator);
        DECLARE_PROCESSOR(DescribeLimits);
        DECLARE_PROCESSOR(DescribeStreamSummary);
        DECLARE_PROCESSOR(DecreaseStreamRetentionPeriod);
        DECLARE_PROCESSOR(IncreaseStreamRetentionPeriod);
        DECLARE_PROCESSOR(UpdateShardCount);
        DECLARE_PROCESSOR(RegisterStreamConsumer);
        DECLARE_PROCESSOR(DeregisterStreamConsumer);
        DECLARE_PROCESSOR(DescribeStreamConsumer);
        DECLARE_PROCESSOR(ListStreamConsumers);
        DECLARE_PROCESSOR(AddTagsToStream);
        DECLARE_PROCESSOR(DisableEnhancedMonitoring);
        DECLARE_PROCESSOR(EnableEnhancedMonitoring);
        DECLARE_PROCESSOR(ListTagsForStream);
        DECLARE_PROCESSOR(MergeShards);
        DECLARE_PROCESSOR(RemoveTagsFromStream);
        DECLARE_PROCESSOR(SplitShard);
        DECLARE_PROCESSOR(StartStreamEncryption);
        DECLARE_PROCESSOR(StopStreamEncryption);
        #undef DECLARE_PROCESSOR
    }

    bool THttpRequestProcessors::Execute(const TString& name, THttpRequestContext&& context, THolder<NKikimr::NSQS::TAwsRequestSignV4> signature, const TActorContext& ctx) {
        // TODO: To be removed by CLOUD-79086
        if (name == "RegisterStreamConsumer" ||
            name == "DeregisterStreamConsumer" ||
            name == "ListStreamConsumers") {
            context.SendBadRequest(NYdb::EStatus::BAD_REQUEST, TStringBuilder() << "Unsupported method name " << name, ctx);
            return false;
        }

        if (auto proc = Name2Processor.find(name); proc != Name2Processor.end()) {
            proc->second->Execute(std::move(context), std::move(signature), ctx);
            return true;
        }
        context.SendBadRequest(NYdb::EStatus::BAD_REQUEST, TStringBuilder() << "Unknown method name " << name, ctx);
        return false;
    }


    TString GenerateRequestId(const TString& sourceReqId) {
        if (!sourceReqId.empty()) {
            return CreateGuidAsString() + "-" + sourceReqId;
        } else {
            return CreateGuidAsString();
        }
    }

    void THttpRequestContext::ParseHeaders(TStringBuf str)
    {
        TString sourceReqId;
        NHttp::THeaders headers(str);
        for (const auto& header : headers.Headers) {
            if (AsciiEqualsIgnoreCase(header.first, IAM_HEADER)) {
                IamToken = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, AUTHORIZATION_HEADER)) {
                if (header.second.StartsWith("Bearer ")) {
                    IamToken = header.second;
                }
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_ID_HEADER)) {
                sourceReqId = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_FORWARDED_FOR)) {
                SourceAddress = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_TARGET_HEADER)) {
                TString requestTarget = TString(header.second);
                TVector<TString> parts = SplitString(requestTarget, ".");
                ApiVersion = parts[0];
                MethodName = parts[1];
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_DATE_HEADER)) {
            }
        }
        RequestId = GenerateRequestId(sourceReqId);
    }


} // namespace NKikimr::NHttpProxy
