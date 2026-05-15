#include "auth_factory.h"
#include "custom_metrics.h"
#include "exceptions_mapping.h"
#include "http_req.h"
#include "json_proto_conversion.h"
#include "serialization.h"
#include "utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/grpc_caching/cached_grpc_request_actor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/serverless_proxy_config.pb.h>
#include <ydb/core/security/ticket_parser_impl.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/iam_token_service.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/iam_token_service.h>
#include <ydb/public/api/grpc/draft/ydb_sqs_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/services/datastreams/datastreams_proxy.h>
#include <ydb/services/datastreams/next_token.h>
#include <ydb/services/datastreams/shard_iterator.h>
#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>
#include <ydb/services/sqs_topic/sqs_topic_proxy.h>
#include <ydb/services/sqs_topic/utils.h>
#include <ydb/services/ymq/grpc_service.h>
#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/utils.h>
#include <ydb/services/ymq/ymq_proxy.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/digest/old_crc/crc.h>
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

#include <nlohmann/json.hpp>

namespace NKikimr::NHttpProxy {

    using namespace google::protobuf;
    using namespace Ydb::DataStreams::V1;
    using namespace NYdb::NDataStreams::V1;

    constexpr TStringBuf IAM_HEADER = "x-yacloud-subjecttoken";
    constexpr TStringBuf SECURITY_TOKEN_HEADER = "x-amz-security-token";
    constexpr TStringBuf AUTHORIZATION_HEADER = "authorization";
    constexpr TStringBuf REQUEST_ID_HEADER = "x-request-id";
    constexpr TStringBuf REQUEST_ID_HEADER_EXT = "x-amzn-requestid";
    constexpr TStringBuf REQUEST_DATE_HEADER = "x-amz-date";
    constexpr TStringBuf REQUEST_FORWARDED_FOR = "x-forwarded-for";
    constexpr TStringBuf REQUEST_TARGET_HEADER = "x-amz-target";
    constexpr TStringBuf REQUEST_CONTENT_TYPE_HEADER = "content-type";
    constexpr TStringBuf CREDENTIAL_PARAM = "Credential";

    THttpRequestProcessors::THttpRequestProcessors(const NKikimrConfig::TServerlessProxyConfig&) {
    }

    bool THttpRequestProcessors::Execute(const TString& name, THttpRequestContext&& context,
                                         THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                                         const TActorContext& ctx) {

        const auto* controller = GetHttpControllerRegistry().GetController(context.ApiVersion, context.ServiceConfig);
        if (controller) {
            auto proc = controller->GetProcessor(name, context);
            if (proc.has_value()) {
                try {
                    proc.value()->Execute(std::move(context), std::move(signature), ctx);
                } catch (const NKikimr::NSQS::TSQSException& e) {
                    context.DoReply(controller->MakeError(context.ContentType, NYdb::EStatus::BAD_REQUEST,
                        e.what(), static_cast<size_t>(NYds::EErrorCodes::ACCESS_DENIED)));
                }
                return true;
            } else {
                switch (proc.error()) {
                    case IHttpController::EError::MethodNotFound:
                        context.DoReply(controller->MakeError(context.ContentType, NYdb::EStatus::UNSUPPORTED,
                            TStringBuilder() << "Unknown method name " << name.Quote(), static_cast<size_t>(NYds::EErrorCodes::MISSING_ACTION)));
                        return false;
                }
            }
        }

        context.DoReply(THttpResponseData{
            .HttpCode = 404,
            .ContentType = "text/plain",
            .Message = "Not Found",
            .Body = "Not Found"
        });

        return false;
    }

    TString GenerateRequestId(const TString& sourceReqId) {
        if (!sourceReqId.empty()) {
            return CreateGuidAsString() + "-" + sourceReqId;
        } else {
            return CreateGuidAsString();
        }
    }

    THttpRequestContext::THttpRequestContext(
        const NKikimrConfig::TServerlessProxyConfig& config,
        NHttp::THttpIncomingRequestPtr request,
        NActors::TActorId sender,
        NYdb::TDriver* driver,
        std::shared_ptr<NYdb::ICredentialsProvider> serviceAccountCredentialsProvider)
        : ServiceConfig(config)
        , Request(request)
        , Sender(sender)
        , Driver(driver)
        , ServiceAccountCredentialsProvider(serviceAccountCredentialsProvider) {
        char address[INET6_ADDRSTRLEN];
        if (inet_ntop(AF_INET6, &(Request->Address), address, INET6_ADDRSTRLEN) == nullptr) {
            SourceAddress = "unknown";
        } else {
            SourceAddress = address;
        }

        DatabasePath = Request->URL.Before('?');
        if (DatabasePath == "/") {
           DatabasePath = "";
        }
        auto params = TCgiParameters(Request->URL.After('?'));
        if (auto it = params.Find("folderId"); it != params.end()) {
            FolderId = it->second;
        }

        //TODO: find out databaseId
        ParseHeaders(Request->Headers);
    }

    THolder<NKikimr::NSQS::TAwsRequestSignV4> THttpRequestContext::GetSignature() {
        THolder<NKikimr::NSQS::TAwsRequestSignV4> signature;
        if (IamToken.empty()) {
            const TString fullRequest = TString(Request->Method) + " " +
                Request->URL + " " +
                Request->Protocol + "/" + Request->Version + "\r\n" +
                Request->Headers +
                Request->Body;
            signature = MakeHolder<NKikimr::NSQS::TAwsRequestSignV4>(fullRequest);
        }

        return signature;
    }

    void THttpRequestContext::DoReply(THttpResponseData&& data) {
        auto ctx = TlsActivationContext->AsActorContext();
        LOG_SP_INFO_S(ctx, NKikimrServices::HTTP_PROXY,
            "reply with status: " << data.HttpCode << " message: " << data.Message);

        NHttp::THttpOutgoingResponsePtr response = new NHttp::THttpOutgoingResponse(
            Request,
            "HTTP",
            "1.1",
            TStringBuilder() << data.HttpCode,
            data.Message
        );
        response->Set<&NHttp::THttpResponse::Connection>(Request->GetConnection());
        response->Set(REQUEST_ID_HEADER_EXT, RequestId);
        if (!data.ContentType.empty() && !data.Body.empty()) {
            response->Set<&NHttp::THttpResponse::ContentType>(data.ContentType);
            if (!Request->Endpoint->CompressContentTypes.empty()) {
                TStringBuf buffer = data.ContentType;
                auto contentType = NHttp::Trim(buffer.Before(';'), ' ');
                if (Count(Request->Endpoint->CompressContentTypes, contentType) != 0) {
                    response->EnableCompression();
                }
            }
        }

        if (response->IsNeedBody() || !data.Body.empty()) {
            if (Request->Method == "HEAD") {
                response->Set<&NHttp::THttpResponse::ContentLength>(ToString(data.Body.size()));
            } else {
                response->SetBody(data.Body);
            }
        }

        ctx.Send(Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    TMaybe<TStringBuf> ExtractUserName(const TStringBuf& authorizationHeader) {
        const size_t spacePos = authorizationHeader.find(' ');
        if (spacePos == TString::npos) {
            return Nothing();
        }
        auto restOfHeader = authorizationHeader.substr(spacePos + 1);
        if (restOfHeader.StartsWith(CREDENTIAL_PARAM)) {
            const size_t equalsPos = restOfHeader.find('=');
            if (equalsPos == TString::npos) {
                return Nothing();
            }
            const size_t slashPos = restOfHeader.find('/');
            if (slashPos == TString::npos || slashPos < equalsPos) {
                return Nothing();
            }
            return restOfHeader.substr(equalsPos + 1, slashPos - equalsPos - 1);
        }
        return Nothing();
    }

    void THttpRequestContext::ParseHeaders(TStringBuf str) {
        TString sourceReqId;
        NHttp::THeaders headers(str);
        for (const auto& header : headers.Headers) {
            if (AsciiEqualsIgnoreCase(header.first, IAM_HEADER)) {
                IamToken = header.second;
            } else if(AsciiEqualsIgnoreCase(header.first, SECURITY_TOKEN_HEADER)) {
                SecurityToken = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, AUTHORIZATION_HEADER)) {
                if (header.second.StartsWith("Bearer ")) {
                    IamToken = header.second;
                } else {
                    auto userName = ExtractUserName(header.second);
                    if (userName.Defined()) {
                        UserName = userName.GetRef();
                    }
                }
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_ID_HEADER)) {
                sourceReqId = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_FORWARDED_FOR)) {
                SourceAddress = header.second;
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_TARGET_HEADER)) {
                TString requestTarget = TString(header.second);
                TVector<TString> parts = SplitString(requestTarget, ".");
                ApiVersion = parts.size() > 0 ? parts[0] : "";
                MethodName = parts.size() > 1 ? parts[1] : "";
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_CONTENT_TYPE_HEADER)) {
                ContentType = mimeByStr(header.second);
            } else if (AsciiEqualsIgnoreCase(header.first, REQUEST_DATE_HEADER)) {
            }
        }
        RequestId = GenerateRequestId(sourceReqId);
    }

    TString AsAwsContentType(MimeTypes contentType) {
        switch (contentType) {
            case MIME_JSON:
                return "application/x-amz-json-1.1";
            case MIME_CBOR:
                return "application/x-amz-cbor-1.1";
            default:
                return strByMime(contentType);
        }
    }


} // namespace NKikimr::NHttpProxy


template <>
void Out<NKikimr::NHttpProxy::THttpResponseData>(IOutputStream& o, const NKikimr::NHttpProxy::THttpResponseData& p) {
    TString s = TStringBuilder() << "NYdb status: " << p.HttpCode <<
        ". Content type: " << p.ContentType <<  
        ". Message: " << p.Message <<
        ". Body: " << p.Body;

    o.Write(s.data(), s.length());
}
