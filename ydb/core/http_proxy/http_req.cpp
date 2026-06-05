#include "http_req.h"

#include "auth_factory.h"
#include "utils.h"

#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <ydb/library/http_proxy/error/error.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/uri/uri.h>

#include <util/string/ascii.h>
#include <util/string/vector.h>


namespace NKikimr::NHttpProxy {

    using namespace google::protobuf;

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

    bool THttpRequestProcessors::Execute(const TString&, THttpRequestContext&& context,
                                         THolder<NKikimr::NSQS::TAwsRequestSignV4> signature,
                                         const TActorContext&) {

        const auto* controller = GetHttpControllerRegistry().GetController(context.ApiVersion, context.ServiceConfig);
        if (controller) {
            return controller->Execute(std::move(context), std::move(signature));
        }

        context.DoReply(THttpResponseData{
            .HttpCode = 404,
            .ContentType = MimeTypes::MIME_TEXT,
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
        if (!data.Body.empty()) {
            const auto contentType = AsAwsContentType(data.ContentType);
            response->Set<&NHttp::THttpResponse::ContentType>(contentType);
            if (!Request->Endpoint->CompressContentTypes.empty()) {
                TStringBuf buffer = contentType;
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
            } else if (AsciiEqualsIgnoreCase(header.first, SECURITY_TOKEN_HEADER)) {
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
