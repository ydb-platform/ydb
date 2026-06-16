#pragma once

#include "http_req.h"
#include "serialization.h"

#include <ydb/core/http_proxy/sqs_xml/params.h>
#include <ydb/library/http_proxy/error/error.h>

namespace NKikimr::NHttpProxy::NSQS {

    template<typename TValue>
    void PrepareValue(TValue& value) {
        Y_UNUSED(value);
    }

    void DeserializeXml(NProtoBuf::Message& message, const TStringBuf& input) {
        TCgiParameters cgiParameters(TStringBuf(input.Data(), input.Size()));

        TParameters params;
        TParametersParser parser(&params);
        for (auto pi = cgiParameters.begin(); pi != cgiParameters.end(); ++pi) {
            parser.Append(pi->first, pi->second);
        }
    }

    template<typename TValue>
    void Deserialize(const MimeTypes mimeType, TValue& value, const TStringBuf& input)
        requires std::is_base_of_v<NProtoBuf::Message, TValue> {

        if (input.empty()) {
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) << "Empty body";
        }

        PrepareValue(value);

        switch (mimeType) {
        case MIME_CBOR:
            DeserializeCbor(value, input);
            break;
        case MIME_JSON:
            DeserializeJson(value, input);
            break;
        case MIME_XML:
            DeserializeXml(value, input);
            break;
        default:
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Unknown ContentType";
        }
    };

    TString Serialize(const THttpRequestContext& httpContext, const NProtoBuf::Message& value);

    struct TErrorResponse {
        TString StatusCode;
        TString ErrorText;
    };
    TString Serialize(const THttpRequestContext& httpContext, TErrorResponse&& value);

} // namespace NKikimr::NHttpProxy::NSQS
