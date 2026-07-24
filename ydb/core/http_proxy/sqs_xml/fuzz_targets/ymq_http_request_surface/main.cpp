#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/http_proxy/sqs_xml/params.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <util/string/builder.h>

namespace {

void ExerciseParsedRequest(const TString& firstLine) {
    TParsedHttpFull parsed(firstLine);
    (void)parsed.Method;
    (void)parsed.Path;
    (void)parsed.Cgi;
}

void ExerciseParams(const TString& encodedParams) {
    NKikimr::NHttpProxy::NSQS::TParameters params;
    NKikimr::NHttpProxy::NSQS::TParametersParser parser(&params);
    TCgiParameters cgi{TStringBuf(encodedParams)};

    for (auto it = cgi.begin(); it != cgi.end(); ++it) {
        try {
            parser.Append(it->first, it->second);
        } catch (...) {
        }

        bool hasYandexPrefix = false;
        TString description;
        try {
            (void)NKikimr::NSQS::ValidateMessageAttributeName(it->first, hasYandexPrefix, true);
        } catch (...) {
        }
        try {
            (void)NKikimr::NSQS::ValidateMessageBody(it->second, description);
        } catch (...) {
        }
    }

    if (params.MessageBody) {
        TString description;
        try {
            (void)NKikimr::NSQS::ValidateMessageBody(*params.MessageBody, description);
        } catch (...) {
        }
    }

    for (const auto& [idx, attr] : params.MessageAttributes) {
        bool hasYandexPrefix = false;
        TString description;
        try {
            (void)NKikimr::NSQS::ValidateMessageAttributeName(attr.Name, hasYandexPrefix, true);
        } catch (...) {
        }
        if (attr.StringValue) {
            try {
                (void)NKikimr::NSQS::ValidateMessageBody(*attr.StringValue, description);
            } catch (...) {
            }
        }
        (void)idx;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    const TString method = fdp.ConsumeBool() ? "POST" : "GET";
    const TString path = fdp.ConsumeRandomLengthString(128);
    const TString cgi = fdp.ConsumeRandomLengthString(1024);
    const TString firstLine = TStringBuilder() << method << " /" << path << "?" << cgi << " HTTP/1.1";
    const TString body = fdp.ConsumeRemainingBytesAsString();

    try {
        ExerciseParsedRequest(firstLine);
    } catch (...) {
    }
    try {
        ExerciseParams(cgi);
    } catch (...) {
    }
    try {
        ExerciseParams(body);
    } catch (...) {
    }

    return 0;
}
