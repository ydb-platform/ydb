#include "yql_s3_actors_util.h"

#include <util/string/builder.h>

#ifdef THROW
#undef THROW
#endif
#include <library/cpp/xml/document/xml-document.h>

namespace NYql::NDq {

TString ParseS3ErrorResponse(long httpCode, const TString& response) {
    TStringBuilder str;
    str << "HTTP response code: " << httpCode ;
    try {
        const NXml::TDocument xml(response, NXml::TDocument::String);
        if (const auto& root = xml.Root(); root.Name() == "Error") {
            const auto& code = root.Node("Code", true).Value<TString>();
            const auto& message = root.Node("Message", true).Value<TString>();
            str << ", " << message << ", error code: " << code;
        } else {
            str << Endl << response;
        }
    }
    catch (std::exception&) {
        str << Endl << response;
    }

    return str;
}

}
