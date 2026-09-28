#include "session.h"

#include <yql/essentials/utils/json/reflection.h>

namespace NYql::NJson {

YQL_DERIVE_JSON_FROM(NLsp::TClientInfo);

YQL_DERIVE_JSON_FROM(NLsp::TInitializeParams);

JSON_DEFINE_TO(NLsp::ETextDocumentSyncKind, value) {
    return TJsonValue(static_cast<int>(value));
}

YQL_DERIVE_JSON_TO(NLsp::TTextDocumentSyncOptions);

YQL_DERIVE_JSON_TO(NLsp::TCompletionOptions);

YQL_DERIVE_JSON_TO(NLsp::TDocumentFormattingOptions);

YQL_DERIVE_JSON_TO(NLsp::TDiagnosticOptions);

YQL_DERIVE_JSON_TO(NLsp::TServerCapabilities);

YQL_DERIVE_JSON_TO(NLsp::TServerInfo);

YQL_DERIVE_JSON_TO(NLsp::TInitializeResult);

YQL_DERIVE_JSON_FROM(NLsp::TInitializedParams);

JSON_DEFINE_FROM(NLsp::ETraceValue, json) {
    if (!json.IsString()) {
        return UnexpectedField("trace value", "must be a string");
    }

    const TString& value = json.GetStringSafe();
    if (value == "off") {
        return NLsp::ETraceValue::Off;
    } else if (value == "messages") {
        return NLsp::ETraceValue::Messages;
    } else if (value == "verbose") {
        return NLsp::ETraceValue::Verbose;
    } else {
        return UnexpectedField("trace value", TString::Join("unknown ", value));
    }
}

YQL_DERIVE_JSON_FROM(NLsp::TSetTraceParams);

} // namespace NYql::NJson
