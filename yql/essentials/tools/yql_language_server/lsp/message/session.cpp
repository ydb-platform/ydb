#include "session.h"

namespace NYql::NJson {

JSON_DEFINE_FROM(NLsp::TClientInfo, json) {
    NLsp::TClientInfo x;
    JSON_MOVE_FROM(json, "name", x.Name);
    JSON_MOVE_FROM(json, "version", x.Version);
    return x;
}

JSON_DEFINE_FROM(NLsp::TInitializeParams, json) {
    NLsp::TInitializeParams x;
    JSON_MOVE_FROM(json, "clientInfo", x.ClientInfo);
    JSON_MOVE_FROM(json, "initializationOptions", x.InitializationOptions);
    JSON_MOVE_FROM(json, "capabilities", x.Capabilities);
    return x;
}

JSON_DEFINE_TO(NLsp::ETextDocumentSyncKind, value) {
    return TJsonValue(static_cast<int>(value));
}

JSON_DEFINE_TO(NLsp::TTextDocumentSyncOptions, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "openClose", value.OpenClose);
    SaveTo(json, "change", value.Change);
    return json;
}

JSON_DEFINE_TO(NLsp::TCompletionOptions, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "triggerCharacters", std::move(value.TriggerCharacters));
    return json;
}

JSON_DEFINE_TO(NLsp::TServerCapabilities, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "textDocumentSync", std::move(value.TextDocumentSync));
    SaveTo(json, "completionProvider", std::move(value.CompletionProvider));
    return json;
}

JSON_DEFINE_TO(NLsp::TServerInfo, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "name", std::move(value.Name));
    SaveTo(json, "version", std::move(value.Version));
    return json;
}

JSON_DEFINE_TO(NLsp::TInitializeResult, value) {
    TJsonValue json(JSON_MAP);
    SaveTo(json, "capabilities", std::move(value.Capabilities));
    SaveTo(json, "serverInfo", std::move(value.ServerInfo));
    return json;
}

JSON_DEFINE_FROM(NLsp::TInitializedParams, json) {
    Y_UNUSED(json);
    return NLsp::TInitializedParams();
}

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

JSON_DEFINE_FROM(NLsp::TSetTraceParams, json) {
    NLsp::TSetTraceParams x;
    JSON_MOVE_FROM(json, "value", x.Value);
    return x;
}

} // namespace NYql::NJson
