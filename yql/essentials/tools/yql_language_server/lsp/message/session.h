#pragma once

#include <yql/essentials/utils/json/from.h>
#include <yql/essentials/utils/json/to.h>
#include <yql/essentials/utils/meta/reflection.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NLsp {

struct TClientInfo {
    TString Name;
    TMaybe<TString> Version;
};

struct TInitializeParams {
    TMaybe<TClientInfo> ClientInfo;
    TMaybe<NJson::TJsonValue> InitializationOptions;
    NJson::TJsonValue Capabilities;
};

enum class ETextDocumentSyncKind {
    None,
    Full,
    Incremental,
};

struct TTextDocumentSyncOptions {
    TMaybe<bool> OpenClose;
    TMaybe<ETextDocumentSyncKind> Change;
};

struct TCompletionOptions {
    TMaybe<TVector<TString>> TriggerCharacters;
};

struct TDocumentFormattingOptions {
};

struct TDiagnosticOptions {
    TMaybe<TString> Identifier;
    bool InterFileDependencies = false;
    bool WorkspaceDiagnostics = false;
};

struct TServerCapabilities {
    TMaybe<TTextDocumentSyncOptions> TextDocumentSync;
    TMaybe<TCompletionOptions> CompletionProvider;
    TMaybe<TDocumentFormattingOptions> DocumentFormattingProvider;
    TMaybe<TDiagnosticOptions> DiagnosticProvider;
};

struct TServerInfo {
    TString Name;
    TMaybe<TString> Version;
};

struct TInitializeResult {
    TServerCapabilities Capabilities;
    TMaybe<TServerInfo> ServerInfo;
};

struct TInitializedParams {
};

enum class ETraceValue {
    Off,
    Messages,
    Verbose,
};

struct TSetTraceParams {
    ETraceValue Value;
};

} // namespace NLsp

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NLsp::TClientInfo, (Name)(Version));
YQL_DEFINE_REFLECTING(NLsp::TInitializeParams, (ClientInfo)(InitializationOptions)(Capabilities));
YQL_DEFINE_REFLECTING(NLsp::TTextDocumentSyncOptions, (OpenClose)(Change));
YQL_DEFINE_REFLECTING(NLsp::TCompletionOptions, (TriggerCharacters));
YQL_DEFINE_REFLECTING(NLsp::TDocumentFormattingOptions, );
YQL_DEFINE_REFLECTING(NLsp::TDiagnosticOptions, (Identifier)(InterFileDependencies)(WorkspaceDiagnostics));
YQL_DEFINE_REFLECTING(NLsp::TServerCapabilities, (TextDocumentSync)(CompletionProvider)(DocumentFormattingProvider)(DiagnosticProvider));
YQL_DEFINE_REFLECTING(NLsp::TServerInfo, (Name)(Version));
YQL_DEFINE_REFLECTING(NLsp::TInitializeResult, (Capabilities)(ServerInfo));
YQL_DEFINE_REFLECTING(NLsp::TInitializedParams, );
YQL_DEFINE_REFLECTING(NLsp::TSetTraceParams, (Value));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TClientInfo, json);
JSON_DECLARE_FROM(NLsp::TInitializeParams, json);
JSON_DECLARE_TO(NLsp::ETextDocumentSyncKind, value);
JSON_DECLARE_TO(NLsp::TTextDocumentSyncOptions, value);
JSON_DECLARE_TO(NLsp::TCompletionOptions, value);
JSON_DECLARE_TO(NLsp::TDocumentFormattingOptions, value);
JSON_DECLARE_TO(NLsp::TDiagnosticOptions, value);
JSON_DECLARE_TO(NLsp::TServerCapabilities, value);
JSON_DECLARE_TO(NLsp::TServerInfo, value);
JSON_DECLARE_TO(NLsp::TInitializeResult, value);
JSON_DECLARE_FROM(NLsp::TInitializedParams, json);
JSON_DECLARE_FROM(NLsp::ETraceValue, json);
JSON_DECLARE_FROM(NLsp::TSetTraceParams, json);

} // namespace NYql::NJson
