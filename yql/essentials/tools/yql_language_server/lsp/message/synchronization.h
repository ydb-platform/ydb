#pragma once

#include "text_document.h"

#include <yql/essentials/utils/json/from.h>

namespace NLsp {

struct TVersionedTextDocumentIdentifier: TTextDocumentIdentifier {
    TTextDocumentVersion Version = 0;
};

struct TDidOpenTextDocumentParams {
    TTextDocumentItem TextDocument;
};

struct TTextDocumentContentChangeEvent {
    TMaybe<TRange> Range;
    TString Text;

    bool IsIncremental() const;
};

struct TDidChangeTextDocumentParams {
    TVersionedTextDocumentIdentifier TextDocument;
    TVector<TTextDocumentContentChangeEvent> ContentChanges;
};

struct TDidCloseTextDocumentParams {
    TTextDocumentIdentifier TextDocument;
};

} // namespace NLsp

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TVersionedTextDocumentIdentifier, json);
JSON_DECLARE_FROM(NLsp::TDidOpenTextDocumentParams, json);
JSON_DECLARE_FROM(NLsp::TTextDocumentContentChangeEvent, json);
JSON_DECLARE_FROM(NLsp::TDidChangeTextDocumentParams, json);
JSON_DECLARE_FROM(NLsp::TDidCloseTextDocumentParams, json);

} // namespace NYql::NJson
