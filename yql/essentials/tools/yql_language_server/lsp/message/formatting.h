#pragma once

#include "text_document.h"

namespace NLsp {

struct TFormattingOptions {
    ui64 TabSize = 4;
    bool InsertSpaces = true;
};

struct TDocumentFormattingParams {
    TTextDocumentIdentifier TextDocument;
    TFormattingOptions Options;
};

} // namespace NLsp

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TFormattingOptions, json);
JSON_DECLARE_FROM(NLsp::TDocumentFormattingParams, json);

} // namespace NYql::NJson
