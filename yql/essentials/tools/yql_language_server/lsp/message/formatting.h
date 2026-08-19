#pragma once

#include "text_document.h"

#include <yql/essentials/utils/json/from.h>
#include <yql/essentials/utils/meta/reflection.h>

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

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NLsp::TFormattingOptions, (TabSize)(InsertSpaces));
YQL_DEFINE_REFLECTING(NLsp::TDocumentFormattingParams, (TextDocument)(Options));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TFormattingOptions, json);
JSON_DECLARE_FROM(NLsp::TDocumentFormattingParams, json);

} // namespace NYql::NJson
