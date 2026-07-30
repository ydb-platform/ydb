#pragma once

#include <yql/essentials/utils/json/from.h>
#include <yql/essentials/utils/json/to.h>

#include <util/generic/string.h>

#include <expected>

namespace NLsp {

/// In symbols.
struct TPosition {
    ui64 Line = 0;
    ui64 Character = 0;
};

struct TRange {
    TPosition Start;
    TPosition End;
};

using TDocumentUri = TString;

struct TTextDocumentIdentifier {
    TDocumentUri Uri;
};

using TTextDocumentVersion = i64;

struct TTextDocumentItem {
    TDocumentUri Uri;
    TString LanguageId;
    TTextDocumentVersion Version = 0;
    TString Text;
};

struct TTextDocumentPositionParams {
    TTextDocumentIdentifier TextDocument;
    TPosition Position;
};

} // namespace NLsp

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TPosition, json);
JSON_DECLARE_TO(NLsp::TPosition, value);
JSON_DECLARE_FROM(NLsp::TRange, json);
JSON_DECLARE_FROM(NLsp::TTextDocumentIdentifier, json);
JSON_DECLARE_FROM(NLsp::TTextDocumentItem, json);
JSON_DECLARE_FROM(NLsp::TTextDocumentPositionParams, json);

} // namespace NYql::NJson
