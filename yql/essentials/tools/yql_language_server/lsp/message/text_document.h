#pragma once

#include <yql/essentials/utils/json/bidirectional.h>
#include <yql/essentials/utils/meta/reflection.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

#include <expected>

namespace NLsp {

/// In symbols.
struct TPosition {
    ui64 Line = 0;
    ui64 Character = 0;

    bool operator==(const TPosition&) const = default;
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

struct TTextEdit {
    TRange Range;
    TString NewText;
};

struct TLocation {
    TDocumentUri Uri;
    TRange Range;
};

} // namespace NLsp

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NLsp::TPosition, (Line)(Character));
YQL_DEFINE_REFLECTING(NLsp::TRange, (Start)(End));
YQL_DEFINE_REFLECTING(NLsp::TTextDocumentIdentifier, (Uri));
YQL_DEFINE_REFLECTING(NLsp::TTextDocumentItem, (Uri)(LanguageId)(Version)(Text));
YQL_DEFINE_REFLECTING(NLsp::TTextDocumentPositionParams, (TextDocument)(Position));
YQL_DEFINE_REFLECTING(NLsp::TTextEdit, (Range)(NewText));
YQL_DEFINE_REFLECTING(NLsp::TLocation, (Uri)(Range));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_BIDIRECTIONAL(NLsp::TPosition);
JSON_DECLARE_BIDIRECTIONAL(NLsp::TRange);
JSON_DECLARE_BIDIRECTIONAL(NLsp::TTextDocumentIdentifier);
JSON_DECLARE_BIDIRECTIONAL(NLsp::TTextDocumentItem);
JSON_DECLARE_BIDIRECTIONAL(NLsp::TTextDocumentPositionParams);
JSON_DECLARE_BIDIRECTIONAL(NLsp::TTextEdit);
JSON_DECLARE_BIDIRECTIONAL(NLsp::TLocation);

} // namespace NYql::NJson

Y_DECLARE_OUT_SPEC(inline, NLsp::TPosition, stream, value) {
    stream << "{" << value.Line << ", " << value.Character << "}";
}
