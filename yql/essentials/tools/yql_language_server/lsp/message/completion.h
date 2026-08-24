#pragma once

#include "text_document.h"

#include <yql/essentials/utils/json/from.h>
#include <yql/essentials/utils/json/to.h>
#include <yql/essentials/utils/meta/reflection.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NLsp {

struct TCompletionParams: TTextDocumentPositionParams {
};

enum class ECompletionItemKind {
    Text,
    Method,
    Function,
    Constructor,
    Field,
    Variable,
    Class,
    Interface,
    Module,
    Property,
    Unit,
    Value,
    Enum,
    Keyword,
    Snippet,
    Color,
    File,
    Reference,
    Folder,
    EnumMember,
    Constant,
    Struct,
    Event,
    Operator,
    TypeParameter,
};

enum class EMarkupKind {
    PlainText /* "plaintext" */,
    Markdown /* "markdown" */,
};

struct TMarkupContent {
    EMarkupKind Kind;
    TString Value;
};

enum class EInsertTextFormat {
    PlainText,
    Snippet,
};

struct TCompletionItem {
    TString Label;
    ECompletionItemKind Kind;
    TMaybe<TString> Detail;
    TMaybe<TMarkupContent> Documentation;
    TMaybe<TString> SortText;
    TMaybe<TString> FilterText;
    TMaybe<TString> InsertText;
    TMaybe<EInsertTextFormat> InsertTextFormat;
};

struct TCompletionList {
    bool IsIncomplete = false;
    TVector<TCompletionItem> Items;
};

} // namespace NLsp

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NLsp::TMarkupContent, (Kind)(Value));
YQL_DEFINE_REFLECTING(NLsp::TCompletionItem, (Label)(Kind)(Detail)(Documentation)(SortText)(FilterText)(InsertText)(InsertTextFormat));
YQL_DEFINE_REFLECTING(NLsp::TCompletionList, (IsIncomplete)(Items));

} // namespace NYql::NReflection

namespace NYql::NJson {

JSON_DECLARE_FROM(NLsp::TCompletionParams, json);
JSON_DECLARE_TO(NLsp::ECompletionItemKind, value);
JSON_DECLARE_TO(NLsp::EMarkupKind, value);
JSON_DECLARE_TO(NLsp::TMarkupContent, value);
JSON_DECLARE_TO(NLsp::EInsertTextFormat, value);
JSON_DECLARE_TO(NLsp::TCompletionItem, value);
JSON_DECLARE_TO(NLsp::TCompletionList, value);

} // namespace NYql::NJson
