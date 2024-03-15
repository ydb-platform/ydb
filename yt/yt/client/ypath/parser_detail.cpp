#include "parser_detail.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/misc/parser_helpers.h>

#include <yt/yt/core/yson/token.h>
#include <yt/yt/core/yson/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYPath {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

const NYson::ETokenType BeginColumnSelectorToken = NYson::ETokenType::LeftBrace;
const NYson::ETokenType EndColumnSelectorToken = NYson::ETokenType::RightBrace;
const NYson::ETokenType ColumnSeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType BeginRowSelectorToken = NYson::ETokenType::LeftBracket;
const NYson::ETokenType EndRowSelectorToken = NYson::ETokenType::RightBracket;
const NYson::ETokenType RowIndexMarkerToken = NYson::ETokenType::Hash;
const NYson::ETokenType BeginTupleToken = NYson::ETokenType::LeftParenthesis;
const NYson::ETokenType EndTupleToken = NYson::ETokenType::RightParenthesis;
const NYson::ETokenType KeySeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType RangeToken = NYson::ETokenType::Colon;
const NYson::ETokenType RangeSeparatorToken = NYson::ETokenType::Comma;

////////////////////////////////////////////////////////////////////////////////

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Unexpected token %Qv",
        token);
}

TString ParseAttributes(const TString& str, const IAttributeDictionaryPtr& attributes)
{
    int spaceCount = 0;
    {
        size_t index = 0;
        while (index < str.size() && IsSpace(str[index])) {
            ++index;
        }
        if (index == str.size() || str[index] != TokenTypeToChar(NYson::ETokenType::LeftAngle)) {
            return str;
        }
        spaceCount = index;
    }

    NYson::TTokenizer tokenizer(TStringBuf(str).SubStr(spaceCount));
    tokenizer.ParseNext();
    if (tokenizer.CurrentToken().GetType() != NYson::ETokenType::LeftAngle) {
        ThrowUnexpectedToken(tokenizer.CurrentToken());
    }

    int depth = 0;
    int attrStartPosition = spaceCount + 1;

    while (true) {
        switch (tokenizer.CurrentToken().GetType()) {
            case NYson::ETokenType::LeftAngle:
                ++depth;
                break;
            case NYson::ETokenType::RightAngle:
                --depth;
                break;
            default:
                break;
        }

        if (depth == 0) {
            break;
        }

        if (!tokenizer.ParseNext()) {
            THROW_ERROR_EXCEPTION("Unmatched '<' in YPath");
        }
    }

    int attrEndPosition = spaceCount + tokenizer.GetPosition() - 1;
    int pathStartPosition = attrEndPosition + 1;

    TYsonString attrYson(
        str.substr(attrStartPosition, attrEndPosition - attrStartPosition),
        NYson::EYsonType::MapFragment);
    attributes->MergeFrom(*ConvertToAttributes(attrYson));

    return TrimLeadingWhitespaces(str.substr(pathStartPosition));
}

bool IsValidClusterSymbol(char c)
{
    return IsAsciiAlnum(c) || c == '-' || c == '_';
}

bool IsRootDesignator(char c)
{
    return c == '/' || c == '#';
}

bool StartsWithRootDesignator(TStringBuf str)
{
    size_t nonSpaceIndex = str.find_first_not_of(' ');
    if (nonSpaceIndex != TStringBuf::npos && !IsRootDesignator(str[nonSpaceIndex])) {
        return false;
    }

    return true;
}

TString ParseCluster(TString str, const IAttributeDictionaryPtr& attributes)
{
    if (str.empty()) {
        return str;
    }

    if (StartsWithRootDesignator(str)) {
        return str;
    }

    auto clusterSeparatorIndex = str.find_first_of(':');
    if (clusterSeparatorIndex == TString::npos) {
        THROW_ERROR_EXCEPTION(
            "Path %Qv does not start with a valid root-designator, cluster://path short-form assumed; "
            "no \':\' separator symbol found to parse cluster",
            str);
    }

    const auto clusterName = str.substr(0, clusterSeparatorIndex);

    if (clusterName.empty()) {
        THROW_ERROR_EXCEPTION(
            "Path %Qv does not start with a valid root-designator, cluster://path short-form assumed; "
            "cluster name cannot be empty",
            str);
    }

    auto illegalSymbolIt = std::find_if_not(clusterName.begin(), clusterName.end(), &IsValidClusterSymbol);
    if (illegalSymbolIt != clusterName.end()) {
        THROW_ERROR_EXCEPTION(
            "Path %Qv does not start with a valid root-designator, cluster://path short-form assumed; "
            "cluster name contains illegal symbol %Qv",
            str,
            *illegalSymbolIt);
    }

    auto remainingString = str.substr(clusterSeparatorIndex + 1);

    if (!StartsWithRootDesignator(remainingString)) {
        THROW_ERROR_EXCEPTION(
            "Path %Qv does not start with a valid root-designator, cluster://path short-form assumed; "
            "path %Qv after cluster-separator does not start with a valid root-designator",
            str,
            remainingString);
    }

    attributes->Set("cluster", clusterName);
    return remainingString;
}

void ParseColumns(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() != BeginColumnSelectorToken) {
        return;
    }

    std::vector<TString> columns;

    tokenizer.ParseNext();
    while (tokenizer.GetCurrentType() != EndColumnSelectorToken) {
        TString begin;
        switch (tokenizer.GetCurrentType()) {
            case NYson::ETokenType::String:
                begin.assign(tokenizer.CurrentToken().GetStringValue());
                tokenizer.ParseNext();
                break;
            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                YT_ABORT();
        }

        columns.push_back(begin);

        switch (tokenizer.GetCurrentType()) {
            case ColumnSeparatorToken:
                tokenizer.ParseNext();
                break;
            case EndColumnSelectorToken:
                break;
            default:
                ThrowUnexpectedToken(tokenizer.CurrentToken());
                YT_ABORT();
        }
    }
    tokenizer.ParseNext();

    attributes->Set("columns", ConvertToYsonString(columns));
}

void ParseKeyPart(
    NYson::TTokenizer& tokenizer,
    TUnversionedOwningRowBuilder* rowBuilder)
{
    // We don't fill id here, because key part columns are well known.
    // Also we don't have a name table for them :)
    TUnversionedValue value;

    switch (tokenizer.GetCurrentType()) {
        case NYson::ETokenType::String: {
            auto str = tokenizer.CurrentToken().GetStringValue();
            value = MakeUnversionedStringValue(str);
            break;
        }

        case NYson::ETokenType::Int64: {
            value = MakeUnversionedInt64Value(tokenizer.CurrentToken().GetInt64Value());
            break;
        }

        case NYson::ETokenType::Uint64: {
            value = MakeUnversionedUint64Value(tokenizer.CurrentToken().GetUint64Value());
            break;
        }

        case NYson::ETokenType::Double: {
            value = MakeUnversionedDoubleValue(tokenizer.CurrentToken().GetDoubleValue());
            break;
        }

        case NYson::ETokenType::Boolean: {
            value = MakeUnversionedBooleanValue(tokenizer.CurrentToken().GetBooleanValue());
            break;
        }

        case NYson::ETokenType::Hash: {
            value = MakeUnversionedSentinelValue(EValueType::Null);
            break;
        }

        default:
            ThrowUnexpectedToken(tokenizer.CurrentToken());
            break;
    }
    rowBuilder->AddValue(value);
    tokenizer.ParseNext();
}

// NB: since our final result while parsing YPath is an attribute dictionary, we intentionally refrain
// from using TReadLimit (or even TLegacyReadLimit) as an intermediate structure here. YPath short
// form is parsed using merely NYTree primitives and unversioned rows.

//! This enum and the following structure define one of the following row limit specifications:
//! - (None): empty limit;
//! - (RowIndex): row index;
//! - (Key): row (note that in short form this key may not include min/max sentinels).
DEFINE_ENUM(EYPathLimitKind,
    (None)
    (RowIndex)
    (Key)
);

//! This is an intermediate representation used only while parsing rich YPath short form.
struct TYPathLimit
{
    EYPathLimitKind Kind = EYPathLimitKind::None;
    //! Actual row for (Key) and null row for (RowIndex) and (None).
    TUnversionedOwningRow Row;
    //! Actual row index for (RowIndex) and std::nullopt for (Key), and (None).
    std::optional<i64> RowIndex;
};

void Serialize(const TYPathLimit& limit, IYsonConsumer* consumer)
{
    switch (limit.Kind) {
        case EYPathLimitKind::RowIndex:
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("row_index").Value(limit.RowIndex)
                .EndMap();
            return;
        case EYPathLimitKind::Key:
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("key").Value(limit.Row)
                .EndMap();
            return;
        default:
            // None is not handled by this function.
            YT_ABORT();
    }
}

void ParseRowLimit(
    NYson::TTokenizer& tokenizer,
    std::vector<NYson::ETokenType> separators,
    TYPathLimit* limit)
{
    if (std::find(separators.begin(), separators.end(), tokenizer.GetCurrentType()) != separators.end()) {
        return;
    }

    TUnversionedOwningRowBuilder rowBuilder;
    bool hasKeyLimit = false;
    switch (tokenizer.GetCurrentType()) {
        case RowIndexMarkerToken:
            tokenizer.ParseNext();
            limit->RowIndex = tokenizer.CurrentToken().GetInt64Value();
            limit->Kind = EYPathLimitKind::RowIndex;
            tokenizer.ParseNext();
            break;

        case BeginTupleToken:
            tokenizer.ParseNext();
            hasKeyLimit = true;
            while (tokenizer.GetCurrentType() != EndTupleToken) {
                ParseKeyPart(tokenizer, &rowBuilder);
                switch (tokenizer.GetCurrentType()) {
                    case KeySeparatorToken:
                        tokenizer.ParseNext();
                        break;
                    case EndTupleToken:
                        break;
                    default:
                        ThrowUnexpectedToken(tokenizer.CurrentToken());
                        YT_ABORT();
                }
            }
            tokenizer.ParseNext();
            break;

        default:
            ParseKeyPart(tokenizer, &rowBuilder);
            hasKeyLimit = true;
            break;
    }

    if (hasKeyLimit) {
        auto key = rowBuilder.FinishRow();
        limit->Row = key;
        limit->Kind = EYPathLimitKind::Key;
    }

    tokenizer.CurrentToken().ExpectTypes(separators);
}

void ParseRowRanges(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() == BeginRowSelectorToken) {
        tokenizer.ParseNext();

        std::vector<INodePtr> ranges;

        bool finished = false;
        while (!finished) {
            auto rangeNode = GetEphemeralNodeFactory()->CreateMap();

            TYPathLimit lowerLimit;
            TYPathLimit upperLimit;
            bool isTwoSideRange = false;
            ParseRowLimit(
                tokenizer,
                {RangeToken, RangeSeparatorToken, EndRowSelectorToken},
                &lowerLimit);
            if (tokenizer.GetCurrentType() == RangeToken) {
                isTwoSideRange = true;
                tokenizer.ParseNext();

                ParseRowLimit(
                    tokenizer,
                    {RangeSeparatorToken, EndRowSelectorToken},
                    &upperLimit);
            }

            if (isTwoSideRange) {
                if (lowerLimit.Kind != EYPathLimitKind::None) {
                    rangeNode->AddChild("lower_limit", ConvertToNode(lowerLimit));
                }
                if (upperLimit.Kind != EYPathLimitKind::None) {
                    rangeNode->AddChild("upper_limit", ConvertToNode(upperLimit));
                }
            } else {
                if (lowerLimit.Kind != EYPathLimitKind::None) {
                    rangeNode->AddChild("exact", ConvertToNode(lowerLimit));
                } else {
                    // Universal limit is represented by an empty map, so do nothing.
                }
            }

            if (tokenizer.CurrentToken().GetType() == EndRowSelectorToken) {
                finished = true;
            }
            tokenizer.ParseNext();

            ranges.emplace_back(std::move(rangeNode));
        }

        attributes->Set("ranges", ConvertToYsonString(ranges));
    }
}

////////////////////////////////////////////////////////////////////////////////

TRichYPath ParseRichYPathImpl(const TString& str)
{
    auto attributes = CreateEphemeralAttributes();

    auto strWithoutAttributes = ParseAttributes(str, attributes);
    strWithoutAttributes = ParseCluster(std::move(strWithoutAttributes), attributes);
    TTokenizer ypathTokenizer(strWithoutAttributes);

    while (ypathTokenizer.GetType() != ETokenType::EndOfStream && ypathTokenizer.GetType() != ETokenType::Range) {
        ypathTokenizer.Advance();
    }
    auto path = TYPath(ypathTokenizer.GetPrefix());
    auto rangeStr = ypathTokenizer.GetToken();

    if (ypathTokenizer.GetType() == ETokenType::Range) {
        NYson::TTokenizer ysonTokenizer(rangeStr);
        ysonTokenizer.ParseNext();
        ParseColumns(ysonTokenizer, attributes.Get());
        ParseRowRanges(ysonTokenizer, attributes.Get());
        ysonTokenizer.CurrentToken().ExpectType(NYson::ETokenType::EndOfStream);
    }
    return TRichYPath(path, *attributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
