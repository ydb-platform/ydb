#include "parser_detail.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/misc/parser_helpers.h>

#include <yt/yt/core/yson/token.h>
#include <yt/yt/core/yson/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYPath {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
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

namespace {

void ThrowUnexpectedToken(const TToken& token)
{
    THROW_ERROR_EXCEPTION("Unexpected token %Qv",
        token);
}

TStringBuf ParseAttributes(TStringBuf str, const IAttributeDictionaryPtr& attributes)
{
    {
        size_t index = 0;
        while (index < str.size() && IsSpace(str[index])) {
            ++index;
        }
        if (index == str.size() || str[index] != TokenTypeToChar(NYson::ETokenType::LeftAngle)) {
            return str;
        }
    }

    NYson::TTokenizer tokenizer(str);
    tokenizer.ParseNext();
    if (tokenizer.CurrentToken().GetType() != NYson::ETokenType::LeftAngle) {
        ThrowUnexpectedToken(tokenizer.CurrentToken());
    }

    int depth = 0;
    int attrStartPosition = tokenizer.GetPosition();

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

    int attrEndPosition = tokenizer.GetPosition() - 1;
    YT_ASSERT(attrEndPosition >= attrStartPosition);
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

TStringBuf ParseCluster(TStringBuf str, const IAttributeDictionaryPtr& attributes)
{
    if (str.empty()) {
        return str;
    }

    if (StartsWithRootDesignator(str)) {
        return str;
    }

    auto clusterSeparatorIndex = str.find_first_of(':');
    if (clusterSeparatorIndex == TStringBuf::npos) {
        THROW_ERROR_EXCEPTION("Path %Qv does not start with a valid root-designator",
            str);
    }

    auto clusterName = str.substr(0, clusterSeparatorIndex);
    if (clusterName.empty()) {
        THROW_ERROR_EXCEPTION("Cluster name in path %Qv cannot be empty",
            str);
    }

    auto illegalSymbolIt = std::ranges::find_if_not(clusterName, &IsValidClusterSymbol);
    if (illegalSymbolIt != clusterName.end()) {
        THROW_ERROR_EXCEPTION("Possible cluster name in path %Qv contains illegal symbol %Qv",
            str,
            *illegalSymbolIt);
    }

    auto remainingString = str.substr(clusterSeparatorIndex + 1);
    if (!StartsWithRootDesignator(remainingString)) {
        THROW_ERROR_EXCEPTION("Path %Qv does not start with a valid root-designator",
            str);
    }

    attributes->Set("cluster", clusterName);
    return remainingString;
}

void ParseColumns(NYson::TTokenizer& tokenizer, IAttributeDictionary* attributes)
{
    if (tokenizer.GetCurrentType() != BeginColumnSelectorToken) {
        return;
    }

    std::vector<std::string> columns;

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

// NB: Since our final result while parsing YPath is an attribute dictionary, we intentionally refrain
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

// Used implicitly
[[maybe_unused]] void Serialize(const TYPathLimit& limit, IYsonConsumer* consumer)
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

void AppendAttributes(TStringBuilderBase* builder, const IAttributeDictionary& attributes, EYsonFormat ysonFormat)
{
    TString attrString;
    TStringOutput output(attrString);
    TYsonWriter writer(&output, ysonFormat, EYsonType::MapFragment);

    BuildYsonAttributesFluently(&writer)
        .Items(attributes);

    if (!attrString.empty()) {
        builder->AppendChar(TokenTypeToChar(NYson::ETokenType::LeftAngle));
        builder->AppendString(attrString);
        builder->AppendChar(TokenTypeToChar(NYson::ETokenType::RightAngle));
    }
}

TUnversionedValue TryConvertValue(const TUnversionedValue& from, EValueType dstType)
{
    if (from.Type != NTableClient::EValueType::Int64 && from.Type != NTableClient::EValueType::Uint64) {
        return from;
    }

    auto convert = [&] (auto srcValue, bool isSigned) {
        const ui64 maxInt64 = std::numeric_limits<i64>::max();
        switch (dstType) {
            case EValueType::Uint64:
                if (isSigned && srcValue < 0) {
                    // Casting from negative int64 to uint64 doesn't seem to be a valid thing expected by user,
                    // so we just return without conversion.
                    return from;
                }
                return MakeUnversionedUint64Value(static_cast<ui64>(srcValue));
            case EValueType::Int64:
                if (!isSigned && static_cast<ui64>(srcValue) > maxInt64) {
                    // The unsigned value is too large to fit into int64. We also don't perform the conversion.
                    return from;
                }
                return MakeUnversionedInt64Value(static_cast<i64>(srcValue));
            case EValueType::Double:
                return MakeUnversionedDoubleValue(static_cast<double>(srcValue));
            default:
                return from;
        }
        YT_ABORT();
    };

    switch (from.Type) {
        case NTableClient::EValueType::Int64:
            return convert(from.Data.Int64, true);
        case NTableClient::EValueType::Uint64:
            return convert(from.Data.Uint64, false);
        default:
            return from;
    }
    YT_ABORT();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::pair<TYPath, IAttributeDictionaryPtr> ParseRichYPathImpl(TStringBuf str)
{
    auto attributes = CreateEphemeralAttributes();

    auto strWithoutAttributes = ParseAttributes(str, attributes);
    strWithoutAttributes = ParseCluster(strWithoutAttributes, attributes);
    TTokenizer ypathTokenizer(strWithoutAttributes);

    while (ypathTokenizer.GetType() != ETokenType::EndOfStream && ypathTokenizer.GetType() != ETokenType::Range) {
        ypathTokenizer.Advance();
    }
    TYPath path(ypathTokenizer.GetPrefix());
    auto rangeStr = ypathTokenizer.GetToken();

    if (ypathTokenizer.GetType() == ETokenType::Range) {
        NYson::TTokenizer ysonTokenizer(rangeStr);
        ysonTokenizer.ParseNext();
        ParseColumns(ysonTokenizer, attributes.Get());
        ParseRowRanges(ysonTokenizer, attributes.Get());
        ysonTokenizer.CurrentToken().ExpectType(NYson::ETokenType::EndOfStream);
    }
    return {std::move(path), attributes};
}

TString ConvertToStringImpl(const TYPath& path, const IAttributeDictionary& attributes, EYsonFormat ysonFormat)
{
    TStringBuilder builder;
    AppendAttributes(&builder, attributes, ysonFormat);
    builder.AppendString(path);
    return builder.Flush();
}

//! Return read range corresponding to given map node.
TReadRange RangeNodeToReadRange(
    const NTableClient::TComparator& comparator,
    const IMapNodePtr& rangeNode,
    const TKeyColumnTypes& conversionTypeHints)
{
    auto lowerLimitNode = rangeNode->FindChild("lower_limit");
    auto upperLimitNode = rangeNode->FindChild("upper_limit");
    auto exactNode = rangeNode->FindChild("exact");

    if (exactNode && (lowerLimitNode || upperLimitNode)) {
        THROW_ERROR_EXCEPTION("Exact limit cannot be specified simultaneously with lower or upper limits");
    }

    auto deserializeLimit = [&] (const IMapNodePtr& limitNode, TReadLimit& readLimit, bool isUpper, bool isExact) {
        auto keyNode = limitNode->FindChild("key");
        if (keyNode) {
            limitNode->RemoveChild(keyNode);
        }

        auto keyBoundNode = limitNode->FindChild("key_bound");

        // Check that key bound is not specified in exact clause.
        if (keyBoundNode && isExact) {
            THROW_ERROR_EXCEPTION("Key bound cannot be specified in exact limit, specify lower or upper limit instead");
        }

        // Check that key or key bound do not appear for unsorted tables.
        if ((keyNode || keyBoundNode) && !comparator) {
            THROW_ERROR_EXCEPTION("Cannot use key or key bound in read limit for an unsorted object");
        }

        // NB: For the sake of compatibility, we support specifying both key and key bound in read limit.
        // In this case we consider only key bound and completely ignore key.

        if (keyNode && !keyBoundNode) {
            // Before deserializing, we may need to transform legacy key into key bound.
            auto owningKey = ConvertTo<TUnversionedOwningRow>(keyNode);
            TOwningKeyBound keyBound;

            // Perform type conversion, if required.
            if (!conversionTypeHints.empty()) {
                TUnversionedOwningRowBuilder newOwningKey;
                int typedKeyCount = std::min<int>(owningKey.GetCount(), std::ssize(conversionTypeHints));
                for (int i = 0; i < typedKeyCount; ++i) {
                    newOwningKey.AddValue(TryConvertValue(owningKey[i], conversionTypeHints[i]));
                }
                for (int i = typedKeyCount; i < owningKey.GetCount(); ++i) {
                    newOwningKey.AddValue(owningKey[i]);
                }
                owningKey = newOwningKey.FinishRow();
            }

            bool containsSentinels = std::find_if(
                owningKey.begin(),
                owningKey.end(),
                [&] (const TUnversionedValue& value) {
                    return IsSentinelType(value.Type);
                }) != owningKey.end();

            // For tables with descending sort order we are allowed to impose stricter rules regarding
            // what can be specified as a "key" in a read limit.
            if (comparator.HasDescendingSortOrder()) {
                // First of all, we do not want to state if <min> is less than any value in descending sort order
                // or not. That's why we simply prohibit keys with sentinels when comparator is not totally ascending.
                if (containsSentinels) {
                    THROW_ERROR_EXCEPTION(
                        "Sentinel values are not allowed in read limit key when at least one column "
                        "has descending sort order");
                }
                // Also, we prohibit keys that are longer than comparator.
                if (owningKey.GetCount() > comparator.GetLength()) {
                    THROW_ERROR_EXCEPTION(
                        "Read limit key cannot be longer than schema key prefix when at least one column "
                        "has descending sort order");
                }

                keyBound = TOwningKeyBound::FromRow(owningKey, /* isInclusive */ !isUpper, isUpper);
            } else {
                // Existing code may specify arbitrary garbage as legacy keys and we have no solution other
                // than interpreting it as a key bound using interop method.

                if (isExact && (owningKey.GetCount() > comparator.GetLength() || containsSentinels)) {
                    // NB: There are two tricky cases when read limit is exact:
                    // - (1) if specified key is longer than comparator. Recall that (in old terms)
                    // there may be no keys between (foo, bar) and (foo, bar, <max>) in a table with single
                    // key column.
                    // - (2) if specified key contains sentinels.
                    //
                    // For non-totally ascending comparators we throw error right in deserializeLimit in
                    // both cases.
                    //
                    // For totally ascending comparators we can't throw error and we must somehow
                    // represent an empty range because there may be already existing code that specifies
                    // such limits and expects the empty result, but not an error.
                    //
                    // That's why we replace such key with (empty) key bound >[] to make sure this range
                    // will not contain any key. Also, note that simply skipping such range is incorrect
                    // as it would break range indices (kudos to ifsmirnov@ for this nice observation).
                    keyBound = TOwningKeyBound::MakeEmpty(/* isUpper */ false);
                } else {
                    keyBound = KeyBoundFromLegacyRow(owningKey, isUpper, comparator.GetLength());
                }
            }
            limitNode->AddChild("key_bound", ConvertToNode(keyBound));
        }

        // We got rid of "key" map node key, so we may just use Deserialize which simply constructs
        // read limit from node representation.
        Deserialize(readLimit, limitNode);

        // Note that "key_bound" is either specified explicitly or obtained from "key" at this point.
        // For the former case, we did not yet perform validation of key bound sanity, we will do that
        // later. For the latter case the check is redundant, but let's perform it anyway.

        // If key bound was explicitly specified (i.e. we did not transform legacy key into key bound),
        // we still need to check that key bound length is no more than comparator length.
        if (readLimit.KeyBound() && readLimit.KeyBound().Prefix.GetCount() > comparator.GetLength()) {
            THROW_ERROR_EXCEPTION("Key bound length must not exceed schema key prefix length");
        }

        // Check that key bound is of correct direction.
        if (readLimit.KeyBound() && readLimit.KeyBound().IsUpper != isUpper) {
            // Key bound for exact limit is formed only by us and it has always a correct (lower) direction.
            YT_VERIFY(!isExact);
            THROW_ERROR_EXCEPTION(
                "Key bound for %v limit has wrong relation type %Qv",
                isUpper ? "upper" : "lower",
                readLimit.KeyBound().GetRelation());
        }

        // Validate that exact limit contains at most one independent selector.
        if (exactNode && (readLimit.HasIndependentSelectors() || readLimit.IsTrivial())) {
            THROW_ERROR_EXCEPTION("Exact read limit must have exactly one independent selector specified");
        }
    };

    TReadRange result;

    if (lowerLimitNode) {
        deserializeLimit(lowerLimitNode->AsMap(), result.LowerLimit(), /* isUpper */ false, /* isExact */ false);
    }
    if (upperLimitNode) {
        deserializeLimit(upperLimitNode->AsMap(), result.UpperLimit(), /* isUpper */ true, /* isExact */ false);
    }
    if (exactNode) {
        // Exact limit is transformed into pair of lower and upper limit. First, deserialize exact limit
        // as if it was lower limit.
        deserializeLimit(exactNode->AsMap(), result.LowerLimit(), /* isUpper */ false, /* isExact */ true);

        // Now copy lower limit to upper limit and either increment single integer selector, or
        // invert key bound selector.
        result.UpperLimit() = result.LowerLimit().ToExactUpperCounterpart();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
