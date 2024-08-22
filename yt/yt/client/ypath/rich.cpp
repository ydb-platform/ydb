#include "rich.h"

#include "parser_detail.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_io_options.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NYPath {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

TRichYPath::TRichYPath()
{ }

TRichYPath::TRichYPath(const TRichYPath& other)
    : Path_(other.Path_)
    , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
{ }

TRichYPath::TRichYPath(const char* path)
    : Path_(path)
{
    *this = Normalize();
}

TRichYPath::TRichYPath(const TYPath& path)
    : Path_(path)
{
    *this = Normalize();
}

TRichYPath::TRichYPath(TRichYPath&& other)
    : Path_(std::move(other.Path_))
    , Attributes_(std::move(other.Attributes_))
{ }

TRichYPath::TRichYPath(const TYPath& path, const IAttributeDictionary& attributes)
    : Path_(path)
    , Attributes_(attributes.Clone())
{ }

const TYPath& TRichYPath::GetPath() const
{
    return Path_;
}

void TRichYPath::SetPath(const TYPath& path)
{
    Path_ = path;
}

const IAttributeDictionary& TRichYPath::Attributes() const
{
    return Attributes_ ? *Attributes_ : EmptyAttributes();
}

IAttributeDictionary& TRichYPath::Attributes()
{
    if (!Attributes_) {
        Attributes_ = CreateEphemeralAttributes();
    }
    return *Attributes_;
}

TRichYPath& TRichYPath::operator = (const TRichYPath& other)
{
    if (this != &other) {
        Path_ = other.Path_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

bool operator== (const TRichYPath& lhs, const TRichYPath& rhs)
{
    return lhs.GetPath() == rhs.GetPath() && lhs.Attributes() == rhs.Attributes();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

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

template <class TFunc>
auto RunAttributeAccessor(const TRichYPath& path, const TString& key, TFunc accessor) -> decltype(accessor())
{
    try {
        return accessor();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv of rich YPath %v",
            key,
            path.GetPath()) << ex;
    }
}

template <class T>
T GetAttribute(const TRichYPath& path, const TString& key, const T& defaultValue)
{
    return RunAttributeAccessor(path, key, [&] {
        return path.Attributes().Get(key, defaultValue);
    });
}

template <class T>
typename TOptionalTraits<T>::TOptional FindAttribute(const TRichYPath& path, const TString& key)
{
    return RunAttributeAccessor(path, key, [&] {
        return path.Attributes().Find<T>(key);
    });
}

TYsonString FindAttributeYson(const TRichYPath& path, const TString& key)
{
    return RunAttributeAccessor(path, key, [&] {
        return path.Attributes().FindYson(key);
    });
}

} // namespace

TRichYPath TRichYPath::Parse(const TString& str)
{
    return ParseRichYPathImpl(str);
}

TRichYPath TRichYPath::Normalize() const
{
    auto parsed = TRichYPath::Parse(Path_);
    parsed.Attributes().MergeFrom(Attributes());
    return parsed;
}

void TRichYPath::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Path_);
    Save(context, Attributes_);
}

void TRichYPath::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    Load(context, Path_);
    Load(context, Attributes_);
}

bool TRichYPath::GetAppend(bool defaultValue) const
{
    return GetAttribute(*this, "append", defaultValue);
}

void TRichYPath::SetAppend(bool value)
{
    Attributes().Set("append", value);
}

bool TRichYPath::GetTeleport() const
{
    return GetAttribute(*this, "teleport", false);
}

bool TRichYPath::GetPrimary() const
{
    return GetAttribute(*this, "primary", false);
}

bool TRichYPath::GetForeign() const
{
    return GetAttribute(*this, "foreign", false);
}

void TRichYPath::SetForeign(bool value)
{
    Attributes().Set("foreign", value);
}

bool TRichYPath::GetReadViaExecNode() const
{
    return GetAttribute(*this, "read_via_exec_node", false);
}

void TRichYPath::SetReadViaExecNode(bool value)
{
    Attributes().Set("read_via_exec_node", value);
}

std::optional<std::vector<TString>> TRichYPath::GetColumns() const
{
    if (Attributes().Contains("channel")) {
        THROW_ERROR_EXCEPTION("Deprecated attribute \"channel\" in YPath");
    }
    return FindAttribute<std::vector<TString>>(*this, "columns");
}

void TRichYPath::SetColumns(const std::vector<TString>& columns)
{
    Attributes().Set("columns", columns);
}

std::vector<NChunkClient::TLegacyReadRange> TRichYPath::GetRanges() const
{
    // COMPAT(ignat): top-level "lower_limit" and "upper_limit" are processed for compatibility.
    auto optionalLowerLimit = FindAttribute<TLegacyReadLimit>(*this, "lower_limit");
    auto optionalUpperLimit = FindAttribute<TLegacyReadLimit>(*this, "upper_limit");
    auto optionalRanges = FindAttribute<std::vector<TLegacyReadRange>>(*this, "ranges");

    if (optionalLowerLimit || optionalUpperLimit) {
        if (optionalRanges) {
            THROW_ERROR_EXCEPTION("YPath cannot be annotated with both multiple (\"ranges\" attribute) "
                "and single (\"lower_limit\" or \"upper_limit\" attributes) ranges");
        }
        return std::vector<TLegacyReadRange>({
            TLegacyReadRange(
                optionalLowerLimit.value_or(TLegacyReadLimit()),
                optionalUpperLimit.value_or(TLegacyReadLimit())),
            });
    } else {
        return optionalRanges.value_or(std::vector<TLegacyReadRange>({TLegacyReadRange()}));
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

//! Return read range corresponding to given map node.
NChunkClient::TReadRange RangeNodeToReadRange(
    const NTableClient::TComparator& comparator,
    const IMapNodePtr& rangeNode,
    const NTableClient::TKeyColumnTypes& conversionTypeHints)
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

        // NB: for the sake of compatibility, we support specifying both key and key bound in read limit.
        // In this case we consider only key bound and completely ignore key.

        if (keyNode && !keyBoundNode) {
            // Before deserializing, we may need to transform legacy key into key bound.
            auto owningKey = ConvertTo<TUnversionedOwningRow>(keyNode);
            TOwningKeyBound keyBound;

            // Perform type conversion, if required.
            if (!conversionTypeHints.empty()) {
                TUnversionedOwningRowBuilder newOwningKey;
                const int typedKeyCount = std::min(owningKey.GetCount(), static_cast<int>(conversionTypeHints.size()));
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
                    // NB: there are two tricky cases when read limit is exact:
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

std::vector<NChunkClient::TReadRange> TRichYPath::GetNewRanges(
    const NTableClient::TComparator& comparator,
    const NTableClient::TKeyColumnTypes& conversionTypeHints) const
{
    // TODO(max42): YT-14242. Top-level "lower_limit" and "upper_limit" are processed for compatibility.
    // But we should deprecate this one day.
    auto optionalRangeNodes = FindAttribute<std::vector<IMapNodePtr>>(*this, "ranges");
    auto optionalLowerLimitNode = FindAttribute<IMapNodePtr>(*this, "lower_limit");
    auto optionalUpperLimitNode = FindAttribute<IMapNodePtr>(*this, "upper_limit");

    if (optionalLowerLimitNode || optionalUpperLimitNode) {
        if (optionalRangeNodes) {
            THROW_ERROR_EXCEPTION("YPath cannot be annotated with both multiple (\"ranges\" attribute) "
                "and single (\"lower_limit\" or \"upper_limit\" attributes) ranges");
        }
        THashMap<TString, IMapNodePtr> rangeNode;
        if (optionalLowerLimitNode) {
            rangeNode["lower_limit"] = optionalLowerLimitNode;
        }
        if (optionalUpperLimitNode) {
            rangeNode["upper_limit"] = optionalUpperLimitNode;
        }
        optionalRangeNodes = {ConvertToNode(rangeNode)->AsMap()};
    }

    if (!optionalRangeNodes) {
        return {TReadRange()};
    }

    std::vector<NChunkClient::TReadRange> readRanges;

    for (const auto& rangeNode : *optionalRangeNodes) {
        // For the sake of unchanged range node in error message.
        auto rangeNodeCopy = ConvertToNode(rangeNode);
        try {
            readRanges.emplace_back(RangeNodeToReadRange(comparator, rangeNode, conversionTypeHints));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NYPath::EErrorCode::InvalidReadRange, "Invalid read range")
                << TErrorAttribute("range", rangeNodeCopy)
                << ex;
        }
    }

    return readRanges;
}

void TRichYPath::SetRanges(const std::vector<NChunkClient::TReadRange>& ranges)
{
    Attributes().Set("ranges", ranges);
    // COMPAT(ignat)
    Attributes().Remove("lower_limit");
    Attributes().Remove("upper_limit");
}

bool TRichYPath::HasNontrivialRanges() const
{
    auto optionalLowerLimit = FindAttribute<TLegacyReadLimit>(*this, "lower_limit");
    auto optionalUpperLimit = FindAttribute<TLegacyReadLimit>(*this, "upper_limit");
    auto optionalRanges = FindAttribute<std::vector<TLegacyReadRange>>(*this, "ranges");

    return optionalLowerLimit || optionalUpperLimit || optionalRanges;
}

std::optional<TString> TRichYPath::GetFileName() const
{
    return FindAttribute<TString>(*this, "file_name");
}

std::optional<bool> TRichYPath::GetExecutable() const
{
    return FindAttribute<bool>(*this, "executable");
}

TYsonString TRichYPath::GetFormat() const
{
    return FindAttributeYson(*this, "format");
}

TTableSchemaPtr TRichYPath::GetSchema() const
{
    return RunAttributeAccessor(*this, "schema", [&] {
        auto schema = FindAttribute<TTableSchemaPtr>(*this, "schema");
        if (schema) {
            ValidateTableSchema(*schema);
        }
        return schema;
    });
}

std::optional<TColumnRenameDescriptors> TRichYPath::GetColumnRenameDescriptors() const
{
    return FindAttribute<TColumnRenameDescriptors>(*this, "rename_columns");
}

TSortColumns TRichYPath::GetSortedBy() const
{
    return GetAttribute(*this, "sorted_by", TSortColumns());
}

void TRichYPath::SetSortedBy(const TSortColumns& value)
{
    if (value.empty()) {
        Attributes().Remove("sorted_by");
    } else {
        Attributes().Set("sorted_by", value);
    }
}

std::optional<i64> TRichYPath::GetRowCountLimit() const
{
    return RunAttributeAccessor(*this, "row_count_limit", [&] {
        auto rowCountLimit = FindAttribute<i64>(*this, "row_count_limit");
        if (rowCountLimit && *rowCountLimit < 0) {
            THROW_ERROR_EXCEPTION("Row count limit should be non-negative")
                << TErrorAttribute("row_count_limit", rowCountLimit);
        }
        return rowCountLimit;
    });
}

std::optional<NTransactionClient::TTimestamp> TRichYPath::GetTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "timestamp");
}

std::optional<NTransactionClient::TTimestamp> TRichYPath::GetRetentionTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "retention_timestamp");
}

std::optional<NTransactionClient::TTimestamp> TRichYPath::GetOutputTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "output_timestamp");
}

std::optional<NTableClient::EOptimizeFor> TRichYPath::GetOptimizeFor() const
{
    return FindAttribute<NTableClient::EOptimizeFor>(*this, "optimize_for");
}

std::optional<NChunkClient::EChunkFormat> TRichYPath::GetChunkFormat() const
{
    return FindAttribute<NChunkClient::EChunkFormat>(*this, "chunk_format");
}

std::optional<NCompression::ECodec> TRichYPath::GetCompressionCodec() const
{
    return FindAttribute<NCompression::ECodec>(*this, "compression_codec");
}

std::optional<NErasure::ECodec> TRichYPath::GetErasureCodec() const
{
    return FindAttribute<NErasure::ECodec>(*this, "erasure_codec");
}

bool TRichYPath::GetAutoMerge() const
{
    return GetAttribute<bool>(*this, "auto_merge", true);
}

std::optional<NObjectClient::TTransactionId> TRichYPath::GetTransactionId() const
{
    return FindAttribute<NObjectClient::TTransactionId>(*this, "transaction_id");
}

std::optional<std::vector<TSecurityTag>> TRichYPath::GetSecurityTags() const
{
    return FindAttribute<std::vector<TSecurityTag>>(*this, "security_tags");
}

bool TRichYPath::GetBypassArtifactCache() const
{
    return GetAttribute<bool>(*this, "bypass_artifact_cache", false);
}

ETableSchemaModification TRichYPath::GetSchemaModification() const
{
    return GetAttribute<ETableSchemaModification>(
        *this,
        "schema_modification",
        ETableSchemaModification::None);
}

bool TRichYPath::GetPartiallySorted() const
{
    return GetAttribute<bool>(*this, "partially_sorted", false);
}

std::optional<bool> TRichYPath::GetChunkUniqueKeys() const
{
    return FindAttribute<bool>(*this, "chunk_unique_keys");
}

std::optional<bool> TRichYPath::GetCopyFile() const
{
    return FindAttribute<bool>(*this, "copy_file");
}

std::optional<TSortColumns> TRichYPath::GetChunkSortColumns() const
{
    return FindAttribute<TSortColumns>(*this, "chunk_sort_columns");
}

std::optional<TString> TRichYPath::GetCluster() const
{
    return FindAttribute<TString>(*this, "cluster");
}

void TRichYPath::SetCluster(const TString& value)
{
    Attributes().Set("cluster", value);
}

std::optional<std::vector<TString>> TRichYPath::GetClusters() const
{
    return FindAttribute<std::vector<TString>>(*this, "clusters");
}

void TRichYPath::SetClusters(const std::vector<TString>& value)
{
    Attributes().Set("clusters", value);
}

bool TRichYPath::GetCreate() const
{
    return GetAttribute<bool>(*this, "create", false);
}

TVersionedReadOptions TRichYPath::GetVersionedReadOptions() const
{
    return GetAttribute(*this, "versioned_read_options", TVersionedReadOptions());
}

////////////////////////////////////////////////////////////////////////////////

TString ConvertToString(const TRichYPath& path, EYsonFormat ysonFormat)
{
    TStringBuilder builder;
    AppendAttributes(&builder, path.Attributes(), ysonFormat);
    builder.AppendString(path.GetPath());
    return builder.Flush();
}

void FormatValue(TStringBuilderBase* builder, const TRichYPath& path, TStringBuf spec)
{
    // NB: we intentionally use Text format since string-representation of rich ypath should be readable.
    FormatValue(builder, ConvertToString(path, EYsonFormat::Text), spec);
}

std::vector<TRichYPath> Normalize(const std::vector<TRichYPath>& paths)
{
    std::vector<TRichYPath> result;
    for (const auto& path : paths) {
        result.push_back(path.Normalize());
    }
    return result;
}

void Serialize(const TRichYPath& richPath, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(richPath.Attributes())
        .EndAttributes()
        .Value(richPath.GetPath());
}

void Deserialize(TRichYPath& richPath, INodePtr node)
{
    if (node->GetType() != ENodeType::String) {
        THROW_ERROR_EXCEPTION("YPath can only be parsed from %Qlv but got %Qlv",
            ENodeType::String,
            node->GetType());
    }
    richPath.SetPath(node->GetValue<TString>());
    richPath.Attributes().Clear();
    richPath.Attributes().MergeFrom(node->Attributes());
    richPath = richPath.Normalize();
}

void Deserialize(TRichYPath& richPath, TYsonPullParserCursor* cursor)
{
    Deserialize(richPath, ExtractTo<INodePtr>(cursor));
}

void ToProto(TString* protoPath, const TRichYPath& path)
{
    *protoPath = ConvertToString(path, EYsonFormat::Binary);
}

void FromProto(TRichYPath* path, const TString& protoPath)
{
    *path = TRichYPath::Parse(protoPath);
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& GetWellKnownRichYPathAttributes()
{
    static const std::vector<TString> WellKnownAttributes = {
        "append",
        "teleport",
        "primary",
        "foreign",
        "columns",
        "rename_columns",
        "ranges",
        "file_name",
        "executable",
        "format",
        "schema",
        "sorted_by",
        "row_count_limit",
        "timestamp",
        "retention_timestamp",
        "output_timestamp",
        "optimize_for",
        "chunk_format",
        "compression_codec",
        "erasure_codec",
        "auto_merge",
        "transaction_id",
        "security_tags",
        "bypass_artifact_cache",
        "schema_modification",
        "partially_sorted",
        "chunk_unique_keys",
        "copy_file",
        "chunk_sort_columns",
        "cluster",
        "clusters",
        "create",
        "read_via_exec_node",
        "versioned_read_options",
    };
    return WellKnownAttributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

size_t THash<NYT::NYPath::TRichYPath>::operator()(const NYT::NYPath::TRichYPath& richYPath) const
{
    return ComputeHash(NYT::NYPath::ToString(richYPath));
}
