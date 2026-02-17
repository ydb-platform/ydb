#ifndef RICH_CONSTRAINED_INL_H_
#error "Direct inclusion of this file is not allowed, include rich_constrained.h"
// For the sake of sane code completion.
#include "rich_constrained.h"
#endif
#undef RICH_CONSTRAINED_INL_H_

#include "parser_detail.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_io_options.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NYPath {

namespace {

template <class TFunc, class... TValidator>
auto RunAttributeAccessor(const TConstrainedRichYPath<TValidator...>& path, const std::string& key, TFunc accessor) -> decltype(accessor())
{
    try {
        return accessor();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute %Qv of rich YPath %v",
            key,
            path.GetPath()) << ex;
    }
}

template <class T, class... TValidator>
T GetAttribute(const TConstrainedRichYPath<TValidator...>& path, const std::string& key, const T& defaultValue)
{
    return RunAttributeAccessor(path, key, [&] {
        return path.Attributes().Get(key, defaultValue);
    });
}

template <class T, class... TValidator>
typename TOptionalTraits<T>::TOptional FindAttribute(const TConstrainedRichYPath<TValidator...>& path, const std::string& key)
{
    return RunAttributeAccessor(path, key, [&] {
        return path.Attributes().template Find<T>(key);
    });
}

template <class... TValidator>
NYson::TYsonString FindAttributeYson(const TConstrainedRichYPath<TValidator...>& path, const std::string& key)
{
    return RunAttributeAccessor(path, key, [&] {
        return path.Attributes().FindYson(key);
    });
}

//! Collect all ranges.
//! Either from `ranges` key or from top-level `{upper,lower}_limit` keys
template <class... TValidator>
std::optional<std::vector<NYTree::IMapNodePtr>> CollectRawRangeNodes(const TConstrainedRichYPath<TValidator...>& path)
{
    // TODO(max42): YT-14242. Top-level "lower_limit" and "upper_limit" are processed for compatibility.
    // But we should deprecate this one day.
    auto optionalRangeNodes = FindAttribute<std::vector<NYTree::IMapNodePtr>>(path, "ranges");
    auto optionalLowerLimitNode = FindAttribute<NYTree::IMapNodePtr>(path, "lower_limit");
    auto optionalUpperLimitNode = FindAttribute<NYTree::IMapNodePtr>(path, "upper_limit");

    if (optionalLowerLimitNode || optionalUpperLimitNode) {
        if (optionalRangeNodes) {
            THROW_ERROR_EXCEPTION("YPath cannot be annotated with both multiple (\"ranges\" attribute) "
                "and single (\"lower_limit\" or \"upper_limit\" attributes) ranges");
        }
        THashMap<TString, NYTree::IMapNodePtr> rangeNode;
        if (optionalLowerLimitNode) {
            rangeNode["lower_limit"] = optionalLowerLimitNode;
        }
        if (optionalUpperLimitNode) {
            rangeNode["upper_limit"] = optionalUpperLimitNode;
        }
        return std::vector<NYTree::IMapNodePtr>({ConvertToNode(rangeNode)->AsMap()});
    }

    return optionalRangeNodes;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath()
{
    Validate();
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(const TConstrainedRichYPath& other) noexcept
    : Path_(other.Path_)
    , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
{ }

template <class... TValidator>
template <class... TOtherValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(const TConstrainedRichYPath<TOtherValidator...>& other)
    : Path_(other.Path_)
    , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
{
    Validate();
}

template <class... TValidator>
template <class... TOtherValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(TConstrainedRichYPath<TOtherValidator...>&& other)
    : Path_(std::move(other.Path_))
    , Attributes_(std::move(other.Attributes_))
{
    Validate();
}

template <class... TValidator>
auto TConstrainedRichYPath<TValidator...>::operator=(const TConstrainedRichYPath& other) noexcept -> TConstrainedRichYPath&
{
    if (this != &other) {
        Path_ = other.Path_;
        Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
    }
    return *this;
}

template <class... TValidator>
template <class... TOtherValidator>
auto TConstrainedRichYPath<TValidator...>::operator=(const TConstrainedRichYPath<TOtherValidator...>& other) -> TConstrainedRichYPath&
{
    Validate(other);
    Path_ = other.Path_;
    Attributes_ = other.Attributes_ ? other.Attributes_->Clone() : nullptr;
    return *this;
}

template <class... TValidator>
template <class... TOtherValidator>
auto TConstrainedRichYPath<TValidator...>::operator=(TConstrainedRichYPath<TOtherValidator...>&& other) -> TConstrainedRichYPath&
{
    Validate(other);
    Path_ = std::move(other.Path_);
    Attributes_ = std::move(other.Attributes_);
    return *this;
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(const char* path)
    : Path_(path)
{
    *this = Normalize();
    Validate();
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(TYPath path)
    : Path_(std::move(path))
{
    *this = Normalize();
    Validate();
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...>::TConstrainedRichYPath(TYPath path, const NYTree::IAttributeDictionary& attributes)
    : Path_(std::move(path))
    , Attributes_(attributes.Clone())
{
    Validate();
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...> TConstrainedRichYPath<TValidator...>::Parse(TStringBuf str)
{
    auto [path, attributes] = ParseRichYPathImpl(str);
    return TConstrainedRichYPath<TValidator...>(std::move(path), *attributes);
}

template <class... TValidator>
TConstrainedRichYPath<TValidator...> TConstrainedRichYPath<TValidator...>::Normalize() const
{
    auto [path, attributes] = ParseRichYPathImpl(Path_);
    if (!attributes) {
        attributes = NYTree::CreateEphemeralAttributes();
    }
    attributes->MergeFrom(Attributes());
    return TConstrainedRichYPath<TValidator...>(std::move(path), *attributes);;
}

template <class... TValidator>
const TYPath& TConstrainedRichYPath<TValidator...>::GetPath() const
{
    return Path_;
}

// Explicit specialization. Defined in rich_constrained.cpp.
template <>
void TConstrainedRichYPath<>::SetPath(TYPath path);

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetPath(TYPath path)
{
    TConstrainedRichYPath<> other(std::move(path), Attributes());
    Validate(other);
    Path_ = std::move(other.Path_);
}

template <class... TValidator>
const NYTree::IAttributeDictionary& TConstrainedRichYPath<TValidator...>::Attributes() const
{
    return Attributes_ ? *Attributes_ : NYTree::EmptyAttributes();
}

template <class... TValidator>
NYTree::IAttributeDictionary& TConstrainedRichYPath<TValidator...>::Attributes() requires AreValidatorsEmpty
{
    if (!Attributes_) {
        Attributes_ = NYTree::CreateEphemeralAttributes();
    }
    return *Attributes_;
}

template <>
template <class T>
void TConstrainedRichYPath<>::SetAttribute(NYTree::IAttributeDictionary::TKeyView key, const T& value)
{
    Attributes().Set(key, value);
}

template <class... TValidator>
template <class T>
void TConstrainedRichYPath<TValidator...>::SetAttribute(NYTree::IAttributeDictionary::TKeyView key, const T& value)
{
    TConstrainedRichYPath<> other(Path_, Attributes());
    other.SetAttribute(key, value);
    Validate(other);
    Attributes_ = std::move(other.Attributes_);
}

// Explicit specialization. Defined in rich_constrained.cpp.
template <>
void TConstrainedRichYPath<>::RemoveAttribute(NYTree::IAttributeDictionary::TKeyView key);

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::RemoveAttribute(NYTree::IAttributeDictionary::TKeyView key)
{
    TConstrainedRichYPath<> other(Path_, Attributes());
    other.RemoveAttribute(key);
    Validate(other);
    Attributes_ = std::move(other.Attributes_);
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, Path_);
    Save(context, Attributes_);
}

// Explicit specialization. Defined in rich_constrained.cpp.
template <>
void TConstrainedRichYPath<>::Load(TStreamLoadContext& context);

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    TConstrainedRichYPath<> other;
    Load(context, other);
    *this = std::move(other);
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetAppend(bool defaultValue) const
{
    return GetAttribute(*this, "append", defaultValue);
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetAppend(bool value)
{
    SetAttribute("append", value);
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetTeleport() const
{
    return GetAttribute(*this, "teleport", false);
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetPrimary() const
{
    return GetAttribute(*this, "primary", false);
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetForeign() const
{
    return GetAttribute(*this, "foreign", false);
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetForeign(bool value)
{
    SetAttribute("foreign", value);
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetReadViaExecNode() const
{
    return GetAttribute(*this, "read_via_exec_node", false);
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetReadViaExecNode(bool value)
{
    SetAttribute("read_via_exec_node", value);
}

template <class... TValidator>
std::optional<std::vector<std::string>> TConstrainedRichYPath<TValidator...>::GetColumns() const
{
    if (Attributes().Contains("channel")) {
        THROW_ERROR_EXCEPTION("Deprecated attribute \"channel\" in YPath");
    }
    return FindAttribute<std::vector<std::string>>(*this, "columns");
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetColumns(const std::vector<std::string>& columns)
{
    SetAttribute("columns", columns);
}

template <class... TValidator>
std::vector<NChunkClient::TLegacyReadRange> TConstrainedRichYPath<TValidator...>::GetRanges() const
{
    // COMPAT(ignat): top-level "lower_limit" and "upper_limit" are processed for compatibility.
    auto optionalLowerLimit = FindAttribute<NChunkClient::TLegacyReadLimit>(*this, "lower_limit");
    auto optionalUpperLimit = FindAttribute<NChunkClient::TLegacyReadLimit>(*this, "upper_limit");
    auto optionalRanges = FindAttribute<std::vector<NChunkClient::TLegacyReadRange>>(*this, "ranges");

    if (optionalLowerLimit || optionalUpperLimit) {
        if (optionalRanges) {
            THROW_ERROR_EXCEPTION("YPath cannot be annotated with both multiple (\"ranges\" attribute) "
                "and single (\"lower_limit\" or \"upper_limit\" attributes) ranges");
        }
        return std::vector<NChunkClient::TLegacyReadRange>({
            NChunkClient::TLegacyReadRange(
                optionalLowerLimit.value_or(NChunkClient::TLegacyReadLimit()),
                optionalUpperLimit.value_or(NChunkClient::TLegacyReadLimit())),
            });
    } else {
        return optionalRanges.value_or(std::vector<NChunkClient::TLegacyReadRange>({NChunkClient::TLegacyReadRange()}));
    }
}

template <class... TValidator>
std::vector<NChunkClient::TReadRange> TConstrainedRichYPath<TValidator...>::GetNewRanges(
    const NTableClient::TComparator& comparator,
    const NTableClient::TKeyColumnTypes& conversionTypeHints) const
{
    auto optionalRangeNodes = CollectRawRangeNodes(*this);

    if (!optionalRangeNodes) {
        return {NChunkClient::TReadRange()};
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

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::HasRowIndexInRanges() const
{
    auto optionalRangeNodes = CollectRawRangeNodes(*this);
    if (!optionalRangeNodes) {
        return false;
    }

    for (const auto& rangeNode : *optionalRangeNodes) {
        auto hasRowIndex = [] (const NYTree::INodePtr& limitNode) {
            if (!limitNode) {
                return false;
            }

            return static_cast<bool>(limitNode->AsMap()->FindChild("row_index"));
        };

        if (hasRowIndex(rangeNode->FindChild("lower_limit")) ||
            hasRowIndex(rangeNode->FindChild("upper_limit")) ||
            hasRowIndex(rangeNode->FindChild("exact")))
        {
            return true;
        }
    }

    return false;
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetRanges(const std::vector<NChunkClient::TReadRange>& ranges)
{
    SetAttribute("ranges", ranges);
    // COMPAT(ignat)
    RemoveAttribute("lower_limit");
    RemoveAttribute("upper_limit");
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::HasNontrivialRanges() const
{
    auto optionalLowerLimit = FindAttribute<NChunkClient::TLegacyReadLimit>(*this, "lower_limit");
    auto optionalUpperLimit = FindAttribute<NChunkClient::TLegacyReadLimit>(*this, "upper_limit");
    auto optionalRanges = FindAttribute<std::vector<NChunkClient::TLegacyReadRange>>(*this, "ranges");

    return optionalLowerLimit || optionalUpperLimit || optionalRanges;
}

template <class... TValidator>
std::optional<TString> TConstrainedRichYPath<TValidator...>::GetFileName() const
{
    return FindAttribute<TString>(*this, "file_name");
}

template <class... TValidator>
std::optional<bool> TConstrainedRichYPath<TValidator...>::GetExecutable() const
{
    return FindAttribute<bool>(*this, "executable");
}

template <class... TValidator>
NYson::TYsonString TConstrainedRichYPath<TValidator...>::GetFormat() const
{
    return FindAttributeYson(*this, "format");
}

template <class... TValidator>
NTableClient::TTableSchemaPtr TConstrainedRichYPath<TValidator...>::GetSchema() const
{
    return RunAttributeAccessor(*this, "schema", [&] {
        auto schema = FindAttribute<NTableClient::TTableSchemaPtr>(*this, "schema");
        if (schema) {
            ValidateTableSchema(*schema);
        }
        return schema;
    });
}

template <class... TValidator>
std::optional<NTableClient::TColumnRenameDescriptors> TConstrainedRichYPath<TValidator...>::GetColumnRenameDescriptors() const
{
    return FindAttribute<NTableClient::TColumnRenameDescriptors>(*this, "rename_columns");
}

template <class... TValidator>
NTableClient::TSortColumns TConstrainedRichYPath<TValidator...>::GetSortedBy() const
{
    return GetAttribute(*this, "sorted_by", NTableClient::TSortColumns());
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetSortedBy(const NTableClient::TSortColumns& value)
{
    if (value.empty()) {
        RemoveAttribute("sorted_by");
    } else {
        SetAttribute("sorted_by", value);
    }
}

template <class... TValidator>
std::optional<i64> TConstrainedRichYPath<TValidator...>::GetRowCountLimit() const
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

template <class... TValidator>
std::optional<NTransactionClient::TTimestamp> TConstrainedRichYPath<TValidator...>::GetTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "timestamp");
}

template <class... TValidator>
std::optional<NTransactionClient::TTimestamp> TConstrainedRichYPath<TValidator...>::GetRetentionTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "retention_timestamp");
}

template <class... TValidator>
std::optional<NTransactionClient::TTimestamp> TConstrainedRichYPath<TValidator...>::GetOutputTimestamp() const
{
    return FindAttribute<NTransactionClient::TTimestamp>(*this, "output_timestamp");
}

template <class... TValidator>
std::optional<NTableClient::EOptimizeFor> TConstrainedRichYPath<TValidator...>::GetOptimizeFor() const
{
    return FindAttribute<NTableClient::EOptimizeFor>(*this, "optimize_for");
}

template <class... TValidator>
std::optional<NChunkClient::EChunkFormat> TConstrainedRichYPath<TValidator...>::GetChunkFormat() const
{
    return FindAttribute<NChunkClient::EChunkFormat>(*this, "chunk_format");
}

template <class... TValidator>
std::optional<NCompression::ECodec> TConstrainedRichYPath<TValidator...>::GetCompressionCodec() const
{
    return FindAttribute<NCompression::ECodec>(*this, "compression_codec");
}

template <class... TValidator>
std::optional<NErasure::ECodec> TConstrainedRichYPath<TValidator...>::GetErasureCodec() const
{
    return FindAttribute<NErasure::ECodec>(*this, "erasure_codec");
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetAutoMerge() const
{
    return GetAttribute<bool>(*this, "auto_merge", true);
}

template <class... TValidator>
std::optional<NObjectClient::TTransactionId> TConstrainedRichYPath<TValidator...>::GetTransactionId() const
{
    return FindAttribute<NObjectClient::TTransactionId>(*this, "transaction_id");
}

template <class... TValidator>
std::optional<std::vector<NSecurityClient::TSecurityTag>> TConstrainedRichYPath<TValidator...>::GetSecurityTags() const
{
    return FindAttribute<std::vector<NSecurityClient::TSecurityTag>>(*this, "security_tags");
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetBypassArtifactCache() const
{
    return GetAttribute<bool>(*this, "bypass_artifact_cache", false);
}

template <class... TValidator>
NTableClient::ETableSchemaModification TConstrainedRichYPath<TValidator...>::GetSchemaModification() const
{
    return GetAttribute<NTableClient::ETableSchemaModification>(
        *this,
        "schema_modification",
        NTableClient::ETableSchemaModification::None);
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::GetPartiallySorted() const
{
    return GetAttribute<bool>(*this, "partially_sorted", false);
}

template <class... TValidator>
std::optional<bool> TConstrainedRichYPath<TValidator...>::GetChunkUniqueKeys() const
{
    return FindAttribute<bool>(*this, "chunk_unique_keys");
}

template <class... TValidator>
std::optional<bool> TConstrainedRichYPath<TValidator...>::GetCopyFile() const
{
    return FindAttribute<bool>(*this, "copy_file");
}

template <class... TValidator>
std::optional<NTableClient::TSortColumns> TConstrainedRichYPath<TValidator...>::GetChunkSortColumns() const
{
    return FindAttribute<NTableClient::TSortColumns>(*this, "chunk_sort_columns");
}

template <class... TValidator>
std::optional<std::string> TConstrainedRichYPath<TValidator...>::GetCluster() const
{
    return FindAttribute<std::string>(*this, "cluster");
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetCluster(const std::string& value)
{
    SetAttribute("cluster", value);
}

template <class... TValidator>
std::optional<std::vector<std::string>> TConstrainedRichYPath<TValidator...>::GetClusters() const
{
    return FindAttribute<std::vector<std::string>>(*this, "clusters");
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::SetClusters(const std::vector<std::string>& value)
{
    SetAttribute("clusters", value);
}

template <class... TValidator>
std::variant<bool, NYTree::IAttributeDictionaryPtr> TConstrainedRichYPath<TValidator...>::GetCreate() const
{
    auto yson = Attributes().FindYson("create");
    if (!yson) {
        return false;
    }

    auto node = ConvertTo<NYTree::INodePtr>(yson);
    switch (node->GetType()) {
        case NYTree::ENodeType::Boolean:
        case NYTree::ENodeType::String:
            return ConvertTo<bool>(node);
        case NYTree::ENodeType::Map:
            return NYTree::IAttributeDictionary::FromMap(node->AsMap());
        default:
            THROW_ERROR_EXCEPTION("Attribute \"create\" cannot be of type %Qlv", node->GetType());
    }
}

template <class... TValidator>
NTableClient::TVersionedReadOptions TConstrainedRichYPath<TValidator...>::GetVersionedReadOptions() const
{
    return GetAttribute(*this, "versioned_read_options", NTableClient::TVersionedReadOptions());
}

template <class... TValidator>
NTableClient::TVersionedWriteOptions TConstrainedRichYPath<TValidator...>::GetVersionedWriteOptions() const
{
    return GetAttribute(*this, "versioned_write_options", NTableClient::TVersionedWriteOptions());
}

template <class... TValidator>
std::optional<TString> TConstrainedRichYPath<TValidator...>::GetAccessMethod() const
{
    return FindAttribute<TString>(*this, "access_method");
}

template <class... TValidator>
std::optional<TString> TConstrainedRichYPath<TValidator...>::GetInputQuery() const
{
    return FindAttribute<TString>(*this, "input_query");
}

template <class... TValidator>
bool TConstrainedRichYPath<TValidator...>::operator==(const TConstrainedRichYPath& other) const
{
    return GetPath() == other.GetPath() && Attributes() == other.Attributes();
}

template <class... TValidator>
void TConstrainedRichYPath<TValidator...>::Validate() const
{
    (TValidator()(*this), ...);
}

template <class... TValidator>
template <class... TOtherValidator>
void TConstrainedRichYPath<TValidator...>::Validate(const TConstrainedRichYPath<TOtherValidator...>& other) const
{
    (TValidator()(other), ...);
}

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
template <class... TValidator>
void TRequiredAttributesValidator<AttributeKey...>::operator()(const TConstrainedRichYPath<TValidator...>& path) const
{
    auto validateOne = [&] (const char* attributeName) {
        THROW_ERROR_EXCEPTION_IF(!path.Attributes().Contains(attributeName), "YPath %Qv does not have attribute %Qv", path, attributeName);
    };
    (validateOne(AttributeKey), ...);
}

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
template <class... TValidator>
void TWhitelistAttributesValidator<AttributeKey...>::operator()(const TConstrainedRichYPath<TValidator...>& path) const
{
    for (const auto& key : path.Attributes().ListKeys()) {
        if (((key == AttributeKey) || ...)) {
            continue;
        }

        THROW_ERROR_EXCEPTION("YPath %Qv has unexpected attribute %Qv", path, key);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
void FormatValue(TStringBuilderBase* builder, const TConstrainedRichYPath<TValidator...>& path, TStringBuf spec)
{
    // NB: We intentionally use Text format since string-representation of rich ypath should be readable.
    FormatValue(builder, ConvertToStringImpl(path.GetPath(), path.Attributes(), NYson::EYsonFormat::Text), spec);
}

template <class... TValidator>
std::vector<TConstrainedRichYPath<TValidator...>> Normalize(const std::vector<TConstrainedRichYPath<TValidator...>>& paths)
{
    std::vector<TConstrainedRichYPath<TValidator...>> result;
    for (const auto& path : paths) {
        result.push_back(path.Normalize());
    }
    return result;
}

template <class... TValidator>
void Serialize(const TConstrainedRichYPath<TValidator...>& richPath, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginAttributes()
            .Items(richPath.Attributes())
        .EndAttributes()
        .Value(richPath.GetPath());
}

template <class... TValidator>
void Deserialize(TConstrainedRichYPath<TValidator...>& richPath, NYTree::INodePtr node)
{
    TConstrainedRichYPath<> other;
    Deserialize(other, node);
    richPath = std::move(other);
}

template <class... TValidator>
void Deserialize(TConstrainedRichYPath<TValidator...>& richPath, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(richPath, ExtractTo<NYTree::INodePtr>(cursor));
}

template <class... TValidator>
void ToProto(TString* protoPath, const TConstrainedRichYPath<TValidator...>& path)
{
    *protoPath = ConvertToStringImpl(path.GetPath(), path.Attributes(), NYson::EYsonFormat::Binary);
}

template <class... TValidator>
void FromProto(TConstrainedRichYPath<TValidator...>* path, const TString& protoPath)
{
    *path = TConstrainedRichYPath<TValidator...>::Parse(protoPath);
}

template <class... TValidator>
void ToProto(std::string* protoPath, const TConstrainedRichYPath<TValidator...>& path)
{
    *protoPath = ConvertToStringImpl(path.GetPath(), path.Attributes(), NYson::EYsonFormat::Binary);
}

template <class... TValidator>
void FromProto(TConstrainedRichYPath<TValidator...>* path, const std::string& protoPath)
{
    *path = TConstrainedRichYPath<TValidator...>::Parse(TString(protoPath));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

template <class... TValidator>
size_t THash<NYT::NYPath::TConstrainedRichYPath<TValidator...>>::operator()(const NYT::NYPath::TConstrainedRichYPath<TValidator...>& richYPath) const
{
    return ComputeHash(ToString(richYPath));
}
