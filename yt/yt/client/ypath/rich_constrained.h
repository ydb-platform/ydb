#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
class TConstrainedRichYPath
{
private:
    static constexpr bool AreValidatorsEmpty = (sizeof...(TValidator)) == 0;

public:
    TConstrainedRichYPath();

    TConstrainedRichYPath(const TConstrainedRichYPath& other) noexcept;
    TConstrainedRichYPath& operator=(const TConstrainedRichYPath& other) noexcept;

    TConstrainedRichYPath(TConstrainedRichYPath&& other) noexcept = default;
    TConstrainedRichYPath& operator=(TConstrainedRichYPath&& other) noexcept = default;

    template <class... TOtherValidators>
    TConstrainedRichYPath(const TConstrainedRichYPath<TOtherValidators...>& other);
    template <class... TOtherValidators>
    TConstrainedRichYPath(TConstrainedRichYPath<TOtherValidators...>&& other);

    template <class... TOtherValidators>
    TConstrainedRichYPath& operator=(const TConstrainedRichYPath<TOtherValidators...>& other);
    template <class... TOtherValidators>
    TConstrainedRichYPath& operator=(TConstrainedRichYPath<TOtherValidators...>&& other);

    TConstrainedRichYPath(const char* path);
    TConstrainedRichYPath(TYPath path);
    TConstrainedRichYPath(TYPath path, const NYTree::IAttributeDictionary& attributes);

    static TConstrainedRichYPath Parse(TStringBuf str);
    TConstrainedRichYPath Normalize() const;

    const TYPath& GetPath() const;
    void SetPath(TYPath path);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes() requires AreValidatorsEmpty;

    // Functions to modify attributes even if there are validators.
    template <class T>
    void SetAttribute(NYTree::IAttributeDictionary::TKeyView key, const T& value);
    void RemoveAttribute(NYTree::IAttributeDictionary::TKeyView key);

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    // Attribute accessors.
    // "append"
    bool GetAppend(bool defaultValue = false) const;
    void SetAppend(bool value);

    // "teleport"
    bool GetTeleport() const;

    // "primary"
    bool GetPrimary() const;

    // "foreign"
    bool GetForeign() const;
    void SetForeign(bool value);

    // "read_via_exec_node"
    bool GetReadViaExecNode() const;
    void SetReadViaExecNode(bool value);

    // "columns"
    std::optional<std::vector<std::string>> GetColumns() const;
    void SetColumns(const std::vector<std::string>& columns);

    // "rename_columns"
    std::optional<NTableClient::TColumnRenameDescriptors> GetColumnRenameDescriptors() const;

    // "ranges"
    //! COMPAT(ignat): also "lower_limit" and "upper_limit"
    //! This method returns legacy read ranges completely ignoring key bounds.
    std::vector<NChunkClient::TLegacyReadRange> GetRanges() const;

    //! Get ranges, possibly transforming legacy keys into new key bounds using provided comparator.
    //! This method throws if comparator is not present and keys are present.
    //! In case when at least one column is non-ascending, some requirements are even stronger,
    //! refer to the implementation for details.
    std::vector<NChunkClient::TReadRange> GetNewRanges(
        const NTableClient::TComparator& comparator = NTableClient::TComparator(),
        const NTableClient::TKeyColumnTypes& conversionTypeHints = {}) const;

    //! Check whether ranges contain a range with row_index specified in either
    //! limit. This method is intended to be more lightweight than #GetNewRanges
    //! and it does not require a comparator.
    bool HasRowIndexInRanges() const;

    void SetRanges(const std::vector<NChunkClient::TReadRange>& ranges);
    bool HasNontrivialRanges() const;

    // "file_name"
    std::optional<TString> GetFileName() const;

    // "executable"
    std::optional<bool> GetExecutable() const;

    // "format"
    NYson::TYsonString GetFormat() const;

    // "schema"
    NTableClient::TTableSchemaPtr GetSchema() const;

    // "sorted_by"
    NTableClient::TSortColumns GetSortedBy() const;
    void SetSortedBy(const NTableClient::TSortColumns& value);

    // "row_count_limit"
    std::optional<i64> GetRowCountLimit() const;

    // "timestamp"
    std::optional<NTransactionClient::TTimestamp> GetTimestamp() const;

    // "retention_timestamp"
    std::optional<NTransactionClient::TTimestamp> GetRetentionTimestamp() const;

    // "output_timestamp"
    std::optional<NTransactionClient::TTimestamp> GetOutputTimestamp() const;

    // "optimize_for"
    std::optional<NTableClient::EOptimizeFor> GetOptimizeFor() const;

    // "chunk_format"
    std::optional<NChunkClient::EChunkFormat> GetChunkFormat() const;

    // "compression_codec"
    std::optional<NCompression::ECodec> GetCompressionCodec() const;

    // "erasure_codec"
    std::optional<NErasure::ECodec> GetErasureCodec() const;

    // "auto_merge"
    bool GetAutoMerge() const;

    // "transaction_id"
    std::optional<NObjectClient::TTransactionId> GetTransactionId() const;

    // "security_tags"
    std::optional<std::vector<NSecurityClient::TSecurityTag>> GetSecurityTags() const;

    // "bypass_artifact_cache"
    bool GetBypassArtifactCache() const;

    // "schema_modification"
    NTableClient::ETableSchemaModification GetSchemaModification() const;

    // "partially_sorted"
    bool GetPartiallySorted() const;

    // "chunk_unique_keys"
    std::optional<bool> GetChunkUniqueKeys() const;

    // "copy_file"
    std::optional<bool> GetCopyFile() const;

    // "chunk_sort_columns"
    std::optional<NTableClient::TSortColumns> GetChunkSortColumns() const;

    // "cluster"
    std::optional<std::string> GetCluster() const;
    void SetCluster(const std::string& value);

    // "clusters"
    std::optional<std::vector<std::string>> GetClusters() const;
    void SetClusters(const std::vector<std::string>& value);

    // "create"
    std::variant<bool, NYTree::IAttributeDictionaryPtr> GetCreate() const;

    // "versioned_read_options"
    NTableClient::TVersionedReadOptions GetVersionedReadOptions() const;

    // "versioned_write_options"
    NTableClient::TVersionedWriteOptions GetVersionedWriteOptions() const;

    // "access_method"
    std::optional<TString> GetAccessMethod() const;

    // "input_query"
    std::optional<TString> GetInputQuery() const;

    bool operator==(const TConstrainedRichYPath& other) const;

private:
    template <class... TOtherValidator>
    friend class TConstrainedRichYPath;

    TYPath Path_;
    NYTree::IAttributeDictionaryPtr Attributes_;

    template <class... TOtherValidator>
    void Validate(const TConstrainedRichYPath<TOtherValidator...>& other) const;
    void Validate() const;
};

////////////////////////////////////////////////////////////////////////////////

template <class... TValidator>
void FormatValue(TStringBuilderBase* builder, const TConstrainedRichYPath<TValidator...>& path, TStringBuf spec);

template <class... TValidator>
std::vector<TConstrainedRichYPath<TValidator...>> Normalize(const std::vector<TConstrainedRichYPath<TValidator...>>& paths);

template <class... TValidator>
void Serialize(const TConstrainedRichYPath<TValidator...>& richPath, NYson::IYsonConsumer* consumer);
template <class... TValidator>
void Deserialize(TConstrainedRichYPath<TValidator...>& richPath, NYTree::INodePtr node);
template <class... TValidator>
void Deserialize(TConstrainedRichYPath<TValidator...>& richPath, NYson::TYsonPullParserCursor* cursor);

template <>
void Deserialize(TConstrainedRichYPath<>& richPath, NYTree::INodePtr node);

template <class... TValidator>
void ToProto(TString* protoPath, const TConstrainedRichYPath<TValidator...>& path);
template <class... TValidator>
void FromProto(TConstrainedRichYPath<TValidator...>* path, const TString& protoPath);

template <class... TValidator>
void ToProto(std::string* protoPath, const TConstrainedRichYPath<TValidator...>& path);
template <class... TValidator>
void FromProto(TConstrainedRichYPath<TValidator...>* path, const std::string& protoPath);

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& GetWellKnownRichYPathAttributes();

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
struct TRequiredAttributesValidator
{
    template <class... TValidator>
    void operator()(const TConstrainedRichYPath<TValidator...>& path) const;
};

////////////////////////////////////////////////////////////////////////////////

template <const char... AttributeKey[]>
struct TWhitelistAttributesValidator
{
    template <class... TValidator>
    void operator()(const TConstrainedRichYPath<TValidator...>& path) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

template <class... TValidator>
struct THash<NYT::NYPath::TConstrainedRichYPath<TValidator...>>
{
    size_t operator()(const NYT::NYPath::TConstrainedRichYPath<TValidator...>& richYPath) const;
};

#define RICH_CONSTRAINED_INL_H_
#include "rich_constrained-inl.h"
#undef RICH_CONSTRAINED_INL_H_
