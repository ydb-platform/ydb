#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/security_client/public.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

//! YPath string plus attributes.
class TRichYPath
{
public:
    TRichYPath();
    TRichYPath(const TRichYPath& other);
    TRichYPath(TRichYPath&& other);
    TRichYPath(const char* path);
    TRichYPath(const TYPath& path);
    TRichYPath(const TYPath& path, const NYTree::IAttributeDictionary& attributes);
    TRichYPath& operator = (const TRichYPath& other);

    static TRichYPath Parse(const TString& str);
    TRichYPath Normalize() const;

    const TYPath& GetPath() const;
    void SetPath(const TYPath& path);

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary& Attributes();

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
    std::optional<std::vector<TString>> GetColumns() const;
    void SetColumns(const std::vector<TString>& columns);

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
    std::optional<TString> GetCluster() const;
    void SetCluster(const TString& value);

    // "clusters"
    std::optional<std::vector<TString>> GetClusters() const;
    void SetClusters(const std::vector<TString>& value);

    // "create"
    bool GetCreate() const;

private:
    TYPath Path_;
    NYTree::IAttributeDictionaryPtr Attributes_;
};

bool operator== (const TRichYPath& lhs, const TRichYPath& rhs);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TRichYPath& path, TStringBuf spec);

std::vector<TRichYPath> Normalize(const std::vector<TRichYPath>& paths);

void Serialize(const TRichYPath& richPath, NYson::IYsonConsumer* consumer);
void Deserialize(TRichYPath& richPath, NYTree::INodePtr node);
void Deserialize(TRichYPath& richPath, NYson::TYsonPullParserCursor* cursor);

void ToProto(TString* protoPath, const TRichYPath& path);
void FromProto(TRichYPath* path, const TString& protoPath);

////////////////////////////////////////////////////////////////////////////////

const std::vector<TString>& GetWellKnownRichYPathAttributes();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath

template <>
struct THash<NYT::NYPath::TRichYPath>
{
    size_t operator()(const NYT::NYPath::TRichYPath& richYPath) const;
};
