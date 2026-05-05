#include "rich_constrained.h"

namespace NYT::NYPath {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <>
void TConstrainedRichYPath<>::SetPath(TYPath path)
{
    Path_ = std::move(path);
}

template <>
void TConstrainedRichYPath<>::RemoveAttribute(IAttributeDictionary::TKeyView key)
{
    Attributes().Remove(key);
}

template <>
void TConstrainedRichYPath<>::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    Load(context, Path_);
    Load(context, Attributes_);
}

////////////////////////////////////////////////////////////////////////////////

template <>
void Deserialize(TConstrainedRichYPath<>& richPath, INodePtr node)
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
        "versioned_write_options",
    };
    return WellKnownAttributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
