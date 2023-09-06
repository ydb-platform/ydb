#pragma once

///
/// @file yt/cpp/mapreduce/interface/cypress.h
///
/// Header containing interface to execute [Cypress](https://ytsaurus.tech/docs/en/user-guide/storage/cypress.html)-related commands.

#include "fwd.h"

#include "client_method_options.h"
#include "common.h"
#include "node.h"

#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// Client interface to execute [Cypress](https://ytsaurus.tech/docs/en/user-guide/storage/cypress.html)-related commands.
class ICypressClient
{
public:
    virtual ~ICypressClient() = default;

    ///
    /// @brief Create Cypress node of given type.
    ///
    /// @param path Path in Cypress to the new object.
    /// @param type New node type.
    /// @param options Optional parameters.
    ///
    /// @return Id of the created node.
    ///
    /// @note  All but the last components must exist unless @ref NYT::TCreateOptions::Recursive is `true`.
    ///
    /// @note The node itself must not exist unless @ref NYT::TCreateOptions::IgnoreExisting or @ref NYT::TCreateOptions::Force are `true`.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#create)
    virtual TNodeId Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) = 0;

    ///
    /// @brief Create table with schema inferred from the template argument.
    ///
    /// @tparam TRowType type of C++ representation of the row to be stored in the table.
    /// @param path Path in Cypress to the new table.
    /// @param sortColumns List of columns to mark as sorted in schema.
    /// @param options Optional parameters.
    ///
    /// @return Id of the created node.
    ///
    /// @note If "schema" is passed in `options.Attributes` it has priority over the deduced schema (the latter is ignored).
    template <typename TRowType>
    TNodeId CreateTable(
        const TYPath& path,
        const TSortColumns& sortColumns = TSortColumns(),
        const TCreateOptions& options = TCreateOptions());

    ///
    /// @brief Remove Cypress node.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#remove)
    virtual void Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) = 0;

    ///
    /// @brief Check if Cypress node exists.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#exists)
    virtual bool Exists(
        const TYPath& path,
        const TExistsOptions& options = TExistsOptions()) = 0;

    ///
    /// @brief Get Cypress node contents.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get)
    virtual TNode Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) = 0;

    ///
    /// @brief Set Cypress node contents.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#set)
    virtual void Set(
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = TSetOptions()) = 0;

    ///
    /// @brief Set multiple attributes for cypress path.
    ///
    /// @param path Path to root of the attributes to be set e.g. "//path/to/table/@";
    ///     it is important to make sure that path ends with "/@".
    /// @param attributes Map with attributes
    /// @param options Optional parameters.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#multiset_attributes)
    virtual void MultisetAttributes(
        const TYPath& path,
        const TNode::TMapType& attributes,
        const TMultisetAttributesOptions& options = TMultisetAttributesOptions()) = 0;

    ///
    /// @brief List Cypress map or attribute node keys.
    ///
    /// @param path Path in the tree to the node in question.
    /// @param options Optional parameters.
    ///
    /// @return List of keys with attributes (if they were required in @ref NYT::TListOptions::AttributeFilter).
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#list)
    virtual TNode::TListType List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) = 0;

    ///
    /// @brief Copy Cypress node.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#copy)
    virtual TNodeId Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) = 0;

    ///
    /// @brief Move Cypress node (equivalent to copy-then-remove).
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#move)
    virtual TNodeId Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) = 0;

    ///
    /// @brief Create link to Cypress node.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#link)
    virtual TNodeId Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) = 0;

    ///
    /// @brief Concatenate several tables into one.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#concatenate)
    virtual void Concatenate(
        const TVector<TRichYPath>& sourcePaths,
        const TRichYPath& destinationPath,
        const TConcatenateOptions& options = TConcatenateOptions()) = 0;

    ///
    /// @brief Concatenate several tables into one.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#concatenate)
    virtual void Concatenate(
        const TVector<TYPath>& sourcePaths,
        const TYPath& destinationPath,
        const TConcatenateOptions& options = TConcatenateOptions());

    ///
    /// @brief Canonize YPath, moving all the complex YPath features to attributes.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#parse_ypath)
    virtual TRichYPath CanonizeYPath(const TRichYPath& path) = 0;

    ///
    /// @brief Get statistics for given sets of columns in given table ranges.
    ///
    /// @note Paths must contain column selectors.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_table_columnar_statistics)
    virtual TVector<TTableColumnarStatistics> GetTableColumnarStatistics(
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options = {}) = 0;

    ///
    /// @brief Divide input tables into disjoint partitions.
    ///
    /// Resulted partitions are vectors of rich YPaths.
    /// Each partition can be given to a separate worker for further independent processing.
    ///
    virtual TMultiTablePartitions GetTablePartitions(
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options) = 0;

    ///
    /// @brief Get file from file cache.
    ///
    /// @param md5Signature MD5 digest of the file.
    /// @param cachePath Path to the file cache.
    /// @param options Optional parameters.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#get_file_from_cache)
    virtual TMaybe<TYPath> GetFileFromCache(
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions()) = 0;

    ///
    /// @brief Put file to file cache.
    ///
    /// @param filePath Path in Cypress to the file to cache.
    /// @param md5Signature Expected MD5 digest of the file.
    /// @param cachePath Path to the file cache.
    /// @param options Optional parameters.
    ///
    /// @note The file in `filePath` must have been written with @ref NYT::TFileWriterOptions::ComputeMD5 set to `true`.
    ///
    /// @see [YT doc](https://ytsaurus.tech/docs/en/api/commands.html#put_file_to_cache)
    virtual TYPath PutFileToCache(
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = TPutFileToCacheOptions()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRowType>
TNodeId ICypressClient::CreateTable(
    const TYPath& path,
    const TSortColumns& sortColumns,
    const TCreateOptions& options)
{
    static_assert(
        std::is_base_of_v<::google::protobuf::Message, TRowType>,
        "TRowType must be inherited from google::protobuf::Message");

    TCreateOptions actualOptions = options;
    if (!actualOptions.Attributes_) {
        actualOptions.Attributes_ = TNode::CreateMap();
    }

    if (!actualOptions.Attributes_->HasKey("schema")) {
        actualOptions.Attributes_->AsMap().emplace(
            "schema",
            CreateTableSchema<TRowType>(sortColumns).ToNode());
    }

    return Create(path, ENodeType::NT_TABLE, actualOptions);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
