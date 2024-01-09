#pragma once

#include "client_common.h"

#include <yt/yt/core/ytree/attribute_filter.h>
#include <yt/yt/core/ytree/request_complexity_limits.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TReadCypressRequestOptions
{
    NYTree::TReadRequestComplexityOverrides ComplexityLimits;
};

struct TGetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMasterReadOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
    , public TReadCypressRequestOptions
{
    // NB(eshcherbin): Used in profiling Orchid.
    NYTree::IAttributeDictionaryPtr Options;
    NYTree::TAttributeFilter Attributes;
    std::optional<i64> MaxSize;
};

struct TSetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
};

struct TMultisetAttributesNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    bool Force = false;
};

struct TRemoveNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = true;
    bool Force = false;
};

struct TListNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMasterReadOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
    , public TReadCypressRequestOptions
{
    NYTree::TAttributeFilter Attributes;
    std::optional<i64> MaxSize;
};

struct TCreateNodeOptions
    : public TCreateObjectOptions
    , public TTransactionalOptions
{
    bool Recursive = false;
    bool LockExisting = false;
    bool Force = false;
    bool IgnoreTypeMismatch = false;
};

struct TLockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Waitable = false;
    std::optional<TString> ChildKey;
    std::optional<TString> AttributeKey;
};

struct TLockNodeResult
{
    NCypressClient::TLockId LockId;
    NCypressClient::TNodeId NodeId;
    NHydra::TRevision Revision = NHydra::NullRevision;
};

struct TUnlockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TCopyNodeOptionsBase
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = false;
    bool PreserveCreationTime = false;
    bool PreserveModificationTime = false;
    bool PreserveOwner = false;
    bool PreserveExpirationTime = false;
    bool PreserveExpirationTimeout = false;
    bool PreserveAcl = false;
    bool PessimisticQuotaCheck = true;
    bool EnableCrossCellCopying = true;
};

struct TCopyNodeOptions
    : public TCopyNodeOptionsBase
{
    bool IgnoreExisting = false;
    bool LockExisting = false;
};

struct TMoveNodeOptions
    : public TCopyNodeOptionsBase
{
    TMoveNodeOptions()
    {
        // COMPAT(babenko): YT-11903, consider dropping this override
        PreserveCreationTime = true;
    }
};

struct TLinkNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    //! Attributes of a newly created link node.
    NYTree::IAttributeDictionaryPtr Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
    bool LockExisting = false;
    bool Force = false;
};

struct TConcatenateNodesOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{
    NChunkClient::TFetcherConfigPtr ChunkMetaFetcherConfig;

    bool UniqualizeChunks = false;
};

struct TNodeExistsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{ };

struct TExternalizeNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
{ };

struct TInternalizeNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct ICypressClientBase
{
    virtual ~ICypressClientBase() = default;

    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = {}) = 0;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options = {}) = 0;

    virtual TFuture<void> MultisetAttributesNode(
        const NYPath::TYPath& path,
        const NYTree::IMapNodePtr& attributes,
        const TMultisetAttributesNodeOptions& options = {}) = 0;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = {}) = 0;

    virtual TFuture<TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = {}) = 0;

    virtual TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = {}) = 0;

    virtual TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options = {}) = 0;

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = {}) = 0;

    virtual TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options = {}) = 0;

    virtual TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

