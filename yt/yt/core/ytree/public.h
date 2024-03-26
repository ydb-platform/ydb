#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TAttributeDictionary;
class TAttributeFilter;

} // namespace NProto

struct TAttributeFilter;

struct IYsonStructMeta;
class TYsonStructBase;
class TYsonStructLite;

template <class T>
concept CYsonStructDerived = std::derived_from<T, TYsonStructBase>;

DECLARE_REFCOUNTED_STRUCT(INode)
using IConstNodePtr = TIntrusivePtr<const INode>;
DECLARE_REFCOUNTED_STRUCT(ICompositeNode)
using ICompositeNodePtr = TIntrusivePtr<ICompositeNode>;
DECLARE_REFCOUNTED_STRUCT(IStringNode)
DECLARE_REFCOUNTED_STRUCT(IInt64Node)
DECLARE_REFCOUNTED_STRUCT(IUint64Node)
DECLARE_REFCOUNTED_STRUCT(IDoubleNode)
DECLARE_REFCOUNTED_STRUCT(IBooleanNode)
DECLARE_REFCOUNTED_STRUCT(IListNode)
DECLARE_REFCOUNTED_STRUCT(IMapNode)
DECLARE_REFCOUNTED_STRUCT(IEntityNode)

struct INodeFactory;
struct ITransactionalNodeFactory;

DECLARE_REFCOUNTED_STRUCT(IAttributeDictionary)

struct IAttributeOwner;

struct ISystemAttributeProvider;

DECLARE_REFCOUNTED_STRUCT(IYPathService)
DECLARE_REFCOUNTED_STRUCT(IYPathServiceContext)
DECLARE_REFCOUNTED_STRUCT(ICachedYPathService)
DECLARE_REFCOUNTED_CLASS(TCompositeMapService)

DECLARE_REFCOUNTED_CLASS(TYPathRequest)
DECLARE_REFCOUNTED_CLASS(TYPathResponse)

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathRequest;

template <class TRequestMessage, class TResponseMessage>
class TTypedYPathResponse;

DECLARE_REFCOUNTED_CLASS(TServiceCombiner)

using NYPath::TYPath;

//! Default limit for List and Get requests to virtual nodes.
constexpr i64 DefaultVirtualChildLimit = 1000;

//! The global limit for the number of resolve iterations in #ResolveYPath.
//! This effectively bounds the maximum depth of YPath the system can handle.
//! Also this protects us from infinite cycles in resolution (which can be caused,
//! e.g., by cyclic symlinks in Cypress).
//! NB: Changing this value will invalidate all changelogs!
constexpr int MaxYPathResolveIterations = 256;

DECLARE_REFCOUNTED_CLASS(TYsonStruct)

DECLARE_REFCOUNTED_CLASS(TYPathServiceContextWrapper)

template <class TRequestMessage, class TResponseMessage>
using TTypedYPathServiceContext = NRpc::TGenericTypedServiceContext<
    IYPathServiceContext,
    TYPathServiceContextWrapper,
    TRequestMessage,
    TResponseMessage
>;

////////////////////////////////////////////////////////////////////////////////

//! A static node type.
DEFINE_ENUM(ENodeType,
    // Node contains a string (TString).
    (String)
    // Node contains an int64 number (i64).
    (Int64)
    // Node contains an uint64 number (ui64).
    (Uint64)
    // Node contains an FP number (double).
    (Double)
    // Node contains an boolean (bool).
    (Boolean)
    // Node contains a map from strings to other nodes.
    (Map)
    // Node contains a list (vector) of other nodes.
    (List)
    // Node is atomic, i.e. has no visible properties (aside from attributes).
    (Entity)
    // Either List or Map.
    (Composite)
);

YT_DEFINE_ERROR_ENUM(
    ((ResolveError)                        (500))
    ((AlreadyExists)                       (501))
    ((MaxChildCountViolation)              (502))
    ((MaxStringLengthViolation)            (503))
    ((MaxAttributeSizeViolation)           (504))
    ((MaxKeyLengthViolation)               (505))
);

////////////////////////////////////////////////////////////////////////////////

struct TReadRequestComplexity;
struct TReadRequestComplexityOverrides;

DECLARE_REFCOUNTED_CLASS(TReadRequestComplexityLimiter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
