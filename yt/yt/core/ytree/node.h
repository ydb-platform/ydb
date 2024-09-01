#pragma once

#include "public.h"
#include "attribute_owner.h"
#include "ypath_service.h"

#include <yt/yt/core/misc/mpl.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! A base DOM-like interface representing a node.
struct INode
    : public virtual IYPathService
    , public virtual IAttributeOwner
{
    //! Returns the static type of the node.
    virtual ENodeType GetType() const = 0;

    //! Returns a new instance of transactional factory for creating new nodes.
    /*!
     *  Every YTree implementation provides its own set of
     *  node implementations. E.g., for an ephemeral implementation
     *  this factory creates ephemeral nodes while for
     *  a persistent implementation (see Cypress) this factory
     *  creates persistent nodes.
     */
    virtual std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const = 0;

    //! Returns a YPath for this node.
    virtual TYPath GetPath() const = 0;

    // A bunch of "AsSomething" methods that return a pointer
    // to the same node but typed as "Something".
    // These methods throw an exception on type mismatch.
#define DECLARE_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() = 0; \
    virtual TIntrusivePtr<const I##name##Node> As##name() const = 0;

    DECLARE_AS_METHODS(Entity)
    DECLARE_AS_METHODS(Composite)
    DECLARE_AS_METHODS(String)
    DECLARE_AS_METHODS(Int64)
    DECLARE_AS_METHODS(Uint64)
    DECLARE_AS_METHODS(Double)
    DECLARE_AS_METHODS(Boolean)
    DECLARE_AS_METHODS(List)
    DECLARE_AS_METHODS(Map)
#undef DECLARE_AS_METHODS

    //! Returns the parent of the node.
    //! |nullptr| indicates that the current node is the root.
    virtual ICompositeNodePtr GetParent() const = 0;

    //! Sets the parent of the node.
    /*!
     *  This method is called automatically when one subtree (possibly)
     *  consisting of a single node is attached to another.
     *
     *  This method must not be called explicitly.
     */
    virtual void SetParent(const ICompositeNodePtr& parent) = 0;

    // Extension methods.

    //! Converts node value to a given type.
    template <class T>
    T GetValue() const;
};

DEFINE_REFCOUNTED_TYPE(INode)

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all scalar nodes, i.e. nodes containing a single atomic value.
template <class T>
struct IScalarNode
    : public virtual INode
{
    using TValue = T;

    //! Gets the value.
    virtual typename NMpl::TCallTraits<TValue>::TType GetValue() const = 0;

    //! Sets the value.
    virtual void SetValue(typename NMpl::TCallTraits<TValue>::TType value) = 0;
};

#define ITERATE_SCALAR_YTREE_NODE_TYPES(XX) \
    XX(Int64, i64) \
    XX(Uint64, ui64) \
    XX(Double, double) \
    XX(Boolean, bool) \
    XX(String, TString)

////////////////////////////////////////////////////////////////////////////////

//! String node.
struct IStringNode
    : public IScalarNode<TString>
{ };

DEFINE_REFCOUNTED_TYPE(IStringNode)

//! Int64 node.
struct IInt64Node
    : public IScalarNode<i64>
{ };

DEFINE_REFCOUNTED_TYPE(IInt64Node)

//! Uint64 node.
struct IUint64Node
    : public IScalarNode<ui64>
{ };

DEFINE_REFCOUNTED_TYPE(IUint64Node)

//! Double node.
struct IDoubleNode
    : public IScalarNode<double>
{ };

DEFINE_REFCOUNTED_TYPE(IDoubleNode)

//! Boolean node.
struct IBooleanNode
    : public IScalarNode<bool>
{ };

DEFINE_REFCOUNTED_TYPE(IBooleanNode)

////////////////////////////////////////////////////////////////////////////////

//! A base interface for all composite nodes, i.e. nodes containing other nodes.
struct ICompositeNode
    : public virtual INode
{
    //! Removes all child nodes.
    virtual void Clear() = 0;

    //! Returns the number of child nodes.
    virtual int GetChildCount() const = 0;

    //! Replaces one child by the other.
    //! #newChild must be a root.
    virtual void ReplaceChild(const INodePtr& oldChild, const INodePtr& newChild) = 0;

    //! Removes a child.
    //! The removed child becomes a root.
    virtual void RemoveChild(const INodePtr& child) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompositeNode)

////////////////////////////////////////////////////////////////////////////////

//! A map node, which maps keys (std::string) to child nodes.
struct IMapNode
    : public virtual ICompositeNode
{
    using ICompositeNode::RemoveChild;

    //! Returns the current snapshot of the map.
    /*!
     *  Map items are returned in unspecified order.
     */
    virtual std::vector<std::pair<std::string, INodePtr>> GetChildren() const = 0;

    //! Returns map keys.
    /*!
     *  Keys are returned in unspecified order.
     */
    virtual std::vector<std::string> GetKeys() const = 0;

    //! Gets a child by its key.
    /*!
     *  \param key A key.
     *  \return A child with the given #key or null if no child with the given #key exists.
     */
    virtual INodePtr FindChild(const std::string& key) const = 0;

    //! Adds a new child with a given key.
    /*!
     *  \param child A child.
     *  \param key A key.
     *  \return True iff the key was not in the map already and thus the child is inserted.
     *
     *  \note
     *  #child must be a root.
     */
    virtual bool AddChild(const std::string& key, const INodePtr& child) = 0;

    //! Removes a child by its key.
    /*!
     *  \param key A key.
     *  \return True iff there was a child with the given key.
     */
    virtual bool RemoveChild(const std::string& key) = 0;

    //! Similar to #FindChild but throws if no child is found.
    INodePtr GetChildOrThrow(const std::string& key) const;

    //! Returns the key for a given child.
    /*!
     *  \param child A possible child.
     *  \return Child's key or null if the node is not a child.
     */
    virtual std::optional<std::string> FindChildKey(const IConstNodePtr& child) = 0;

    //! Returns the key for a given child or throws if the node is not a child.
    /*!
     *  \param child A possible child.
     *  \return Child's key.
     */
    std::string GetChildKeyOrThrow(const IConstNodePtr& child);

    // Extension methods.

    //! Converts the value of the child with #key to a given type.
    //! Throws if no child with #key exists.
    template <class T>
    T GetChildValueOrThrow(const std::string& key) const;

    //! Converts the value of the child with #key to a given type.
    //! Returns #defaultValue if no child with #key exists.
    template <class T>
    T GetChildValueOrDefault(const std::string& key, const T& defaultValue) const;

    //! Converts the value of the child with #key to a given type.
    //! Returns null if no child with #key exists.
    template <class T>
    std::optional<T> FindChildValue(const std::string& key) const;
};

DEFINE_REFCOUNTED_TYPE(IMapNode)

////////////////////////////////////////////////////////////////////////////////

//! A list node, which keeps a list (vector) of children.
struct IListNode
    : public virtual ICompositeNode
{
    using ICompositeNode::RemoveChild;

    //! Returns the current snapshot of the list.
    virtual std::vector<INodePtr> GetChildren() const = 0;

    //! Gets a child by its index.
    /*!
     *  \param index An index.
     *  \return A child with the given index or NULL if the index is not valid.
     */
    virtual INodePtr FindChild(int index) const = 0;

    //! Adds a new child at a given position.
    /*!
     *  \param child A child.
     *  \param beforeIndex A position before which the insertion must happen.
     *  -1 indicates the end of the list.
     *
     *  \note
     *  #child must be a root.
     */

    virtual void AddChild(const INodePtr& child, int beforeIndex = -1) = 0;

    //! Removes a child by its index.
    /*!
     *  \param index An index.
     *  \return True iff the index is valid and thus the child is removed.
     */
    virtual bool RemoveChild(int index) = 0;

    //! Similar to #FindChild but throws if the index is not valid.
    INodePtr GetChildOrThrow(int index) const;

    //! Returns the index for a given child or null if the node is not a child.
    /*!
     *  \param child A node that must be a child.
     *  \return Child's index or null if the node is not a child.
     */
    virtual std::optional<int> FindChildIndex(const IConstNodePtr& child) = 0;

    //! Returns the index for a given child or throws if the node is not a child.
    /*!
     *  \param child A node that must be a child.
     *  \return Child's index.
     */
    int GetChildIndexOrThrow(const IConstNodePtr& child);

    //! Normalizes negative indexes (by adding child count).
    //! Throws if the index is invalid.
    /*!
     *  \param index Original (possibly negative) index.
     *  \returns Adjusted (valid non-negative) index.
     */
    int AdjustChildIndexOrThrow(int index) const;

    // Extension methods.

    //! Converts the value of the child with #index to a given type.
    //! Throws if no child with #key exists.
    template <class T>
    T GetChildValueOrThrow(int index) const;

    //! Converts the value of the child with #index to a given type.
    //! Returns #defaultValue if no child with #key exists.
    template <class T>
    T GetChildValueOrDefault(int index, const T& defaultValue) const;

    //! Converts the value of the child with #index to a given type.
    //! Returns null if no child with #key exists.
    template <class T>
    std::optional<T> FindChildValue(int index) const;
};

DEFINE_REFCOUNTED_TYPE(IListNode)

////////////////////////////////////////////////////////////////////////////////

//! An structureless entity node.
struct IEntityNode
    : public virtual INode
{ };

DEFINE_REFCOUNTED_TYPE(IEntityNode)

////////////////////////////////////////////////////////////////////////////////

//! A factory for creating nodes.
/*!
 *  All freshly created nodes are roots, i.e. have no parent.
 */
struct INodeFactory
{
    virtual ~INodeFactory() = default;

    //! Creates a string node.
    virtual IStringNodePtr CreateString() = 0;

    //! Creates an int64 node.
    virtual IInt64NodePtr CreateInt64() = 0;

    //! Creates an uint64 node.
    virtual IUint64NodePtr CreateUint64() = 0;

    //! Creates an FP number node.
    virtual IDoubleNodePtr CreateDouble() = 0;

    //! Creates an boolean node.
    virtual IBooleanNodePtr CreateBoolean() = 0;

    //! Creates a map node.
    virtual IMapNodePtr CreateMap() = 0;

    //! Creates a list node.
    virtual IListNodePtr CreateList() = 0;

    //! Creates an entity node.
    virtual IEntityNodePtr CreateEntity() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! A node factory with extended transactional capabilities.
/*!
 *  The factory also acts as a "transaction context" that holds all created nodes.
 *
 *  One must call #Commit at the end if the operation was a success.
 *  Releasing the instance without calling #Commit or calling #Rollback abandons all changes.
 */
struct ITransactionalNodeFactory
    : public INodeFactory
{
    //! Must be called before releasing the factory to indicate that all created nodes
    //! must persist.
    virtual void Commit() = 0;

    //! Invokes all rollback handlers.
    virtual void Rollback() = 0;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const INode& value, NYson::IYsonConsumer* consumer);
void Deserialize(INodePtr& value, const INodePtr& node);
void Deserialize(IStringNodePtr& value, const INodePtr& node);
void Deserialize(IInt64NodePtr& value, const INodePtr& node);
void Deserialize(IUint64NodePtr& value, const INodePtr& node);
void Deserialize(IDoubleNodePtr& value, const INodePtr& node);
void Deserialize(IBooleanNodePtr& value, const INodePtr& node);
void Deserialize(IMapNodePtr& value, const INodePtr& node);
void Deserialize(IListNodePtr& value, const INodePtr& node);
void Deserialize(IEntityNodePtr& value, const INodePtr& node);

void Deserialize(INodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IStringNodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IInt64NodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IUint64NodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IDoubleNodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IBooleanNodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IMapNodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IListNodePtr& value, NYson::TYsonPullParserCursor* cursor);
void Deserialize(IEntityNodePtr& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define NODE_INL_H_
#include "node-inl.h"
#undef NODE_INL_H_
