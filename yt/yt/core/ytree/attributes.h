#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/misc/optional.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeDictionary
    : public TRefCounted
{
    using TKey = std::string;
    using TKeyView = TStringBuf;
    using TValue = NYson::TYsonString;
    using TKeyValuePair = std::pair<TKey, TValue>;

    //! Returns the list of all keys in the dictionary.
    virtual std::vector<TKey> ListKeys() const = 0;

    //! Returns the list of all key-value pairs in the dictionary.
    virtual std::vector<TKeyValuePair> ListPairs() const = 0;

    //! Returns the value of the attribute (null indicates that the attribute is not found).
    virtual TValue FindYson(TKeyView key) const = 0;

    //! Sets the value of the attribute.
    virtual void SetYson(TKeyView key, const TValue& value) = 0;

    //! Removes the attribute.
    //! Returns |true| if the attribute was removed or |false| if there is no attribute with this key.
    virtual bool Remove(TKeyView key) = 0;

    // Extension methods

    //! Removes all attributes.
    void Clear();

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    TValue GetYson(TKeyView key) const;

    //! Same as #GetYson but removes the value.
    TValue GetYsonAndRemove(TKeyView key);

    //! Finds the attribute and deserializes its value.
    //! Throws if no such value is found.
    template <class T>
    T Get(TKeyView key) const;

    //! Same as #Get but removes the value.
    template <class T>
    T GetAndRemove(TKeyView key);

    //! Finds the attribute and deserializes its value.
    //! Uses default value if no such attribute is found.
    template <class T>
    T Get(TKeyView key, const T& defaultValue) const;

    //! Same as #Get but removes the value if it exists.
    template <class T>
    T GetAndRemove(TKeyView key, const T& defaultValue);

    //! Finds the attribute and deserializes its value.
    //! Returns null if no such attribute is found.
    template <class T>
    typename TOptionalTraits<T>::TOptional Find(TKeyView key) const;

    //! Same as #Find but removes the value if it exists.
    template <class T>
    typename TOptionalTraits<T>::TOptional FindAndRemove(TKeyView key);

    //! Returns |true| iff the given key is present.
    bool Contains(TKeyView key) const;

    //! Sets the attribute with a serialized value.
    template <class T>
    void Set(TKeyView key, const T& value);

    //! Constructs an instance from a map node (by serializing the values).
    static IAttributeDictionaryPtr FromMap(const IMapNodePtr& node);

    //! Converts attributes to map node.
    IMapNodePtr ToMap() const;

    //! Adds more attributes from another map node.
    void MergeFrom(const IMapNodePtr& other);

    //! Adds more attributes from another attribute dictionary.
    void MergeFrom(const IAttributeDictionary& other);

    //! Constructs an ephemeral copy.
    IAttributeDictionaryPtr Clone() const;
};

DEFINE_REFCOUNTED_TYPE(IAttributeDictionary)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree


#define ATTRIBUTES_INL_H_
#include "attributes-inl.h"
#undef ATTRIBUTES_INL_H_
