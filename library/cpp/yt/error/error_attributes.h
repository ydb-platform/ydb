#pragma once

#include "convert_to_cpo.h"
#include "mergeable_dictionary.h"

#include <library/cpp/yt/misc/optional.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// For now this is just an opaque handle to error attributes
// used to remove dependency on IAttributeDictionary in public API.
// Eventually it would be a simple hash map.
// NB(arkady-e1ppa): For now most methods are defined in yt/yt/core/misc/stripped_error.cpp
// eventually they will be moved here.
// NB(arkady-e1ppa): For now we use TYsonString as value.
// TODO(arkady-e1ppa): Try switching to TString/std::string eventually.
// representing text-encoded yson string eventually (maybe).
class TErrorAttributes
{
public:
    using TKey = TString;
    using TValue = NYson::TYsonString;
    using TKeyValuePair = std::pair<TKey, TValue>;

    //! Returns the list of all keys in the dictionary.
    std::vector<TString> ListKeys() const;

    //! Returns the list of all key-value pairs in the dictionary.
    std::vector<TKeyValuePair> ListPairs() const;

    //! Returns the value of the attribute (null indicates that the attribute is not found).
    NYson::TYsonString FindYson(TStringBuf key) const;

    //! Sets the value of the attribute.
    void SetYson(const TString& key, const NYson::TYsonString& value);

    //! Removes the attribute.
    //! Returns |true| if the attribute was removed or |false| if there is no attribute with this key.
    bool Remove(const TString& key);

    //! Removes all attributes.
    void Clear();

    //! Returns the value of the attribute (throws an exception if the attribute is not found).
    NYson::TYsonString GetYson(TStringBuf key) const;

    //! Same as #GetYson but removes the value.
    NYson::TYsonString GetYsonAndRemove(const TString& key);

    //! Returns |true| iff the given key is present.
    bool Contains(TStringBuf key) const;

    // TODO(arkady-e1ppa): By default deserialization is located at yt/core
    // consider using deserialization of some default types (guid, string, int, double)
    // to be supported and everything else not supported without inclusion of yt/core.
    //! Finds the attribute and deserializes its value.
    //! Throws if no such value is found.
    template <class T>
        requires CConvertToWorks<T, TValue>
    T Get(TStringBuf key) const;

    //! Same as #Get but removes the value.
    template <class T>
        requires CConvertToWorks<T, TValue>
    T GetAndRemove(const TString& key);

    //! Finds the attribute and deserializes its value.
    //! Uses default value if no such attribute is found.
    template <class T>
        requires CConvertToWorks<T, TValue>
    T Get(TStringBuf key, const T& defaultValue) const;

    //! Same as #Get but removes the value if it exists.
    template <class T>
        requires CConvertToWorks<T, TValue>
    T GetAndRemove(const TString& key, const T& defaultValue);

    //! Finds the attribute and deserializes its value.
    //! Returns null if no such attribute is found.
    template <class T>
        requires CConvertToWorks<T, TValue>
    typename TOptionalTraits<T>::TOptional Find(TStringBuf key) const;

    //! Same as #Find but removes the value if it exists.
    template <class T>
        requires CConvertToWorks<T, TValue>
    typename TOptionalTraits<T>::TOptional FindAndRemove(const TString& key);

    template <CMergeableDictionary TDictionary>
    void MergeFrom(const TDictionary& dict);

private:
    void* Attributes_; // IAttributesDictionary*

    friend class TErrorOr<void>;
    explicit TErrorAttributes(void* attributes);

    TErrorAttributes(const TErrorAttributes& other) = default;
    TErrorAttributes& operator= (const TErrorAttributes& other) = default;

    TErrorAttributes(TErrorAttributes&& other) = default;
    TErrorAttributes& operator= (TErrorAttributes&& other) = default;

    // defined in yt/yt/core/misc/stripped_error.cpp right now.
    [[noreturn]] static void ThrowCannotParseAttributeException(TStringBuf key, const std::exception& ex);
};

bool operator == (const TErrorAttributes& lhs, const TErrorAttributes& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ERROR_ATTRIBUTES_INL_H_
#include "error_attributes-inl.h"
#undef ERROR_ATTRIBUTES_INL_H_
