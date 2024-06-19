#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

#include <any>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Structure representing a whitelist of attributes to be returned by get/list-like requests.
/*!
 *  A filter is defined by a collection of top-level keys and a collection of YPaths. Result
 *  is constructed as a union of subtrees defined by all keys and paths.
 *
 *  It is allowed for keys and paths to define intersecting or coinciding subtrees.
 *
 *  A special case of filter is universal filter, which admits all attributes. Note that in such case
 *  a particular YPath service may have its own policy whether to produce all attributes or not,
 *  e.g. Cypress documents produce all attributes, while Cypress nodes produce no attributes
 *  (i.e. act the same as non-universal empty filter).
 *
 *  In general, universal filter is treated as an unspecified filter. Default-constructed filter
 *  is universal; universal filter casts to boolean false value.
 *
 *  Example 1:
 *    Attributes = {
 *      foo = 42;
 *      bar = {x = 2; y = []};
 *      baz = {x = {a = 1; b = 2}; y = {a = 3; b = 4}};
 *    }
 *    Filter = {.Keys = {"bar"}; .Paths = {"/baz/y"}, .Universal = false}
 *    Result = {
 *      bar = {x = 2; y = []};
 *      baz = {y = {a = 3; b = 4}};
 *    }
 *
 *  Example 2:
 *    Attributes = {
 *      foo = [a; b; c; d];
 *    }
 *    Filter = {.Keys = {}; .Paths = {"/foo/0", "/foo/2"}, .Universal = false}
 *    Result = {
 *      foo = {a; #; c; #];
 *    }
 *
 *  Example 3:
 *    Attributes = {
 *      foo = 42;
 *      bar = baz;
 *    }
 *    Filter = {.Keys = {}; .Paths = {}, .Universal = false}
 *    Result = {}
 *
 *  Example 4:
 *    Attributes = {
 *      foo = 42;
 *      bar = baz;
 *    }
 *    Filter = {.Keys = {}; .Paths = {}, .Universal = true}
 *    Result depends on implementation.
 */
struct TAttributeFilter
{
    //! Whitelist of top-level keys to be returned.
    std::vector<TString> Keys;
    std::vector<NYPath::TYPath> Paths;

    //! If true, filter is universal, i.e. behavior depends on service's own policy;
    //! in such case #Keys and #Paths are always empty.
    bool Universal = true;

    //! Creates a universal filter.
    TAttributeFilter() = default;

    //! Creates a non-universal filter from given keys and paths.
    //! This constructor is intentionally non-explicit so that common idiom attributeFilter = {"foo", "bar"} works.
    TAttributeFilter(std::vector<TString> keys, std::vector<NYPath::TYPath> paths = {});

    TAttributeFilter& operator =(std::vector<TString> keys);

    //! Returns true for non-universal filter and false otherwise.
    explicit operator bool() const;

    //! Returns true for non-universal filter with empty keys and paths.
    bool IsEmpty() const;

    //! If #Paths are non-empty, throws an exception. Suitable for YPath service implementations
    //! that are not ready for by-path attribute filtering. Context argument allows customizing
    //! error message.
    void ValidateKeysOnly(TStringBuf context = "this context") const;

    //! Returns true if #key appears in Keys or "/#key" appears in Paths using linear search.
    bool AdmitsKeySlow(TStringBuf key) const;

    // std::nullopt stands for "take the whole attribute without path filtration" (i.e. an equivalent of {""}).
    using TPathFilter = std::optional<std::vector<TYPath>>;
    using TKeyToFilter = THashMap<TString, TPathFilter>;

    //! Normalization procedure removes redundant keys or paths and returns a mapping of form
    //! top-level key -> list of YPaths inside this top-level key (i.e. with /<key> stripped off) or
    //! to std::nullopt, which stands for "take the top-level key as is".
    TKeyToFilter Normalize() const;

    //! This helper structure enabling us to either return given IYsonConsumer* as is
    //! without creating any new consumers, or to wrap it into another consumer actual
    //! filtering.
    struct IFilteringConsumer
    {
        virtual ~IFilteringConsumer() = default;
        //! Returns the sync consumer to be used for filtering.
        virtual NYson::IYsonConsumer* GetConsumer() = 0;
        //! Call of this method indicates that the whole input is fed to a filtering consumer and it is
        //! safe to transfer the filtered result to the target consumer.
        virtual void Finish() = 0;
    };

    //! Similar as above, but suitable for asynchronous consumer interface.
    struct IAsyncFilteringConsumer
    {
        virtual ~IAsyncFilteringConsumer() = default;
        //! Returns the sync consumer to be used for filtering.
        virtual NYson::IAsyncYsonConsumer* GetAsyncConsumer() = 0;
        //! Call of this method indicates that the whole input is fed to a filtering consumer and it is
        //! safe to transfer the filtered result to the target consumer.
        virtual void Finish() = 0;
    };

    //! Performs a one-pass filtration of YSON stream according to the given path filter and produces
    //! the result to the #targetConsumer as a value corresponding to a key #key.
    //! In particular, if #pathFilter is an std::nullopt, #targetConsumer is returned as-is effectively
    //! implementing a zero-cost bypassing.
    static std::unique_ptr<IFilteringConsumer> CreateFilteringConsumer(
        NYson::IYsonConsumer* targetConsumer,
        const TPathFilter& pathFilter);

    //! Same as CreateFilteringConsumer above, but asynchronous.
    static std::unique_ptr<IAsyncFilteringConsumer> CreateAsyncFilteringConsumer(
        NYson::IAsyncYsonConsumer* targetConsumer,
        const TPathFilter& pathFilter);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter);
void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter);

void Serialize(const TAttributeFilter& filter, NYson::IYsonConsumer* consumer);
void Deserialize(TAttributeFilter& filter, const INodePtr& node);
void Deserialize(TAttributeFilter& attributeFilter, NYson::TYsonPullParserCursor* cursor);

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeFilter& attributeFilter,
    TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
