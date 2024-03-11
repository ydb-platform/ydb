#pragma once

///
/// @file yt/cpp/mapreduce/interface/fluent.h
///
/// Adapters for working with @ref NYson::IYsonConsumer in a structured way, with compile-time syntax checks.
///
/// The following documentation is copied verbatim from `yt/core/ytree/fluent.h`.
///
/// WHAT IS THIS
///
/// Fluent adapters encapsulate invocation of IYsonConsumer methods in a
/// convenient structured manner. Key advantage of fluent-like code is that
/// attempt of building syntactically incorrect YSON structure will result
/// in a compile-time error.
///
/// Each fluent object is associated with a context that defines possible YSON
/// tokens that may appear next. For example, TFluentMap is a fluent object
/// that corresponds to a location within YSON map right before a key-value
/// pair or the end of the map.
///
/// More precisely, each object that may be obtained by a sequence of fluent
/// method calls has the full history of its enclosing YSON composite types in
/// its single template argument hereinafter referred to as TParent. This allows
/// us not to forget the original context after opening and closing the embedded
/// composite structure.
///
/// It is possible to invoke a separate YSON building procedure by calling
/// one of convenience Do* methods. There are two possibilities here: it is
/// possible to delegate invocation context either as a fluent object (like
/// TFluentMap, TFluentList, TFluentAttributes or TFluentAny) or as a raw
/// IYsonConsumer*. The latter is discouraged since it is impossible to check
/// if a given side-built YSON structure fits current fluent context.
/// For example it is possible to call Do() method inside YSON map passing
/// consumer to a procedure that will treat context like it is in a list.
/// Passing typed fluent builder saves you from such a misbehaviour.
///
/// TFluentXxx corresponds to an internal class of TXxx
/// without any history hidden in template argument. It allows you to
/// write procedures of form:
///
///   void BuildSomeAttributesInYson(TFluentMap fluent) { ... }
///
/// without thinking about the exact way how this procedure is nested in other
/// procedures.
///
/// An important notation: we will refer to a function whose first argument
/// is TFluentXxx as TFuncXxx.
///
///
/// BRIEF LIST OF AVAILABLE METHODS
///
/// Only the most popular methods are covered here. Refer to the code for the
/// rest of them.
///
/// TAny:
/// * Value(T value) -> TParent, serialize `value` using underlying consumer.
///   T should be such that free function Serialize(NYson::IYsonConsumer*, const T&) is
///   defined;
/// * BeginMap() -> TFluentMap, open map;
/// * BeginList() -> TFluentList, open list;
/// * BeginAttributes() -> TFluentAttributes, open attributes;
///
/// * Do(TFuncAny func) -> TAny, delegate invocation to a separate procedure.
/// * DoIf(bool condition, TFuncAny func) -> TAny, same as Do() but invoke
///   `func` only if `condition` is true;
/// * DoFor(TCollection collection, TFuncAny func) -> TAny, same as Do()
///   but iterate over `collection` and pass each of its elements as a second
///   argument to `func`. Instead of passing a collection you may it is possible
///   to pass two iterators as an argument;
///
/// * DoMap(TFuncMap func) -> TAny, open a map, delegate invocation to a separate
///   procedure and close map;
/// * DoMapFor(TCollection collection, TFuncMap func) -> TAny, open a map, iterate
///   over `collection` and pass each of its elements as a second argument to `func`
///   and close map;
/// * DoList(TFuncList func) -> TAny, same as DoMap();
/// * DoListFor(TCollection collection, TFuncList func) -> TAny; same as DoMapFor().
///
///
/// TFluentMap:
/// * Item(TStringBuf key) -> TAny, open an element keyed with `key`;
/// * EndMap() -> TParent, close map;
/// * Do(TFuncMap func) -> TFluentMap, same as Do() for TAny;
/// * DoIf(bool condition, TFuncMap func) -> TFluentMap, same as DoIf() for TAny;
/// * DoFor(TCollection collection, TFuncMap func) -> TFluentMap, same as DoFor() for TAny.
///
///
/// TFluentList:
/// * Item() -> TAny, open an new list element;
/// * EndList() -> TParent, close list;
/// * Do(TFuncList func) -> TFluentList, same as Do() for TAny;
/// * DoIf(bool condition, TFuncList func) -> TFluentList, same as DoIf() for TAny;
/// * DoFor(TCollection collection, TListMap func) -> TFluentList, same as DoFor() for TAny.
///
///
/// TFluentAttributes:
/// * Item(TStringBuf key) -> TAny, open an element keyed with `key`.
/// * EndAttributes() -> TParentWithoutAttributes, close attributes. Note that
///   this method leads to a context that is forces not to have attributes,
///   preventing us from putting attributes twice before an object.
/// * Do(TFuncAttributes func) -> TFluentAttributes, same as Do() for TAny;
/// * DoIf(bool condition, TFuncAttributes func) -> TFluentAttributes, same as DoIf()
///   for TAny;
/// * DoFor(TCollection collection, TListAttributes func) -> TFluentAttributes, same as DoFor()
///   for TAny.
///


#include "common.h"
#include "serialize.h"

#include <library/cpp/yson/node/serialize.h>
#include <library/cpp/yson/node/node_builder.h>

#include <library/cpp/yson/consumer.h>
#include <library/cpp/yson/writer.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFluentYsonUnwrapper
{
    using TUnwrapped = T;

    static TUnwrapped Unwrap(T t)
    {
        return std::move(t);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFluentYsonVoid
{ };

template <>
struct TFluentYsonUnwrapper<TFluentYsonVoid>
{
    using TUnwrapped = void;

    static TUnwrapped Unwrap(TFluentYsonVoid)
    { }
};

////////////////////////////////////////////////////////////////////////////////

/// This class is actually a namespace for specific fluent adapter classes.
class TFluentYsonBuilder
    : private TNonCopyable
{
private:
    template <class T>
    static void WriteValue(NYT::NYson::IYsonConsumer* consumer, const T& value)
    {
        Serialize(value, consumer);
    }

public:
    class TFluentAny;
    template <class TParent> class TAny;
    template <class TParent> class TToAttributes;
    template <class TParent> class TAttributes;
    template <class TParent> class TListType;
    template <class TParent> class TMapType;

    /// Base class for all fluent adapters.
    template <class TParent>
    class TFluentBase
    {
    public:
        /// Implicit conversion to yson consumer
        operator NYT::NYson::IYsonConsumer* () const
        {
            return Consumer;
        }

    protected:
        /// @cond Doxygen_Suppress
        NYT::NYson::IYsonConsumer* Consumer;
        TParent Parent;

        TFluentBase(NYT::NYson::IYsonConsumer* consumer, TParent parent)
            : Consumer(consumer)
            , Parent(std::move(parent))
        { }

        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        TUnwrappedParent GetUnwrappedParent()
        {
            return TFluentYsonUnwrapper<TParent>::Unwrap(std::move(Parent));
        }
        /// @endcond Doxygen_Suppress
    };

    /// Base class for fluent adapters for fragment of list, map or attributes.
    template <template <class TParent> class TThis, class TParent>
    class TFluentFragmentBase
        : public TFluentBase<TParent>
    {
    public:
        using TDeepThis = TThis<TParent>;
        using TShallowThis = TThis<TFluentYsonVoid>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TFluentFragmentBase(NYT::NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

        /// Delegate invocation to a separate procedure.
        template <class TFunc>
        TDeepThis& Do(const TFunc& func)
        {
            func(TShallowThis(this->Consumer));
            return *static_cast<TDeepThis*>(this);
        }

        /// Conditionally delegate invocation to a separate procedure.
        template <class TFunc>
        TDeepThis& DoIf(bool condition, const TFunc& func)
        {
            if (condition) {
                func(TShallowThis(this->Consumer));
            }
            return *static_cast<TDeepThis*>(this);
        }

        /// Calls `func(*this, element)` for each `element` in range `[begin, end)`.
        template <class TFunc, class TIterator>
        TDeepThis& DoFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            for (auto current = begin; current != end; ++current) {
                func(TShallowThis(this->Consumer), current);
            }
            return *static_cast<TDeepThis*>(this);
        }

        /// Calls `func(*this, element)` for each `element` in `collection`.
        template <class TFunc, class TCollection>
        TDeepThis& DoFor(const TCollection& collection, const TFunc& func)
        {
            for (const auto& item : collection) {
                func(TShallowThis(this->Consumer), item);
            }
            return *static_cast<TDeepThis*>(this);
        }

    };

    /// Fluent adapter of a value without attributes.
    template <class TParent>
    class TAnyWithoutAttributes
        : public TFluentBase<TParent>
    {
    public:
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        TAnyWithoutAttributes(NYT::NYson::IYsonConsumer* consumer, TParent parent)
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

        /// Pass `value` to underlying consumer.
        template <class T>
        TUnwrappedParent Value(const T& value)
        {
            WriteValue(this->Consumer, value);
            return this->GetUnwrappedParent();
        }

        /// Call `OnEntity()` of underlying consumer.
        TUnwrappedParent Entity()
        {
            this->Consumer->OnEntity();
            return this->GetUnwrappedParent();
        }

        /// Serialize `collection` to underlying consumer as a list.
        template <class TCollection>
        TUnwrappedParent List(const TCollection& collection)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                this->Consumer->OnListItem();
                WriteValue(this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        /// Serialize maximum `maxSize` elements of `collection` to underlying consumer as a list.
        template <class TCollection>
        TUnwrappedParent ListLimited(const TCollection& collection, size_t maxSize)
        {
            this->Consumer->OnBeginAttributes();
            this->Consumer->OnKeyedItem("count");
            this->Consumer->OnInt64Scalar(collection.size());
            this->Consumer->OnEndAttributes();
            this->Consumer->OnBeginList();
            size_t printedSize = 0;
            for (const auto& item : collection) {
                if (printedSize >= maxSize)
                    break;
                this->Consumer->OnListItem();
                WriteValue(this->Consumer, item);
                ++printedSize;
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        /// Open a list.
        TListType<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TListType<TParent>(this->Consumer, this->Parent);
        }

        /// Open a list, delegate invocation to `func`, then close the list.
        template <class TFunc>
        TUnwrappedParent DoList(const TFunc& func)
        {
            this->Consumer->OnBeginList();
            func(TListType<TFluentYsonVoid>(this->Consumer));
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        /// Open a list, call `func(*this, element)` for each `element` of range, then close the list.
        template <class TFunc, class TIterator>
        TUnwrappedParent DoListFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                func(TListType<TFluentYsonVoid>(this->Consumer), current);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        /// Open a list, call `func(*this, element)` for each `element` of `collection`, then close the list.
        template <class TFunc, class TCollection>
        TUnwrappedParent DoListFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                func(TListType<TFluentYsonVoid>(this->Consumer), item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        /// Open a map.
        TMapType<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TMapType<TParent>(this->Consumer, this->Parent);
        }

        /// Open a map, delegate invocation to `func`, then close the map.
        template <class TFunc>
        TUnwrappedParent DoMap(const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            func(TMapType<TFluentYsonVoid>(this->Consumer));
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        /// Open a map, call `func(*this, element)` for each `element` of range, then close the map.
        template <class TFunc, class TIterator>
        TUnwrappedParent DoMapFor(const TIterator& begin, const TIterator& end, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                func(TMapType<TFluentYsonVoid>(this->Consumer), current);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        /// Open a map, call `func(*this, element)` for each `element` of `collection`, then close the map.
        template <class TFunc, class TCollection>
        TUnwrappedParent DoMapFor(const TCollection& collection, const TFunc& func)
        {
            this->Consumer->OnBeginMap();
            for (const auto& item : collection) {
                func(TMapType<TFluentYsonVoid>(this->Consumer), item);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };

    /// Fluent adapter of any value.
    template <class TParent>
    class TAny
        : public TAnyWithoutAttributes<TParent>
    {
    public:
        using TBase = TAnyWithoutAttributes<TParent>;

        explicit TAny(NYT::NYson::IYsonConsumer* consumer, TParent parent)
            : TBase(consumer, std::move(parent))
        { }

        /// Open attributes.
        TAttributes<TBase> BeginAttributes()
        {
            this->Consumer->OnBeginAttributes();
            return TAttributes<TBase>(
                this->Consumer,
                TBase(this->Consumer, this->Parent));
        }
    };

    /// Fluent adapter of attributes fragment (the inside part of attributes).
    template <class TParent = TFluentYsonVoid>
    class TAttributes
        : public TFluentFragmentBase<TAttributes, TParent>
    {
    public:
        using TThis = TAttributes<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TAttributes(NYT::NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TAttributes, TParent>(consumer, std::move(parent))
        { }

        /// Pass attribute key to underlying consumer.
        TAny<TThis> Item(const TStringBuf& key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, *this);
        }

        /// Pass attribute key to underlying consumer.
        template <size_t Size>
        TAny<TThis> Item(const char (&key)[Size])
        {
            return Item(TStringBuf(key, Size - 1));
        }

        //TODO: from TNode

        /// Close the attributes.
        TUnwrappedParent EndAttributes()
        {
            this->Consumer->OnEndAttributes();
            return this->GetUnwrappedParent();
        }
    };

    /// Fluent adapter of list fragment (the inside part of a list).
    template <class TParent = TFluentYsonVoid>
    class TListType
        : public TFluentFragmentBase<TListType, TParent>
    {
    public:
        using TThis = TListType<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TListType(NYT::NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TListType, TParent>(consumer, std::move(parent))
        { }

        /// Call `OnListItem()` of underlying consumer.
        TAny<TThis> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TThis>(this->Consumer, *this);
        }

        // TODO: from TNode

        /// Close the list.
        TUnwrappedParent EndList()
        {
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }
    };

    /// Fluent adapter of map fragment (the inside part of a map).
    template <class TParent = TFluentYsonVoid>
    class TMapType
        : public TFluentFragmentBase<TMapType, TParent>
    {
    public:
        using TThis = TMapType<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TMapType(NYT::NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TMapType, TParent>(consumer, std::move(parent))
        { }

        /// Pass map key to underlying consumer.
        template <size_t Size>
        TAny<TThis> Item(const char (&key)[Size])
        {
            return Item(TStringBuf(key, Size - 1));
        }

        /// Pass map key to underlying consumer.
        TAny<TThis> Item(const TStringBuf& key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, *this);
        }

        // TODO: from TNode

        /// Close the map.
        TUnwrappedParent EndMap()
        {
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };

};

////////////////////////////////////////////////////////////////////////////////

/// Builder representing any value.
using TFluentAny = TFluentYsonBuilder::TAny<TFluentYsonVoid>;

/// Builder representing the inside of a list (list fragment).
using TFluentList = TFluentYsonBuilder::TListType<TFluentYsonVoid>;

/// Builder representing the inside of a map (map fragment).
using TFluentMap = TFluentYsonBuilder::TMapType<TFluentYsonVoid>;

/// Builder representing the inside of attributes.
using TFluentAttributes = TFluentYsonBuilder::TAttributes<TFluentYsonVoid>;

////////////////////////////////////////////////////////////////////////////////

/// Create a fluent adapter to invoke methods of `consumer`.
static inline TFluentAny BuildYsonFluently(NYT::NYson::IYsonConsumer* consumer)
{
    return TFluentAny(consumer, TFluentYsonVoid());
}

/// Create a fluent adapter to invoke methods of `consumer` describing the contents of a list.
static inline TFluentList BuildYsonListFluently(NYT::NYson::IYsonConsumer* consumer)
{
    return TFluentList(consumer);
}

/// Create a fluent adapter to invoke methods of `consumer` describing the contents of a map.
static inline TFluentMap BuildYsonMapFluently(NYT::NYson::IYsonConsumer* consumer)
{
    return TFluentMap(consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonWriterState
    : public TThrRefBase
{
public:
    using TValue = TString;

    explicit TFluentYsonWriterState(::NYson::EYsonFormat format)
        : Writer(&Output, format)
    { }

    TString GetValue()
    {
        return Output.Str();
    }

    NYT::NYson::IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    ::NYson::TYsonWriter Writer;
};

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilderState
    : public TThrRefBase
{
public:
    using TValue = TNode;

    explicit TFluentYsonBuilderState()
        : Builder(&Node)
    { }

    TNode GetValue()
    {
        return std::move(Node);
    }

    NYT::NYson::IYsonConsumer* GetConsumer()
    {
        return &Builder;
    }

private:
    TNode Node;
    TNodeBuilder Builder;
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
class TFluentYsonHolder
{
public:
    explicit TFluentYsonHolder(::TIntrusivePtr<TState> state)
        : State(state)
    { }

    ::TIntrusivePtr<TState> GetState() const
    {
        return State;
    }

private:
    ::TIntrusivePtr<TState> State;
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
struct TFluentYsonUnwrapper< TFluentYsonHolder<TState> >
{
    using TUnwrapped = typename TState::TValue;

    static TUnwrapped Unwrap(const TFluentYsonHolder<TState>& holder)
    {
        return std::move(holder.GetState()->GetValue());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
TFluentYsonBuilder::TAny<TFluentYsonHolder<TState>>
BuildYsonFluentlyWithState(::TIntrusivePtr<TState> state)
{
    return TFluentYsonBuilder::TAny<TFluentYsonHolder<TState>>(
        state->GetConsumer(),
        TFluentYsonHolder<TState>(state));
}

/// Create a fluent adapter returning a `TString` with corresponding YSON when construction is finished.
inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentYsonWriterState>>
BuildYsonStringFluently(::NYson::EYsonFormat format = ::NYson::EYsonFormat::Text)
{
    ::TIntrusivePtr<TFluentYsonWriterState> state(new TFluentYsonWriterState(format));
    return BuildYsonFluentlyWithState(state);
}

/// Create a fluent adapter returning a @ref NYT::TNode when construction is finished.
inline TFluentYsonBuilder::TAny<TFluentYsonHolder<TFluentYsonBuilderState>>
BuildYsonNodeFluently()
{
    ::TIntrusivePtr<TFluentYsonBuilderState> state(new TFluentYsonBuilderState);
    return BuildYsonFluentlyWithState(state);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
