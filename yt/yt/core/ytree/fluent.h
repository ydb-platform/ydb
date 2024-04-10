#pragma once

#include "public.h"
#include "tree_visitor.h"
#include "tree_builder.h"
#include "convert.h"
#include "attributes.h"
#include "attribute_consumer.h"
#include "helpers.h"

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/parser.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

/*
// WHAT IS THIS
//
// Fluent adapters encapsulate invocation of IYsonConsumer methods in a
// convenient structured manner. Key advantage of fluent-like code is that
// attempt of building syntactically incorrect YSON structure will result
// in a compile-time error.
//
// Each fluent object is associated with a context that defines possible YSON
// tokens that may appear next. For example, TFluentMap is a fluent object
// that corresponds to a location within YSON map right before a key-value
// pair or the end of the map.
//
// More precisely, each object that may be obtained by a sequence of fluent
// method calls has the full history of its enclosing YSON composite types in
// its single template argument hereinafter referred to as TParent. This allows
// us not to forget the original context after opening and closing the embedded
// composite structure.
//
// It is possible to invoke a separate YSON building procedure by calling
// one of convenience Do* methods. There are two possibilities here: it is
// possible to delegate invocation context either as a fluent object (like
// TFluentMap, TFluentList, TFluentAttributes or TFluentAny) or as a raw
// IYsonConsumer*. The latter is discouraged since it is impossible to check
// if a given side-built YSON structure fits current fluent context.
// For example it is possible to call Do() method inside YSON map passing
// consumer to a procedure that will treat context like it is in a list.
// Passing typed fluent builder saves you from such a misbehaviour.
//
// TFluentXxx corresponds to an internal class of TXxx
// without any history hidden in template argument. It allows you to
// write procedures of form:
//
//   void BuildSomeAttributesInYson(TFluentMap fluent) { ... }
//
// without thinking about the exact way how this procedure is nested in other
// procedures.
//
// An important notation: we will refer to a function whose first argument
// is TFluentXxx as TFuncXxx.
//
//
// BRIEF LIST OF AVAILABLE METHODS
//
// Only the most popular methods are covered here. Refer to the code for the
// rest of them.
//
// TAny:
// * Value(T value) -> TParent, serialize `value` using underlying consumer.
//   T should be such that free function Serialize(IYsonConsumer*, const T&) is
//   defined;
// * BeginMap() -> TFluentMap, open map;
// * BeginList() -> TFluentList, open list;
// * BeginAttributes() -> TFluentAttributes, open attributes;
//
// * Do(TFuncAny func) -> TAny, delegate invocation to a separate procedure.
// * DoIf(bool condition, TFuncAny func) -> TAny, same as Do() but invoke
//   `func` only if `condition` is true;
// * DoFor(TCollection collection, TFuncAny func) -> TAny, same as Do()
//   but iterate over `collection` and pass each of its elements as a second
//   argument to `func`. Instead of passing a collection you may it is possible
//   to pass two iterators as an argument;
//
// * DoMap(TFuncMap func) -> TAny, open a map, delegate invocation to a separate
//   procedure and close map;
// * DoMapFor(TCollection collection, TFuncMap func) -> TAny, open a map, iterate
//   over `collection` and pass each of its elements as a second argument to `func`
//   and close map;
// * DoList(TFuncList func) -> TAny, same as DoMap();
// * DoListFor(TCollection collection, TFuncList func) -> TAny; same as DoMapFor().
//
//
// TFluentMap:
// * Item(TStringBuf key) -> TAny, open an element keyed with `key`;
// * EndMap() -> TParent, close map;
// * Do(TFuncMap func) -> TFluentMap, same as Do() for TAny;
// * DoIf(bool condition, TFuncMap func) -> TFluentMap, same as DoIf() for TAny;
// * DoFor(TCollection collection, TFuncMap func) -> TFluentMap, same as DoFor() for TAny.
//
//
// TFluentList:
// * Item() -> TAny, open an new list element;
// * EndList() -> TParent, close list;
// * Do(TFuncList func) -> TFluentList, same as Do() for TAny;
// * DoIf(bool condition, TFuncList func) -> TFluentList, same as DoIf() for TAny;
// * DoFor(TCollection collection, TListMap func) -> TFluentList, same as DoFor() for TAny.
//
//
// TFluentAttributes:
// * Item(TStringBuf key) -> TAny, open an element keyed with `key`.
// * EndAttributes() -> TParentWithoutAttributes, close attributes. Note that
//   this method leads to a context that is forces not to have attributes,
//   preventing us from putting attributes twice before an object.
// * Do(TFuncAttributes func) -> TFluentAttributes, same as Do() for TAny;
// * DoIf(bool condition, TFuncAttributes func) -> TFluentAttributes, same as DoIf()
//   for TAny;
// * DoFor(TCollection collection, TListAttributes func) -> TFluentAttributes, same as DoFor()
//   for TAny.
//
 */

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

template <class TFluent, class TFunc, class... TArgs>
void InvokeFluentFunc(TFunc func, NYson::IYsonConsumer* consumer, TArgs&&... args)
{
    func(TFluent(consumer), std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilder
    : private TNonCopyable
{
private:
    template <class T, class... TExtraArgs>
    static void WriteValue(NYson::IYsonConsumer* consumer, const T& value, TExtraArgs&&... extraArgs)
    {
        Serialize(value, consumer, std::forward<TExtraArgs>(extraArgs)...);
    }

public:
    class TFluentAny;
    template <class TParent> class TAny;
    template <class TParent> class TToAttributes;
    template <class TParent> class TFluentAttributes;
    template <class TParent> class TFluentList;
    template <class TParent> class TFluentMap;

    template <class TParent>
    class TFluentBase
    {
    public:
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        NYson::IYsonConsumer* GetConsumer() const
        {
            return Consumer;
        }

        TUnwrappedParent Finish()
        {
            return GetUnwrappedParent();
        }

    protected:
        NYson::IYsonConsumer* Consumer;
        TParent Parent;
        bool Unwrapped_ = false;

        TFluentBase(NYson::IYsonConsumer* consumer, TParent parent)
            : Consumer(consumer)
            , Parent(std::move(parent))
        { }

        TUnwrappedParent GetUnwrappedParent()
        {
            YT_VERIFY(!Unwrapped_);
            Unwrapped_ = true;
            return TFluentYsonUnwrapper<TParent>::Unwrap(std::move(Parent));
        }
    };

    template <template <class TParent> class TThis, class TParent, class TShallowThis>
    class TFluentFragmentBase
        : public TFluentBase<TParent>
    {
    public:
        using TDeepThis = TThis<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TFluentFragmentBase(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

        TDeepThis Do(auto func)
        {
            InvokeFluentFunc<TShallowThis>(func, this->Consumer);
            return static_cast<TDeepThis&&>(*this);
        }

        TDeepThis DoIf(bool condition, auto func)
        {
            if (condition) {
                InvokeFluentFunc<TShallowThis>(func, this->Consumer);
            }
            return static_cast<TDeepThis&&>(*this);
        }

        TDeepThis DoFor(auto begin, auto end, auto func)
        {
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TShallowThis>(func, this->Consumer, current);
            }
            return static_cast<TDeepThis&&>(*this);
        }

        TDeepThis DoFor(const auto& collection, auto func)
        {
            for (const auto& item : collection) {
                InvokeFluentFunc<TShallowThis>(func, this->Consumer, item);
            }
            return static_cast<TDeepThis&&>(*this);
        }
    };

    template <class TParent, class TShallowThis>
    class TAnyBase
        : public TFluentBase<TParent>
    {
    public:
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        TAnyBase(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentBase<TParent>(consumer, std::move(parent))
        { }

        TUnwrappedParent Do(auto funcAny)
        {
            InvokeFluentFunc<TShallowThis>(funcAny, this->Consumer);
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent Value(const auto& value, auto&&... extraArgs)
        {
            WriteValue(this->Consumer, value, std::forward<decltype(extraArgs)>(extraArgs)...);
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent Entity()
        {
            this->Consumer->OnEntity();
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent List(const auto& collection)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                this->Consumer->OnListItem();
                WriteValue(this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent ListLimited(const auto& collection, size_t maxSize)
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

        TFluentList<TParent> BeginList()
        {
            this->Consumer->OnBeginList();
            return TFluentList<TParent>(this->Consumer, std::move(this->Parent));
        }

        TUnwrappedParent DoList(auto funcList)
        {
            this->Consumer->OnBeginList();
            InvokeFluentFunc<TFluentList<TFluentYsonVoid>>(funcList, this->Consumer);
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent DoListFor(auto begin, auto end, auto funcList)
        {
            this->Consumer->OnBeginList();
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TFluentList<TFluentYsonVoid>>(funcList, this->Consumer, current);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent DoListFor(const auto& collection, auto funcList)
        {
            this->Consumer->OnBeginList();
            for (const auto& item : collection) {
                InvokeFluentFunc<TFluentList<TFluentYsonVoid>>(funcList, this->Consumer, item);
            }
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }

        TFluentMap<TParent> BeginMap()
        {
            this->Consumer->OnBeginMap();
            return TFluentMap<TParent>(this->Consumer, std::move(this->Parent));
        }

        TUnwrappedParent DoMap(auto funcMap)
        {
            this->Consumer->OnBeginMap();
            InvokeFluentFunc<TFluentMap<TFluentYsonVoid>>(funcMap, this->Consumer);
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent DoMapFor(auto begin, auto end, auto funcMap)
        {
            this->Consumer->OnBeginMap();
            for (auto current = begin; current != end; ++current) {
                InvokeFluentFunc<TFluentMap<TFluentYsonVoid>>(funcMap, this->Consumer, current);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }

        TUnwrappedParent DoMapFor(const auto& collection, auto funcMap)
        {
            this->Consumer->OnBeginMap();
            for (const auto& item : collection) {
                InvokeFluentFunc<TFluentMap<TFluentYsonVoid>>(funcMap, this->Consumer, item);
            }
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };

    template <class TParent>
    class TAnyWithoutAttributes
        : public TAnyBase<TParent, TAnyWithoutAttributes<TFluentYsonVoid>>
    {
    public:
        using TBase = TAnyBase<TParent, TAnyWithoutAttributes<TFluentYsonVoid>>;

        explicit TAnyWithoutAttributes(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TBase(consumer, std::move(parent))
        { }
    };

    template <class TParent>
    class TAny
        : public TAnyBase<TParent, TAny<TFluentYsonVoid>>
    {
    public:
        using TBase = TAnyBase<TParent, TAny<TFluentYsonVoid>>;

        explicit TAny(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TBase(consumer, std::move(parent))
        { }

        TFluentAttributes<TAnyWithoutAttributes<TParent>> BeginAttributes()
        {
            this->Consumer->OnBeginAttributes();
            return TFluentAttributes<TAnyWithoutAttributes<TParent>>(
                this->Consumer,
                TAnyWithoutAttributes<TParent>(this->Consumer, std::move(this->Parent)));
        }
    };

    template <class TParent = TFluentYsonVoid>
    class TFluentAttributes
        : public TFluentFragmentBase<TFluentAttributes, TParent, TFluentMap<TFluentYsonVoid>>
    {
    public:
        using TThis = TFluentAttributes<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TFluentAttributes(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TFluentAttributes, TParent, TFluentMap<TFluentYsonVoid>>(consumer, std::move(parent))
        { }

        template <size_t Size>
        TAny<TThis> Item(const char (&key)[Size])
        {
            return Item(TStringBuf(key, Size - 1));
        }

        TAny<TThis> Item(TStringBuf key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, std::move(*this));
        }

        TThis& Items(const IMapNodePtr& map)
        {
            for (const auto& [key, child] : map->GetChildren()) {
                this->Consumer->OnKeyedItem(key);
                VisitTree(child, this->Consumer, true);
            }
            return *this;
        }

        TThis& Items(const IAttributeDictionary& attributes)
        {
            for (const auto& [key, value] : attributes.ListPairs()) {
                this->Consumer->OnKeyedItem(key);
                this->Consumer->OnRaw(value);
            }
            return *this;
        }

        TThis& Items(const NYson::TYsonString& attributes)
        {
            YT_VERIFY(attributes.GetType() == NYson::EYsonType::MapFragment);
            this->Consumer->OnRaw(attributes);
            return *this;
        }

        TThis& OptionalItem(TStringBuf key, const auto& optionalValue, auto&&... extraArgs)
        {
            if (optionalValue) {
                this->Consumer->OnKeyedItem(key);
                WriteValue(this->Consumer, optionalValue, std::forward<decltype(extraArgs)>(extraArgs)...);
            }
            return *this;
        }

        TUnwrappedParent EndAttributes()
        {
            this->Consumer->OnEndAttributes();
            return this->GetUnwrappedParent();
        }
    };

    template <class TParent = TFluentYsonVoid>
    class TFluentList
        : public TFluentFragmentBase<TFluentList, TParent, TFluentList<TFluentYsonVoid>>
    {
    public:
        using TThis = TFluentList<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TFluentList(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TFluentList, TParent, TFluentList<TFluentYsonVoid>>(consumer, std::move(parent))
        { }

        TAny<TThis> Item()
        {
            this->Consumer->OnListItem();
            return TAny<TThis>(this->Consumer, std::move(*this));
        }

        TThis& Items(const IListNodePtr& list)
        {
            for (auto item : list->GetChildren()) {
                this->Consumer->OnListItem();
                VisitTree(std::move(item), this->Consumer, true);
            }
            return *this;
        }

        TThis& OptionalItem(const auto& optionalValue, auto&&... extraArgs)
        {
            if (optionalValue) {
                this->Consumer->OnListItem();
                WriteValue(this->Consumer, optionalValue, std::forward<decltype(extraArgs)>(extraArgs)...);
            }
            return *this;
        }

        TUnwrappedParent EndList()
        {
            this->Consumer->OnEndList();
            return this->GetUnwrappedParent();
        }
    };

    template <class TParent = TFluentYsonVoid>
    class TFluentMap
        : public TFluentFragmentBase<TFluentMap, TParent, TFluentMap<TFluentYsonVoid>>
    {
    public:
        using TThis = TFluentMap<TParent>;
        using TUnwrappedParent = typename TFluentYsonUnwrapper<TParent>::TUnwrapped;

        explicit TFluentMap(NYson::IYsonConsumer* consumer, TParent parent = TParent())
            : TFluentFragmentBase<TFluentYsonBuilder::TFluentMap, TParent, TFluentMap<TFluentYsonVoid>>(consumer, std::move(parent))
        { }

        template <size_t Size>
        TAny<TThis> Item(const char (&key)[Size])
        {
            return Item(TStringBuf(key, Size - 1));
        }

        TAny<TThis> Item(TStringBuf key)
        {
            this->Consumer->OnKeyedItem(key);
            return TAny<TThis>(this->Consumer, std::move(*this));
        }

        TThis& Items(const IMapNodePtr& map)
        {
            for (const auto& [key, child] : map->GetChildren()) {
                this->Consumer->OnKeyedItem(key);
                VisitTree(child, this->Consumer, true);
            }
            return *this;
        }

        TThis& Items(const IAttributeDictionary& attributes)
        {
            for (const auto& [key, value] : attributes.ListPairs()) {
                this->Consumer->OnKeyedItem(key);
                this->Consumer->OnRaw(value);
            }
            return *this;
        }

        TThis& Items(const NYson::TYsonString& attributes)
        {
            YT_VERIFY(attributes.GetType() == NYson::EYsonType::MapFragment);
            this->Consumer->OnRaw(attributes);
            return *this;
        }

        TThis& OptionalItem(TStringBuf key, const auto& optionalValue, auto&&... extraArgs)
        {
            if (optionalValue) {
                this->Consumer->OnKeyedItem(key);
                WriteValue(this->Consumer, optionalValue, std::forward<decltype(extraArgs)>(extraArgs)...);
            }
            return *this;
        }

        TUnwrappedParent EndMap()
        {
            this->Consumer->OnEndMap();
            return this->GetUnwrappedParent();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

template <NYson::EYsonType>
struct TFluentType;

template <>
struct TFluentType<NYson::EYsonType::Node>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TAny<T>;
};

template <>
struct TFluentType<NYson::EYsonType::MapFragment>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TFluentMap<T>;
};

template <>
struct TFluentType<NYson::EYsonType::ListFragment>
{
    template <class T>
    using TValue = TFluentYsonBuilder::TFluentList<T>;
};

using TFluentList = TFluentYsonBuilder::TFluentList<TFluentYsonVoid>;
using TFluentMap = TFluentYsonBuilder::TFluentMap<TFluentYsonVoid>;
using TFluentAttributes = TFluentYsonBuilder::TFluentAttributes<TFluentYsonVoid>;
using TFluentAny = TFluentYsonBuilder::TAny<TFluentYsonVoid>;
using TFluentAnyWithoutAttributes = TFluentYsonBuilder::TAnyWithoutAttributes<TFluentYsonVoid>;

////////////////////////////////////////////////////////////////////////////////

static inline TFluentAny BuildYsonFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentYsonBuilder::TAny<TFluentYsonVoid>(consumer, TFluentYsonVoid());
}

static inline TFluentList BuildYsonListFragmentFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentList(consumer);
}

static inline TFluentMap BuildYsonMapFragmentFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentMap(consumer);
}

static inline TFluentAttributes BuildYsonAttributesFluently(NYson::IYsonConsumer* consumer)
{
    return TFluentAttributes(consumer);
}

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonWriterState
    : public TRefCounted
{
public:
    using TValue = NYson::TYsonString;

    TFluentYsonWriterState(NYson::EYsonFormat format, NYson::EYsonType type)
        : Writer(&Output, format, type, true /*enableRaw*/)
        , Type(type)
    { }

    NYson::TYsonString GetValue()
    {
        return NYson::TYsonString(Output.Str(), Type);
    }

    NYson::IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    NYson::TYsonWriter Writer;
    NYson::EYsonType Type;

};

////////////////////////////////////////////////////////////////////////////////

class TFluentYsonBuilderState
    : public TRefCounted
{
public:
    using TValue = INodePtr;

    explicit TFluentYsonBuilderState(INodeFactory* factory)
        : Builder(CreateBuilderFromFactory(factory))
    { }

    INodePtr GetValue()
    {
        return Builder->EndTree();
    }

    NYson::IYsonConsumer* GetConsumer()
    {
        return Builder.get();
    }

private:
    const std::unique_ptr<ITreeBuilder> Builder;

};

////////////////////////////////////////////////////////////////////////////////

class TFluentAttributeConsumerState
    : public TRefCounted
{
public:
    using TValue = IAttributeDictionaryPtr;

    explicit TFluentAttributeConsumerState(std::optional<int> ysonNestingLevelLimit)
        : Dictionary_(CreateEphemeralAttributes(ysonNestingLevelLimit))
        , Consumer_(std::make_unique<TAttributeConsumer>(Dictionary_.Get()))
    { }

    IAttributeDictionaryPtr GetValue()
    {
        return Dictionary_;
    }

    NYson::IYsonConsumer* GetConsumer()
    {
        return Consumer_.get();
    }

private:
    const IAttributeDictionaryPtr Dictionary_;
    const std::unique_ptr<TAttributeConsumer> Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
class TFluentYsonHolder
{
public:
    explicit TFluentYsonHolder(TIntrusivePtr<TState> state)
        : State(std::move(state))
    { }

    TIntrusivePtr<TState> GetState() const
    {
        return State;
    }

private:
    const TIntrusivePtr<TState> State;

};

////////////////////////////////////////////////////////////////////////////////

template <class TState>
struct TFluentYsonUnwrapper<TFluentYsonHolder<TState>>
{
    using TUnwrapped = typename TState::TValue;

    static TUnwrapped Unwrap(const TFluentYsonHolder<TState>& holder)
    {
        return holder.GetState()->GetValue();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TState, NYson::EYsonType type>
auto BuildYsonFluentlyWithState(TIntrusivePtr<TState> state)
{
    using TReturnType = typename TFluentType<type>::template TValue<TFluentYsonHolder<TState>>;
    return TReturnType(
        state->GetConsumer(),
        TFluentYsonHolder<TState>(state));
}

// XXX(max42): return types below look pretty nasty :( CLion is not able to deduce
// them automatically that leads to lots of syntax errors.
// Remove them when CLion does not suck.

template <NYson::EYsonType type = NYson::EYsonType::Node>
auto BuildYsonStringFluently(NYson::EYsonFormat format = NYson::EYsonFormat::Binary)
    -> typename TFluentType<type>::template TValue<TFluentYsonHolder<TFluentYsonWriterState>>
{
    return BuildYsonFluentlyWithState<TFluentYsonWriterState, type>(New<TFluentYsonWriterState>(format, type));
}

inline auto BuildYsonNodeFluently(INodeFactory* factory = GetEphemeralNodeFactory())
    -> typename TFluentType<NYson::EYsonType::Node>::template TValue<TFluentYsonHolder<TFluentYsonBuilderState>>
{
    return BuildYsonFluentlyWithState<TFluentYsonBuilderState, NYson::EYsonType::Node>(New<TFluentYsonBuilderState>(factory));
}

inline auto BuildAttributeDictionaryFluently(std::optional<int> ysonNestingLevelLimit = {})
    -> typename TFluentType<NYson::EYsonType::MapFragment>::template TValue<TFluentYsonHolder<TFluentAttributeConsumerState>>
{
    return BuildYsonFluentlyWithState<TFluentAttributeConsumerState, NYson::EYsonType::MapFragment>(New<TFluentAttributeConsumerState>(ysonNestingLevelLimit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

