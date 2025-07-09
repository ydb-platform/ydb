#ifndef JINJA2CPP_SRC_INTERNAL_VALUE_H
#define JINJA2CPP_SRC_INTERNAL_VALUE_H

#include <jinja2cpp/value.h>
//#include <jinja2cpp/value_ptr.h>

#include <boost/iterator/iterator_facade.hpp>
#include <boost/variant/recursive_wrapper.hpp>
#include <boost/unordered_map.hpp>

#include <fmt/core.h>

#if defined(_MSC_VER) && _MSC_VER <= 1900 // robin_hood hash map doesn't compatible with MSVC 14.0
#include <unordered_map>
#else
#include "robin_hood.h"
#endif


#include <string_view>
#include <variant>

#include <functional>

namespace jinja2
{

template <class T>
class ReferenceWrapper
{
public:
    using type = T;

    ReferenceWrapper(T& ref) noexcept
        : m_ptr(std::addressof(ref))
    {
    }

    ReferenceWrapper(T&&) = delete;
    ReferenceWrapper(const ReferenceWrapper&) noexcept = default;

    // assignment
    ReferenceWrapper& operator=(const ReferenceWrapper& x) noexcept = default;

    // access
    T& get() const noexcept
    {
        return *m_ptr;
    }

private:
    T* m_ptr;
};

template<typename T, size_t SizeHint = 48>
class RecursiveWrapper
{
public:
    RecursiveWrapper() = default;

    RecursiveWrapper(const T& value)
        : m_data(value)
    {}

    RecursiveWrapper(T&& value)
        : m_data(std::move(value))
    {}

    const T& GetValue() const {return m_data.get();}
    T& GetValue() {return m_data.get();}

private:
    boost::recursive_wrapper<T> m_data;

#if 0
    enum class State
    {
        Undefined,
        Inplace,
        Ptr
    };

    State m_state;

    union
    {
        uint64_t dummy;
        nonstd::value_ptr<T> ptr;
    } m_data;
#endif
};

template<typename T>
auto MakeWrapped(T&& val)
{
    return RecursiveWrapper<std::decay_t<T>>(std::forward<T>(val));
}

using ValueRef = ReferenceWrapper<const Value>;
using TargetString = std::variant<std::string, std::wstring>;
using TargetStringView = std::variant<std::string_view, std::wstring_view>;

class ListAdapter;
class MapAdapter;
class RenderContext;
class OutStream;
class Callable;
struct CallParams;
struct KeyValuePair;
class IRendererBase;

class InternalValue;
using InternalValueData = std::variant<
    EmptyValue,
    bool,
    std::string,
    TargetString,
    TargetStringView,
    int64_t,
    double,
    ValueRef,
    ListAdapter,
    MapAdapter,
    RecursiveWrapper<KeyValuePair>,
    RecursiveWrapper<Callable>,
    std::shared_ptr<IRendererBase>>;


using InternalValueRef = ReferenceWrapper<InternalValue>;
using InternalValueList = std::vector<InternalValue>;

template<typename T, bool isRecursive = false>
struct ValueGetter
{
    template<typename V>
    static auto& Get(V&& val)
    {
        return std::get<T>(std::forward<V>(val).GetData());
    }

    static auto GetPtr(const InternalValue* val);

    static auto GetPtr(InternalValue* val);

    template<typename V>
    static auto GetPtr(V* val, std::enable_if_t<!std::is_same<V, InternalValue>::value>* = nullptr)
    {
        return std::get_if<T>(val);
    }
};

template<typename T>
struct ValueGetter<T, true>
{
    template<typename V>
    static auto& Get(V&& val)
    {
        auto& ref = std::get<RecursiveWrapper<T>>(std::forward<V>(val));
        return ref.GetValue();
    }

    static auto GetPtr(const InternalValue* val);

    static auto GetPtr(InternalValue* val);

    template<typename V>
    static auto GetPtr(V* val, std::enable_if_t<!std::is_same<V, InternalValue>::value>* = nullptr)
    {
        auto ref = std::get_if<RecursiveWrapper<T>>(val);
        return !ref ? nullptr : &ref->GetValue();
    }
};

template<typename T>
struct IsRecursive : std::false_type {};

template<>
struct IsRecursive<KeyValuePair> : std::true_type {};

template<>
struct IsRecursive<Callable> : std::true_type {};

struct IListAccessorEnumerator : virtual IComparable
{
    virtual ~IListAccessorEnumerator() {}

    virtual void Reset() = 0;

    virtual bool MoveNext() = 0;
    virtual InternalValue GetCurrent() const = 0;

    virtual IListAccessorEnumerator* Clone() const = 0;
    virtual IListAccessorEnumerator* Transfer() = 0;
/*  
    struct Cloner
    {
        Cloner() = default;

        IListAccessorEnumerator* operator()(const IListAccessorEnumerator &x) const
        {
            return x.Clone();
        }

        IListAccessorEnumerator* operator()(IListAccessorEnumerator &&x) const
        {
            return x.Transfer();
        }
    };
    */
};

using ListAccessorEnumeratorPtr = types::ValuePtr<IListAccessorEnumerator>;

struct IListAccessor
{
    virtual ~IListAccessor() {}

    virtual std::optional<size_t> GetSize() const = 0;
    virtual std::optional<InternalValue> GetItem(int64_t idx) const = 0;
    virtual ListAccessorEnumeratorPtr CreateListAccessorEnumerator() const = 0;
    virtual GenericList CreateGenericList() const = 0;
    virtual bool ShouldExtendLifetime() const = 0;
};


using ListAccessorProvider = std::function<const IListAccessor*()>;

struct IMapAccessor
{
    virtual ~IMapAccessor() = default;
    virtual size_t GetSize() const = 0;
    virtual bool HasValue(const std::string& name) const = 0;
    virtual InternalValue GetItem(const std::string& name) const = 0;
    virtual std::vector<std::string> GetKeys() const = 0;
    virtual bool SetValue(std::string, const InternalValue&) {return false;}
    virtual GenericMap CreateGenericMap() const = 0;
    virtual bool ShouldExtendLifetime() const = 0;
};

using MapAccessorProvider = std::function<IMapAccessor*()>;

class ListAdapter
{
public:
    ListAdapter() {}
    explicit ListAdapter(ListAccessorProvider prov) : m_accessorProvider(std::move(prov)) {}
    ListAdapter(const ListAdapter&) = default;
    ListAdapter(ListAdapter&&) = default;

    static ListAdapter CreateAdapter(InternalValueList&& values);
    static ListAdapter CreateAdapter(const GenericList& values);
    static ListAdapter CreateAdapter(const ValuesList& values);
    static ListAdapter CreateAdapter(GenericList&& values);
    static ListAdapter CreateAdapter(ValuesList&& values);
    static ListAdapter CreateAdapter(std::function<std::optional<InternalValue> ()> fn);
    static ListAdapter CreateAdapter(size_t listSize, std::function<InternalValue (size_t idx)> fn);

    ListAdapter& operator = (const ListAdapter&) = default;
    ListAdapter& operator = (ListAdapter&&) = default;

    std::optional<size_t> GetSize() const
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->GetSize();
        }

        return 0;
    }
    InternalValue GetValueByIndex(int64_t idx) const;
    bool ShouldExtendLifetime() const
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->ShouldExtendLifetime();
        }

        return false;
    }

    ListAdapter ToSubscriptedList(const InternalValue& subscript, bool asRef = false) const;
    InternalValueList ToValueList() const;
    GenericList CreateGenericList() const
    {
        if (m_accessorProvider && m_accessorProvider())
            return m_accessorProvider()->CreateGenericList();

        return GenericList();
    }
    ListAccessorEnumeratorPtr GetEnumerator() const;

    class Iterator;

    Iterator begin() const;
    Iterator end() const;

private:
    ListAccessorProvider m_accessorProvider;
};

class MapAdapter
{
public:
    MapAdapter() = default;
    explicit MapAdapter(MapAccessorProvider prov) : m_accessorProvider(std::move(prov)) {}

    size_t GetSize() const
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->GetSize();
        }

        return 0;
    }
    // InternalValue GetValueByIndex(int64_t idx) const;
    bool HasValue(const std::string& name) const
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->HasValue(name);
        }

        return false;
    }
    InternalValue GetValueByName(const std::string& name) const;
    std::vector<std::string> GetKeys() const
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->GetKeys();
        }

        return std::vector<std::string>();
    }
    bool SetValue(std::string name, const InternalValue& val)
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->SetValue(std::move(name), val);
        }

        return false;
    }
    bool ShouldExtendLifetime() const
    {
        if (m_accessorProvider && m_accessorProvider())
        {
            return m_accessorProvider()->ShouldExtendLifetime();
        }

        return false;
    }

    GenericMap CreateGenericMap() const
    {
        if (m_accessorProvider && m_accessorProvider())
            return m_accessorProvider()->CreateGenericMap();

        return GenericMap();
    }

private:
    MapAccessorProvider m_accessorProvider;
};


class InternalValue
{
public:
    InternalValue() = default;

    template<typename T>
    InternalValue(T&& val, typename std::enable_if<!std::is_same<std::decay_t<T>, InternalValue>::value>::type* = nullptr)
        : m_data(InternalValueData(std::forward<T>(val)))
    {
    }

    auto& GetData() const {return m_data;}
    auto& GetData() {return m_data;}

    auto& GetParentData() {return m_parentData;}
    auto& GetParentData() const {return m_parentData;}

    void SetParentData(const InternalValue& val);

    void SetParentData(InternalValue&& val);

    bool ShouldExtendLifetime() const
    {
        if (m_parentData.index() != 0)
            return true;

        const MapAdapter* ma = std::get_if<MapAdapter>(&m_data);
        if (ma != nullptr)
            return ma->ShouldExtendLifetime();

        const ListAdapter* la = std::get_if<ListAdapter>(&m_data);
        if (la != nullptr)
            return la->ShouldExtendLifetime();

        return false;
    }

    bool IsEmpty() const {return m_data.index() == 0;}

    bool IsEqual(const InternalValue& other) const;

private:
    InternalValueData m_data;
    InternalValueData m_parentData;
};

inline bool operator==(const InternalValue& lhs, const InternalValue& rhs)
{
    return lhs.IsEqual(rhs);
}
inline bool operator!=(const InternalValue& lhs, const InternalValue& rhs)
{
    return !(lhs == rhs);
}

class ListAdapter::Iterator
        : public boost::iterator_facade<
            Iterator,
            const InternalValue,
            boost::forward_traversal_tag>
{
public:
    Iterator() = default;

    explicit Iterator(ListAccessorEnumeratorPtr&& iter)
        : m_iterator(std::move(iter))
        , m_isFinished(!m_iterator->MoveNext())
        , m_currentVal(m_isFinished ? InternalValue() : m_iterator->GetCurrent())
    {}

private:
    friend class boost::iterator_core_access;

    void increment();

    bool equal(const Iterator& other) const
    {
        if (!this->m_iterator)
            return !other.m_iterator ? true : other.equal(*this);

        if (!other.m_iterator)
            return this->m_isFinished;
//        return true;
        //const InternalValue& lhs = *(this->m_iterator);
        //const InternalValue& rhs = *(other.m_iterator);
        //return lhs == rhs;
        return this->m_iterator->GetCurrent() == other.m_iterator->GetCurrent() && this->m_currentIndex == other.m_currentIndex;
        ///return *(this->m_iterator) == *(other.m_iterator) && this->m_currentIndex == other.m_currentIndex;
    }

    const InternalValue& dereference() const
    {
        return m_currentVal;
    }

    ListAccessorEnumeratorPtr m_iterator;
    bool m_isFinished = true;
    mutable uint64_t m_currentIndex = 0;
    mutable InternalValue m_currentVal;
};

#if defined(_MSC_VER) && _MSC_VER <= 1900 // robin_hood hash map doesn't compatible with MSVC 14.0
typedef std::unordered_map<std::string, InternalValue> InternalValueMap;
#else
typedef robin_hood::unordered_map<std::string, InternalValue> InternalValueMap;
#endif


MapAdapter CreateMapAdapter(InternalValueMap&& values);
MapAdapter CreateMapAdapter(const InternalValueMap* values);
MapAdapter CreateMapAdapter(const GenericMap& values);
MapAdapter CreateMapAdapter(GenericMap&& values);
MapAdapter CreateMapAdapter(const ValuesMap& values);
MapAdapter CreateMapAdapter(ValuesMap&& values);

template<typename T, bool V>
inline auto ValueGetter<T, V>::GetPtr(const InternalValue* val)
{
    return std::get_if<T>(&val->GetData());
}

template<typename T, bool V>
inline auto ValueGetter<T, V>::GetPtr(InternalValue* val)
{
    return std::get_if<T>(&val->GetData());
}

template<typename T>
inline auto ValueGetter<T, true>::GetPtr(const InternalValue* val)
{
    auto ref = std::get_if<RecursiveWrapper<T>>(&val->GetData());
    return !ref ? nullptr : &ref->GetValue();
}

template<typename T>
inline auto ValueGetter<T, true>::GetPtr(InternalValue* val)
{
    auto ref = std::get_if<RecursiveWrapper<T>>(&val->GetData());
    return !ref ? nullptr : &ref->GetValue();
}

template<typename T, typename V>
auto& Get(V&& val)
{
    return ValueGetter<T, IsRecursive<T>::value>::Get(std::forward<V>(val).GetData());
}

template<typename T, typename V>
auto GetIf(V* val)
{
    return ValueGetter<T, IsRecursive<T>::value>::GetPtr(val);
}


inline InternalValue ListAdapter::GetValueByIndex(int64_t idx) const
{
    if (m_accessorProvider && m_accessorProvider())
    {
        const auto& val = m_accessorProvider()->GetItem(idx);
        if (val)
            return std::move(val.value());

        return InternalValue();
    }

    return InternalValue();
}

//inline InternalValue MapAdapter::GetValueByIndex(int64_t idx) const
//{
//    if (m_accessorProvider && m_accessorProvider())
//    {
//        return static_cast<const IListAccessor*>(m_accessorProvider())->GetItem(idx);
//    }

//    return InternalValue();
//}

inline InternalValue MapAdapter::GetValueByName(const std::string& name) const
{
    if (m_accessorProvider && m_accessorProvider())
    {
        return m_accessorProvider()->GetItem(name);
    }

    return InternalValue();
}

inline ListAccessorEnumeratorPtr ListAdapter::GetEnumerator() const {return m_accessorProvider()->CreateListAccessorEnumerator();}
inline ListAdapter::Iterator ListAdapter::begin() const {return Iterator(m_accessorProvider()->CreateListAccessorEnumerator());}
inline ListAdapter::Iterator ListAdapter::end() const {return Iterator();}


struct KeyValuePair
{
    std::string key;
    InternalValue value;
};


class Callable
{
public:
    enum Kind
    {
        GlobalFunc,
        SpecialFunc,
        Macro,
        UserCallable
    };
    using ExpressionCallable = std::function<InternalValue (const CallParams&, RenderContext&)>;
    using StatementCallable = std::function<void (const CallParams&, OutStream&, RenderContext&)>;

    using CallableHolder = std::variant<ExpressionCallable, StatementCallable>;

    enum class Type
    {
        Expression,
        Statement
    };

    Callable(Kind kind, ExpressionCallable&& callable)
        : m_kind(kind)
        , m_callable(std::move(callable))
    {
    }

    Callable(Kind kind, StatementCallable&& callable)
        : m_kind(kind)
        , m_callable(std::move(callable))
    {
    }

    auto GetType() const
    {
        return m_callable.index() == 0 ? Type::Expression : Type::Statement;
    }

    auto GetKind() const
    {
        return m_kind;
    }

    auto& GetCallable() const
    {
        return m_callable;
    }

    auto& GetExpressionCallable() const
    {
        return std::get<ExpressionCallable>(m_callable);
    }

    auto& GetStatementCallable() const
    {
        return std::get<StatementCallable>(m_callable);
    }

private:
    Kind m_kind;
    CallableHolder m_callable;
};

inline bool IsEmpty(const InternalValue& val)
{
    return val.IsEmpty() || std::get_if<EmptyValue>(&val.GetData()) != nullptr;
}

class RenderContext;

template<typename Fn>
auto MakeDynamicProperty(Fn&& fn)
{
    return CreateMapAdapter(InternalValueMap{
        {"value()", Callable(Callable::GlobalFunc, std::forward<Fn>(fn))}
    });
}

template<typename CharT>
auto sv_to_string(const std::basic_string_view<CharT>& sv)
{
    return std::basic_string<CharT>(sv.begin(), sv.end());
}

InternalValue Subscript(const InternalValue& val, const InternalValue& subscript, RenderContext* values);
InternalValue Subscript(const InternalValue& val, const std::string& subscript, RenderContext* values);
std::string AsString(const InternalValue& val);
ListAdapter ConvertToList(const InternalValue& val, bool& isConverted, bool strictConversion = true);
ListAdapter ConvertToList(const InternalValue& val, InternalValue subscipt, bool& isConverted, bool strictConversion = true);
Value IntValue2Value(const InternalValue& val);
Value OptIntValue2Value(std::optional<InternalValue> val);

} // namespace jinja2

#endif // JINJA2CPP_SRC_INTERNAL_VALUE_H
