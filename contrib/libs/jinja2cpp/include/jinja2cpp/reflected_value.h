#ifndef JINJA2CPP_REFLECTED_VALUE_H
#define JINJA2CPP_REFLECTED_VALUE_H

#include "value.h"

#include <optional>

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

namespace jinja2
{

/*!
 * \brief Reflect the arbitrary C++ type to the Jinja2C++ engine
 *
 * Generic method which reflects arbitrary C++ type to the Jinja2C++ template engine. The way of reflection depends on the actual reflected type and could be
 * - Reflect as an exact value if the type is basic type (such as `char`, `int`, `double`, `std::string` etc.)
 * - Reflect as a GenericList/GenericMap for standard containers respectively
 * - Reflect as a GenericMap for the user types
 * Also pointers/shared pointers to the types could be reflected.
 *
 * Reflected value takes ownership on the object, passed to the `Reflect` method by r-value reference or value. Actually, such object is moved. For const
 * references or pointers reflected value holds the pointer to the reflected object. So, it's necessary to be sure that life time of the reflected object is
 * longer than it's usage within the template.
 *
 * In order to reflect custom (user) the \ref jinja2::TypeReflection template should be specialized in the following way:
 * ```c++
 * struct jinja2::TypeReflection<TestStruct> : jinja2::TypeReflected<TestStruct>
 * {
 *     using FieldAccessor = typename jinja2::TypeReflected<TestStruct>::FieldAccessor;
 *     static auto& GetAccessors()
 *     {
 *         static std::unordered_map<std::string, FieldAccessor> accessors = {
 *             {"intValue", [](const TestStruct& obj) {assert(obj.isAlive); return jinja2::Reflect(obj.intValue);}},
 *             {"intEvenValue", [](const TestStruct& obj) -> Value
 *              {
 *                  assert(obj.isAlive);
 *                  if (obj.intValue % 2)
 *                     return {};
 *                  return {obj.intValue};
 *              }},
 * 	   };
 *
 *         return accessors;
 *     }
 * };
 *
 * `TestStruct` here is a type which should be reflected. Specialization of the \ref TypeReflection template should derived from \ref TypeReflected template
 * and define only one method: `GetAccessors`. This method returns the unordered map object which maps field name (as a string) to the corresponded field
 * accessor. And field accessor here is a lambda object which takes the reflected object reference and returns the value of the field from it.
 *
 * @tparam T Type of value to reflect
 * @param val Value to reflect
 *
 * @return jinja2::Value which contains the reflected value or the empty one
 */
template<typename T>
Value Reflect(T&& val);

template<typename T, bool val>
struct TypeReflectedImpl : std::integral_constant<bool, val>
{
};

template<typename T>
using FieldAccessor = std::function<Value(const T&)>;

template<typename T>
struct TypeReflected : TypeReflectedImpl<T, true>
{
    using FieldAccessor = jinja2::FieldAccessor<T>;
};



template<typename T, typename = void>
struct TypeReflection : TypeReflectedImpl<T, false>
{
};

#ifndef JINJA2CPP_NO_DOXYGEN
template<typename Derived>
class ReflectedMapImplBase : public IMapItemAccessor
{
public:
    bool HasValue(const std::string& name) const override
    {
        return Derived::GetAccessors().count(name) != 0;
    }
    Value GetValueByName(const std::string& name) const override
    {
        const auto& accessors = Derived::GetAccessors();
        auto p = accessors.find(name);
        if (p == accessors.end())
            throw std::runtime_error("Invalid field access");

        return static_cast<const Derived*>(this)->GetField(p->second);
    }
    std::vector<std::string> GetKeys() const override
    {
        std::vector<std::string> result;
        const auto& accessors = Derived::GetAccessors();
        for (auto& i : accessors)
            result.push_back(i.first);

        return result;
    }
    size_t GetSize() const override
    {
        return Derived::GetAccessors().size();
    }
};

template<typename T, bool byValue = true>
class ReflectedDataHolder;

template<typename T>
class ReflectedDataHolder<T, true>
{
public:
    explicit ReflectedDataHolder(T val) : m_value(std::move(val)) {}
    explicit ReflectedDataHolder(const T* val) : m_valuePtr(val) {}

protected:
    const T* GetValue() const
    {
        return m_valuePtr ? m_valuePtr : (m_value ? &m_value.value() : nullptr);
    }

private:
    std::optional<T> m_value;
    const T* m_valuePtr = nullptr;
};

template<typename T>
class ReflectedDataHolder<T, false>
{
public:
    explicit ReflectedDataHolder(const T* val) : m_valuePtr(val) {}

protected:
    const T* GetValue() const
    {
        return m_valuePtr;
    }

private:
    const T* m_valuePtr = nullptr;
};

template<typename T>
class ReflectedMapImpl : public ReflectedMapImplBase<ReflectedMapImpl<T>>, public ReflectedDataHolder<T>
{
public:
    using ReflectedDataHolder<T>::ReflectedDataHolder;
    using ThisType = ReflectedMapImpl<T>;

    static auto GetAccessors() {return TypeReflection<T>::GetAccessors();}
    template<typename Fn>
    Value GetField(Fn&& accessor) const
    {
        auto v = this->GetValue();
        if (!v)
            return Value();
        return accessor(*v);
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ThisType*>(&other);
        if (!val)
            return false;

        return this->GetValue() == val->GetValue();
    }
};

namespace detail
{
template<typename T, typename Tag = void>
struct Reflector;

template<typename T>
using IsReflectedType = std::enable_if_t<TypeReflection<T>::value>;

template<typename It>
struct Enumerator : public IListEnumerator
{
    using ThisType = Enumerator<It>;
    It m_begin;
    It m_cur;
    It m_end;
    bool m_justInited = true;

    Enumerator(It begin, It end)
        : m_begin(begin)
        , m_cur(end)
        , m_end(end)
    {}

    void Reset() override
    {
        m_justInited = true;
    }

    bool MoveNext() override
    {
        if (m_justInited)
        {
            m_cur = m_begin;
            m_justInited = false;
        }
        else
        {
            ++ m_cur;
        }

        return m_cur != m_end;
    }

    Value GetCurrent() const override
    {
        return Reflect(*m_cur);
    }

    ListEnumeratorPtr Clone() const override
    {
        auto result = std::make_unique<Enumerator<It>>(m_begin, m_end);
        result->m_cur = m_cur;
        result->m_justInited = m_justInited;
        return jinja2::ListEnumeratorPtr(result.release()); //, Deleter);
    }

    ListEnumeratorPtr Move() override
    {
        auto result = std::make_unique<Enumerator<It>>(m_begin, m_end);
        result->m_cur = std::move(m_cur);
        result->m_justInited = m_justInited;
        this->m_justInited = true;
        return jinja2::ListEnumeratorPtr(result.release()); //, Deleter);
    }

    bool IsEqual(const IComparable& other) const override
    {
        auto* val = dynamic_cast<const ThisType*>(&other);
        if (!val)
            return false;
        if (m_begin != val->m_begin)
            return false;
        if (m_cur != val->m_cur)
            return false;
        if (m_end != val->m_end)
            return false;
        if (m_justInited != val->m_justInited)
            return false;
        return true;
    }

    /*  
    It m_begin;
    It m_cur;
    It m_end;
    bool m_justInited = true;
*/
/*
    static void Deleter(IListEnumerator* e)
    {
        delete static_cast<Enumerator<It>*>(e);
    }
    */
};

struct ContainerReflector
{
    template<typename T>
    struct ValueItemAccessor : IListItemAccessor, IIndexBasedAccessor
    {
        using ThisType = ValueItemAccessor<T>;

        T m_value;

        explicit ValueItemAccessor(T&& cont) noexcept
            : m_value(std::move(cont))
        {
        }

        std::optional<size_t> GetSize() const override
        {
            return m_value.size();
        }

        const IIndexBasedAccessor* GetIndexer() const override
        {
            return this;
        }

        ListEnumeratorPtr CreateEnumerator() const override
        {
            using Enum = Enumerator<typename T::const_iterator>;
            return jinja2::ListEnumeratorPtr(new Enum(m_value.begin(), m_value.end()));//, Enum::Deleter);
        }

        Value GetItemByIndex(int64_t idx) const override
        {
            auto p = m_value.begin();
            std::advance(p, static_cast<size_t>(idx));
            return Reflect(*p);
        }

        bool IsEqual(const IComparable& other) const override
        {
            auto* val = dynamic_cast<const ThisType*>(&other);
            if (!val)
                return false;
             auto enumerator = CreateEnumerator();
             auto otherEnum = val->CreateEnumerator();
             if (enumerator && otherEnum && !enumerator->IsEqual(*otherEnum))
                 return false;
             return true;
        }
    };

    template<typename T>
    struct PtrItemAccessor : IListItemAccessor, IIndexBasedAccessor
    {
        using ThisType = PtrItemAccessor<T>;

        const T* m_value{};

        explicit PtrItemAccessor(const T* ptr)
            : m_value(ptr)
        {
        }
        std::optional<size_t> GetSize() const override
        {
            return m_value->size();
        }
        const IIndexBasedAccessor* GetIndexer() const override
        {
            return this;
        }

        ListEnumeratorPtr CreateEnumerator() const override
        {
            using Enum = Enumerator<typename T::const_iterator>;
            return jinja2::ListEnumeratorPtr(new Enum(m_value->begin(), m_value->end()));//, Enum::Deleter);
        }

        Value GetItemByIndex(int64_t idx) const override
        {
            auto p = m_value->begin();
            std::advance(p, static_cast<size_t>(idx));
            return Reflect(*p);
        }

        bool IsEqual(const IComparable& other) const override
        {
            auto* val = dynamic_cast<const ThisType*>(&other);
            if (!val)
                return false;
             auto enumerator = CreateEnumerator();
             auto otherEnum = val->CreateEnumerator();
             if (enumerator && otherEnum && !enumerator->IsEqual(*otherEnum))
                 return false;
             return true;
        }
    };

    template<typename T>
    static Value CreateFromValue(T&& cont)
    {
        return GenericList([accessor = ValueItemAccessor<T>(std::forward<T>(cont))]() {return &accessor;});
    }

    template<typename T>
    static Value CreateFromPtr(const T* cont)
    {
        return GenericList([accessor = PtrItemAccessor<T>(cont)]() {return &accessor;});
    }

    template<typename T>
    static Value CreateFromPtr(std::shared_ptr<T> cont)
    {
        return GenericList([ptr = std::move(cont), accessor = PtrItemAccessor<T>(cont.get())]() {return &accessor;});
    }
};

template<typename T>
struct Reflector<std::set<T>>
{
    static auto Create(std::set<T> val)
    {
        return ContainerReflector::CreateFromValue(std::move(val));
    }
    static auto CreateFromPtr(const std::set<T>* val)
    {
        return ContainerReflector::CreateFromPtr(val);
    }
};

template<typename T>
struct Reflector<std::vector<T>>
{
    static auto Create(std::vector<T> val)
    {
        return ContainerReflector::CreateFromValue(std::move(val));
    }
    template<typename U>
    static auto CreateFromPtr(U&& val)
    {
        return ContainerReflector::CreateFromPtr(std::forward<U>(val));
    }
};

template<typename T>
struct Reflector<T, IsReflectedType<T>>
{
    static auto Create(const T& val)
    {
        return GenericMap([accessor = ReflectedMapImpl<T>(val)]() {return &accessor;});
    }

    static auto CreateFromPtr(const T* val)
    {
        return GenericMap([accessor = ReflectedMapImpl<T>(static_cast<const T*>(val))]() {return &accessor;});
    }

    static auto CreateFromPtr(std::shared_ptr<T> val)
    {
        return GenericMap([ptr = val, accessor = ReflectedMapImpl<T>(val.get())]() {return &accessor;});
    }
};

template<typename T>
struct Reflector<const T&>
{
    static auto Create(const T& val)
    {
        return Reflector<T>::CreateFromPtr(&val);
    }
    static auto Create(const T*& val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }
};

template<typename T>
struct Reflector<const T*&>
{
    static auto Create(const T*& val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }

};

template<typename T>
struct Reflector<const T*const&>
{
    static auto Create(const T*const& val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }

};

template<typename T>
struct Reflector<const std::shared_ptr<T>&>
{
    static auto Create(const std::shared_ptr<T>& val)
    {
        return Reflector<T>::CreateFromPtr(val.get());
    }
};

template<typename T>
struct Reflector<T&>
{
    static auto Create(T& val)
    {
        return Reflector<T>::Create(val);
    }
};

template<typename T>
struct Reflector<const T*>
{
    static auto Create(const T* val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }
    static auto CreateFromPtr(const T* val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }
};

template<typename T>
struct Reflector<T*>
{
    static auto Create(T* val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }
};

template<typename T>
struct Reflector<std::shared_ptr<T>>
{
    static auto Create(std::shared_ptr<T> val)
    {
        return Reflector<T>::CreateFromPtr(val);
    }
};

template<typename CharT>
struct Reflector<std::basic_string<CharT>>
{
    static auto Create(std::basic_string<CharT> str) {
        return Value(std::move(str));
    }
    static auto CreateFromPtr(const std::basic_string<CharT>* str) {
        return Value(*str);
    }
};

template<typename CharT>
struct Reflector<std::basic_string_view<CharT>>
{
    static auto Create(std::basic_string_view<CharT> str) { return Value(std::move(str)); }
    static auto CreateFromPtr(const std::basic_string_view<CharT>* str) { return Value(*str); }
};

template<>
struct Reflector<bool>
{
    static auto Create(bool val)
    {
        return Value(val);
    }
    static auto CreateFromPtr(const bool* val)
    {
        return Value(*val);
    }
};

template<>
struct Reflector<float>
{
    static auto Create(double val) { return Value(val); }
    static auto CreateFromPtr(const float* val) { return Value(static_cast<double>(*val)); }
};

template<>
struct Reflector<double>
{
    static auto Create(double val) { return Value(val); }
    static auto CreateFromPtr(const double* val) { return Value(*val); }
};

#define JINJA2_INT_REFLECTOR(Type) \
template<> \
struct Reflector<Type> \
{ \
    static auto Create(Type val) \
    { \
        return Value(static_cast<int64_t>(val)); \
    } \
    static auto CreateFromPtr(const Type* val) \
    { \
        return Value(static_cast<int64_t>(*val)); \
    } \
}

JINJA2_INT_REFLECTOR(char);
JINJA2_INT_REFLECTOR(wchar_t);
JINJA2_INT_REFLECTOR(int8_t);
JINJA2_INT_REFLECTOR(uint8_t);
JINJA2_INT_REFLECTOR(int16_t);
JINJA2_INT_REFLECTOR(uint16_t);
JINJA2_INT_REFLECTOR(int32_t);
JINJA2_INT_REFLECTOR(uint32_t);
JINJA2_INT_REFLECTOR(int64_t);
JINJA2_INT_REFLECTOR(uint64_t);
} // namespace detail
#endif

template<typename T>
Value Reflect(T&& val)
{
    return detail::Reflector<T>::Create(std::forward<T>(val));
}

} // namespace jinja2

#endif // JINJA2CPP_REFLECTED_VALUE_H
