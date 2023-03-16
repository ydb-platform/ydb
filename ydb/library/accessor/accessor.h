#pragma once

#include <util/generic/ptr.h>

#define YDB_READONLY_FLAG(name, defaultValue) \
private:\
    bool name ## Flag = defaultValue;\
public:\
    bool Is ## name() const noexcept {\
        return name ## Flag;\
    }\
private:

#define YDB_FLAG_ACCESSOR(name, defaultValue) \
YDB_READONLY_FLAG(name, defaultValue) \
public:\
    auto& Set##name(const bool value) noexcept {\
        name ## Flag = value;\
        return *this;\
    }\
    bool Get##name() const noexcept {\
        return name ## Flag;\
    }\
private:

namespace NYDBAccessor {
template <class T>
using TReturnTypeDetector = std::conditional_t<std::is_trivial_v<T> && sizeof(T) <= sizeof(uintptr_t), T, const T&>;
}

#define YDB_READONLY_ACCESSOR_IMPL(name, type, declSpecifiers, defaultValue) \
private:\
    declSpecifiers type name = defaultValue;\
public:\
    NYDBAccessor::TReturnTypeDetector<type> Get ## name() const noexcept {\
        return name;\
    }\
private:

#define YDB_ACCESSOR_IMPL(ClassName, name, type, declSpecifiers, defaultValue, methodsModifier) \
YDB_READONLY_ACCESSOR_IMPL(name, type, declSpecifiers, defaultValue)\
public:\
    ClassName& Set ## name(const type& value) methodsModifier {\
        name = value;\
        return *this;\
    }\
    ClassName& Set##name(type&& value) methodsModifier {\
        name = std::move(value);\
        return *this;\
    }\
    type& Mutable ## name() methodsModifier {\
        return name;\
    }\
private:

#define YDB_ACCESSOR(type, name, defaultValue) YDB_ACCESSOR_IMPL(auto, name, type, private:, defaultValue, noexcept)
#define YDB_ACCESSOR_DEF(type, name) YDB_ACCESSOR_IMPL(auto, name, type, private:, type(), noexcept)
#define YDB_ACCESSOR_MUTABLE(type, name, defaultValue) YDB_ACCESSOR_IMPL(const auto, name, type, mutable, defaultValue, const noexcept)
#define YDB_ACCESSOR_PROTECT(type, name, defaultValue) YDB_ACCESSOR_IMPL(auto, name, type, protected:, defaultValue, noexcept)
#define YDB_ACCESSOR_PROTECT_DEF(type, name) YDB_ACCESSOR_IMPL(auto, name, type, protected:, type(), noexcept)

#define YDB_READONLY(type, name, defaultValue) YDB_READONLY_ACCESSOR_IMPL(name, type, private:, defaultValue)
#define YDB_READONLY_CONST(type, name) YDB_READONLY_ACCESSOR_IMPL(name, type, const, type())
#define YDB_READONLY_DEF(type, name) YDB_READONLY_ACCESSOR_IMPL(name, type, private:, type())
#define YDB_READONLY_PROTECT(type, name, defaultValue) YDB_READONLY_ACCESSOR_IMPL(name, type, protected:, defaultValue)
#define YDB_READONLY_PROTECT_DEF(type, name) YDB_READONLY_ACCESSOR_IMPL(name, type, protected:, type())
#define YDB_READONLY_MUTABLE(type, name, defaultValue) YDB_READONLY_ACCESSOR_IMPL(name, type, mutable, defaultValue)
#define YDB_READONLY_MUTABLE_DEF(type, name) YDB_READONLY_ACCESSOR_IMPL(name, type, mutable, type())

#define YDB_READONLY_ACCESSOR_OPT_IMPL(name, type, declSpecifiers) \
private:\
    declSpecifiers std::optional<type> name;\
public:\
    bool Has##name() const noexcept {\
        return name.has_value();\
    }\
    NYDBAccessor::TReturnTypeDetector<type> Get##name##Unsafe() const noexcept {\
        return *name;\
    }\
    type& Get##name##Unsafe() noexcept {\
        return *name;\
    }\
    const std::optional<type>& Get##name##Optional() const noexcept {\
        return name;\
    }\
    std::optional<type> Get##name##MaybeDetach() const noexcept {\
        if (name) {\
            return *name;\
        } else {\
            return {};\
        }\
    }\
    type Get##name##Def(const type defValue) const noexcept {\
        return name.value_or(defValue);\
    }\
    const type* Get##name##Safe() const noexcept {\
        return name ? &name.value() : nullptr;\
    }\
private:

#define YDB_ACCESSOR_OPT_IMPL(name, type, declSpecifiers) \
YDB_READONLY_ACCESSOR_OPT_IMPL(name, type, declSpecifiers)\
public:\
    auto& Set##name(const type& value) noexcept {\
        name = value;\
        return *this;\
    }\
    auto& Set##name(type&& value) noexcept {\
        name = std::move(value);\
        return *this;\
    }\
    auto& Set##name(std::optional<type>&& value) noexcept {\
        name = std::move(value);\
        return *this;\
    }\
    auto& Set##name(const std::optional<type>& value) noexcept {\
        name = value;\
        return *this;\
    }\
    auto& Drop##name() noexcept {\
        name = {};\
        return *this;\
    }\
    std::optional<type>& Mutable##name##Optional() noexcept {\
        return name;\
    }\
private:

#define YDB_ACCESSOR_OPT_MUTABLE_IMPL(name, type, declSpecifiers) \
YDB_READONLY_ACCESSOR_OPT_IMPL(name, type, declSpecifiers)\
public:\
    const auto& Set##name(const type& value) const noexcept {\
        name = value;\
        return *this;\
    }\
    const auto& Set##name(type&& value) const noexcept {\
        name = std::move(value);\
        return *this;\
    }\
    const auto& Drop##name() const noexcept {\
        name = {};\
        return *this;\
    }\
    std::optional<type>& Mutable##name##Optional() const noexcept {\
        return name;\
    }\
private:

#define YDB_OPT(type, name) YDB_ACCESSOR_OPT_IMPL(name, type, private:)
#define YDB_OPT_MUTABLE(type, name) YDB_ACCESSOR_OPT_MUTABLE_IMPL(name, type, mutable)
#define YDB_OPT_PROTECTED(type, name) YDB_ACCESSOR_OPT_IMPL(name, type, protected:)
#define YDB_READONLY_OPT(type, name) YDB_READONLY_ACCESSOR_OPT_IMPL(name, type, private:)
#define YDB_READONLY_OPT_PROTECTED(type, name) YDB_READONLY_ACCESSOR_OPT_IMPL(name, type, protected:)
