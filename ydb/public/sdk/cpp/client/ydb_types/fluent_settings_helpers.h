#pragma once

#include <util/generic/maybe.h>

#define FLUENT_SETTING_DEPRECATED(type, name) \
    type name##_; \
    TSelf& name(const type& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_OPTIONAL_DEPRECATED(type, name) \
    TMaybe<type> name##_; \
    TSelf& name(const TMaybe<type>& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_DEFAULT_DEPRECATED(type, name, defaultValue) \
    type name##_ = defaultValue; \
    TSelf& name(const type& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_FLAG_DEPRECATED(name) \
    bool name##_ = false; \
    TSelf& name(bool value = true) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_FLAG_ALIAS_DEPRECATED(name, other, value) \
    TSelf& name() { \
        other##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_VECTOR_DEPRECATED(type, name) \
    TVector<type> name##_; \
    TSelf& Append##name(const type& value) { \
        name##_.push_back(value); \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_OPTIONAL_VECTOR_DEPRECATED(type, name) \
    TMaybe<TVector<type>> name##_; \
    TSelf& Append##name(const type& value) { \
        if (!name##_) name##_ = TVector<type>{}; \
        name##_->push_back(value); \
        return static_cast<TSelf&>(*this); \
    }
