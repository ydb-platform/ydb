#pragma once

#include <util/generic/maybe.h>

#define FLUENT_SETTING(type, name) \
    type name##_; \
    TSelf& name(const type& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_OPTIONAL(type, name) \
    TMaybe<type> name##_; \
    TSelf& name(const TMaybe<type>& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_DEFAULT(type, name, defaultValue) \
    type name##_ = defaultValue; \
    TSelf& name(const type& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_FLAG(name) \
    bool name##_ = false; \
    TSelf& name(bool value = true) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_FLAG_ALIAS(name, other, value) \
    TSelf& name() { \
        other##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_VECTOR(type, name) \
    TVector<type> name##_; \
    TSelf& Append##name(const type& value) { \
        name##_.push_back(value); \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_OPTIONAL_VECTOR(type, name) \
    TMaybe<TVector<type>> name##_; \
    TSelf& Append##name(const type& value) { \
        if (!name##_) name##_ = TVector<type>{}; \
        name##_->push_back(value); \
        return static_cast<TSelf&>(*this); \
    }
