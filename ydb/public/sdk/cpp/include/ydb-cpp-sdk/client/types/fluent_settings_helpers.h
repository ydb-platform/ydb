#pragma once

#define FLUENT_SETTING(type, name) \
    type name##_; \
    TSelf& name(const type& value) { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_OPTIONAL(type, name) \
    std::optional<type> name##_; \
    TSelf& name(const std::optional<type>& value) { \
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
    std::vector<type> name##_; \
    TSelf& Append##name(const type& value) { \
        name##_.push_back(value); \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_SETTING_OPTIONAL_VECTOR(type, name) \
    std::optional<std::vector<type>> name##_; \
    TSelf& Append##name(const type& value) { \
        if (!name##_) name##_ = std::vector<type>{}; \
        name##_->push_back(value); \
        return static_cast<TSelf&>(*this); \
    }
