#pragma once

#include <library/cpp/scheme/scheme.h>


// Scheme adapter that holds referenced value
template <typename TScheme>
class TSchemeHolder {
public:
    TSchemeHolder()
        : Value_(NSc::Null())
        , Scheme_(&Value_)
    {
    }

    explicit TSchemeHolder(NSc::TValue&& value)
        : Value_(std::move(value))
        , Scheme_(&Value_)
    {
    }

    explicit TSchemeHolder(const NSc::TValue& value)
        : Value_(value)
        , Scheme_(&Value_)
    {
    }

    TSchemeHolder(TSchemeHolder<TScheme>&& rhs)
        : Value_(std::move(rhs.Value_))
        , Scheme_(&Value_)
    {
    }

    TSchemeHolder(const TSchemeHolder<TScheme>& rhs)
        : Value_(rhs.Value_)
        , Scheme_(&Value_)
    {
    }

    TSchemeHolder<TScheme>& operator=(TSchemeHolder<TScheme>&& rhs) {
        Value_ = std::move(rhs.Value_);
        return *this;
    }
    TSchemeHolder<TScheme>& operator=(const TSchemeHolder<TScheme>& rhs) {
        Value_ = rhs.Value_;
        return *this;
    }

    TScheme& Scheme() {
        return Scheme_;
    }
    const TScheme& Scheme() const {
        return Scheme_;
    }
    TScheme& operator->() {
        return Scheme_;
    }
    const TScheme& operator->() const {
        return Scheme_;
    }

    NSc::TValue& Value() {
        return Value_;
    }
    const NSc::TValue& Value() const {
        return Value_;
    }

    bool IsNull() const {
        return Value_.IsNull();
    }

private:
    NSc::TValue Value_;
    TScheme Scheme_;
};
