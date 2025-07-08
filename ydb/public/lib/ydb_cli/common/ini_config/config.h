#pragma once

#include "value.h"

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/input.h>

namespace NIniConfig {

    class TConfigError : public yexception {};
    class TConfigParseError : public TConfigError {};
    class TTypeMismatch : public TConfigError {};

    // This is a copy of the original TConfig class from the library/cpp/config with only ini-specific functionality

    class TConfig {
    public:
        TConfig();
        TConfig(const TConfig& other) = default;
        TConfig& operator=(const TConfig& other) = default;

        explicit TConfig(IValue* v);

        template <typename T>
        bool IsA() const {
            return V_ && V_->IsA(typeid(T));
        }

        template <typename T>
        const T& Get() const {
            return *static_cast<const T*>(V_->Ptr());
        }

        template <typename T>
        T& GetNonConstant() const {
            return *static_cast<T*>(V_->Ptr());
        }

        template <typename T>
        T As() const {
            return ValueAs<T>(V_.Get());
        }

        template <typename T>
        T As(T def) const {
            return IsNull() ? def : As<T>();
        }

        bool IsNull() const;
        bool Has(const TStringBuf& key) const;
        const TConfig& operator[](const TStringBuf& key) const;
        const TConfig& At(const TStringBuf& key) const;
        const TConfig& Or(const TConfig& r) const;

        static TConfig ReadIni(TStringBuf content);

    private:
        TIntrusivePtr<IValue> V_;
    };

    struct TArray : public TVector<TConfig> {};
    struct TDict : public THashMap<TString, TConfig> {
        const TConfig& Find(const TStringBuf& key) const;
        const TConfig& At(const TStringBuf& key) const;
    };
}
