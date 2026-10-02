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
    class TConfigError: public TWithBackTrace<yexception> {
    };

    class TConfigParseError: public TConfigError {
    };

    class TTypeMismatch: public TConfigError {
    };

    struct TArray;
    struct TDict;

    class TConfig {
    public:
        inline TConfig()
            : V_(Null())
        {
        }

        inline TConfig(IValue* v)
            : V_(v)
        {
        }

        TConfig(const TConfig& config) = default;
        TConfig& operator=(const TConfig& config) = default;

        template <class T>
        inline bool IsA() const {
            return V_->IsA(typeid(T));
        }

        inline bool IsNumeric() const {
            return IsA<double>() || IsA<i64>() || IsA<ui64>();
        }

        template <class T>
        inline const T& Get() const {
            return GetNonConstant<T>();
        }

        template <class T>
        inline T& GetNonConstant() const {
            if (this->IsA<T>()) {
                return *(T*)V_->Ptr();
            }

            if constexpr (std::is_same_v<T, ::NIniConfig::TArray>) {
                NCfgPrivate::ReportTypeMismatch(V_->TypeName(), "array");
            } else if constexpr (std::is_same_v<T, ::NIniConfig::TDict>) {
                NCfgPrivate::ReportTypeMismatch(V_->TypeName(), "dict");
            } else if constexpr (std::is_same_v<T, TString>) {
                NCfgPrivate::ReportTypeMismatch(V_->TypeName(), "string");
            } else {
                NCfgPrivate::ReportTypeMismatch(V_->TypeName(), ::TypeName<T>());
            }
        }

        template <class T>
        inline T As() const {
            return ValueAs<T>(V_.Get());
        }

        template <class T>
        inline T As(T def) const {
            return IsNull() ? def : As<T>();
        }

        inline bool IsNull() const noexcept {
            return V_.Get() == Null();
        }

        const TConfig& Or(const TConfig& r) const {
            return IsNull() ? r : *this;
        }

        //assume value is dict
        bool Has(const TStringBuf& key) const;
        const TConfig& operator[](const TStringBuf& key) const;
        const TConfig& At(const TStringBuf& key) const;

        //assume value is array
        const TConfig& operator[](size_t index) const;
        size_t GetArraySize() const;

        static TConfig ReadIni(TStringBuf content);

    private:
        TIntrusivePtr<IValue> V_;
    };

    struct TArray: public TDeque<TConfig> {
        const TConfig& Index(size_t index) const;
        const TConfig& At(size_t index) const;
    };

    struct TDict: public THashMap<TString, TConfig> {
        const TConfig& Find(const TStringBuf& key) const;
        const TConfig& At(const TStringBuf& key) const;
    };
}
