#pragma once

#include "yql_setting.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/builder.h>
#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/generic/algorithm.h>


namespace NYql {

namespace NPrivate {

template <typename TType>
using TParser = std::function<TType(const TString&)>;

template <typename TType>
TParser<TType> GetDefaultParser() {
    return [] (const TString&) -> TType { throw yexception() << "Unsupported parser"; };
}

template <>
TParser<TString> GetDefaultParser<TString>();

template<>
TParser<bool> GetDefaultParser<bool>();

template <>
TParser<TGUID> GetDefaultParser<TGUID>();

template<>
TParser<NSize::TSize> GetDefaultParser<NSize::TSize>();

template<>
TParser<TInstant> GetDefaultParser<TInstant>();


#define YQL_PRIMITIVE_SETTING_PARSER_TYPES(XX)  \
    XX(ui8)                                     \
    XX(ui16)                                    \
    XX(ui32)                                    \
    XX(ui64)                                    \
    XX(i8)                                      \
    XX(i16)                                     \
    XX(i32)                                     \
    XX(i64)                                     \
    XX(float)                                   \
    XX(double)                                  \
    XX(TDuration)

#define YQL_CONTAINER_SETTING_PARSER_TYPES(XX)  \
    XX(TVector<TString>)                        \
    XX(TSet<TString>)                           \
    XX(THashSet<TString>)

#define YQL_DECLARE_SETTING_PARSER(type)    \
    template<>                              \
    TParser<type> GetDefaultParser<type>();

YQL_PRIMITIVE_SETTING_PARSER_TYPES(YQL_DECLARE_SETTING_PARSER)
YQL_CONTAINER_SETTING_PARSER_TYPES(YQL_DECLARE_SETTING_PARSER)

template<typename TType>
TMaybe<TType> GetValue(const NCommon::TConfSetting<TType, true>& setting, const TString& cluster) {
    return setting.Get(cluster);
}

template<typename TType>
TMaybe<TType> GetValue(const NCommon::TConfSetting<TType, false>& setting, const TString& cluster) {
    Y_UNUSED(cluster);
    return setting.Get();
}

}

namespace NCommon {

class TSettingDispatcher: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TSettingDispatcher>;
    // Returns true if can continue
    using TErrorCallback = std::function<bool(const TString& message, bool isError)>;

    enum class EStage {
        CONFIG,
        STATIC,
        RUNTIME,
    };

    class TSettingHandler: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<TSettingHandler>;

        TSettingHandler(const TString& name)
            : Name_(name)
        {
        }

        const TString& GetDisplayName() const {
            return Name_ ;
        }

        virtual bool Handle(const TString& cluster, const TMaybe<TString>& value, bool validateOnly, const TErrorCallback& errorCallback) = 0;
        virtual void FreezeDefault() = 0;
        virtual void Restore(const TString& cluster) = 0;
        virtual bool IsRuntime() const = 0;

    protected:
        TString Name_;
    };

    template <typename TType, bool RUNTIME>
    class TSettingHandlerImpl: public TSettingHandler {
    public:
        using TValueCallback = std::function<void(const TString&, TType)>;

    private:
        friend class TSettingDispatcher;

        TSettingHandlerImpl(const TString& name, TConfSetting<TType, RUNTIME>& setting)
            : TSettingHandler(name)
            , Setting_(setting)
            , Parser_(::NYql::NPrivate::GetDefaultParser<TType>())
            , ValueSetter_([this](const TString& cluster, TType value) {
                Setting_[cluster] = value;
            })
        {
        }

        bool Handle(const TString& cluster, const TMaybe<TString>& value, bool validateOnly, const TErrorCallback& errorCallback) override {
            if (value) {
                try {
                    TType v = Parser_(*value);

                    for (auto& validate: Validators_) {
                        validate(cluster, v);
                    }
                    if (!validateOnly) {
                        ValueSetter_(cluster, v);
                        if (Warning_) {
                            return errorCallback(Warning_, false);
                        }
                    }
                } catch (...) {
                    return errorCallback(TStringBuilder() << "Bad " << Name_.Quote() << " setting for " << cluster.Quote() << " cluster: " << CurrentExceptionMessage(), true);
                }
            } else if (!validateOnly) {
                try {
                    Restore(cluster);
                } catch (...) {
                    return errorCallback(CurrentExceptionMessage(), true);
                }
            }
            return true;
        }

        void FreezeDefault() override {
            Defaul_ = Setting_;
        }

        void Restore(const TString& cluster) override {
            if (!Defaul_) {
                ythrow yexception() << "Cannot restore " << Name_.Quote() << " setting without freeze";
            }
            if (ALL_CLUSTERS == cluster) {
                Setting_ = Defaul_.GetRef();
            } else {
                if (auto value = NPrivate::GetValue(Defaul_.GetRef(), cluster)) {
                    Setting_[cluster] = *value;
                } else {
                    Setting_.Clear();
                }
            }
        }

    public:
        bool IsRuntime() const override {
            return Setting_.IsRuntime();
        }

        TSettingHandlerImpl& Lower(TType lower) {
            Validators_.push_back([lower](const TString&, TType value) {
                if (value < lower) {
                    throw yexception() << "Value " << value << " is less than " << lower << " allowed lower bound";
                }
            });
            return *this;
        }

        TSettingHandlerImpl& Upper(TType upper) {
            Validators_.push_back([upper](const TString&, TType value) {
                if (value > upper) {
                    throw yexception() << "Value " << value << " is greater than " << upper << " allowed upper bound";
                }
            });
            return *this;
        }

        template <class TContainer>
        TSettingHandlerImpl& Enum(const TContainer& container) {
            THashSet<TType> allowed(container.cbegin(), container.cend());
            Validators_.push_back([allowed = std::move(allowed)](const TString&, TType value) {
                if (!allowed.has(value)) {
                    throw yexception() << "Value " << value << " is not in set of allowed values: " << JoinSeq(TStringBuf(","), allowed);
                }
            });
            return *this;
        }

        TSettingHandlerImpl& Enum(std::initializer_list<TType> list) {
            THashSet<TType> allowed(list);
            Validators_.push_back([allowed = std::move(allowed)](const TString&, TType value) {
                if (!allowed.contains(value)) {
                    throw yexception() << "Value " << value << " is not in set of allowed values: " << JoinSeq(TStringBuf(","), allowed);
                }
            });
            return *this;
        }

        TSettingHandlerImpl& NonEmpty() {
            Validators_.push_back([](const TString&, TType value) {
                if (value.empty()) {
                    throw yexception() << "Value is empty";
                }
            });
            return *this;
        }

        TSettingHandlerImpl& GlobalOnly() {
            Validators_.push_back([] (const TString& cluster, TType) {
                if (cluster != NCommon::ALL_CLUSTERS) {
                    throw yexception() << "Option cannot be used with specific cluster";
                }
            });
            return *this;
        }

        TSettingHandlerImpl& Validator(TValueCallback&& validator) {
            Validators_.push_back(std::move(validator));
            return *this;
        }

        TSettingHandlerImpl& Validator(const TValueCallback& validator) {
            Validators_.push_back(validator);
            return *this;
        }

        TSettingHandlerImpl& Parser(::NYql::NPrivate::TParser<TType>&& parser) {
            Parser_ = std::move(parser);
            return *this;
        }

        TSettingHandlerImpl& Parser(const ::NYql::NPrivate::TParser<TType>& parser) {
            Parser_ = parser;
            return *this;
        }

        TSettingHandlerImpl& ValueSetter(TValueCallback&& hook) {
            ValueSetter_ = std::move(hook);
            return *this;
        }

        TSettingHandlerImpl& ValueSetter(const TValueCallback& hook) {
            ValueSetter_ = hook;
            return *this;
        }

        TSettingHandlerImpl& ValueSetterWithRestore(TValueCallback&& hook) {
            ValueSetter_ = [this, hook = std::move(hook)] (const TString& cluster, TType value) {
                if (Defaul_) {
                    Restore(cluster);
                }
                hook(cluster, value);
            };
            return *this;
        }

        TSettingHandlerImpl& ValueSetterWithRestore(const TValueCallback& hook) {
            ValueSetter_ = [this, hook] (const TString& cluster, TType value) {
                if (Defaul_) {
                    Restore(cluster);
                }
                hook(cluster, value);
            };
            return *this;
        }

        TSettingHandlerImpl& Warning(const TString& message) {
            Warning_ = message;
            return *this;
        }

        TSettingHandlerImpl& Deprecated() {
            Warning_ = TStringBuilder() << "Pragma \"" << Name_ << "\" is deprecated and has no effect";
            return *this;
        }

    private:
        TConfSetting<TType, RUNTIME>& Setting_;
        TMaybe<TConfSetting<TType, RUNTIME>> Defaul_;
        ::NYql::NPrivate::TParser<TType> Parser_;
        TValueCallback ValueSetter_;
        TVector<TValueCallback> Validators_;
        TString Warning_;
    };

    TSettingDispatcher() = default;
    TSettingDispatcher(const TSettingDispatcher&) = delete;

    template <class TContainer>
    TSettingDispatcher(const TContainer& validClusters)
        : ValidClusters(validClusters.begin(), validClusters.end())
    {
    }

    template <class TContainer>
    void SetValidClusters(const TContainer& validClusters) {
        ValidClusters.clear();
        ValidClusters.insert(validClusters.begin(), validClusters.end());
    }

    void AddValidCluster(const TString& cluster) {
        ValidClusters.insert(cluster);
    }

    template <typename TType, bool RUNTIME>
    TSettingHandlerImpl<TType, RUNTIME>& AddSetting(const TString& name, TConfSetting<TType, RUNTIME>& setting) {
        TIntrusivePtr<TSettingHandlerImpl<TType, RUNTIME>> handler = new TSettingHandlerImpl<TType, RUNTIME>(name, setting);
        if (!Handlers.insert({NormalizeName(name), handler}).second) {
            ythrow yexception() << "Duplicate configuration setting name " << name.Quote();
        }
        return *handler;
    }

    bool IsRuntime(const TString& name);

    bool Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value, EStage stage, const TErrorCallback& errorCallback);

    template <class TContainer, typename TFilter>
    void Dispatch(const TString& cluster, const TContainer& clusterValues, const TFilter& filter) {
        auto errorCallback = GetDefaultErrorCallback();
        for (auto& v: clusterValues) {
            if (filter(v)) {
                Dispatch(cluster, v.GetName(), v.GetValue(), EStage::CONFIG, errorCallback);
            }
        }
    }

    template <class TContainer>
    void Dispatch(const TString& cluster, const TContainer& clusterValues) {
        auto errorCallback = GetDefaultErrorCallback();
        for (auto& v: clusterValues) {
            Dispatch(cluster, v.GetName(), v.GetValue(), EStage::CONFIG, errorCallback);
        }
    }

    template <class TContainer, typename TFilter>
    void Dispatch(const TContainer& globalValues, const TFilter& filter) {
        Dispatch(ALL_CLUSTERS, globalValues, filter);
    }

    template <class TContainer>
    void Dispatch(const TContainer& globalValues) {
        Dispatch(ALL_CLUSTERS, globalValues);
    }

    void FreezeDefaults();
    void Restore();
    static TErrorCallback GetDefaultErrorCallback();
    static TErrorCallback GetErrorCallback(TPositionHandle pos, TExprContext& ctx);

protected:
    THashSet<TString> ValidClusters;
    THashMap<TString, TSettingHandler::TPtr> Handlers;
};

} // namespace NCommon
} // namespace NYql

#define REGISTER_SETTING(dispatcher, setting) \
    (dispatcher).AddSetting(#setting, setting)
