#pragma once

#include "yql_setting.h"

#include <yql/essentials/providers/common/config/yql_config_qplayer.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/sql_types/normalize_name.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>

#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/builder.h>
#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/generic/algorithm.h>

#include <concepts>
#include <functional>
#include <type_traits>
#include <ranges>
#include <utility>

namespace NYql::NCommon {

namespace NPrivate {

template <typename TType>
using TParser = std::function<TType(const TString&)>;

template <typename TType>
TParser<TType> GetDefaultParser() {
    return [](const TString&) -> TType { throw yexception() << "Unsupported parser"; };
}

template <>
TParser<TString> GetDefaultParser<TString>();

template <>
TParser<bool> GetDefaultParser<bool>();

template <>
TParser<TGUID> GetDefaultParser<TGUID>();

template <>
TParser<NSize::TSize> GetDefaultParser<NSize::TSize>();

template <>
TParser<TInstant> GetDefaultParser<TInstant>();

#define YQL_PRIMITIVE_SETTING_PARSER_TYPES(XX) \
    XX(ui8)                                    \
    XX(ui16)                                   \
    XX(ui32)                                   \
    XX(ui64)                                   \
    XX(i8)                                     \
    XX(i16)                                    \
    XX(i32)                                    \
    XX(i64)                                    \
    XX(float)                                  \
    XX(double)                                 \
    XX(TDuration)

#define YQL_CONTAINER_SETTING_PARSER_TYPES(XX) \
    XX(TVector<TString>)                       \
    XX(TSet<TString>)                          \
    XX(THashSet<TString>)

#define YQL_DECLARE_SETTING_PARSER(type) \
    template <>                          \
    TParser<type> GetDefaultParser<type>();

YQL_PRIMITIVE_SETTING_PARSER_TYPES(YQL_DECLARE_SETTING_PARSER)
YQL_CONTAINER_SETTING_PARSER_TYPES(YQL_DECLARE_SETTING_PARSER)

template <typename TType>
using TSerializer = std::function<TString(const TType&)>;

template <typename TType>
TSerializer<TType> GetDefaultSerializer() {
    return [](const TType&) -> TString { throw yexception() << "Unsupported serializer"; };
}

template <>
TSerializer<TString> GetDefaultSerializer<TString>();

template <>
TSerializer<bool> GetDefaultSerializer<bool>();

template <>
TSerializer<TGUID> GetDefaultSerializer<TGUID>();

template <>
TSerializer<NSize::TSize> GetDefaultSerializer<NSize::TSize>();

template <>
TSerializer<TInstant> GetDefaultSerializer<TInstant>();

#define YQL_DECLARE_SETTING_SERIALIZER(type) \
    template <>                              \
    TSerializer<type> GetDefaultSerializer<type>();

YQL_PRIMITIVE_SETTING_PARSER_TYPES(YQL_DECLARE_SETTING_SERIALIZER)
YQL_CONTAINER_SETTING_PARSER_TYPES(YQL_DECLARE_SETTING_SERIALIZER)

template <typename TType>
TMaybe<TType> GetValue(const NCommon::TConfSetting<TType, NCommon::EConfSettingType::StaticPerCluster>& setting, const TString& cluster) {
    return setting.Get(cluster);
}

template <typename TType>
TMaybe<TType> GetValue(const NCommon::TConfSetting<TType, NCommon::EConfSettingType::Dynamic>& setting, const TString& cluster) {
    return setting.Get(cluster);
}

template <typename TType>
TMaybe<TType> GetValue(const NCommon::TConfSetting<TType, NCommon::EConfSettingType::Static>& setting, const TString& cluster) {
    Y_UNUSED(cluster);
    return setting.Get();
}

template <typename T, typename TType>
concept EnumContainer = std::ranges::input_range<T> &&
                        std::convertible_to<std::ranges::range_value_t<T>, TType>;

template <typename T>
concept StringContainer = std::ranges::input_range<T> &&
                          std::convertible_to<std::ranges::range_value_t<T>, TString>;

template <typename T>
concept ConfigFeature = requires(const T& elem) {
    { elem.GetName() } -> std::convertible_to<TString>;
    { elem.GetValue() } -> std::convertible_to<TMaybe<TString>>;
};

template <typename T>
concept ConfigFeatureList = std::ranges::input_range<T> && ConfigFeature<std::ranges::range_value_t<T>>;

template <typename TFilter, typename TContainer>
concept AttributeFilter = std::predicate<TFilter, std::ranges::range_value_t<TContainer>>;

} // namespace NPrivate

class ISettingDispatcher: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ISettingDispatcher>;
    // Returns true if can continue
    using TErrorCallback = std::function<bool(const TString& message, bool isError)>;

    enum class EStage {
        CONFIG,
        STATIC,
        RUNTIME,
    };

    virtual bool Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value,
                          EStage stage, const TErrorCallback& errorCallback) = 0;
    virtual bool IsRuntime(const TString& name) const = 0;
};

ISettingDispatcher::TErrorCallback GetDefaultErrorCallback();
ISettingDispatcher::TErrorCallback GetErrorCallback(TPositionHandle pos, TExprContext& ctx);

template <typename TType, EConfSettingType SettingType>
TConfSetting<TType, SettingType>& AsConfSetting(TConfSetting<TType, SettingType>& setting) {
    return setting;
}

template <typename TType, EConfSettingType SettingType>
const TConfSetting<TType, SettingType>& AsConfSetting(const TConfSetting<TType, SettingType>& setting) {
    return setting;
}

class TSettingHandlerBase: public TThrRefBase {
public:
    virtual bool IsRuntime() const = 0;
    virtual bool IsPerCluster() const = 0;
    virtual bool IgnoreInFullReplay() const = 0;

    bool Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value,
                  ISettingDispatcher::EStage stage, const ISettingDispatcher::TErrorCallback& errorCallback,
                  const TQContext& qContext, const THashSet<TString>& validClusters, const std::function<bool(bool validateOnly)>& handleSetting) const;

private:
    TMaybe<TString> ValidateDispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value,
                                     const THashSet<TString>& validClusters) const;
};

template <typename TSettings>
class TDispatcher {
public:
    using TErrorCallback = ISettingDispatcher::TErrorCallback;
    using EStage = ISettingDispatcher::EStage;

    class TSettingHandler: public TSettingHandlerBase {
    public:
        using TPtr = TIntrusivePtr<TSettingHandler>;

        explicit TSettingHandler(TString name)
            : Name_(std::move(name))
        {
        }

        const TString& GetDisplayName() const {
            return Name_;
        }

        virtual bool Handle(const TString& cluster, const TMaybe<TString>& value, bool validateOnly, const TErrorCallback& errorCallback, TSettings& settings) const = 0;
        virtual void FreezeDefault(const TSettings& settings) = 0;
        virtual void Restore(const TString& cluster, TSettings& settings) const = 0;
        virtual bool IsDeprecated() const = 0;
        virtual bool HasSerializableValue(const TSettings& settings) const = 0;
        virtual void Serialize(const std::function<void(const TString&, const TString&)>& callback, const TSettings& settings) const = 0;

    protected:
        TString Name_;
    };

    template <typename TAccessor>
    class TSettingHandlerImpl: public TSettingHandler {
        using TSetting = std::remove_cvref_t<std::invoke_result_t<const TAccessor&, TSettings&>>;
        using TType = typename TSetting::TValueType;

        static_assert(std::same_as<std::invoke_result_t<const TAccessor&, TSettings&>, TSetting&>,
                      "Setting accessor must return a mutable setting reference");
        static_assert(std::same_as<std::invoke_result_t<const TAccessor&, const TSettings&>, const TSetting&>,
                      "Setting accessor must preserve constness");

    public:
        using TValueCallback = std::function<void(TSettings&, const TString&, TType)>;
        using TValidator = std::function<void(const TSettings&, const TString&, TType)>;

    private:
        friend class TDispatcher;
        using TSettingHandler::Name_;

        TSettingHandlerImpl(const TString& name, TAccessor accessor)
            : TSettingHandler(name)
            , SettingAccessor_(std::move(accessor))
            , Parser_(NPrivate::GetDefaultParser<TType>())
            , Serializer_(NPrivate::GetDefaultSerializer<TType>())
            , ValueSetter_([this](TSettings& settings, const TString& cluster, TType value) {
                std::invoke(SettingAccessor_, settings)[cluster] = value;
            })
        {
        }

        bool Handle(const TString& cluster, const TMaybe<TString>& value, bool validateOnly, const TErrorCallback& errorCallback, TSettings& settings) const override {
            if (value) {
                try {
                    TType v = Parser_(*value);

                    for (auto& validate : Validators_) {
                        validate(settings, cluster, v);
                    }
                    if (!validateOnly) {
                        ValueSetter_(settings, cluster, v);
                        if (Warning_) {
                            return errorCallback(Warning_, false);
                        }
                    }
                } catch (...) {
                    return errorCallback(TStringBuilder() << "Bad " << Name_.Quote() << " setting for " << cluster.Quote() << " cluster: " << CurrentExceptionMessage(), true);
                }
            } else if (!validateOnly) {
                try {
                    Restore(cluster, settings);
                } catch (...) {
                    return errorCallback(CurrentExceptionMessage(), true);
                }
            }
            return true;
        }

        void FreezeDefault(const TSettings& settings) override {
            Default_ = std::invoke(SettingAccessor_, settings);
        }

        void Restore(const TString& cluster, TSettings& settings) const override {
            auto& setting = std::invoke(SettingAccessor_, settings);
            if (!Default_) {
                ythrow yexception() << "Cannot restore " << Name_.Quote() << " setting without freeze";
            }
            if (ALL_CLUSTERS == cluster) {
                setting = Default_.GetRef();
            } else {
                if (auto value = NPrivate::GetValue(Default_.GetRef(), cluster)) {
                    setting[cluster] = *value;
                } else {
                    setting.Clear();
                }
            }
        }

    public:
        bool IsRuntime() const override {
            return TSetting::IsRuntime();
        }

        bool IsPerCluster() const override {
            return TSetting::IsPerCluster();
        }

        bool IsDeprecated() const override {
            return Deprecated_;
        }

        bool IgnoreInFullReplay() const override {
            return IgnoreInFullReplay_;
        }

        bool HasSerializableValue(const TSettings& settings) const override {
            YQL_ENSURE(!TSetting::IsPerCluster(), "Only serialization for static settings is supported now");
            if constexpr (!TSetting::IsPerCluster()) {
                return std::invoke(SettingAccessor_, settings).Get().Defined();
            }
            return false;
        }

        TSettingHandlerImpl& Lower(TType lower) {
            Validators_.push_back([lower](const TSettings&, const TString&, TType value) {
                if (value < lower) {
                    throw yexception() << "Value " << value << " is less than " << lower << " allowed lower bound";
                }
            });
            return *this;
        }

        TSettingHandlerImpl& Upper(TType upper) {
            Validators_.push_back([upper](const TSettings&, const TString&, TType value) {
                if (value > upper) {
                    throw yexception() << "Value " << value << " is greater than " << upper << " allowed upper bound";
                }
            });
            return *this;
        }

        template <NPrivate::EnumContainer<TType> TContainer>
        TSettingHandlerImpl& Enum(const TContainer& container) {
            THashSet<TType> allowed(container.cbegin(), container.cend());
            Validators_.push_back([allowed = std::move(allowed)](const TSettings&, const TString&, TType value) {
                if (!allowed.contains(value)) {
                    throw yexception() << "Value " << value << " is not in set of allowed values: " << JoinSeq(TStringBuf(","), allowed);
                }
            });
            return *this;
        }

        TSettingHandlerImpl& Enum(std::initializer_list<TType> list) {
            THashSet<TType> allowed(list);
            Validators_.push_back([allowed = std::move(allowed)](const TSettings&, const TString&, TType value) {
                if (!allowed.contains(value)) {
                    throw yexception() << "Value " << value << " is not in set of allowed values: " << JoinSeq(TStringBuf(","), allowed);
                }
            });
            return *this;
        }

        TSettingHandlerImpl& NonEmpty() {
            Validators_.push_back([](const TSettings&, const TString&, TType value) {
                if (value.empty()) {
                    throw yexception() << "Value is empty";
                }
            });
            return *this;
        }

        TSettingHandlerImpl& GlobalOnly() {
            Validators_.push_back([](const TSettings&, const TString& cluster, TType) {
                if (cluster != NCommon::ALL_CLUSTERS) {
                    throw yexception() << "Option cannot be used with specific cluster";
                }
            });
            return *this;
        }

        TSettingHandlerImpl& Validator(TValidator&& validator) {
            Validators_.push_back(std::move(validator));
            return *this;
        }

        TSettingHandlerImpl& Validator(const TValidator& validator) {
            Validators_.push_back(validator);
            return *this;
        }

        TSettingHandlerImpl& Parser(NPrivate::TParser<TType>&& parser) {
            Parser_ = std::move(parser);
            return *this;
        }

        TSettingHandlerImpl& Parser(const NPrivate::TParser<TType>& parser) {
            Parser_ = parser;
            return *this;
        }

        TSettingHandlerImpl& Serializer(NPrivate::TSerializer<TType>&& serializer) {
            Serializer_ = std::move(serializer);
            return *this;
        }

        TSettingHandlerImpl& Serializer(const NPrivate::TSerializer<TType>& serializer) {
            Serializer_ = serializer;
            return *this;
        }

        void Serialize(const std::function<void(const TString&, const TString&)>& callback, const TSettings& settings) const override {
            YQL_ENSURE(!TSetting::IsPerCluster(), "Only serialization for static settings is supported now");
            if constexpr (!TSetting::IsPerCluster()) {
                YQL_ENSURE(HasSerializableValue(settings), "Setting is not serializable");
                callback(Name_, Serializer_(*std::invoke(SettingAccessor_, settings).Get()));
            }
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
            ValueSetter_ = [this, hook = std::move(hook)](TSettings& settings, const TString& cluster, TType value) {
                if (Default_) {
                    Restore(cluster, settings);
                }
                hook(settings, cluster, value);
            };
            return *this;
        }

        TSettingHandlerImpl& ValueSetterWithRestore(const TValueCallback& hook) {
            ValueSetter_ = [this, hook](TSettings& settings, const TString& cluster, TType value) {
                if (Default_) {
                    Restore(cluster, settings);
                }
                hook(settings, cluster, value);
            };
            return *this;
        }

        TSettingHandlerImpl& Warning(const TString& message) {
            Warning_ = message;
            return *this;
        }

        TSettingHandlerImpl& Deprecated(const TString& message = {}) {
            Warning_ = message ? message : (TStringBuilder() << "Pragma \"" << Name_ << "\" is deprecated and has no effect");
            Deprecated_ = true;
            return *this;
        }

        TSettingHandlerImpl& IgnoreInFullReplay() {
            IgnoreInFullReplay_ = true;
            return *this;
        }

    private:
        const TAccessor SettingAccessor_;
        TMaybe<TSetting> Default_;
        NPrivate::TParser<TType> Parser_;
        NPrivate::TSerializer<TType> Serializer_;
        TValueCallback ValueSetter_;
        TVector<TValidator> Validators_;
        TString Warning_;
        bool Deprecated_ = false;
        bool IgnoreInFullReplay_ = false;
    };

    explicit TDispatcher(const TStringBuf& providerName = "", bool strictConfigValidation = false)
        : ProviderName_(providerName)
        , StrictConfigValidation_(strictConfigValidation)
    {
    }

    TDispatcher(const TDispatcher&) = delete;

    template <NPrivate::StringContainer TContainer>
    explicit TDispatcher(const TContainer& validClusters, bool strictConfigValidation = false)
        : ValidClusters(validClusters.begin(), validClusters.end())
        , StrictConfigValidation_(strictConfigValidation)
    {
    }

    template <NPrivate::StringContainer TContainer>
    void SetValidClusters(const TContainer& validClusters) {
        ValidClusters.clear();
        ValidClusters.insert(validClusters.begin(), validClusters.end());
    }

    void AddValidCluster(const TString& cluster) {
        ValidClusters.insert(cluster);
    }

    bool IsValidCluster(const TString& cluster) const {
        return ValidClusters.contains(cluster);
    }

    const THashSet<TString>& GetValidClusters() const {
        return ValidClusters;
    }

    template <typename TAccessor>
    TSettingHandlerImpl<TAccessor>& AddSetting(const TString& name, TAccessor accessor) {
        using THandler = TSettingHandlerImpl<TAccessor>;
        TIntrusivePtr<THandler> handler = new THandler(name, std::move(accessor));
        if (!Handlers_.insert({NormalizeName(name), handler}).second) {
            ythrow yexception() << "Duplicate configuration setting name " << name.Quote();
        }

        if (!name.StartsWith('_')) {
            Names_.insert(name);
        }

        return *handler;
    }

    bool IsRuntime(const TString& name) const {
        auto normalizedName = NormalizeName(name);
        if (auto handler = Handlers_.Value(normalizedName, typename TSettingHandler::TPtr())) {
            return handler->IsRuntime();
        }
        return false;
    }

    template <NPrivate::ConfigFeatureList TContainer, NPrivate::AttributeFilter<TContainer> TFilter>
    void Dispatch(TSettings& settings, const TString& cluster, const TContainer& clusterValues, const TFilter& filter, const TQContext& qContext) const {
        using TAttribute = typename TContainer::value_type;

        auto errorCallback = GetDefaultErrorCallback();
        TString activationLabel = TStringBuilder() << ProviderName_ << "_" << cluster;

        const auto flags = NCommon::SelectAndSaveActivatedFlags<TAttribute>(
            activationLabel, qContext, clusterValues, filter, !ProviderName_.empty());

        for (const auto& flag : flags) {
            Dispatch(settings, cluster, flag.GetName(), flag.GetValue(), EStage::CONFIG, errorCallback, qContext);
        }
    }

    template <NPrivate::ConfigFeatureList TContainer>
    void Dispatch(TSettings& settings, const TString& cluster, const TContainer& clusterValues, const TQContext& qContext) const {
        auto errorCallback = GetDefaultErrorCallback();
        for (const auto& value : clusterValues) {
            Dispatch(settings, cluster, value.GetName(), value.GetValue(), EStage::CONFIG, errorCallback, qContext);
        }
    }

    template <NPrivate::ConfigFeatureList TContainer, typename TActivationPolicy>
    void DispatchWithActivationPolicy(TSettings& settings, const TString& cluster, const TContainer& clusterValues, const TActivationPolicy& activationPolicy, const TQContext& qContext) const {
        using TAttribute = typename TContainer::value_type;

        TString activationLabel = TStringBuilder() << ProviderName_ << "_" << cluster;
        const auto flags = activationPolicy.template SelectAndSave<TAttribute>(
            activationLabel, qContext, clusterValues, !ProviderName_.empty());
        Dispatch(settings, cluster, flags, qContext);
    }

    template <NPrivate::ConfigFeatureList TContainer, NPrivate::AttributeFilter<TContainer> TFilter>
    void Dispatch(TSettings& settings, const TContainer& globalValues, const TFilter& filter, const TQContext& qContext) const {
        Dispatch(settings, ALL_CLUSTERS, globalValues, filter, qContext);
    }

    template <NPrivate::ConfigFeatureList TContainer>
    void Dispatch(TSettings& settings, const TContainer& globalValues, const TQContext& qContext) const {
        Dispatch(settings, ALL_CLUSTERS, globalValues, qContext);
    }

    template <NPrivate::ConfigFeatureList TContainer, typename TActivationPolicy>
    void DispatchWithActivationPolicy(TSettings& settings, const TContainer& globalValues, const TActivationPolicy& activationPolicy, const TQContext& qContext) const {
        DispatchWithActivationPolicy(settings, ALL_CLUSTERS, globalValues, activationPolicy, qContext);
    }

    void FreezeDefaults(const TSettings& settings) {
        for (auto& item : Handlers_) {
            item.second->FreezeDefault(settings);
        }
    }

    void Restore(TSettings& settings) const {
        for (auto& item : Handlers_) {
            item.second->Restore(ALL_CLUSTERS, settings);
        }
    }

    void Enumerate(std::function<void(std::string_view)> callback) const {
        for (const auto& name : Names_) {
            if (!Handlers_.at(NormalizeName(name))->IsDeprecated()) {
                callback(name);
            }
        }
    }

    void SerializeStaticSettings(const TSettings& settings, const std::function<void(const TString&, const TString&)>& callback) const {
        for (const auto& [name, handler] : Handlers_) {
            if (handler->HasSerializableValue(settings)) {
                handler->Serialize(callback, settings);
            }
        }
    }

    ui64 CountSerializableStaticSettings(const TSettings& settings) const {
        ui64 count = 0;
        for (const auto& [name, handler] : Handlers_) {
            if (handler->HasSerializableValue(settings)) {
                ++count;
            }
        }
        return count;
    }

    bool Dispatch(TSettings& settings, const TString& cluster, const TString& name, const TMaybe<TString>& value,
                  EStage stage, const TErrorCallback& errorCallback, const TQContext& qContext) const {
        auto normalizedName = NormalizeName(name);
        if (auto handler = Handlers_.Value(normalizedName, typename TSettingHandler::TPtr())) {
            auto handleSetting = [&](bool validateOnly) {
                return handler->Handle(cluster, value, validateOnly, errorCallback, settings);
            };
            return handler->Dispatch(cluster, name, value, stage, errorCallback, qContext, ValidClusters, std::ref(handleSetting));
        } else {
            // ignore unknown names in config
            if (stage == EStage::CONFIG && !StrictConfigValidation_) {
                return true;
            }

            TStringBuilder nearHandlerMsg;
            for (auto& item : Handlers_) {
                if (NLevenshtein::Distance(normalizedName, item.first) < DefaultMistypeDistance) {
                    nearHandlerMsg << ", did you mean " << item.second->GetDisplayName().Quote() << '?';
                    break;
                }
            }
            return errorCallback(TStringBuilder() << "Unknown setting name " << name.Quote() << nearHandlerMsg, true);
        }
    }

protected:
    THashSet<TString> ValidClusters; // NOLINT(readability-identifier-naming)
    // We use TMap instead of THashMap to keep the order of settings serialization.
    TMap<TString, typename TSettingHandler::TPtr> Handlers_;
    TSet<TString> Names_;

    const TString ProviderName_;
    bool StrictConfigValidation_ = false;
};

template <typename TSettings>
class TDispatcherAndSettings: public TSettings, public TDispatcher<TSettings>, public ISettingDispatcher {
public:
    using EStage = ISettingDispatcher::EStage;
    using TErrorCallback = ISettingDispatcher::TErrorCallback;

    TDispatcherAndSettings() = default;

    explicit TDispatcherAndSettings(const TStringBuf& providerName)
        : TDispatcher<TSettings>(providerName)
    {
    }

    explicit TDispatcherAndSettings(const TStringBuf& providerName, const TQContext& qContext)
        : TDispatcherAndSettings(providerName, qContext, false)
    {
    }

    explicit TDispatcherAndSettings(const TStringBuf& providerName, const TQContext& qContext, bool strictConfigValidation)
        : TDispatcher<TSettings>(providerName, strictConfigValidation)
        , QContext_(qContext)
    {
    }

    template <NPrivate::StringContainer TContainer>
    explicit TDispatcherAndSettings(const TContainer& validClusters)
        : TDispatcher<TSettings>(validClusters)
    {
    }

    template <NPrivate::StringContainer TContainer>
    explicit TDispatcherAndSettings(const TContainer& validClusters, bool strictConfigValidation)
        : TDispatcher<TSettings>(validClusters, strictConfigValidation)
    {
    }

    bool Dispatch(const TString& cluster, const TString& name, const TMaybe<TString>& value,
                  EStage stage, const TErrorCallback& errorCallback) final {
        return TDispatcher<TSettings>::Dispatch(*this, cluster, name, value, stage, errorCallback, QContext_);
    }

    template <typename... TArgs>
    decltype(auto) Dispatch(TArgs&&... args) {
        return TDispatcher<TSettings>::Dispatch(*this, std::forward<TArgs>(args)..., QContext_);
    }

    template <typename... TArgs>
    void DispatchWithActivationPolicy(TArgs&&... args) {
        TDispatcher<TSettings>::DispatchWithActivationPolicy(*this, std::forward<TArgs>(args)..., QContext_);
    }

    bool IsRuntime(const TString& name) const final {
        return TDispatcher<TSettings>::IsRuntime(name);
    }

    void FreezeDefaults() {
        TDispatcher<TSettings>::FreezeDefaults(*this);
    }

    void Restore() {
        TDispatcher<TSettings>::Restore(*this);
    }

    void SerializeStaticSettings(const std::function<void(const TString&, const TString&)>& callback) const {
        TDispatcher<TSettings>::SerializeStaticSettings(*this, callback);
    }

    ui64 CountSerializableStaticSettings() const {
        return TDispatcher<TSettings>::CountSerializableStaticSettings(*this);
    }

private:
    const TQContext QContext_;
};

} // namespace NYql::NCommon

#define REGISTER_SETTING(dispatcher, setting)                                                                         \
    (dispatcher).AddSetting(#setting,                                                                                 \
                            [member = &std::remove_cvref_t<decltype(dispatcher)>::setting](auto& settings) -> auto& { \
                                return NYql::NCommon::AsConfSetting(settings.*member);                                \
                            })
