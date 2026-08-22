#ifndef CONFIGURABLE_SINGLETON_DEF_INL_H_
#error "Direct inclusion of this file is not allowed, include configurable_singleton_def.h"
// For the sake of sane code completion.
#include "configurable_singleton_def.h"
#endif

#include <library/cpp/yt/misc/static_initializer.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TManagerConfig>
using TRegisterSingletonField = std::function<void(NYTree::TYsonStructRegistrar<TManagerConfig> registrar)>;
using TConfigureSingleton = std::function<void(const std::any& config)>;
using TReconfigureSingleton = std::function<void(const std::any& config, const std::any& dynamicConfig)>;
using TCloneSingleton = std::function<std::any(const std::any& config)>;

struct TSingletonTraits
{
    TRegisterSingletonField<TSingletonsConfig> RegisterField;
    TRegisterSingletonField<TSingletonsDynamicConfig> RegisterDynamicField;
    TConfigureSingleton Configure;
    TReconfigureSingleton Reconfigure;
    TCloneSingleton Clone;
};

struct TSingletonConfigHelpers
{
    static void RegisterSingleton(
        const std::string& singletonName,
        TSingletonTraits singletonTraits);

    template <class TSingletonConfig, class TManagerConfig>
    static TRegisterSingletonField<TManagerConfig> MakeRegisterField(const std::string& singletonName)
    {
        return [=] (NYTree::TYsonStructRegistrar<TManagerConfig> registrar) {
            SetupSingletonConfigParameter(
                registrar.template ParameterWithUniversalAccessor<TIntrusivePtr<TSingletonConfig>>(
                    singletonName,
                    [=] (TManagerConfig* config) -> auto& {
                        auto it = config->NameToConfig_.find(singletonName);
                        if (it == config->NameToConfig_.end()) {
                            it = config->NameToConfig_.emplace(singletonName, std::any(TIntrusivePtr<TSingletonConfig>())).first;
                            EmplaceOrCrash(config->TypeToConfig_, std::type_index(typeid(TSingletonConfig)), &it->second);
                        }
                        return *std::any_cast<TIntrusivePtr<TSingletonConfig>>(&it->second);
                    }));
        };
    }

    template <class TSingletonConfig>
    static TConfigureSingleton MakeConfigureSingleton()
    {
        return [] (const std::any& config) {
            auto typedConfig = std::any_cast<TIntrusivePtr<TSingletonConfig>>(config);
            ConfigureSingleton(typedConfig);
        };
    }

    template <class TSingletonConfig, class TDynamicSingletonConfig>
    static TReconfigureSingleton MakeReconfigureSingleton()
    {
        return [] (const std::any& config, const std::any& dynamicConfig) {
            auto typedConfig = std::any_cast<TIntrusivePtr<TSingletonConfig>>(config);
            auto typedDynamicConfig = std::any_cast<TIntrusivePtr<TDynamicSingletonConfig>>(dynamicConfig);
            ReconfigureSingleton(typedConfig, typedDynamicConfig);
        };
    }

    template <class TSingletonConfig>
    static TCloneSingleton MakeCloneSingleton()
    {
        return [] (const std::any& config) {
            auto typedConfig = std::any_cast<TIntrusivePtr<TSingletonConfig>>(config);
            return NYTree::CloneYsonStruct(typedConfig);
        };
    }

    template <class TSingletonConfig>
    static void RegisterSingleton(const std::string& singletonName)
    {
        RegisterSingleton(
            singletonName,
            TSingletonTraits{
                .RegisterField = MakeRegisterField<TSingletonConfig, TSingletonsConfig>(singletonName),
                .Configure = MakeConfigureSingleton<TSingletonConfig>(),
                .Clone = MakeCloneSingleton<TSingletonConfig>(),
            });
    }

    template <class TSingletonConfig, class TDynamicSingletonConfig>
    static void RegisterReconfigurableSingleton(const std::string& singletonName)
    {
        RegisterSingleton(
            singletonName,
            TSingletonTraits{
                .RegisterField = MakeRegisterField<TSingletonConfig, TSingletonsConfig>(singletonName),
                .RegisterDynamicField = MakeRegisterField<TDynamicSingletonConfig, TSingletonsDynamicConfig>(singletonName),
                .Configure = MakeConfigureSingleton<TSingletonConfig>(),
                .Reconfigure = MakeReconfigureSingleton<TSingletonConfig, TDynamicSingletonConfig>(),
                .Clone = MakeCloneSingleton<TSingletonConfig>(),
            });
    }

    static TSingletonTraits GetSingletonTraits(const std::string& singletonName);
};

////////////////////////////////////////////////////////////////////////////////

template <bool Static>
template <class TConfig>
TIntrusivePtr<TConfig> TSingletonsConfigBase<Static>::TryGetSingletonConfig()
{
    CheckSingletonConfigRegistered(TSingletonConfigTag<TConfig, Static>());
    return std::any_cast<TIntrusivePtr<TConfig>>(*GetOrCrash(TypeToConfig_, typeid(TConfig)));
}

template <bool Static>
template <class TConfig>
TIntrusivePtr<TConfig> TSingletonsConfigBase<Static>::GetSingletonConfig()
{
    auto config = TryGetSingletonConfig<TConfig>();
    YT_VERIFY(config);
    return config;
}

template <bool Static>
template <class TConfig>
void TSingletonsConfigBase<Static>::SetSingletonConfig(TIntrusivePtr<TConfig> config)
{
    CheckSingletonConfigRegistered(TSingletonConfigTag<TConfig, Static>());
    *GetOrCrash(TypeToConfig_, typeid(TConfig)) = std::move(config);
}

template<bool Static>
void TSingletonsConfigBase<Static>::MergeAllSingletonConfigsFrom(const TSingletonsConfigBase<Static>& src)
{
    THashMap<const std::any*, std::type_index> configToType(src.TypeToConfig_.size());
    for (const auto& [type, config]: src.TypeToConfig_) {
        EmplaceOrCrash(configToType, config, type);
    }

    for (const auto& [name, config]: src.NameToConfig_) {
        auto configClone = TSingletonConfigHelpers::GetSingletonTraits(name).Clone(config);
        auto it = NameToConfig_.insert_or_assign(name, configClone).first;
        auto type = GetOrCrash(configToType, &config);
        TypeToConfig_.insert_or_assign(type, &it->second);
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#undef YT_DEFINE_CONFIGURABLE_SINGLETON
#undef YT_DEFINE_RECONFIGURABLE_SINGLETON

#define YT_DEFINE_CONFIGURABLE_SINGLETON(singletonName, configType) \
    [[maybe_unused]] void CheckSingletonConfigRegistered(::NYT::NDetail::TSingletonConfigTag<configType, true>) \
    { } \
    \
    YT_STATIC_INITIALIZER({ \
        ::NYT::NDetail::TSingletonConfigHelpers::RegisterSingleton<configType>(singletonName); \
    })

#define YT_DEFINE_RECONFIGURABLE_SINGLETON(singletonName, configType, dynamicConfigType) \
    [[maybe_unused]] void CheckSingletonConfigRegistered(::NYT::NDetail::TSingletonConfigTag<configType, true>) \
    { } \
    \
    [[maybe_unused]] void CheckSingletonConfigRegistered(::NYT::NDetail::TSingletonConfigTag<dynamicConfigType, false>) \
    { } \
    \
    YT_STATIC_INITIALIZER({ \
         ::NYT::NDetail::TSingletonConfigHelpers::RegisterReconfigurableSingleton<configType, dynamicConfigType>(singletonName); \
    })

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
