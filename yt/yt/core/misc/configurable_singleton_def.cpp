#include "configurable_singleton_def.h"

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TSingletonManagerImpl
{
public:
    static TSingletonManagerImpl* Get()
    {
        return LeakySingleton<TSingletonManagerImpl>();
    }

    void Register(
        const std::string& singletonName,
        TSingletonTraits singletonTraits)
    {
        YT_VERIFY(!AllRegistered_.load());
        EmplaceOrCrash(SingletonMap_, singletonName, std::move(singletonTraits));
    }


    void Configure(const TSingletonsConfigPtr& config)
    {
        auto guard = Guard(ConfigureLock_);

        if (std::exchange(Configured_, true)) {
            THROW_ERROR_EXCEPTION("Singletons have already been configured");
        }

        Config_ = CloneYsonStruct(config);

        for (const auto& [name, traits] : Singletons()) {
            const auto& field = GetOrCrash(Config_->NameToConfig_, name);
            traits.Configure(field);
        }
    }

    void Reconfigure(const TSingletonsDynamicConfigPtr& dynamicConfig)
    {
        auto guard = Guard(ConfigureLock_);

        if (!Configured_) {
            THROW_ERROR_EXCEPTION("Singletons are not configured yet");
        }

        DynamicConfig_ = CloneYsonStruct(dynamicConfig);

        for (const auto& [name, traits] : Singletons()) {
            if (const auto& reconfigure = traits.Reconfigure) {
                const auto& singletonConfig = GetOrCrash(Config_->NameToConfig_, name);
                const auto& singletonDynamicConfig = GetOrCrash(DynamicConfig_->NameToConfig_, name);
                reconfigure(singletonConfig, singletonDynamicConfig);
            }
        }
    }

    TSingletonsConfigPtr GetConfig()
    {
        auto guard = Guard(ConfigureLock_);
        return Config_;
    }

    TSingletonsDynamicConfigPtr GetDynamicConfig()
    {
        auto guard = Guard(ConfigureLock_);
        return DynamicConfig_;
    }

    using TSingletonMap = THashMap<std::string, TSingletonTraits>;

    const TSingletonMap& Singletons() const
    {
        AllRegistered_.store(true);
        return SingletonMap_;
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();
    TSingletonManagerImpl() = default;

    mutable std::atomic<bool> AllRegistered_ = false;
    THashMap<std::string, TSingletonTraits> SingletonMap_;

    NThreading::TSpinLock ConfigureLock_;
    TSingletonsConfigPtr Config_;
    TSingletonsDynamicConfigPtr DynamicConfig_;
    bool Configured_ = false;
};

////////////////////////////////////////////////////////////////////////////////

void TSingletonConfigHelpers::RegisterSingleton(
    const std::string& fieldName,
    TSingletonTraits singletonTraits)
{
    TSingletonManagerImpl::Get()->Register(
        fieldName,
        std::move(singletonTraits));
}

////////////////////////////////////////////////////////////////////////////////

template <bool Static>
void TSingletonsConfigBase<Static>::RegisterSingletons(
    auto&& registrar,
    auto&& registerFieldSelector)
{
    for (const auto& [_, traits] : NDetail::TSingletonManagerImpl::Get()->Singletons()) {
        if (const auto& register_ = registerFieldSelector(traits)) {
            register_(registrar);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TSingletonsConfigBase<false>;
template class TSingletonsConfigBase<true>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void TSingletonManager::Configure(const TSingletonsConfigPtr& config)
{
    NDetail::TSingletonManagerImpl::Get()->Configure(config);
}

void TSingletonManager::Reconfigure(const TSingletonsDynamicConfigPtr& dynamicConfig)
{
    NDetail::TSingletonManagerImpl::Get()->Reconfigure(dynamicConfig);
}

TSingletonsConfigPtr TSingletonManager::GetConfig()
{
    return NDetail::TSingletonManagerImpl::Get()->GetConfig();
}

TSingletonsDynamicConfigPtr TSingletonManager::GetDynamicConfig()
{
    return NDetail::TSingletonManagerImpl::Get()->GetDynamicConfig();
}

////////////////////////////////////////////////////////////////////////////////

void TSingletonsConfig::Register(TRegistrar registrar)
{
    RegisterSingletons(
        registrar,
        [] (const NDetail::TSingletonTraits& traits) { return traits.RegisterField; });
}

////////////////////////////////////////////////////////////////////////////////

void TSingletonsDynamicConfig::Register(TRegistrar registrar)
{
    RegisterSingletons(
        registrar,
        [] (const NDetail::TSingletonTraits& traits) { return traits.RegisterDynamicField; });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
