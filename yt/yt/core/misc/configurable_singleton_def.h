#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

#include <any>
#include <typeindex>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TSingletonConfigHelpers;
class TSingletonManagerImpl;

template <bool Static>
class TSingletonsConfigBase
{
public:
    template <class TConfig>
    TIntrusivePtr<TConfig> TryGetSingletonConfig();

    template <class TConfig>
    TIntrusivePtr<TConfig> GetSingletonConfig();

    template <class TConfig>
    void SetSingletonConfig(TIntrusivePtr<TConfig> config);

protected:
    static void RegisterSingletons(
        auto&& registrar,
        auto&& registerFieldSelector);

private:
    friend struct NYT::NDetail::TSingletonConfigHelpers;
    friend class NYT::NDetail::TSingletonManagerImpl;

    THashMap<std::string, std::any> NameToConfig_;
    THashMap<std::type_index, std::any*> TypeToConfig_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TSingletonsConfig
    : public NDetail::TSingletonsConfigBase<true>
    , public virtual NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TSingletonsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig);

////////////////////////////////////////////////////////////////////////////////

class TSingletonsDynamicConfig
    : public NDetail::TSingletonsConfigBase<false>
    , public virtual NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TSingletonsDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingletonsDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

#define YT_DEFINE_CONFIGURABLE_SINGLETON(singletonName, configType)
#define YT_DEFINE_RECONFIGURABLE_SINGLETON(singletonName, configType, dynamicConfigType)

////////////////////////////////////////////////////////////////////////////////

class TSingletonManager
{
public:
    static void Configure(const TSingletonsConfigPtr& config);
    static void Reconfigure(const TSingletonsDynamicConfigPtr& dynamicConfig);

    static TSingletonsConfigPtr GetConfig();
    static TSingletonsDynamicConfigPtr GetDynamicConfig();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONFIGURABLE_SINGLETON_DEF_INL_H_
#include "configurable_singleton_def-inl.h"
#undef CONFIGURABLE_SINGLETON_DEF_INL_H_
