#ifndef YSON_STRUCT_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct.h"
// For the sake of sane code completion.
#include "yson_struct.h"
#endif

#include "convert.h"
#include "serialize.h"
#include "tree_visitor.h"
#include "yson_struct_detail.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/datetime/base.h>

#include <util/system/sanitizers.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
const std::type_info& CallCtor()
{
    if constexpr (std::convertible_to<T*, TRefCountedBase*>) {
        auto dummy = New<T>();
        // NB: |New| returns pointer to TRefCountedWrapper<T>.
        return typeid(*dummy);
    } else {
        T dummy;
        return typeid(T);
    }
}

////////////////////////////////////////////////////////////////////////////////

// This method is called from constructor of every descendant of TYsonStructBase.
// When it is first called for a particular struct it will initialize TYsonStructMeta for that struct.
// Also this method initializes defaults for the struct.
template <class TStruct>
void TYsonStructRegistry::InitializeStruct(TStruct* target)
{
    TForbidCachedDynamicCastGuard guard(target);

    // It takes place only inside special constructor call inside lambda below.
    if (CurrentlyInitializingMeta_) {
        // TODO(renadeen): assert target is from the same type hierarchy.
        // Call initialization method that is provided by user.
        TStruct::Register(TYsonStructRegistrar<TStruct>(CurrentlyInitializingMeta_));
        return;
    }

    auto metaConstructor = [] {
        auto* result = new TYsonStructMeta();
        NSan::MarkAsIntentionallyLeaked(result);

        // NB: Here initialization of TYsonStructMeta of particular struct takes place.
        // First we store meta in static thread local `CurrentlyInitializingMeta_` as we need to access it later.
        // Then we make special constructor call that will traverse through all TStruct's type hierarchy.
        // During this call constructors of each base class will call TYsonStructRegistry::Initialize again
        // and `if` statement at the start of this function will call TStruct::Register
        // where registration of yson parameters takes place.
        // This way all parameters of the whole type hierarchy will fill `CurrentlyInitializingMeta_`.
        // We prevent context switch cause we don't want another fiber to use `CurrentlyInitializingMeta_` before we finish initialization.
        YT_VERIFY(!CurrentlyInitializingMeta_);
        CurrentlyInitializingMeta_ = result;
        {
            NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
            const std::type_info& typeInfo = CallCtor<TStruct>();
            result->FinishInitialization(typeInfo);
        }
        CurrentlyInitializingMeta_ = nullptr;

        return result;
    };

    static TYsonStructMeta* meta = metaConstructor();
    target->Meta_ = meta;
}

template <class TTargetStruct>
TTargetStruct* TYsonStructRegistry::CachedDynamicCast(const TYsonStructBase* constSource)
{
    YT_VERIFY(constSource->CachedDynamicCastAllowed_);

    // We cannot have TSyncMap as singleton directly because we need separate cache for each instantiated template of this method.
    struct CacheHolder
    {
        NConcurrency::TSyncMap<std::type_index, ptrdiff_t> OffsetCache;
    };
    auto holder = LeakySingleton<CacheHolder>();

    // TODO(renadeen): is there a better way to use same function for const and non-const contexts?
    auto* source = const_cast<TYsonStructBase*>(constSource);
    ptrdiff_t* offset = holder->OffsetCache.FindOrInsert(std::type_index(typeid(*source)), [=] () {
        auto* target = dynamic_cast<TTargetStruct*>(source);
        // NB: Unfortunately, it is possible that dynamic cast fails.
        // For example, when class is derived from template class with `const char *` template parameter
        // and variable with internal linkage is used for this parameter.
        YT_VERIFY(target);
        return reinterpret_cast<intptr_t>(target) - reinterpret_cast<intptr_t>(source);
    }).first;
    return reinterpret_cast<TTargetStruct*>(reinterpret_cast<intptr_t>(source) + *offset);
}

////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
TYsonStructRegistrar<TStruct>::TYsonStructRegistrar(IYsonStructMeta* meta)
    : Meta_(meta)
{ }

template <class TStruct>
template <class TValue>
TYsonStructParameter<TValue>& TYsonStructRegistrar<TStruct>::Parameter(const TString& key, TValue(TStruct::*field))
{
    return BaseClassParameter<TStruct, TValue>(key, field);
}

template <class TStruct>
template <class TBase, class TValue>
TYsonStructParameter<TValue>& TYsonStructRegistrar<TStruct>::BaseClassParameter(const TString& key, TValue(TBase::*field))
{
    static_assert(std::is_base_of<TBase, TStruct>::value);
    auto parameter = New<TYsonStructParameter<TValue>>(key, std::make_unique<TYsonFieldAccessor<TBase, TValue>>(field));
    Meta_->RegisterParameter(key, parameter);
    return *parameter;
}

template <class TStruct>
void TYsonStructRegistrar<TStruct>::Preprocessor(std::function<void(TStruct*)> preprocessor)
{
    Meta_->RegisterPreprocessor([preprocessor = std::move(preprocessor)] (TYsonStructBase* target) {
        preprocessor(TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(target));
    });
}

template <class TStruct>
void TYsonStructRegistrar<TStruct>::Postprocessor(std::function<void(TStruct*)> postprocessor)
{
    Meta_->RegisterPostprocessor([postprocessor = std::move(postprocessor)] (TYsonStructBase* target) {
        postprocessor(TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(target));
    });
}

template <class TStruct>
void TYsonStructRegistrar<TStruct>::UnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    Meta_->SetUnrecognizedStrategy(strategy);
}

template <class TStruct>
template<class TBase>
TYsonStructRegistrar<TStruct>::operator TYsonStructRegistrar<TBase>()
{
    static_assert(std::is_base_of<TBase, TStruct>::value);
    return TYsonStructRegistrar<TBase>(Meta_);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> CloneYsonStruct(const TIntrusivePtr<const T>& obj)
{
    if (!obj) {
        return nullptr;
    }
    return ConvertTo<TIntrusivePtr<T>>(NYson::ConvertToYsonString(*obj));
}

template <class T>
TIntrusivePtr<T> CloneYsonStruct(const TIntrusivePtr<T>& obj)
{
    return CloneYsonStruct(ConstPointerCast<const T>(obj));
}

template <class T>
std::vector<TIntrusivePtr<T>> CloneYsonStructs(const std::vector<TIntrusivePtr<T>>& objs)
{
    std::vector<TIntrusivePtr<T>> clonedObjs;
    clonedObjs.reserve(objs.size());
    for (const auto& obj : objs) {
        clonedObjs.push_back(CloneYsonStruct(obj));
    }
    return clonedObjs;
}

template <class T>
THashMap<TString, TIntrusivePtr<T>> CloneYsonStructs(const THashMap<TString, TIntrusivePtr<T>>& objs)
{
    THashMap<TString, TIntrusivePtr<T>> clonedObjs;
    clonedObjs.reserve(objs.size());
    for (const auto& [key, obj] : objs) {
        clonedObjs.emplace(key, CloneYsonStruct(obj));
    }
    return clonedObjs;
}

template <class T>
TIntrusivePtr<T> UpdateYsonStruct(
    const TIntrusivePtr<T>& obj,
    const INodePtr& patch)
{
    static_assert(
        std::convertible_to<T*, TYsonStruct*>,
        "'obj' must be convertible to TYsonStruct");

    if (patch) {
        return ConvertTo<TIntrusivePtr<T>>(PatchNode(ConvertTo<INodePtr>(obj), patch));
    } else {
        return CloneYsonStruct(obj);
    }
}

template <class T>
TIntrusivePtr<T> UpdateYsonStruct(
    const TIntrusivePtr<T>& obj,
    const NYson::TYsonString& patch)
{
    if (!patch) {
        return obj;
    }

    return UpdateYsonStruct(obj, ConvertToNode(patch));
}

template <class T>
bool ReconfigureYsonStruct(
    const TIntrusivePtr<T>& config,
    const NYson::TYsonString& newConfigYson)
{
    return ReconfigureYsonStruct(config, ConvertToNode(newConfigYson));
}

template <class T>
bool ReconfigureYsonStruct(
    const TIntrusivePtr<T>& config,
    const TIntrusivePtr<T>& newConfig)
{
    return ReconfigureYsonStruct(config, ConvertToNode(newConfig));
}

template <class T>
bool ReconfigureYsonStruct(
    const TIntrusivePtr<T>& config,
    const INodePtr& newConfigNode)
{
    auto configNode = ConvertToNode(config);

    auto newConfig = ConvertTo<TIntrusivePtr<T>>(newConfigNode);
    auto newCanonicalConfigNode = ConvertToNode(newConfig);

    if (NYTree::AreNodesEqual(configNode, newCanonicalConfigNode)) {
        return false;
    }

    config->Load(newConfigNode, /*postprocess*/ true, /*setDefaults*/ false);
    return true;
}

template <class TSrc, class TDst>
void UpdateYsonStructField(TDst& dst, const std::optional<TSrc>& src)
{
    if (src) {
        dst = *src;
    }
}

template <class TSrc, class TDst>
void UpdateYsonStructField(TIntrusivePtr<TDst>& dst, const TIntrusivePtr<TSrc>& src)
{
    if (src) {
        dst = src;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
