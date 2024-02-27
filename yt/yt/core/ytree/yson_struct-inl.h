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

//! Creates TSerializer object which has preprocessors applied
//! to a TStruct object referred to by writable.
template <std::default_initializable TStruct, class TSerializer>
TSerializer TExternalizedYsonStruct::CreateWritable(TStruct& writable, bool setDefaults)
{
    static_assert(std::derived_from<TSerializer, TExternalizedYsonStruct>);
    if (setDefaults) {
        return TSerializer(&writable);
    }

    auto ret = TSerializer();
    ret.SetThat(&writable);
    return ret;
}

//! Creates TSerializer object which has preprocessors applied
//! to a dummy object and has pointer to readOnly assigned afterwards.
template <std::default_initializable TStruct, class TSerializer>
TSerializer TExternalizedYsonStruct::CreateReadOnly(const TStruct& readOnly)
{
    static_assert(std::derived_from<TSerializer, TExternalizedYsonStruct>);
    auto ret = TSerializer();
    ret.SetThat(const_cast<TStruct*>(&readOnly));
    return ret;
}

//! We need some writable instance of TStruct to refer to in order
//! to have a default constructor required by TYsonStructRegistry::InitializeStruct.
template <std::default_initializable TStruct>
TStruct* TExternalizedYsonStruct::GetDefault() noexcept
{
    thread_local TStruct defaultThat = {};
    //! NB: We reset default after every invocation
    //! so that different constructions of the same class
    //! do not affect each other.
    defaultThat = {};
    return &defaultThat;
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
    static_assert(std::derived_from<TStruct, TBase>);
    auto parameter = New<TYsonStructParameter<TValue>>(key, std::make_unique<TYsonFieldAccessor<TBase, TValue>>(field));
    Meta_->RegisterParameter(key, parameter);
    return *parameter;
}

template <class TStruct>
template <class TValue>
TYsonStructParameter<TValue>& TYsonStructRegistrar<TStruct>::ParameterWithUniversalAccessor(const TString& key, std::function<TValue&(TStruct*)> accessor)
{
    auto parameter = New<TYsonStructParameter<TValue>>(key, std::make_unique<TUniversalYsonParameterAccessor<TStruct, TValue>>(std::move(accessor)));
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
template <class TExternal, class TValue>
    // requires std::derived_from<TStruct, TExternalizedYsonStruct<TExternal, TStruct>>
TYsonStructParameter<TValue>& TYsonStructRegistrar<TStruct>::ExternalClassParameter(const TString& key, TValue(TExternal::*field))
{
    static_assert(std::derived_from<TStruct, TExternalizedYsonStruct>);
    static_assert(std::same_as<typename TStruct::TExternal, TExternal>);
    auto universalAccessor = [field] (TStruct* serializer) -> auto& {
        return serializer->That_->*field;
    };

    return ParameterWithUniversalAccessor<TValue>(key, universalAccessor);
}

template <class TStruct>
template <class TExternalPreprocessor>
    // requires (CInvocable<TExternalPreprocessor, void(typename TStruct::TExternal*)>)
void TYsonStructRegistrar<TStruct>::ExternalPreprocessor(TExternalPreprocessor preprocessor)
{
    static_assert(CInvocable<TExternalPreprocessor, void(typename TStruct::TExternal*)>);
    Meta_->RegisterPreprocessor([preprocessor = std::move(preprocessor)] (TYsonStructBase* target) {
        preprocessor(TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(target)->That_);
    });
}

template <class TStruct>
template <class TExternalPostprocessor>
    // requires (CInvocable<TExternalPostprocessor, void(typename TStruct::TExternal*)>)
void TYsonStructRegistrar<TStruct>::ExternalPostprocessor(TExternalPostprocessor postprocessor)
{
    static_assert(CInvocable<TExternalPostprocessor, void(typename TStruct::TExternal*)>);
    Meta_->RegisterPostprocessor([postprocessor = std::move(postprocessor)] (TYsonStructBase* target) {
        postprocessor(TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(target)->That_);
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
    static_assert(std::derived_from<TStruct, TBase>);
    return TYsonStructRegistrar<TBase>(Meta_);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires CExternallySerializable<T>
void Serialize(const T& value, NYson::IYsonConsumer* consumer)
{
    using TSerializer = typename TGetExternalizedYsonStructTraits<T>::TExternalSerializer;
    auto serializer = TSerializer::template CreateReadOnly<T, TSerializer>(value);
    Serialize(serializer, consumer);
}

template <class T>
    requires CExternallySerializable<T>
void DeserializeExternalized(T& value, INodePtr node, bool postprocess, bool setDefaults)
{
    using TTraits = TGetExternalizedYsonStructTraits<T>;
    using TSerializer = typename TTraits::TExternalSerializer;
    auto serializer = TSerializer::template CreateWritable<T, TSerializer>(value, setDefaults);
    serializer.Load(node, postprocess, setDefaults);
}

template <class T>
    requires CExternallySerializable<T>
void Deserialize(T& value, INodePtr node)
{
    DeserializeExternalized(value, std::move(node), /*postprocess*/ true, /*setDefaults*/ true);
}

template <class T>
    requires CExternallySerializable<T>
void Deserialize(T& value, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(value, NYson::ExtractTo<NYTree::INodePtr>(cursor));
}

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

#undef DECLARE_YSON_STRUCT
#undef REGISTER_YSON_STRUCT
#undef DECLARE_YSON_STRUCT_LITE
#undef REGISTER_YSON_STRUCT_LITE
#undef DEFINE_YSON_STRUCT
#undef DEFINE_YSON_STRUCT_LITE
#undef REGISTER_EXTERNALIZED_YSON_STRUCT
#undef REGISTER_DERIVED_EXTERNALIZED_YSON_STRUCT
#undef ASSIGN_EXTERNAL_YSON_SERIALIZER

#define YSON_STRUCT_IMPL__DECLARE_ALIASES(TStruct) \
private: \
    using TRegistrar = ::NYT::NYTree::TYsonStructRegistrar<TStruct>; \
    using TThis = TStruct; \
    friend class ::NYT::NYTree::TYsonStructRegistry;

#define YSON_STRUCT_IMPL__CTOR_BODY(TStruct) \
    ::NYT::NYTree::TYsonStructRegistry::Get()->InitializeStruct(this);

#define YSON_STRUCT_LITE_IMPL__CTOR_BODY(TStruct) \
    YSON_STRUCT_IMPL__CTOR_BODY(TStruct) \
    if (std::type_index(typeid(TStruct)) == this->FinalType_ && !::NYT::NYTree::TYsonStructRegistry::Get()->InitializationInProgress()) { \
        this->SetDefaults(); \
    } \

//! NB(arkady-e1ppa): Alias is used by registrar postprocessors
//! in order to properly infer template argument.
#define YSON_STRUCT_EXTERNAL_SERIALIZER_IMPL__DECLARE_ALIASES(TStruct, TSerializer) \
public: \
    using TExternal = TStruct; \
    TStruct* That_ = nullptr; \
private: \
    using TRegistrar = ::NYT::NYTree::TYsonStructRegistrar<TSerializer>; \
    using TThat = TStruct; \
    friend class ::NYT::NYTree::TYsonStructRegistry; \
    friend class ::NYT::NYTree::TExternalizedYsonStruct; \
    template <class T> \
    friend const std::type_info& ::NYT::NYTree::CallCtor(); \


#define DECLARE_YSON_STRUCT(TStruct) \
public: \
    TStruct(); \
    YSON_STRUCT_IMPL__DECLARE_ALIASES(TStruct)

#define REGISTER_YSON_STRUCT(TStruct) \
public: \
    TStruct() \
    { \
        static_assert(std::derived_from<TStruct, ::NYT::NYTree::TYsonStruct>, "Class must inherit from TYsonStruct"); \
        YSON_STRUCT_IMPL__CTOR_BODY(TStruct) \
    } \
    YSON_STRUCT_IMPL__DECLARE_ALIASES(TStruct)


#define DECLARE_YSON_STRUCT_LITE(TStruct) \
public: \
    TStruct(); \
    YSON_STRUCT_IMPL__DECLARE_ALIASES(TStruct)

#define REGISTER_YSON_STRUCT_LITE(TStruct) \
public: \
    TStruct() \
        : ::NYT::NYTree::TYsonStructFinalClassHolder(std::type_index(typeid(TStruct))) \
    { \
        static_assert(std::derived_from<TStruct, ::NYT::NYTree::TYsonStructLite>, "Class must inherit from TYsonStructLite"); \
        YSON_STRUCT_LITE_IMPL__CTOR_BODY(TStruct) \
    } \
    YSON_STRUCT_IMPL__DECLARE_ALIASES(TStruct) \

#define DEFINE_YSON_STRUCT_LITE(TStruct) \
TStruct::TStruct() \
    : ::NYT::NYTree::TYsonStructFinalClassHolder(std::type_index(typeid(TStruct))) \
{ \
    static_assert(std::derived_from<TStruct, ::NYT::NYTree::TYsonStructLite>, "Class must inherit from TYsonStructLite"); \
    YSON_STRUCT_LITE_IMPL__CTOR_BODY(TStruct) \
}

#define DEFINE_YSON_STRUCT(TStruct) \
TStruct::TStruct() \
{ \
    static_assert(std::derived_from<TStruct, ::NYT::NYTree::TYsonStruct>, "Class must inherit from TYsonStruct"); \
    YSON_STRUCT_IMPL__CTOR_BODY(TStruct) \
}

//! NB(arkady-e1ppa): These constructors are only used internally.
//! Default one is required by TYsonStructRegistry::InitializeStruct.
//! If you want to create an instance of a TStruct then:
//! 1) Unless you are working with some serialization logic you shouldn't.
//! 2) Use TExternalizedYsonStruct::CreateWritable and TExternalizedYsonStruct::CreateReadOnly
//! instead to guarantee proper initialization as well as const-correctness.
#define EXTERNALIZED_YSON_STRUCT_IMPL__CTORS(TStruct, TSerializer) \
public: \
    TSerializer() \
        : TSerializer(::NYT::NYTree::TExternalizedYsonStruct::template GetDefault<TStruct>()) \
    { }; \
    explicit TSerializer(TStruct* ptr) \
        : ::NYT::NYTree::TYsonStructFinalClassHolder(std::type_index(typeid(TSerializer))) \
    { \
        static_assert(std::derived_from<TSerializer, ::NYT::NYTree::TExternalizedYsonStruct>, "Class must inherit from TExternalizedYsonStruct"); \
        SetThat(ptr); \
        YSON_STRUCT_LITE_IMPL__CTOR_BODY(TSerializer); \
    } \

#define REGISTER_EXTERNALIZED_YSON_STRUCT(TStruct, TSerializer) \
    EXTERNALIZED_YSON_STRUCT_IMPL__CTORS(TStruct, TSerializer) \
public: \
    void SetThat(TStruct* ptr) \
    { \
        That_ = ptr; \
    } \
    YSON_STRUCT_EXTERNAL_SERIALIZER_IMPL__DECLARE_ALIASES(TStruct, TSerializer) \

//! TODO(arkady-e1ppa):
/*
    Code below is terrible both in terms of internal implementation and user experience.
    Ideally we would want to abolish the macro below and instead use magic method
    "GetThat" whenever we want to assign That_. Properly implementing CreateReadOnly
    is likely to require some more tinkering but should ultimately be possible to do.

    Magic GetThat should be something along the lines
    static TStruct* GetThat(this auto& self)
    {
        return self.That_;
    }

    and called as TSerializer::GetThat(*this) which would trigger the correct overload of it and
    deduce the most derived type in auto which would resolve .That_ as a field of the most derived
    class.

    This "deducing this" feature will come in C++23 and is likely to be properly supported in clang-18 or 19.

    Another thing to consider is using some method (which is likely outside of standard) to list all
    direct bases of the type so we can at least remove explicit enumeration of them in the user code.

    Perhaps, there is another approach to consider?
*/

#define BASE_SET_THAT_ENTRY(TBase) \
    TBase::SetThat(ptr); \

#define REGISTER_DERIVED_EXTERNALIZED_YSON_STRUCT(TStruct, TSerializer, TBases) \
    EXTERNALIZED_YSON_STRUCT_IMPL__CTORS(TStruct, TSerializer) \
public: \
    void SetThat(TStruct* ptr) \
    { \
        That_ = ptr; \
        PP_FOR_EACH(BASE_SET_THAT_ENTRY, TBases) \
    } \
    YSON_STRUCT_EXTERNAL_SERIALIZER_IMPL__DECLARE_ALIASES(TStruct, TSerializer) \

#define ASSIGN_EXTERNAL_YSON_SERIALIZER(TStruct, TSerializer) \
    [[maybe_unused]] constexpr auto GetExternalizedYsonStructTraits(TStruct) \
    { \
        struct [[maybe_unused]] TTraits \
        { \
            using TExternalSerializer = TSerializer; \
        }; \
        static_assert(std::derived_from<TTraits::TExternalSerializer, ::NYT::NYTree::TExternalizedYsonStruct>, "External serializer must be derived from TExternalizedYsonStruct"); \
        return TTraits{}; \
    } \
    static_assert(::NYT::NYTree::CExternallySerializable<TStruct>, "You must write this macro in the namespace containing TStruct")

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
