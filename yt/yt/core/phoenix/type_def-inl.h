#ifndef TYPE_DEF_INL_H_
#error "Direct inclusion of this file is not allowed, include type_def.h"
// For the sake of sane code completion.
#include "type_def.h"
#endif

#include "factory.h"
#include "polymorphic.h"
#include "context.h"
#include "descriptors.h"
#include "type_registry.h"

#include <yt/yt/core/concurrency/fls.h>

#include <library/cpp/yt/misc/preprocessor.h>

#include <concepts>

////////////////////////////////////////////////////////////////////////////////

#undef PHOENIX_DEFINE_TYPE
#undef PHOENIX_DEFINE_TEMPLATE_TYPE
#undef PHOENIX_DEFINE_OPAQUE_TYPE

////////////////////////////////////////////////////////////////////////////////

#define PHOENIX_DEFINE_TYPE__STATIC_INIT(type, parenthesizedTypeArgs) \
    [[maybe_unused]] static const void* PhoenixTypeDescriptorStaticInit__ ## type = [] { \
        type PP_DEPAREN(parenthesizedTypeArgs)::GetTypeDescriptor(); \
        return nullptr; \
    }()

#define PHOENIX_DEFINE_TYPE(type) \
    PHOENIX_DEFINE_TYPE__STATIC_INIT(type, ()); \
    \
    const ::NYT::NPhoenix2::TTypeDescriptor& type::GetTypeDescriptor() \
    { \
        static const auto& descriptor = ::NYT::NPhoenix2::NDetail::RegisterTypeDescriptorImpl<type, /*Template*/ false>(); \
        return descriptor; \
    } \
    \
    auto type::GetRuntimeFieldDescriptorMap() -> const ::NYT::NPhoenix2::NDetail::TRuntimeFieldDescriptorMap<type, TLoadContext>& \
    { \
        static const auto map = ::NYT::NPhoenix2::NDetail::BuildRuntimeFieldDescriptorMap<TThis, TLoadContext>(); \
        return map; \
    } \
    \
    void type::Save(TSaveContext& context) const \
    { \
        ::NYT::NPhoenix2::NDetail::SaveImpl(this, context); \
    } \
    \
    void type::Load(TLoadContext& context) \
    { \
        ::NYT::NPhoenix2::NDetail::LoadImpl(this, context); \
    }

#define PHOENIX_DEFINE_TEMPLATE_TYPE(type, parenthesizedTypeArgs) \
    PHOENIX_DEFINE_TYPE__STATIC_INIT(type, parenthesizedTypeArgs)

#define PHOENIX_DEFINE_OPAQUE_TYPE(type) \
    PHOENIX_DEFINE_TYPE__STATIC_INIT(type, ()); \
    \
    const ::NYT::NPhoenix2::TTypeDescriptor& type::GetTypeDescriptor() \
    { \
        static const auto& descriptor = ::NYT::NPhoenix2::NDetail::RegisterOpaqueTypeDescriptorImpl<type>(); \
        return descriptor; \
    }

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NPhoenix2::NDetail {

////////////////////////////////////////////////////////////////////////////////

#define PHOENIX_REGISTRAR_NODISCARD [[nodiscard("Did you forget to call operator()?")]]

class PHOENIX_REGISTRAR_NODISCARD TDummyFieldRegistrar
{
public:
    auto SinceVersion(auto /*version*/) &&
    {
        return std::move(*this);
    }

    auto InVersions(auto /*filter*/) &&
    {
        return std::move(*this);
    }

    auto WhenMissing(auto&& /*handler*/) &&
    {
        return std::move(*this);
    }

    template <class TSerializer>
    auto Serializer() &&
    {
        return std::move(*this);
    }

    void operator()() &&
    { }
};

class TTypeRegistrarBase
{
public:
    template <TFieldTag::TUnderlying TagValue, auto Member>
    TDummyFieldRegistrar Field(auto&& /*name*/)
    {
        return {};
    }

    template <TFieldTag::TUnderlying TagValue>
    TDummyFieldRegistrar DeprecatedField(auto&& /*name*/, auto&& /*handler*/)
    {
        return {};
    }

    template <class TBase>
    void BaseType()
    { }

    void AfterLoad(auto&& /*handler*/)
    { }

    void operator()() &&
    { }
};

template <class TThis>
decltype(auto) RunRegistrar(auto&& registrar)
{
    TThis::RegisterMetadata(registrar);
    return std::move(registrar)();
}

////////////////////////////////////////////////////////////////////////////////

class TTypeSchemaBuilderRegistar
    : public TTypeRegistrarBase
{
public:
    TTypeSchemaBuilderRegistar(
        std::vector<const std::type_info*> typeInfos,
        TTypeTag tag,
        bool isTemplate,
        TPolymorphicConstructor polymorphicConstructor,
        TConcreteConstructor concreteConstructor);

    template <TFieldTag::TUnderlying TagValue, auto Member>
    auto Field(TString name)
    {
        return DoField<TagValue>(std::move(name), /*deprecated*/ false);
    }

    template <TFieldTag::TUnderlying TagValue>
    auto DeprecatedField(TString name, auto&& /*handler*/)
    {
        return DoField<TagValue>(std::move(name), /*deprecated*/ true);
    }

    template <class TBase>
    void BaseType()
    {
        TypeDescriptor_->BaseTypes_.push_back(&TBase::GetTypeDescriptor());
    }

    const TTypeDescriptor& operator()() &&;

private:
    std::unique_ptr<TTypeDescriptor> TypeDescriptor_ = std::make_unique<TTypeDescriptor>();

    template <TFieldTag::TUnderlying TagValue>
    auto DoField(TString name, bool deprecated)
    {
        auto fieldDescriptor = std::make_unique<TFieldDescriptor>();
        fieldDescriptor->Name_ = std::move(name);
        fieldDescriptor->Tag_ = TFieldTag(TagValue);
        fieldDescriptor->Deprecated_ = deprecated;
        TypeDescriptor_->Fields_.push_back(std::move(fieldDescriptor));
        return TDummyFieldRegistrar();
    }
};

template <class T>
std::vector<const std::type_info*> GetTypeInfos()
{
    return {&typeid (T)};
}

template <class T>
    requires std::derived_from<T, TRefCounted>
std::vector<const std::type_info*> GetTypeInfos()
{
    return {&typeid (T), &typeid (TRefCountedWrapper<T>)};
}

template <class TThis, bool Template>
auto MakeTypeSchemaBuilderRegistrar()
{
    return TTypeSchemaBuilderRegistar(
        GetTypeInfos<TThis>(),
        TThis::TypeTag,
        Template,
        TFactoryTraits<TThis>::TFactory::PolymorphicConstructor,
        TFactoryTraits<TThis>::TFactory::ConcreteConstructor);
}

template <class TThis, bool Template>
const TTypeDescriptor& RegisterTypeDescriptorImpl()
{
    return RunRegistrar<TThis>(MakeTypeSchemaBuilderRegistrar<TThis, Template>());
}

template <class TThis>
const TTypeDescriptor& RegisterOpaqueTypeDescriptorImpl()
{
    return MakeTypeSchemaBuilderRegistrar<TThis, /*Template*/ false>()();
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TContext>
class TSaveBaseTypesRegistrar
    : public TTypeRegistrarBase
{
public:
    TSaveBaseTypesRegistrar(const TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    template <class TBase>
    void BaseType()
    {
        This_->TBase::Save(Context_);
    }

private:
    const TThis* const This_;
    TContext& Context_;
};

template <auto Member, class TThis, class TContext, class TFieldSerializer>
class PHOENIX_REGISTRAR_NODISCARD TFieldSaveRegistrar
{
public:
    TFieldSaveRegistrar(const TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    template <class TFieldSerializer_>
    TFieldSaveRegistrar(TFieldSaveRegistrar<Member, TThis, TContext, TFieldSerializer_>&& other)
        : This_(other.This_)
        , Context_(other.Context_)
    { }

    auto SinceVersion(auto /*version*/) &&
    {
        return TFieldSaveRegistrar(std::move(*this));
    }

    auto InVersions(auto /*filter*/) &&
    {
        return TFieldSaveRegistrar(std::move(*this));
    }

    auto WhenMissing(auto&& /*handler*/) &&
    {
        return TFieldSaveRegistrar(std::move(*this));
    }

    template <class TFieldSerializer_>
    auto Serializer() &&
    {
        return TFieldSaveRegistrar<Member, TThis, TContext, TFieldSerializer_>(std::move(*this));
    }

    void operator()() &&
    {
        TFieldSerializer::Save(Context_, This_->*Member);
    }

private:
    template <auto Member_, class TThis_, class TContext_, class TFieldSerializer_>
    friend class TFieldSaveRegistrar;

    const TThis* const This_;
    TContext& Context_;
};

template <class TThis, class TContext>
class TSaveFieldsRegistrar
    : public TTypeRegistrarBase
{
public:
    TSaveFieldsRegistrar(const TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    template <TFieldTag::TUnderlying TagValue, auto Member>
    auto Field(auto&& /*name*/)
    {
        return TFieldSaveRegistrar<Member, TThis, TContext, TDefaultSerializer>(
            This_,
            Context_);
    }

private:
    const TThis* const This_;
    TContext& Context_;
};

template <class TThis, class TContext>
void SaveImpl(const TThis* this_, TContext& context)
{
    RunRegistrar<TThis>(TSaveBaseTypesRegistrar(this_, context));
    RunRegistrar<TThis>(TSaveFieldsRegistrar(this_, context));
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TContext>
class TLoadBaseTypesRegistrar
    : public TTypeRegistrarBase
{
public:
    TLoadBaseTypesRegistrar(TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    template <class TBase>
    void BaseType()
    {
        This_->TBase::Load(Context_);
    }

private:
    TThis* const This_;
    TContext& Context_;
};

template <auto Member, class TThis, class TContext, class TFieldSerializer>
class PHOENIX_REGISTRAR_NODISCARD TFieldLoadRegistrar
{
public:
    TFieldLoadRegistrar(TThis* this_, TContext& context, TStringBuf name)
        : This_(this_)
        , Context_(context)
        , Name_(name)
    { }

    template <class TFieldSerializer_>
    TFieldLoadRegistrar(TFieldLoadRegistrar<Member, TThis, TContext, TFieldSerializer_>&& other)
        : This_(other.This_)
        , Context_(other.Context_)
        , Name_(other.Name_)
        , MinVersion_(other.MinVersion_)
        , VersionFilter_(other.VersionFilter_)
        , MissingHandler_(other.MissingHandler_)
    { }

    using TVersion = decltype(std::declval<typename TThis::TLoadContextImpl>().GetVersion());

    auto SinceVersion(TVersion version) &&
    {
        MinVersion_ = version;
        return TFieldLoadRegistrar(std::move(*this));
    }

    using TMissingHandler = void (*)(TThis* this_, TContext& context);

    auto WhenMissing(TMissingHandler handler) &&
    {
        MissingHandler_ = handler;
        return TFieldLoadRegistrar(std::move(*this));
    }

    using TVersionFilter = bool (*)(TVersion version);

    auto InVersions(TVersionFilter filter) &&
    {
        VersionFilter_ = filter;
        return TFieldLoadRegistrar(std::move(*this));
    }


    template <class TFieldSerializer_>
    auto Serializer() &&
    {
        return TFieldLoadRegistrar<Member, TThis, TContext, TFieldSerializer_>(std::move(*this));
    }

    void operator()() &&
    {
        if (auto version = Context_.GetVersion(); version >= MinVersion_ && (!VersionFilter_ || VersionFilter_(version))) {
            Context_.Dumper().SetFieldName(Name_);
            TFieldSerializer::Load(Context_, This_->*Member);
        } else if (MissingHandler_) {
            MissingHandler_(This_, Context_);
        } else {
            This_->*Member = {};
        }
    }

private:
    template <auto Member_, class TThis_, class TContext_, class TFieldSerializer_>
    friend class TFieldLoadRegistrar;

    TThis* const This_;
    TContext& Context_;
    const TStringBuf Name_;

    TVersion MinVersion_ = static_cast<TVersion>(std::numeric_limits<int>::min());
    TVersionFilter VersionFilter_ = nullptr;
    TMissingHandler MissingHandler_ = nullptr;
};

template <class TThis, class TContext>
class TLoadFieldsRegistrar
    : public TTypeRegistrarBase
{
public:
    TLoadFieldsRegistrar(TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    template <TFieldTag::TUnderlying TagValue, auto Member, size_t NameLength>
    auto Field(const char (&name)[NameLength])
    {
        return TFieldLoadRegistrar<Member, TThis, TContext, TDefaultSerializer>(
            This_,
            Context_,
            TStringBuf(name, NameLength - 1));
    }

private:
    TThis* const This_;
    TContext& Context_;
};

template <class TThis, class TContext>
class TAfterLoadRegistrar
    : public TTypeRegistrarBase
{
public:
    TAfterLoadRegistrar(TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    using TAfterLoad = void (*)(TThis*, TContext&);

    void AfterLoad(TAfterLoad handler)
    {
        handler(This_, Context_);
    }

private:
    TThis* const This_;
    TContext& Context_;
};

template <class TThis, class TContext>
struct TRuntimeTypeLoadSchedule;

template <class TThis, class TContext>
const TRuntimeTypeLoadSchedule<TThis, TContext>* FindCachedRuntimeTypeLoadSchedule();

void CompatLoadImpl(auto* this_, auto& context, const auto& schedule);

template <class TThis, class TContext>
void LoadImpl(TThis* this_, TContext& context)
{
    RunRegistrar<TThis>(TLoadBaseTypesRegistrar(this_, context));
    if (const auto* runtimeSchedule = FindCachedRuntimeTypeLoadSchedule<TThis, TContext>()) {
        CompatLoadImpl(this_, context, *runtimeSchedule);
    } else {
        RunRegistrar<TThis>(TLoadFieldsRegistrar<TThis, TContext>(this_, context));
    }
    RunRegistrar<TThis>(TAfterLoadRegistrar(this_, context));
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TContext>
using TFieldHandler = void (*)(TThis*, TContext&);

template <class TThis, class TContext>
struct TRuntimeFieldDescriptor
{
    TFieldHandler<TThis, TContext> LoadHandler = nullptr;
    TFieldHandler<TThis, TContext> MissingHandler = nullptr;
};

template <class TThis, class TContext>
using TRuntimeFieldDescriptorMap = THashMap<TFieldTag, TRuntimeFieldDescriptor<TThis, TContext>>;

template <auto Member, class TThis, class TContext, class TFieldSerializer>
class PHOENIX_REGISTRAR_NODISCARD TRuntimeFieldDescriptorBuilderRegistar
{
public:
    using TRuntimeFieldDescriptor = NPhoenix2::NDetail::TRuntimeFieldDescriptor<TThis, TContext>;

    explicit TRuntimeFieldDescriptorBuilderRegistar(TRuntimeFieldDescriptor* descriptor)
        : Descriptor_(descriptor)
    { }

    template <class TFieldSerializer_>
    TRuntimeFieldDescriptorBuilderRegistar(TRuntimeFieldDescriptorBuilderRegistar<Member, TThis, TContext, TFieldSerializer_>&& other)
        : Descriptor_(other.Descriptor_)
    { }

    auto SinceVersion(auto /*version*/) &&
    {
        return std::move(*this);
    }

    auto InVersions(auto /*filter*/) &&
    {
        return std::move(*this);
    }

    auto WhenMissing(TFieldHandler<TThis, TContext> handler) &&
    {
        Descriptor_->MissingHandler = handler;
        return std::move(*this);
    }

    template <class TFieldSerializer_>
    auto Serializer() &&
    {
        return TRuntimeFieldDescriptorBuilderRegistar<Member, TThis, TContext, TFieldSerializer_>(std::move(*this));
    }

    void operator()() &&
    {
        Descriptor_->LoadHandler = [] (TThis* this_, TContext& context) {
            TFieldSerializer::Load(context, this_->*Member);
        };
    }

private:
    template <auto Member_, class TThis_, class TContext_, class TFieldSerializer_>
    friend class TRuntimeFieldDescriptorBuilderRegistar;

    TRuntimeFieldDescriptor* const Descriptor_;
};

template <class TThis, class TContext>
class TRuntimeDeprecatedFieldDescriptorBuilderRegistar
{
public:
    using TRuntimeFieldDescriptor = NPhoenix2::NDetail::TRuntimeFieldDescriptor<TThis, TContext>;

    TRuntimeDeprecatedFieldDescriptorBuilderRegistar(TRuntimeFieldDescriptor* descriptor)
        : Descriptor_(descriptor)
    { }

    auto SinceVersion(auto /*version*/) &&
    {
        return *this;
    }

    auto InVersions(auto /*filter*/) &&
    {
        return *this;
    }

    auto WhenMissing(TFieldHandler<TThis, TContext> handler) &&
    {
        Descriptor_->MissingHandler = handler;
        return std::move(*this);
    }

    template <class TFieldSerializer_>
    auto Serializer() &&
    {
        return std::move(*this);
    }

    void operator()() &&
    { }

private:
    TRuntimeFieldDescriptor* const Descriptor_;
};

template <class TThis, class TContext>
class TRuntimeFieldDescriptorMapBuilderRegistrar
    : public TTypeRegistrarBase
{
public:
    using TRuntimeFieldDescriptor = NDetail::TRuntimeFieldDescriptor<TThis, TContext>;

    template <TFieldTag::TUnderlying TagValue, auto Member>
    auto Field(auto&& /*name*/)
    {
        auto* descriptor = AddField<TagValue>();
        descriptor->MissingHandler = [] (TThis* this_, TContext& /*context*/) {
            this_->*Member = {};
        };
        return TRuntimeFieldDescriptorBuilderRegistar<Member, TThis, TContext, TDefaultSerializer>(descriptor);
    }

    template <TFieldTag::TUnderlying TagValue>
    auto DeprecatedField(auto&& /*name*/, TFieldHandler<TThis, TContext> handler)
    {
        auto* descriptor = AddField<TagValue>();
        descriptor->LoadHandler = handler;
        return TRuntimeDeprecatedFieldDescriptorBuilderRegistar<TThis, TContext>(descriptor);
    }

    auto operator()() &&
    {
        return std::move(Map_);
    }

private:
    TRuntimeFieldDescriptorMap<TThis, TContext> Map_;

    template <TFieldTag::TUnderlying TagValue>
    TRuntimeFieldDescriptor* AddField()
    {
        auto it = EmplaceOrCrash(Map_, TFieldTag(TagValue), TRuntimeFieldDescriptor());
        return &it->second;
    }
};

template <class TThis, class TContext>
auto BuildRuntimeFieldDescriptorMap()
{
    return RunRegistrar<TThis>(TRuntimeFieldDescriptorMapBuilderRegistrar<TThis, TContext>());
}

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TLoadEpoch, ui64);

struct TTypeLoadSchedule
{
    std::vector<TFieldTag> LoadFieldTags;
    std::vector<TFieldTag> MissingFieldTags;
};

struct TRuntimeTypeLoadScheduleBase
{
    virtual ~TRuntimeTypeLoadScheduleBase() = default;
};

struct TUniverseLoadSchedule
{
    const TTypeLoadSchedule* FindTypeLoadSchedule(TTypeTag tag);
    THashMap<TTypeTag, TTypeLoadSchedule> LoadScheduleMap;

    template <class TThis, class TContext>
    const TRuntimeTypeLoadSchedule<TThis, TContext>* FindRuntimeTypeLoadSchedule();
    THashMap<std::tuple<std::type_index, std::type_index>, std::unique_ptr<TRuntimeTypeLoadScheduleBase>> RuntimeLoadScheduleMap;
};

template <class TThis, class TContext>
struct TRuntimeTypeLoadSchedule
    : public TRuntimeTypeLoadScheduleBase
{
    std::vector<TFieldHandler<TThis, TContext>> LoadFieldHandlers;
    std::vector<TFieldHandler<TThis, TContext>> MissingFieldHandlers;
};

struct TUniverseLoadState
{
    bool Active = false;
    TLoadEpoch Epoch = {};
    std::unique_ptr<TUniverseLoadSchedule> Schedule;
};

extern NConcurrency::TFlsSlot<TUniverseLoadState> UniverseLoadState;

template <class TThis, class TContext>
std::unique_ptr<TRuntimeTypeLoadSchedule<TThis, TContext>> BuildRuntimeTypeLoadSchedule(const TTypeLoadSchedule* schedule)
{
    if (!schedule) {
        return nullptr;
    }

    auto runtimeSchedule = std::make_unique<TRuntimeTypeLoadSchedule<TThis, TContext>>();
    runtimeSchedule->LoadFieldHandlers.reserve(schedule->LoadFieldTags.size());
    runtimeSchedule->MissingFieldHandlers.reserve(schedule->MissingFieldTags.size());

    const auto& runtimeFieldDescriptorMap = TThis::GetRuntimeFieldDescriptorMap();
    for (auto fieldTag : schedule->LoadFieldTags) {
        runtimeSchedule->LoadFieldHandlers.push_back(GetOrCrash(runtimeFieldDescriptorMap, fieldTag).LoadHandler);
    }
    for (auto fieldTag : schedule->MissingFieldTags) {
        runtimeSchedule->MissingFieldHandlers.push_back(GetOrCrash(runtimeFieldDescriptorMap, fieldTag).MissingHandler);
    }

    return runtimeSchedule;
}

template <class TThis, class TContext>
const TRuntimeTypeLoadSchedule<TThis, TContext>* TUniverseLoadSchedule::FindRuntimeTypeLoadSchedule()
{
    auto runtimeKey = std::tuple(std::type_index(typeid(TThis)), std::type_index(typeid(TContext)));
    auto it = RuntimeLoadScheduleMap.find(runtimeKey);
    if (it != RuntimeLoadScheduleMap.end()) {
        return static_cast<TRuntimeTypeLoadSchedule<TThis, TContext>*>(it->second.get());
    }

    auto* schedule = FindTypeLoadSchedule(TThis::TypeTag);
    auto runtimeSchedule = BuildRuntimeTypeLoadSchedule<TThis, TContext>(schedule);
    auto* runtimeSchedulePtr = runtimeSchedule.get();
    EmplaceOrCrash(RuntimeLoadScheduleMap, runtimeKey, std::move(runtimeSchedule));
    return runtimeSchedulePtr;
}

template <class TThis, class TContext>
const TRuntimeTypeLoadSchedule<TThis, TContext>* FindCachedRuntimeTypeLoadSchedule()
{
    auto& universeLoadState = *UniverseLoadState;
    if (!universeLoadState.Schedule) {
        return nullptr;
    }

    struct TTypeLoadState
    {
        TLoadEpoch Epoch;
        const TRuntimeTypeLoadSchedule<TThis, TContext>* RuntimeSchedule;
    };

    static NConcurrency::TFlsSlot<TTypeLoadState> TypeLoadState;
    auto& typeLoadState = *TypeLoadState;

    if (typeLoadState.Epoch != universeLoadState.Epoch) {
        typeLoadState.Epoch = universeLoadState.Epoch;
        typeLoadState.RuntimeSchedule = universeLoadState.Schedule->FindRuntimeTypeLoadSchedule<TThis, TContext>();
    }

    return typeLoadState.RuntimeSchedule;
}

void CompatLoadImpl(auto* this_, auto& context, const auto& runtimeSchedule)
{
    for (auto handler : runtimeSchedule.LoadFieldHandlers) {
        handler(this_, context);
    }
    for (auto handler : runtimeSchedule.MissingFieldHandlers) {
        handler(this_, context);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TSerializer
{
    template <class T, class C>
    static void Save(C& context, const TIntrusivePtr<T>& ptr)
    {
        SaveImpl(context, ptr.Get());
    }

    template <class T, class C>
    static void Save(C& context, const std::unique_ptr<T>& ptr)
    {
        SaveImpl(context, ptr.get());
    }

    template <class T, class C>
    static void Save(C& context, T* ptr)
    {
        SaveImpl(context, ptr);
    }

    template <class T, class C>
    static void SaveImpl(C& context, T* ptr)
    {
        using NYT::Save;

        if (!ptr) {
            Save(context, NullObjectId);
            return;
        }

        auto* basePtr = static_cast<typename TPolymorphicTraits<T>::TBase*>(ptr);
        auto typeIndex = TPolymorphicTraits<T>::Polymorphic ? std::make_optional<std::type_index>(typeid(*ptr)) : std::nullopt;
        auto objectId = context.FindObjectId(basePtr, typeIndex);
        if (objectId != NullObjectId) {
            Save(context, objectId);
            return;
        }

        objectId = context.GenerateObjectId(basePtr, typeIndex);

        Save(context, TObjectId(objectId.Underlying() | InlineObjectIdMask.Underlying()));
        if constexpr(TPolymorphicTraits<T>::Polymorphic) {
            const auto& universeDescriptor = ITypeRegistry::Get()->GetUniverseDescriptor();
            const auto& typeDescriptor = universeDescriptor.GetTypeDescriptorByTypeIndexOrThrow(*typeIndex);
            Save(context, typeDescriptor.GetTag());
        }
        Save(context, *ptr);
    }


    template <class T, class C>
    static void Load(C& context, TIntrusivePtr<T>& ptr)
    {
        T* rawPtr = nullptr;
        LoadImpl</*Inplace*/ false>(context, rawPtr);
        ptr.Reset(rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, const TIntrusivePtr<T>& ptr)
    {
        T* rawPtr = ptr.Get();
        LoadImpl</*Inplace*/ true>(context, rawPtr);
    }

    template <class T, class C>
    static void Load(C& context, std::unique_ptr<T>& ptr)
    {
        T* rawPtr = nullptr;
        LoadImpl</*Inplace*/ false>(context, rawPtr);
        ptr.reset(rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, const std::unique_ptr<T>& ptr)
    {
        T* rawPtr = ptr.get();
        LoadImpl</*Inplace*/ true>(context, rawPtr);
    }

    template <class T, class C>
    static void Load(C& context, T*& rawPtr)
    {
        rawPtr = nullptr;
        LoadImpl</*Inplace*/ false>(context, rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, T* rawPtr)
    {
        LoadImpl</*Inplace*/ true>(context, rawPtr);
    }

    template <bool Inplace, class T, class C>
    static void LoadImpl(C& context, T*& rawPtr)
    {
        using TBase = typename TPolymorphicTraits<T>::TBase;
        using NYT::Load;

        auto objectId = LoadSuspended<TObjectId>(context);
        if (objectId == NullObjectId) {
            rawPtr = nullptr;
            return;
        }

        if (TObjectId(objectId.Underlying() & InlineObjectIdMask.Underlying())) {
            if constexpr(Inplace) {
                YT_VERIFY(rawPtr);
                if constexpr(TPolymorphicTraits<T>::Polymorphic) {
                    auto runtimeTypeIndex = std::type_index(typeid (*rawPtr));
                    const auto& universeDescriptor = ITypeRegistry::Get()->GetUniverseDescriptor();
                    const auto& typeDescriptor = universeDescriptor.GetTypeDescriptorByTypeIndexOrThrow(runtimeTypeIndex);
                    auto runtimeTag = typeDescriptor.GetTag();
                    auto streamTag = LoadSuspended<TTypeTag>(context);
                    YT_VERIFY(streamTag == runtimeTag);
                }
            } else {
                if constexpr(TPolymorphicTraits<T>::Polymorphic) {
                    auto tag = LoadSuspended<TTypeTag>(context);
                    const auto& descriptor = ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTagOrThrow(tag);
                    rawPtr = descriptor.template ConstructOrThrow<T>();
                } else {
                    using TFactory = typename TFactoryTraits<T>::TFactory;
                    static_assert(TFactory::ConcreteConstructor);
                    rawPtr = static_cast<T*>(TFactory::ConcreteConstructor());
                }
                context.RegisterConstructedObject(rawPtr);
            }

            TBase* basePtr = rawPtr;
            context.RegisterObject(TObjectId(objectId.Underlying() & ~InlineObjectIdMask.Underlying()), basePtr);

            Load(context, *rawPtr);
        } else {
            auto* basePtr = static_cast<TBase*>(context.GetObject(objectId));
            rawPtr = dynamic_cast<T*>(basePtr);
        }
    }
};

#undef PHOENIX_REGISTRAR_NODISCARD

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2::NDetail

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
    requires (std::derived_from<C, NPhoenix2::NDetail::TContextBase>) && (
        std::same_as<T, TIntrusivePtr<typename T::TUnderlying>> ||
        std::same_as<T, std::unique_ptr<typename T::element_type>> ||
        std::is_pointer_v<T>)
struct TSerializerTraits<T, C>
{
    using TSerializer = NPhoenix2::NDetail::TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
