#ifndef TYPE_DEF_INL_H_
#error "Direct inclusion of this file is not allowed, include type_def.h"
// For the sake of sane code completion.
#include "type_def.h"
#endif

#include "factory.h"
#include "polymorphic.h"
#include "context.h"
#include "descriptors.h"
#include "type_decl.h"
#include "type_registry.h"

#include <library/cpp/yt/misc/static_initializer.h>

#include <concepts>

namespace NYT::NPhoenix::NDetail {

////////////////////////////////////////////////////////////////////////////////

#undef PHOENIX_DEFINE_TYPE
#undef PHOENIX_DEFINE_TEMPLATE_TYPE
#undef PHOENIX_DEFINE_OPAQUE_TYPE
#undef PHOENIX_REGISTER_FIELD

////////////////////////////////////////////////////////////////////////////////

#define PHOENIX_DEFINE_TYPE(type) \
    const ::NYT::NPhoenix::TTypeDescriptor& type::GetTypeDescriptor() \
    { \
        static const auto& descriptor = ::NYT::NPhoenix::ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTag(TypeTag); \
        return descriptor; \
    } \
    \
    auto type::GetRuntimeFieldDescriptorMap() -> const ::NYT::NPhoenix::NDetail::TRuntimeFieldDescriptorMap<type, TLoadContext>& \
    { \
        static const auto map = ::NYT::NPhoenix::NDetail::BuildRuntimeFieldDescriptorMap<TThis, TLoadContext>(); \
        return map; \
    } \
    \
    void type::Save(TSaveContext& context) const \
    { \
        ::NYT::NPhoenix::NDetail::SaveImpl(this, context); \
    } \
    \
    void type::Load(TLoadContext& context) \
    { \
        ::NYT::NPhoenix::NDetail::LoadImpl(this, context); \
    } \
    \
    template <class T> \
    struct TPhoenixTypeInitializer__; \
    \
    template <> \
    struct TPhoenixTypeInitializer__<type> \
    { \
        YT_STATIC_INITIALIZER({ \
            ::NYT::NPhoenix::NDetail::RegisterTypeDescriptorImpl<type, false>(); \
        }); \
    }

#define PHOENIX_DEFINE_TEMPLATE_TYPE(type, typeArgs) \
    template <class T> \
    struct TPhoenixTypeInitializer__; \
    \
    template <> \
    struct TPhoenixTypeInitializer__<type<PP_DEPAREN(typeArgs)>> \
    { \
        YT_STATIC_INITIALIZER({ \
            ::NYT::NPhoenix::NDetail::RegisterTypeDescriptorImpl<type<PP_DEPAREN(typeArgs)>, true>(); \
        }); \
    }

#define PHOENIX_DEFINE_OPAQUE_TYPE(type) \
    const ::NYT::NPhoenix::TTypeDescriptor& type::GetTypeDescriptor() \
    { \
        static const auto& descriptor = ::NYT::NPhoenix::ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTag(TypeTag); \
        return descriptor; \
    } \
    \
    template <class T> \
    struct TPhoenixTypeInitializer__; \
    \
    template <> \
    struct TPhoenixTypeInitializer__<type> \
    { \
        YT_STATIC_INITIALIZER({ \
            ::NYT::NPhoenix::NDetail::RegisterOpaqueTypeDescriptorImpl<type>(); \
        }); \
    }

#define PHOENIX_REGISTER_FIELD(fieldTag, fieldName, ...) \
    registrar.template Field<fieldTag, &TThis::fieldName>(#fieldName) __VA_ARGS__ ()

#define PHOENIX_REGISTER_DELETED_FIELD(fieldTag, fieldType, fieldName, version, ...) \
    registrar \
        .template VirtualField<fieldTag>(#fieldName, [] (TThis* /*this_*/, auto& context) { \
            Load<fieldType>(context); \
        }) \
        .BeforeVersion(version) __VA_ARGS__ ()

////////////////////////////////////////////////////////////////////////////////

// COMPAT(coteeq): Older snapshots are broken: they have virtual fields in schema,
// but these fields may not be physically serialized. As we need to check
// the version manually inside the load callback, we need to be able to capture the version.
template <class TThis, class TContext>
using TScheduledFieldLoadHandler = std::function<void(TThis*, TContext&)>;

template <class TThis, class TContext>
using TFieldMissingHandler = void (*)(TThis*, TContext&);

template <class TThis, class TContext>
using TFieldLoadHandler = void (*)(TThis*, TContext&);

template <class TThis, class TContext>
using TFieldSaveHandler = void (*)(const TThis*, TContext&);

////////////////////////////////////////////////////////////////////////////////

template <class TThis>
struct TTraits
{
    using TVersion = decltype(std::declval<typename TThis::TLoadContextImpl>().GetVersion());
};

////////////////////////////////////////////////////////////////////////////////

template <class TThis>
using TVersionFilter = bool (*)(typename TTraits<TThis>::TVersion version);

template <class TVersion>
constexpr TVersion GetPreviousVersion(TVersion version)
{
    return static_cast<TVersion>(static_cast<i64>(version) - 1);
}

////////////////////////////////////////////////////////////////////////////////

#define PHOENIX_REGISTRAR_NODISCARD [[nodiscard("Did you forget to call operator()?")]]

class PHOENIX_REGISTRAR_NODISCARD TDummyFieldRegistrar
{
public:
    auto SinceVersion(auto /*version*/) &&
    {
        return std::move(*this);
    }

    auto BeforeVersion(auto /*version*/) &&
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
    TDummyFieldRegistrar VirtualField(
        auto&& /*name*/,
        auto&& /*loadHandler*/)
    {
        return {};
    }

    template <TFieldTag::TUnderlying TagValue>
    TDummyFieldRegistrar VirtualField(
        auto&& /*name*/,
        auto&& /*loadHandler*/,
        auto&& /*saveHandler*/)
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

class TTypeSchemaBuilderRegistrar
    : public TTypeRegistrarBase
{
public:
    TTypeSchemaBuilderRegistrar(
        std::vector<const std::type_info*> typeInfos,
        TTypeTag tag,
        bool isTemplate,
        TPolymorphicConstructor polymorphicConstructor,
        TConcreteConstructor concreteConstructor);

    template <TFieldTag::TUnderlying TagValue, auto Member>
    auto Field(std::string name)
    {
        return DoField<TagValue>(std::move(name));
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        std::string name,
        auto&& /*loadHandler*/)
    {
        return DoField<TagValue>(std::move(name));
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        std::string name,
        auto&& loadHandler,
        auto&& /*saveHandler*/)
    {
        return VirtualField<TagValue>(std::move(name), loadHandler);
    }

    template <class TBase>
    void BaseType()
    {
        TypeDescriptor_->BaseTypeTags_.push_back(TBase::TypeTag);
    }

    const TTypeDescriptor& operator()() &&;

private:
    std::unique_ptr<TTypeDescriptor> TypeDescriptor_ = std::make_unique<TTypeDescriptor>();

    template <TFieldTag::TUnderlying TagValue>
    auto DoField(std::string name)
    {
        auto fieldDescriptor = std::make_unique<TFieldDescriptor>();
        fieldDescriptor->Name_ = std::move(name);
        fieldDescriptor->Tag_ = TFieldTag(TagValue);
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
    requires std::derived_from<T, TRefCounted> && (!std::is_abstract_v<TRefCountedWrapperMock<T>>)
std::vector<const std::type_info*> GetTypeInfos()
{
    return {&typeid (T), &typeid (TRefCountedWrapper<T>)};
}

template <class TThis, bool Template>
auto MakeTypeSchemaBuilderRegistrar()
{
    return TTypeSchemaBuilderRegistrar(
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
    using TVersion = typename TTraits<TThis>::TVersion;

    TFieldSaveRegistrar(const TThis* this_, TContext& context)
        : This_(this_)
        , Context_(context)
    { }

    template <class TFieldSerializer_>
    TFieldSaveRegistrar(TFieldSaveRegistrar<Member, TThis, TContext, TFieldSerializer_>&& other)
        : This_(other.This_)
        , Context_(other.Context_)
        , VersionFilter_(other.VersionFilter_)
        , MaxVersion_(other.MaxVersion_)
    { }

    auto SinceVersion(auto /*version*/) &&
    {
        return TFieldSaveRegistrar(std::move(*this));
    }

    auto BeforeVersion(TVersion version) &&
    {
        MaxVersion_ = GetPreviousVersion(version);
        return TFieldSaveRegistrar(std::move(*this));
    }

    auto InVersions(TVersionFilter<TThis> filter) &&
    {
        VersionFilter_ = filter;
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
        if (auto version = Context_.GetVersion(); version <= MaxVersion_ && (!VersionFilter_ || VersionFilter_(version))) {
            TFieldSerializer::Save(Context_, This_->*Member);
        }
    }

private:
    template <auto Member_, class TThis_, class TContext_, class TFieldSerializer_>
    friend class TFieldSaveRegistrar;

    const TThis* const This_;
    TContext& Context_;

    TVersionFilter<TThis> VersionFilter_ = nullptr;
    //! Inclusive: the default comparison is then a tautology the optimizer drops.
    TVersion MaxVersion_ = static_cast<TVersion>(std::numeric_limits<int>::max());
};

template <class TThis, class TContext>
class PHOENIX_REGISTRAR_NODISCARD TVirtualFieldSaveRegistrar
{
public:
    using TVersion = typename TTraits<TThis>::TVersion;

    TVirtualFieldSaveRegistrar(
        const TThis* this_,
        TContext& context,
        TFieldSaveHandler<TThis, TContext> saveHandler)
        : This_(this_)
        , Context_(context)
        , SaveHandler_(saveHandler)
    { }

    TVirtualFieldSaveRegistrar(TVirtualFieldSaveRegistrar<TThis, TContext>&& other) noexcept
        : This_(other.This_)
        , Context_(other.Context_)
        , SaveHandler_(other.SaveHandler_)
        , VersionFilter_(other.VersionFilter_)
        , MaxVersion_(other.MaxVersion_)
    { }

    auto SinceVersion(auto /*version*/) &&
    {
        return TVirtualFieldSaveRegistrar(std::move(*this));
    }

    auto BeforeVersion(TVersion version) &&
    {
        MaxVersion_ = GetPreviousVersion(version);
        return TVirtualFieldSaveRegistrar(std::move(*this));
    }

    auto InVersions(TVersionFilter<TThis> filter) &&
    {
        VersionFilter_ = filter;
        return TVirtualFieldSaveRegistrar(std::move(*this));
    }

    auto WhenMissing(auto&& /*handler*/) &&
    {
        return TVirtualFieldSaveRegistrar(std::move(*this));
    }

    void operator()() &&
    {
        if (auto version = Context_.GetVersion(); version <= MaxVersion_ && (!VersionFilter_ || VersionFilter_(version))) {
            SaveHandler_(This_, Context_);
        }
    }

private:
    const TThis* const This_;
    TContext& Context_;
    const TFieldSaveHandler<TThis, TContext> SaveHandler_;

    TVersionFilter<TThis> VersionFilter_ = nullptr;
    //! Inclusive: the default comparison is then a tautology the optimizer drops.
    TVersion MaxVersion_ = static_cast<TVersion>(std::numeric_limits<int>::max());
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

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        auto&& /*name*/,
        auto&& /*loadHandler*/)
    {
        return TDummyFieldRegistrar();
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        auto&& /*name*/,
        auto&& /*loadHandler*/,
        TFieldSaveHandler<TThis, TContext> saveHandler)
    {
        return TVirtualFieldSaveRegistrar<TThis, TContext>(
            This_,
            Context_,
            saveHandler);
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
    TFieldLoadRegistrar(
        TThis* this_,
        TContext& context,
        const char* name)
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
        , MaxVersion_(other.MaxVersion_)
        , VersionFilter_(other.VersionFilter_)
        , MissingHandler_(other.MissingHandler_)
    { }

    using TVersion = typename TTraits<TThis>::TVersion;

    auto SinceVersion(TVersion version) &&
    {
        MinVersion_ = version;
        return TFieldLoadRegistrar(std::move(*this));
    }

    auto BeforeVersion(TVersion version) &&
    {
        MaxVersion_ = GetPreviousVersion(version);
        return TFieldLoadRegistrar(std::move(*this));
    }

    auto WhenMissing(TFieldMissingHandler<TThis, TContext> handler) &&
    {
        MissingHandler_ = handler;
        return TFieldLoadRegistrar(std::move(*this));
    }

    auto InVersions(TVersionFilter<TThis> filter) &&
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
        if (auto version = Context_.GetVersion(); version >= MinVersion_ && version <= MaxVersion_ && (!VersionFilter_ || VersionFilter_(version))) {
            Context_.Dumper().SetFieldName(Name_);
            TFieldSerializer::Load(Context_, This_->*Member);
        } else if (MissingHandler_) {
            MissingHandler_(This_, Context_);
        } else {
            // NB(coteeq): Don't default initialize fields that cannot be default-initialized.
            if constexpr (requires { This_->*Member = {}; }) {
                This_->*Member = {};
            }
        }
    }

private:
    template <auto Member_, class TThis_, class TContext_, class TFieldSerializer_>
    friend class TFieldLoadRegistrar;

    TThis* const This_;
    TContext& Context_;
    const char* const Name_;

    TVersion MinVersion_ = static_cast<TVersion>(std::numeric_limits<int>::min());
    //! Inclusive: the default comparison is then a tautology the optimizer drops.
    TVersion MaxVersion_ = static_cast<TVersion>(std::numeric_limits<int>::max());
    TVersionFilter<TThis> VersionFilter_ = nullptr;
    TFieldMissingHandler<TThis, TContext> MissingHandler_ = nullptr;
};

template <class TThis, class TContext>
class PHOENIX_REGISTRAR_NODISCARD TVirtualFieldLoadRegistrar
{
public:
    TVirtualFieldLoadRegistrar(
        TThis* this_,
        TContext& context,
        const char* name,
        TFieldLoadHandler<TThis, TContext> loadHandler)
        : This_(this_)
        , Context_(context)
        , Name_(name)
        , LoadHandler_(loadHandler)
    { }

    TVirtualFieldLoadRegistrar(TVirtualFieldLoadRegistrar&& other) noexcept
        : This_(other.This_)
        , Context_(other.Context_)
        , Name_(other.Name_)
        , LoadHandler_(other.LoadHandler_)
        , MinVersion_(other.MinVersion_)
        , MaxVersion_(other.MaxVersion_)
        , VersionFilter_(other.VersionFilter_)
        , MissingHandler_(other.MissingHandler_)
    { }

    using TVersion = typename TTraits<TThis>::TVersion;

    auto SinceVersion(TVersion version) &&
    {
        MinVersion_ = version;
        return TVirtualFieldLoadRegistrar(std::move(*this));
    }

    auto BeforeVersion(TVersion version) &&
    {
        MaxVersion_ = GetPreviousVersion(version);
        return TVirtualFieldLoadRegistrar(std::move(*this));
    }

    auto WhenMissing(TFieldMissingHandler<TThis, TContext> handler) &&
    {
        MissingHandler_ = handler;
        return TVirtualFieldLoadRegistrar(std::move(*this));
    }

    using TVersionFilter = bool (*)(TVersion version);

    auto InVersions(TVersionFilter filter) &&
    {
        VersionFilter_ = filter;
        return TVirtualFieldLoadRegistrar(std::move(*this));
    }

    void operator()() &&
    {
        if (auto version = Context_.GetVersion(); version >= MinVersion_ && version <= MaxVersion_ && (!VersionFilter_ || VersionFilter_(version))) {
            Context_.Dumper().SetFieldName(Name_);
            LoadHandler_(This_, Context_);
        } else if (MissingHandler_) {
            MissingHandler_(This_, Context_);
        }
    }

private:
    TThis* const This_;
    TContext& Context_;
    const char* const Name_;
    const TFieldLoadHandler<TThis, TContext> LoadHandler_;

    TVersion MinVersion_ = static_cast<TVersion>(std::numeric_limits<int>::min());
    //! Inclusive: the default comparison is then a tautology the optimizer drops.
    TVersion MaxVersion_ = static_cast<TVersion>(std::numeric_limits<int>::max());
    TVersionFilter VersionFilter_ = nullptr;
    TFieldMissingHandler<TThis, TContext> MissingHandler_ = nullptr;
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

    template <TFieldTag::TUnderlying TagValue, auto Member>
    auto Field(const char* name)
    {
        return TFieldLoadRegistrar<Member, TThis, TContext, TDefaultSerializer>(
            This_,
            Context_,
            name);
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        const char* name,
        TFieldLoadHandler<TThis, TContext> loadHandler)
    {
        return TVirtualFieldLoadRegistrar<TThis, TContext>(
            This_,
            Context_,
            name,
            loadHandler);
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        const char* name,
        TFieldLoadHandler<TThis, TContext> loadHandler,
        auto&& /*saveHandler*/)
    {
        return VirtualField<TagValue>(name, loadHandler);
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
const TRuntimeTypeLoadSchedule<TThis, TContext>* FindRuntimeTypeLoadSchedule(TContext& context);

Y_NO_INLINE void CompatLoadImpl(auto* this_, auto& context, const auto& schedule);

template <class TThis, class TContext>
void LoadImpl(TThis* this_, TContext& context)
{
    static_assert(
        std::derived_from<TContext, TLoadContext>,
        "Phoenix types must be loaded via NPhoenix::TLoadContext or its descendants");

    RunRegistrar<TThis>(TLoadBaseTypesRegistrar(this_, context));
    if (const auto* runtimeSchedule = FindRuntimeTypeLoadSchedule<TThis>(context)) [[unlikely]] {
        CompatLoadImpl(this_, context, *runtimeSchedule);
    } else {
        RunRegistrar<TThis>(TLoadFieldsRegistrar<TThis, TContext>(this_, context));
    }
    RunRegistrar<TThis>(TAfterLoadRegistrar(this_, context));
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TContext>
struct TRuntimeFieldDescriptor
{
    TScheduledFieldLoadHandler<TThis, TContext> LoadHandler;
    TFieldMissingHandler<TThis, TContext> MissingHandler = nullptr;
};

template <auto Member, class TThis, class TContext, class TFieldSerializer>
class PHOENIX_REGISTRAR_NODISCARD TRuntimeFieldDescriptorBuilderRegistrar
{
public:
    using TRuntimeFieldDescriptor = NPhoenix::NDetail::TRuntimeFieldDescriptor<TThis, TContext>;

    explicit TRuntimeFieldDescriptorBuilderRegistrar(TRuntimeFieldDescriptor* descriptor)
        : Descriptor_(descriptor)
    { }

    template <class TFieldSerializer_>
    TRuntimeFieldDescriptorBuilderRegistrar(TRuntimeFieldDescriptorBuilderRegistrar<Member, TThis, TContext, TFieldSerializer_>&& other)
        : Descriptor_(other.Descriptor_)
    { }

    auto SinceVersion(auto /*version*/) &&
    {
        return std::move(*this);
    }

    auto BeforeVersion(auto /*version*/) &&
    {
        return std::move(*this);
    }

    auto InVersions(auto /*filter*/) &&
    {
        return std::move(*this);
    }

    auto WhenMissing(TFieldMissingHandler<TThis, TContext> handler) &&
    {
        Descriptor_->MissingHandler = handler;
        return std::move(*this);
    }

    template <class TFieldSerializer_>
    auto Serializer() &&
    {
        return TRuntimeFieldDescriptorBuilderRegistrar<Member, TThis, TContext, TFieldSerializer_>(std::move(*this));
    }

    void operator()() &&
    {
        Descriptor_->LoadHandler = [] (TThis* this_, TContext& context) {
            TFieldSerializer::Load(context, this_->*Member);
        };
    }

private:
    template <auto Member_, class TThis_, class TContext_, class TFieldSerializer_>
    friend class TRuntimeFieldDescriptorBuilderRegistrar;

    TRuntimeFieldDescriptor* const Descriptor_;
};

template <class TThis, class TContext>
class TRuntimeVirtualFieldDescriptorBuilderRegistrar
{
public:
    using TRuntimeFieldDescriptor = NPhoenix::NDetail::TRuntimeFieldDescriptor<TThis, TContext>;

    TRuntimeVirtualFieldDescriptorBuilderRegistrar(TRuntimeFieldDescriptor* descriptor)
        : Descriptor_(descriptor)
    { }

    using TVersion = typename TTraits<TThis>::TVersion;

    auto SinceVersion(TVersion version) &&
    {
        MinVersion_ = version;
        return *this;
    }

    auto BeforeVersion(TVersion version) &&
    {
        BeforeVersion_ = version;
        return *this;
    }

    auto InVersions(TVersionFilter<TThis> filter) &&
    {
        VersionFilter_ = filter;
        return *this;
    }

    auto WhenMissing(TFieldMissingHandler<TThis, TContext> handler) &&
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
    {
        bool haveSimpleFilter =
            MinVersion_ != static_cast<TVersion>(std::numeric_limits<int>::min()) ||
            BeforeVersion_ != static_cast<TVersion>(std::numeric_limits<int>::max());

        YT_VERIFY(
            !haveSimpleFilter || !VersionFilter_,
            "Cannot specify SinceVersion/BeforeVersion and InVersions at the same time");

        Descriptor_->LoadHandler = [
            since = MinVersion_,
            before = BeforeVersion_,
            filter = VersionFilter_,
            underlyingHandler = Descriptor_->LoadHandler
        ] (TThis* this_, TContext& context) {
            auto version = context.GetVersion();
            if (since <= version && version < before && (!filter || filter(version))) {
                underlyingHandler(this_, context);
            }
        };
    }

private:
    TRuntimeFieldDescriptor* const Descriptor_;
    TVersion MinVersion_ = static_cast<TVersion>(std::numeric_limits<int>::min());
    TVersion BeforeVersion_ = static_cast<TVersion>(std::numeric_limits<int>::max());
    TVersionFilter<TThis> VersionFilter_ = nullptr;
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
            // NB(coteeq): Don't default initialize fields that cannot be default-initialized.
            if constexpr (requires { this_->*Member = {}; }) {
                this_->*Member = {};
            }
        };
        return TRuntimeFieldDescriptorBuilderRegistrar<Member, TThis, TContext, TDefaultSerializer>(descriptor);
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        auto&& /*name*/,
        TFieldLoadHandler<TThis, TContext> loadHandler)
    {
        auto* descriptor = AddField<TagValue>();
        descriptor->LoadHandler = loadHandler;
        return TRuntimeVirtualFieldDescriptorBuilderRegistrar<TThis, TContext>(descriptor);
    }

    template <TFieldTag::TUnderlying TagValue>
    auto VirtualField(
        auto&& name,
        TFieldLoadHandler<TThis, TContext> loadHandler,
        auto&& /*saveHandler*/)
    {
        return VirtualField<TagValue>(name, loadHandler);
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

struct TTypeLoadSchedule
{
    std::vector<TFieldTag> LoadFieldTags;
    std::vector<TFieldTag> MissingFieldTags;
};

struct TRuntimeTypeLoadScheduleBase
{
    virtual ~TRuntimeTypeLoadScheduleBase() = default;
};

template <class TThis, class TContext>
struct TRuntimeTypeLoadSchedule
    : public TRuntimeTypeLoadScheduleBase
{
    std::vector<TScheduledFieldLoadHandler<TThis, TContext>> LoadFieldHandlers;
    std::vector<TFieldMissingHandler<TThis, TContext>> MissingFieldHandlers;
};

int AllocateRuntimeTypeLoadScheduleIndex();

template <class TThis, class TContext>
int GetRuntimeTypeLoadScheduleIndex()
{
    static const int Index = AllocateRuntimeTypeLoadScheduleIndex();
    return Index;
}

struct TUniverseLoadSchedule
{
    struct TRuntimeTypeLoadScheduleSlot
    {
        bool Initialized = false;
        std::unique_ptr<TRuntimeTypeLoadScheduleBase> Schedule;
    };

    THashMap<TTypeTag, TTypeLoadSchedule> LoadScheduleMap;
    //! Indexed by #GetRuntimeTypeLoadScheduleIndex.
    std::vector<TRuntimeTypeLoadScheduleSlot> RuntimeTypeLoadScheduleSlots;

    const TTypeLoadSchedule* FindTypeLoadSchedule(TTypeTag tag);

    template <class TThis, class TContext>
    const TRuntimeTypeLoadSchedule<TThis, TContext>* FindRuntimeTypeLoadSchedule();
};

//! Returns null if no type needs compat loading.
std::unique_ptr<TUniverseLoadSchedule> ComputeUniverseLoadSchedule(const TUniverseSchemaPtr& loadUniverseSchema);

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
    auto index = GetRuntimeTypeLoadScheduleIndex<TThis, TContext>();
    if (index >= std::ssize(RuntimeTypeLoadScheduleSlots)) {
        RuntimeTypeLoadScheduleSlots.resize(index + 1);
    }

    auto& slot = RuntimeTypeLoadScheduleSlots[index];
    if (!slot.Initialized) {
        slot.Schedule = BuildRuntimeTypeLoadSchedule<TThis, TContext>(FindTypeLoadSchedule(TThis::TypeTag));
        slot.Initialized = true;
    }

    return static_cast<const TRuntimeTypeLoadSchedule<TThis, TContext>*>(slot.Schedule.get());
}

template <class TThis, class TContext>
const TRuntimeTypeLoadSchedule<TThis, TContext>* FindRuntimeTypeLoadSchedule(TContext& context)
{
    auto* schedule = context.GetLoadSchedule();
    if (!schedule) [[likely]] {
        return nullptr;
    }

    return schedule->template FindRuntimeTypeLoadSchedule<TThis, TContext>();
}

Y_NO_INLINE void CompatLoadImpl(auto* this_, auto& context, const auto& runtimeSchedule)
{
    for (const auto& handler : runtimeSchedule.LoadFieldHandlers) {
        handler(this_, context);
    }
    for (const auto& handler : runtimeSchedule.MissingFieldHandlers) {
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
    static void Save(C& context, const TWeakPtr<T>& ptr)
    {
        SaveImpl(context, ptr.Lock().Get());
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
        LoadImpl</*Inplace*/ false, /*UseRefCountedConstructor*/ std::is_final_v<T>>(context, rawPtr);
        ptr.Reset(rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, const TIntrusivePtr<T>& ptr)
    {
        T* rawPtr = ptr.Get();
        LoadImpl</*Inplace*/ true, /*UseRefCountedConstructor*/ false>(context, rawPtr);
    }

    template <class T, class C>
    static void Load(C& context, std::unique_ptr<T>& ptr)
    {
        T* rawPtr = nullptr;
        LoadImpl</*Inplace*/ false, /*UseRefCountedConstructor*/ false>(context, rawPtr);
        ptr.reset(rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, const std::unique_ptr<T>& ptr)
    {
        T* rawPtr = ptr.get();
        LoadImpl</*Inplace*/ true, /*UseRefCountedConstructor*/ false>(context, rawPtr);
    }

    template <class T, class C>
    static void Load(C& context, T*& rawPtr)
    {
        rawPtr = nullptr;
        LoadImpl</*Inplace*/ false, /*UseRefCountedConstructor*/ false>(context, rawPtr);
    }

    template <class T, class C>
    static void InplaceLoad(C& context, T* rawPtr)
    {
        LoadImpl</*Inplace*/ true, /*UseRefCountedConstructor*/ false>(context, rawPtr);
    }

    template <class T, class C>
    static void Load(C& context, TWeakPtr<T>& ptr)
    {
        T* rawPtr = nullptr;
        LoadImpl</*Inplace*/ false, /*UseRefCountedConstructor*/ std::is_final_v<T>>(context, rawPtr);
        ptr.Reset(rawPtr);
    }

    template <bool Inplace, bool UseRefCountedConstructor, class T, class C>
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
                } else if constexpr (UseRefCountedConstructor) {
                    rawPtr = static_cast<T*>(TRefCountedFactory<T>::ConcreteConstructor());
                } else {
                    using TFactory = typename TFactoryTraits<T>::TFactory;
                    static_assert(TFactory::ConcreteConstructor);
                    rawPtr = static_cast<T*>(TFactory::ConcreteConstructor());
                }
                if constexpr (std::derived_from<T, TRefCounted> || UseRefCountedConstructor) {
                    context.Deleters_.push_back([=] { Unref(rawPtr); });
                }
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

} // namespace NYT::NPhoenix::NDetail

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
    requires (std::derived_from<C, NPhoenix::NDetail::TContextBase>) && (
        std::same_as<T, TIntrusivePtr<typename T::TUnderlying>> ||
        std::same_as<T, std::unique_ptr<typename T::element_type>> ||
        std::is_pointer_v<T> ||
        std::same_as<T, TWeakPtr<typename T::TUnderlying>>)
struct TSerializerTraits<T, C>
{
    using TSerializer = NPhoenix::NDetail::TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
