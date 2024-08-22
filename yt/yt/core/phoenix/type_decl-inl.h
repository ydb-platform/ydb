#ifndef TYPE_DECL_INL_H_
#error "Direct inclusion of this file is not allowed, include type_decl.h"
// For the sake of sane code completion.
#include "type_decl.h"
#endif

namespace NYT::NPhoenix2::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TTypeLoadSchedule;

template <class TThis, class TContext>
struct TRuntimeTypeLoadSchedule;

template <class TThis, class TContext>
struct TRuntimeFieldDescriptor;

template <auto Member, class TThis, class TContext, class TFieldSerializer>
class TFieldLoadRegistrar;

template <class TThis, class TContext>
using TRuntimeFieldDescriptorMap = THashMap<TFieldTag, TRuntimeFieldDescriptor<TThis, TContext>>;

template <class TThis, class TContext>
std::unique_ptr<TRuntimeTypeLoadSchedule<TThis, TContext>> BuildRuntimeTypeLoadSchedule(const TTypeLoadSchedule* schedule);

template <class TThis>
struct TTraits;

////////////////////////////////////////////////////////////////////////////////

#undef PHOENIX_DECLARE_TYPE
#undef PHOENIX_DECLARE_POLYMORPHIC_TYPE
#undef PHOENIX_DECLARE_TEMPLATE_TYPE
#undef PHOENIX_DECLARE_POLYMORPHIC_TEMPLATE_TYPE
#undef PHOENIX_DECLARE_OPAQUE_TYPE

////////////////////////////////////////////////////////////////////////////////

#define PHOENIX_DECLARE_TYPE__PROLOGUE(type, typeTagValue) \
public: \
    [[maybe_unused]] static constexpr auto TypeTag = ::NYT::NPhoenix2::TTypeTag(typeTagValue); \
    static void RegisterMetadata(auto&& registrar); \
    \
private: \
    using TThis = type; \
    using TLoadContextImpl = TLoadContext; \
    \
    template <class TThis, class TContext> \
    friend std::unique_ptr<::NYT::NPhoenix2::NDetail::TRuntimeTypeLoadSchedule<TThis, TContext>> NYT::NPhoenix2::NDetail::BuildRuntimeTypeLoadSchedule( \
        const ::NYT::NPhoenix2::NDetail::TTypeLoadSchedule* schedule); \
    template <class TThis> \
    friend struct ::NYT::NPhoenix2::NDetail::TTraits

#define PHOENIX_DECLARE_TYPE__IMPL(type, typeTagValue, saveLoadModifier) \
    PHOENIX_DECLARE_TYPE__PROLOGUE(type, typeTagValue); \
public: \
    static const ::NYT::NPhoenix2::TTypeDescriptor& GetTypeDescriptor(); \
    void SaveImpl(TSaveContext& context) const saveLoadModifier; \
    void LoadImpl(TLoadContext& context) saveLoadModifier; \
    void Save(TSaveContext& context) const saveLoadModifier; \
    void Load(TLoadContext& context) saveLoadModifier; \
    void Persist(const TPersistenceContext& context) saveLoadModifier; \
    using TPhoenix2SupportTag = type; \
    \
private: \
    static const ::NYT::NPhoenix2::NDetail::TRuntimeFieldDescriptorMap<type, TLoadContext>& GetRuntimeFieldDescriptorMap()

#define PHOENIX_DECLARE_FRIEND() \
    template <class T> \
    friend struct TPhoenixTypeInitializer__;

#define PHOENIX_DECLARE_TYPE(type, typeTag) \
    PHOENIX_DECLARE_TYPE__IMPL(type, typeTag, )

#define PHOENIX_DECLARE_POLYMORPHIC_TYPE(type, typeTag) \
    PHOENIX_DECLARE_TYPE__IMPL(type, typeTag, override)

#define PHOENIX_DECLARE_TEMPLATE_TYPE__IMPL(type, typeTag, saveLoadModifier) \
    PHOENIX_DECLARE_TYPE__PROLOGUE(type, typeTag); \
    \
public: \
    static const ::NYT::NPhoenix2::TTypeDescriptor& GetTypeDescriptor() \
    { \
        static const auto& descriptor = ::NYT::NPhoenix2::NDetail::RegisterTypeDescriptorImpl<type, /*Template*/ true>(); \
        return descriptor; \
    } \
    \
    static const ::NYT::NPhoenix2::NDetail::TRuntimeFieldDescriptorMap<type, TLoadContext>& GetRuntimeFieldDescriptorMap() \
    { \
        static const auto map = ::NYT::NPhoenix2::NDetail::BuildRuntimeFieldDescriptorMap<TThis, TLoadContext>(); \
        return map; \
    } \
    \
    void SaveImpl(TSaveContext& context) const saveLoadModifier \
    { \
        ::NYT::NPhoenix2::NDetail::SaveImpl(this, context); \
    } \
    \
    void LoadImpl(TLoadContext& context) saveLoadModifier \
    { \
        ::NYT::NPhoenix2::NDetail::LoadImpl(this, context); \
    } \
    \
    void Save(TSaveContext& context) const saveLoadModifier \
    { \
        const_cast<type*>(this)->Persist(context); \
    } \
    \
    void Load(TLoadContext& context) saveLoadModifier \
    { \
        Persist(context); \
    } \
    \
    void Persist(const TPersistenceContext& context) saveLoadModifier \
    { \
        if (context.IsSave()) { \
            type::SaveImpl(context.SaveContext()); \
        } else { \
            YT_VERIFY(context.IsLoad()); \
            type::LoadImpl(context.LoadContext()); \
        } \
    }

#define PHOENIX_DECLARE_TEMPLATE_TYPE(type, typeTag) \
    PHOENIX_DECLARE_TEMPLATE_TYPE__IMPL(type, typeTag, )

#define PHOENIX_DECLARE_POLYMORPHIC_TEMPLATE_TYPE(type, typeTag) \
    PHOENIX_DECLARE_TEMPLATE_TYPE__IMPL(type, typeTag, override)

#define PHOENIX_DECLARE_OPAQUE_TYPE(type, typeTagValue) \
public: \
    [[maybe_unused]] static constexpr auto TypeTag = ::NYT::NPhoenix2::TTypeTag(typeTagValue); \
    static const ::NYT::NPhoenix2::TTypeDescriptor& GetTypeDescriptor()

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2::NDetail
