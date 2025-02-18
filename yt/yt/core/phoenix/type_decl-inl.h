#ifndef TYPE_DECL_INL_H_
#error "Direct inclusion of this file is not allowed, include type_decl.h"
// For the sake of sane code completion.
#include "type_decl.h"
#endif

namespace NYT::NPhoenix::NDetail {

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
    [[maybe_unused]] static constexpr auto TypeTag = ::NYT::NPhoenix::TTypeTag(typeTagValue); \
    static void RegisterMetadata(auto&& registrar); \
    \
private: \
    using TThis = type; \
    using TLoadContextImpl = TLoadContext; \
    \
    template <class TThis, class TContext> \
    friend std::unique_ptr<::NYT::NPhoenix::NDetail::TRuntimeTypeLoadSchedule<TThis, TContext>> NYT::NPhoenix::NDetail::BuildRuntimeTypeLoadSchedule( \
        const ::NYT::NPhoenix::NDetail::TTypeLoadSchedule* schedule); \
    template <class TThis> \
    friend struct ::NYT::NPhoenix::NDetail::TTraits

#define PHOENIX_DECLARE_TYPE__IMPL(type, typeTagValue, saveLoadModifier) \
    PHOENIX_DECLARE_TYPE__PROLOGUE(type, typeTagValue); \
public: \
    static const ::NYT::NPhoenix::TTypeDescriptor& GetTypeDescriptor(); \
    void Save(TSaveContext& context) const saveLoadModifier; \
    void Load(TLoadContext& context) saveLoadModifier; \
    \
private: \
    static const ::NYT::NPhoenix::NDetail::TRuntimeFieldDescriptorMap<type, TLoadContext>& GetRuntimeFieldDescriptorMap()

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
    static const ::NYT::NPhoenix::TTypeDescriptor& GetTypeDescriptor() \
    { \
        static const auto& descriptor = ::NYT::NPhoenix::ITypeRegistry::Get()->GetUniverseDescriptor().GetTypeDescriptorByTag(TypeTag); \
        return descriptor; \
    } \
    \
    static const ::NYT::NPhoenix::NDetail::TRuntimeFieldDescriptorMap<type, TLoadContext>& GetRuntimeFieldDescriptorMap() \
    { \
        static const auto map = ::NYT::NPhoenix::NDetail::BuildRuntimeFieldDescriptorMap<TThis, TLoadContext>(); \
        return map; \
    } \
    \
    void Save(TSaveContext& context) const saveLoadModifier \
    { \
        ::NYT::NPhoenix::NDetail::SaveImpl(this, context); \
    } \
    \
    void Load(TLoadContext& context) saveLoadModifier \
    { \
        ::NYT::NPhoenix::NDetail::LoadImpl(this, context); \
    }

#define PHOENIX_DECLARE_TEMPLATE_TYPE(type, typeTag) \
    PHOENIX_DECLARE_TEMPLATE_TYPE__IMPL(type, typeTag, )

#define PHOENIX_DECLARE_POLYMORPHIC_TEMPLATE_TYPE(type, typeTag) \
    PHOENIX_DECLARE_TEMPLATE_TYPE__IMPL(type, typeTag, override)

#define PHOENIX_DECLARE_OPAQUE_TYPE(type, typeTagValue) \
public: \
    [[maybe_unused]] static constexpr auto TypeTag = ::NYT::NPhoenix::TTypeTag(typeTagValue); \
    static const ::NYT::NPhoenix::TTypeDescriptor& GetTypeDescriptor()

#define PHOENIX_INHERIT_POLYMORPHIC_TYPE(baseType, type, tag) \
class type                                                    \
    : public baseType                                         \
{                                                             \
public:                                                       \
    using baseType::baseType;                                 \
                                                              \
private:                                                      \
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(type, tag);              \
}

#define PHOENIX_INHERIT_POLYMORPHIC_TEMPLATE_TYPE(baseType, type, tag, ...) \
class type                                                                  \
    : public baseType<__VA_ARGS__>                                          \
{                                                                           \
public:                                                                     \
    using baseType::baseType;                                               \
                                                                            \
private:                                                                    \
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(type, tag);                            \
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix::NDetail
