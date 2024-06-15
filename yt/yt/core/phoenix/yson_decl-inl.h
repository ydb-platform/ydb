#ifndef YSON_DECL_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_decl.h"
// For the sake of sane code completion.
#include "yson_decl.h"
#endif

#include <library/cpp/yt/yson/public.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

#undef PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TThis>
class TYsonSerializeBaseTypesRegistrar;

} // namespace NDetail

#define PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN__PROLOGUE(type) \
private: \
    template <class TThis> \
    friend class ::NYT::NPhoenix2::NDetail::TYsonSerializeBaseTypesRegistrar

#define PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN(type) \
    PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN__PROLOGUE(type); \
public: \
    friend void Serialize(const type& obj, ::NYT::NYson::IYsonConsumer* consumer); \
    \
private: \
    void YsonSerializeFields(::NYT::NYson::IYsonConsumer* consumer) const

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
