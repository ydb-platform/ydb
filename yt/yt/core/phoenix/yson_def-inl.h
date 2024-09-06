#ifndef YSON_DEF_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_def.h"
// For the sake of sane code completion.
#include "yson_def.h"
#endif

#include "type_def.h"

#include <yt/yt/core/ytree/serialize.h>

#include <library/cpp/yt/yson/consumer.h>

////////////////////////////////////////////////////////////////////////////////

#undef PHOENIX_DEFINE_YSON_DUMPABLE_TYPE_MIXIN
#undef PHOENIX_DECLARE_YSON_DUMPABLE_TEMPLATE_MIXIN

////////////////////////////////////////////////////////////////////////////////

#define PHOENIX_DEFINE_YSON_DUMPABLE_TYPE_MIXIN(type) \
    [[maybe_unused]] void Serialize(const type& obj, ::NYT::NYson::IYsonConsumer* consumer) \
    { \
        consumer->OnBeginMap(); \
        obj.YsonSerializeFields(consumer); \
        consumer->OnEndMap(); \
    } \
    \
    void type::YsonSerializeFields(::NYT::NYson::IYsonConsumer* consumer) const \
    { \
        ::NYT::NPhoenix2::NDetail::YsonSerializeFieldsImpl(this, consumer); \
    }

#define PHOENIX_DECLARE_YSON_DUMPABLE_TEMPLATE_MIXIN(type) \
    PHOENIX_DECLARE_YSON_DUMPABLE_MIXIN__PROLOGUE(type); \
public: \
    friend void Serialize(const type& obj, ::NYT::NYson::IYsonConsumer* consumer) \
    { \
        consumer->OnBeginMap(); \
        obj.YsonSerializeFields(consumer); \
        consumer->OnEndMap(); \
    } \
    \
private: \
    void YsonSerializeFields(::NYT::NYson::IYsonConsumer* consumer) const \
    { \
        ::NYT::NPhoenix2::NDetail::YsonSerializeFieldsImpl(this, consumer); \
    }

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NPhoenix2::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TThis>
class TYsonSerializeBaseTypesRegistrar
    : public TTypeRegistrarBase
{
public:
    TYsonSerializeBaseTypesRegistrar(const TThis* this_, NYson::IYsonConsumer* consumer)
        : This_(this_)
        , Consumer_(consumer)
    { }

    template <class TBase>
    void BaseType()
    {
        static_cast<const TBase*>(This_)->YsonSerializeFields(Consumer_);
    }

private:
    const TThis* const This_;
    NYson::IYsonConsumer* const Consumer_;
};

template <class TThis>
class TYsonSerializeFieldsRegistrar
    : public TTypeRegistrarBase
{
public:
    TYsonSerializeFieldsRegistrar(const TThis* this_, NYson::IYsonConsumer* consumer)
        : This_(this_)
        , Consumer_(consumer)
    { }

    template <TFieldTag::TUnderlying TagValue, auto Member>
    TDummyFieldRegistrar Field(TStringBuf name)
    {
        Consumer_->OnKeyedItem(name);
        using NYTree::Serialize;
        Serialize(This_->*Member, Consumer_);
        return {};
    }

private:
    const TThis* const This_;
    NYson::IYsonConsumer* const Consumer_;
};

template <class TThis>
void YsonSerializeFieldsImpl(const TThis* this_, NYson::IYsonConsumer* consumer)
{
    RunRegistrar<TThis>(TYsonSerializeBaseTypesRegistrar(this_, consumer));
    RunRegistrar<TThis>(TYsonSerializeFieldsRegistrar(this_, consumer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2::NDetail
