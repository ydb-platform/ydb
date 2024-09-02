#pragma once

#include "public.h"

#include <library/cpp/yt/yson_string/string.h>

#include <util/generic/hash.h>

#include <mutex>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Forward declaration of friends.
class TTypeRegistry;
class TTypeSchemaBuilderRegistar;
class TFieldSchemaRegistrar;
class TTypeSchemaBuilderRegistar;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TFieldDescriptor
{
public:
    const TString& GetName() const;
    TFieldTag GetTag() const;

    const TFieldSchemaPtr& GetSchema() const;

private:
    friend class NDetail::TTypeSchemaBuilderRegistar;
    friend class NDetail::TFieldSchemaRegistrar;

    TString Name_;
    TFieldTag Tag_;
    int MinVersion_ = std::numeric_limits<int>::min();
    int MaxVersion_ = std::numeric_limits<int>::max();

    mutable std::once_flag SchemaOnceFlag_;
    mutable TFieldSchemaPtr Schema_;
};

class TTypeDescriptor
{
public:
    const TString& GetName() const;
    TTypeTag GetTag() const;
    const std::vector<std::unique_ptr<TFieldDescriptor>>& Fields() const;
    const std::vector<TTypeTag>& BaseTypeTags() const;
    bool IsTemplate() const;

    const TTypeSchemaPtr& GetSchema() const;
    const NYson::TYsonString& GetSchemaYson() const;

    template <class T>
    T* TryConstruct() const;
    template <class T>
    T* ConstructOrThrow() const;

private:
    friend class NDetail::TTypeRegistry;
    friend class NDetail::TTypeSchemaBuilderRegistar;

    TString Name_;
    std::vector<const std::type_info*> TypeInfos_;
    TTypeTag Tag_;
    std::vector<std::unique_ptr<TFieldDescriptor>> Fields_;
    std::vector<TTypeTag> BaseTypeTags_;
    bool Template_ = false;
    TPolymorphicConstructor PolymorphicConstructor_ = nullptr;
    TConcreteConstructor ConcreteConstructor_ = nullptr;

    mutable std::once_flag SchemaOnceFlag_;
    mutable TTypeSchemaPtr Schema_;

    mutable std::once_flag SchemaYsonOnceFlag_;
    mutable NYson::TYsonString SchemaYson_;
};

class TUniverseDescriptor
{
public:
    const TUniverseSchemaPtr& GetSchema() const;
    const NYson::TYsonString& GetSchemaYson() const;

    const TTypeDescriptor* FindTypeDescriptorByTag(TTypeTag tag) const ;
    const TTypeDescriptor& GetTypeDescriptorByTag(TTypeTag tag) const;
    const TTypeDescriptor& GetTypeDescriptorByTagOrThrow(TTypeTag tag) const;

    const TTypeDescriptor* FindTypeDescriptorByTypeIndex(std::type_index typeIndex) const ;
    const TTypeDescriptor& GetTypeDescriptorByTypeIndex(std::type_index typeIndex) const;
    const TTypeDescriptor& GetTypeDescriptorByTypeIndexOrThrow(std::type_index typeIndex) const;

private:
    friend class NDetail::TTypeRegistry;

    THashMap<TTypeTag, std::unique_ptr<TTypeDescriptor>> TypeTagToDescriptor_;
    THashMap<std::type_index, const TTypeDescriptor*> TypeIndexToDescriptor_;

    mutable std::once_flag SchemaOnceFlag_;
    mutable TUniverseSchemaPtr Schema_;

    mutable std::once_flag SchemaYsonOnceFlag_;
    mutable NYson::TYsonString SchemaYson_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

#define DESCRIPTORS_INL_H_
#include "descriptors-inl.h"
#undef DESCRIPTORS_INL_H_
