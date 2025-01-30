#include "type_def.h"

namespace NYT::NPhoenix::NDetail {

////////////////////////////////////////////////////////////////////////////////

TTypeSchemaBuilderRegistar::TTypeSchemaBuilderRegistar(
    std::vector<const std::type_info*> typeInfos,
    TTypeTag tag,
    bool isTemplate,
    TPolymorphicConstructor polymorphicConstructor,
    TConcreteConstructor concreteConstructor)
{
    TypeDescriptor_->Name_ = CppDemangle(typeInfos[0]->name());
    TypeDescriptor_->TypeInfos_ = std::move(typeInfos);
    TypeDescriptor_->Tag_ = tag;
    TypeDescriptor_->Template_ = isTemplate;
    TypeDescriptor_->PolymorphicConstructor_ = polymorphicConstructor;
    TypeDescriptor_->ConcreteConstructor_ = concreteConstructor;
}

const TTypeDescriptor& TTypeSchemaBuilderRegistar::operator()() &&
{
    const auto& result = *TypeDescriptor_;
    ::NYT::NPhoenix::ITypeRegistry::Get()->RegisterTypeDescriptor(std::move(TypeDescriptor_));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

NConcurrency::TFlsSlot<TUniverseLoadState> UniverseLoadState;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix::NDetail
