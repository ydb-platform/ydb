#include "type_registry.h"

#include "private.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/generic/hash_set.h>

#include <util/system/type_name.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

static constexpr auto& Logger = PhoenixLogger;

class TTypeRegistry
    : public ITypeRegistry
{
public:
    void RegisterTypeDescriptor(std::unique_ptr<TTypeDescriptor> typeDescriptor) override
    {
        YT_LOG_FATAL_IF(
            Sealed_.load(),
            "Cannot register type descriptor when registry is already sealed");

        YT_LOG_FATAL_IF(
            typeDescriptor->GetTag() == TTypeTag(),
            "Invalid type tag (TypeTag: %x, TypeName: %v)",
            typeDescriptor->GetTag(),
            typeDescriptor->GetName());

        if (auto it = UniverseDescriptor_.TypeTagToDescriptor_.find(typeDescriptor->GetTag())) {
            YT_LOG_FATAL("Duplicate type tag (TypeTag: %x, NewTypeName: %v, OldTypeName: %v)",
                typeDescriptor->GetTag(),
                typeDescriptor->GetName(),
                it->second->GetName());
        }

        THashSet<TFieldTag> fieldTags;
        for (const auto& fieldDescriptor : typeDescriptor->Fields()) {
            YT_LOG_FATAL_IF(
                fieldDescriptor->GetTag() == TFieldTag(),
                "Invalid field tag (TypeName: %v, FieldTag: %v, FieldName: %v)",
                typeDescriptor->GetName(),
                fieldDescriptor->GetTag(),
                fieldDescriptor->GetName());

            if (!fieldTags.insert(fieldDescriptor->GetTag()).second) {
                YT_LOG_FATAL("Duplicate field tag (TypeName: %v, FieldTag: %v, FieldName: %v)",
                    typeDescriptor->GetName(),
                    fieldDescriptor->GetTag(),
                    fieldDescriptor->GetName());
            }
        }

        if (!typeDescriptor->IsTemplate()) {
            for (const auto* typeInfo : typeDescriptor->TypeInfos_) {
                EmplaceOrCrash(
                    UniverseDescriptor_.TypeIndexToDescriptor_,
                    std::type_index(*typeInfo),
                    typeDescriptor.get());
            }
        }

        YT_LOG_DEBUG("Type registered (TypeName: %v, TypeTag: %x)",
            typeDescriptor->GetName(),
            typeDescriptor->GetTag());

        EmplaceOrCrash(
            UniverseDescriptor_.TypeTagToDescriptor_,
            typeDescriptor->GetTag(),
            std::move(typeDescriptor));
    }

    const TUniverseDescriptor& GetUniverseDescriptor() override
    {
        if (!Sealed_.exchange(true)) {
            YT_LOG_INFO("Type registry is sealed");
        }
        return UniverseDescriptor_;
    }

private:
    TUniverseDescriptor UniverseDescriptor_;
    std::atomic<bool> Sealed_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

ITypeRegistry* ITypeRegistry::Get()
{
    return LeakySingleton<NDetail::TTypeRegistry>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

