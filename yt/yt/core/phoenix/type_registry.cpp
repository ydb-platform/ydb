#include "type_registry.h"

#include "private.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <util/generic/hash_set.h>

#include <util/system/type_name.h>

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constinit const auto Logger = PhoenixLogger;

class TTypeRegistry
    : public ITypeRegistry
{
public:
    void RegisterTypeDescriptor(std::unique_ptr<TTypeDescriptor> typeDescriptor) override
    {
        YT_TLOG_FATAL_IF(Sealed_.load(), "Cannot register type descriptor when registry is already sealed");

        YT_TLOG_FATAL_IF(typeDescriptor->GetTag() == TTypeTag(), "Invalid type tag")
            .WithFormat("TypeTag", "%x", typeDescriptor->GetTag())
            .With("TypeName", typeDescriptor->GetName());

        if (auto it = UniverseDescriptor_.TypeTagToDescriptor_.find(typeDescriptor->GetTag())) {
            YT_TLOG_FATAL("Duplicate type tag")
                .WithFormat("TypeTag", "%x", typeDescriptor->GetTag())
                .With("NewTypeName", typeDescriptor->GetName())
                .With("OldTypeName", it->second->GetName());
        }

        THashSet<TFieldTag> fieldTags;
        for (const auto& fieldDescriptor : typeDescriptor->Fields()) {
            YT_TLOG_FATAL_IF(fieldDescriptor->GetTag() == TFieldTag(), "Invalid field tag")
                .With("TypeName", typeDescriptor->GetName())
                .With("FieldTag", fieldDescriptor->GetTag())
                .With("FieldName", fieldDescriptor->GetName());

            if (!fieldTags.insert(fieldDescriptor->GetTag()).second) {
                YT_TLOG_FATAL("Duplicate field tag")
                    .With("TypeName", typeDescriptor->GetName())
                    .With("FieldTag", fieldDescriptor->GetTag())
                    .With("FieldName", fieldDescriptor->GetName());
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

        YT_TLOG_DEBUG("Type registered")
            .With("TypeName", typeDescriptor->GetName())
            .WithFormat("TypeTag", "%x", typeDescriptor->GetTag());

        EmplaceOrCrash(
            UniverseDescriptor_.TypeTagToDescriptor_,
            typeDescriptor->GetTag(),
            std::move(typeDescriptor));
    }

    const TUniverseDescriptor& GetUniverseDescriptor() override
    {
        if (!Sealed_.exchange(true)) {
            YT_TLOG_INFO("Type registry is sealed");
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

} // namespace NYT::NPhoenix
