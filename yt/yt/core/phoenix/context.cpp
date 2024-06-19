#include "context.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(IZeroCopyOutput* output, int version)
    : TStreamSaveContext(output, version)
{
    // Zero id is reserved for nullptr.
    ObjectIdGenerator_.Next();
}

TObjectId TSaveContext::GenerateObjectId(void* basePtr, std::optional<std::type_index> typeIndex)
{
    TObjectEntry entry{
        .Id = static_cast<TObjectId>(ObjectIdGenerator_.Next()),
        .TypeIndex = typeIndex,
    };
    EmplaceOrCrash(PtrToObjectEntry_, basePtr, entry);
    return entry.Id;
}

TObjectId TSaveContext::FindObjectId(void* basePtr, std::optional<std::type_index> typeIndex) const
{
    auto it = PtrToObjectEntry_.find(basePtr);
    if (it == PtrToObjectEntry_.end()) {
        return NullObjectId;
    }

    const auto& entry = it->second;
    // Failure here means an attempt was made to serialize a polymorphic type
    // not inheriting from TPolymorphicBase.
    YT_VERIFY(entry.TypeIndex == typeIndex);
    return entry.Id;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::~TLoadContext()
{
    for (const auto& deletor : Deletors_) {
        deletor();
    }
}

void TLoadContext::RegisterObject(TObjectId id, void* basePtr)
{
    EmplaceOrCrash(IdToPtr_, id, basePtr);
}

void* TLoadContext::GetObject(TObjectId id) const
{
    return GetOrCrash(IdToPtr_, id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
