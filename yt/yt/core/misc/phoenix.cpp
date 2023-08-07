#include "phoenix.h"

#include "collection_helpers.h"

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

TProfiler::TProfiler()
{ }

TProfiler* TProfiler::Get()
{
    return Singleton<TProfiler>();
}

ui32 TProfiler::GetTag(const std::type_info& typeInfo)
{
    return GetEntry(typeInfo).Tag;
}

const TProfiler::TEntry& TProfiler::GetEntry(ui32 tag)
{
    return GetOrCrash(TagToEntry_, tag);
}

const TProfiler::TEntry& TProfiler::GetEntry(const std::type_info& typeInfo)
{
    return *GetOrCrash(TypeInfoToEntry_, &typeInfo);
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(IZeroCopyOutput* output, int version)
    : TStreamSaveContext(output, version)
{
    // Zero id is reserved for nullptr.
    IdGenerator_.Next();
}

ui32 TSaveContext::FindId(void* basePtr, const std::type_info* typeInfo) const
{
    auto it = PtrToEntry_.find(basePtr);
    if (it == PtrToEntry_.end()) {
        return NullObjectId;
    } else {
        const auto& entry = it->second;
        // Failure here means an attempt was made to serialize a polymorphic type
        // not marked with TDynamicTag.
        YT_VERIFY(entry.TypeInfo == typeInfo);
        return entry.Id;
    }
}

ui32 TSaveContext::GenerateId(void* basePtr, const std::type_info* typeInfo)
{
    TEntry entry;
    entry.Id = static_cast<ui32>(IdGenerator_.Next());
    entry.TypeInfo = typeInfo;
    YT_VERIFY(PtrToEntry_.emplace(basePtr, entry).second);
    return entry.Id;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::~TLoadContext()
{
    for (const auto& deletor : Deletors_) {
        deletor();
    }
}

void TLoadContext::RegisterObject(ui32 id, void* basePtr)
{
    YT_VERIFY(IdToPtr_.emplace(id, basePtr).second);
}

void* TLoadContext::GetObject(ui32 id) const
{
    return GetOrCrash(IdToPtr_, id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix
