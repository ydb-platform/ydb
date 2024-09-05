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

} // namespace NYT::NPhoenix
