#ifndef STATISTIC_PATH_INL_H_
    #error "Direct inclusion of this file is not allowed, include format.h"
    // For the sake of sane code completion.
    #include "statistic_path.h"
#endif

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename C>
void NStatisticPath::TStatisticPathSerializer::Save(C& context, const NStatisticPath::TStatisticPath& path)
{
    NYT::Save(context, path.Path());
}

template <typename C>
void NStatisticPath::TStatisticPathSerializer::Load(C& context, NStatisticPath::TStatisticPath& path)
{
    NStatisticPath::TStatisticPathType tmp;
    NYT::Load(context, tmp);
    path = TStatisticPath(std::move(tmp));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
