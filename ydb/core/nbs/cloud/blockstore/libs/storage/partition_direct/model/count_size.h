#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TCountAndSize
{
    size_t Count = 0;
    ui64 Size = 0;

    void Add(ui64 bytes);
    void Sub(ui64 bytes);

    TCountAndSize& operator+=(const TCountAndSize& rhs);

    [[nodiscard]] TString Print(bool humanReadable) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
