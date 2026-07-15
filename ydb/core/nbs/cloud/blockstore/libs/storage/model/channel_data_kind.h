#pragma once

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EChannelDataKind
{
    System = 1,
    Log = 2,
    Index = 3,

    Max,
};

}   // namespace NYdb::NBS::NStorage
