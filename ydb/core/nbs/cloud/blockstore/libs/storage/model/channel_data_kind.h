#pragma once

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EChannelDataKind
{
    System = 1,
    Log = 2,
    Index = 3,
    Mixed = 4,
    Merged = 5,
    Fresh = 6,
    External = 7,

    Max,
};

}   // namespace NYdb::NBS::NStorage
