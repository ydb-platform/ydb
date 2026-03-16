#include <unistd.h>
#include <Core/UUID.h>
#include <Common/thread_local_rng.h>


namespace DB_CHDB
{

namespace UUIDHelpers
{
    UUID generateV4()
    {
        UUID uuid;
        getHighBytes(uuid) = (thread_local_rng() & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        getLowBytes(uuid) = (thread_local_rng() & 0x3fffffffffffffffull) | 0x8000000000000000ull;

        return uuid;
    }

    UUID generate_from_pid()
    {
        UInt128 res{0, 0};
        res.items[1] = (res.items[1] & 0xffffffffffff0fffull) | (static_cast<UInt64>(getpid()));
        return UUID{res};
    }
}

}
