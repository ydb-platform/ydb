#pragma once

#include <util/digest/multi.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include <compare>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Identity of a persistent buffer record. A record is written under the tablet
// generation that is current at write time and keeps it for life, so the
// record is addressed by that generation plus the lsn issued within it. New
// writes of a restarted tablet live in a fresh generation and can never
// collide with records restored from previous generations, whatever lsns they
// carry. The ordering is lexicographic with the generation first, which
// matches real time across tablet restarts: every record of an older
// generation precedes every record of a newer one.
struct TPBufferKey
{
    ui32 Generation = 0;
    ui64 Lsn = 0;

    friend constexpr auto operator<=>(
        const TPBufferKey&,
        const TPBufferKey&) = default;

    [[nodiscard]] TString Print() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore

template <>
struct THash<NYdb::NBS::NBlockStore::TPBufferKey>
{
    size_t operator()(
        const NYdb::NBS::NBlockStore::TPBufferKey& pBufferKey) const
    {
        return MultiHash(pBufferKey.Generation, pBufferKey.Lsn);
    }
};
