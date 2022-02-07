#pragma once

#include "defs.h"
#include "util_fmt_flat.h"
#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NRedo {

    struct TEntry {
        template<typename ... Args>
        static TEntry* Create(NTable::TTxStamp stamp, TArrayRef<const ui32> affects, Args&& ... args)
        {
            auto *ptr = malloc(sizeof(TEntry) + affects.size() * sizeof(ui32));

            return ::new(ptr) TEntry(stamp, affects, std::forward<Args>(args)...);
        }

        void operator delete (void *p)
        {
            free(p);
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Redo{" << NFmt::TStamp(Stamp) << " (" << Embedded.size()
                << " +" << LargeGlobId.Bytes << ")b, ~" << NFmt::Arr(Tables())
                << "}";
        }

        ui32 BytesMem() const noexcept
        {
            return sizeof(TEntry) + sizeof(ui32) * Affects + Embedded.size();
        }

        ui64 BytesData() const noexcept
        {
            return Embedded.size() + LargeGlobId.Bytes;
        }

        ui64 BytesLargeGlobId() const noexcept
        {
            return LargeGlobId.Bytes;
        }

        TArrayRef<const ui32> Tables() const noexcept
        {
            return { (const ui32*)((const TLogoBlobID*)(this + 1)), Affects };
        }

        ui32 FilterTables(const THashMap<ui32, NTable::TSnapEdge> &edges)
        {
            ui32 *begin = (ui32*)((TLogoBlobID*)(this + 1));
            ui32 * const end = begin + Affects;

            for (ui32 *write = begin, *read = begin; ; ++read) {
                if (read == end) {
                    return Affects = write - begin;
                } else {
                    const auto *sn = edges.FindPtr(*read);
                    if (!sn || sn->TxStamp < Stamp)
                        *write++ = *read;
                }
            }
        }

        const NTable::TTxStamp Stamp;
        const NPageCollection::TLargeGlobId LargeGlobId;
        const TString Embedded;
        ui32 Affects = 0;
        ui32 References = 0;

    private:
        TEntry(NTable::TTxStamp stamp, TArrayRef<const ui32> affects, const NPageCollection::TLargeGlobId &largeGlobId)
            : Stamp(stamp)
            , LargeGlobId(largeGlobId)
            , Affects(affects.size())
        {
            std::copy(affects.begin(), affects.end(), (ui32*)(this + 1));
        }

        TEntry(NTable::TTxStamp stamp, TArrayRef<const ui32> affects, TString embedded)
            : Stamp(stamp)
            , Embedded(std::move(embedded))
            , Affects(affects.size())
        {
            std::copy(affects.begin(), affects.end(), (ui32*)(this + 1));
        }
    };
}
}
}
