#pragma once

#include "flat_sausage_packet.h"

#include <util/generic/ptr.h>
#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NTable {

    class IBundle : public virtual TThrRefBase {
    public:
        virtual const TLogoBlobID& BundleId() const = 0;
        virtual ui64 BackingSize() const = 0;
        virtual const NPageCollection::TPageCollection* Packet(ui32 room) const noexcept = 0;

        template<typename TContainer>
        void SaveAllBlobIdsTo(TContainer &vec) const
        {
            ui32 room = 0;

            while (auto *pack = Packet(room++))
                pack->SaveAllBlobIdsTo(vec);
        }
    };

    /**
     * A more generic interface for bundles that may be borrowed
     */
    class IBorrowBundle : public virtual TThrRefBase {
    public:
        virtual const TLogoBlobID& BundleId() const = 0;
        virtual ui64 BackingSize() const = 0;

        virtual void SaveAllBlobIdsTo(TVector<TLogoBlobID> &vec) const = 0;
    };

}
}
