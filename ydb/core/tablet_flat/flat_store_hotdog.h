#pragma once

#include "flat_sausage_packet.h"
#include "flat_sausagecache.h"
#include "flat_part_outset.h"
#include "flat_part_laid.h"
#include "flat_store_bundle.h"
#include <functional>

namespace NKikimrExecutorFlat {
    class TLogTableSnap;
    class TPageCollection;
    class TBundle;
}

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TPageCollectionProtoHelper {
    public:
        using TLargeGlobId = NPageCollection::TLargeGlobId;
        using TPartView = NTable::TPartView;
        using TColdPart = NTable::TColdPart;
        using TPartComponents = NTable::TPartComponents;
        using TBundle = NKikimrExecutorFlat::TBundle;
        using TScreen = NTable::TScreen;
        using TSlices = NTable::TSlices;

        TPageCollectionProtoHelper() = delete;

        TPageCollectionProtoHelper(bool meta)
            : PutMeta(meta)
        {

        }

        static void Snap(NKikimrExecutorFlat::TLogTableSnap *snap, const TPartComponents &pc, ui32 table, ui32 level);
        static void Snap(NKikimrExecutorFlat::TLogTableSnap *snap, const TPartView &partView, ui32 table, ui32 level);
        static void Snap(NKikimrExecutorFlat::TLogTableSnap *snap, const TIntrusiveConstPtr<TColdPart> &part, ui32 table, ui32 level);

        void Do(TBundle *bundle, const TPartView &partView);
        void Do(TBundle *bundle, const TIntrusiveConstPtr<TColdPart> &part);
        void Do(TBundle *bundle, const TPartComponents &pc);

        static TPartComponents MakePageCollectionComponents(const TBundle &proto, bool unsplit = false);

    private:
        void Bundle(NKikimrExecutorFlat::TPageCollection *pageCollectionProto, const TPrivatePageCache::TPageCollection &pageCollection);
        void Bundle(
                NKikimrExecutorFlat::TPageCollection *pageCollectionProto,
                const TLargeGlobId &largeGlobId,
                const NPageCollection::TPageCollection *pack);

    private:
        const bool PutMeta = false;     /* Save page collection metablob in bundle  */
    };
}
}
