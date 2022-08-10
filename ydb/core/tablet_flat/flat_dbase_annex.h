#pragma once

#include "flat_page_label.h"
#include "flat_redo_writer.h"
#include "flat_dbase_scheme.h"
#include "flat_sausage_solid.h"

namespace NKikimr {
namespace NTable {

    class TAnnex : public NRedo::IAnnex  {
        using TFamily = TScheme::TFamily;
        using TGlobId = NPageCollection::TGlobId;

    public:
        TAnnex(const TScheme &scheme) : Scheme(scheme) { }

        TVector<NPageCollection::TMemGlob> Unwrap() noexcept
        {
            return std::move(Blobs);
        }

        explicit operator bool() const noexcept
        {
            return bool(Blobs);
        }

        TArrayRef<const NPageCollection::TMemGlob> Current() const noexcept
        {
            return Blobs;
        }

    private:
        TLimit Limit(ui32 table) noexcept override
        {
            if (Lookup(table)) {
                return TLimit{ Family->Large, 8 * 1024 * 1024 - 8 };
            } else {
                return TLimit{ Max<ui32>(), 0 };
            }
        }

        TResult Place(ui32 table, TTag, TArrayRef<const char> data) noexcept override
        {
            Y_VERIFY(Lookup(table) && data.size() >= Family->Large);

            auto blob = NPage::TLabelWrapper::Wrap(data, EPage::Opaque, 0);

            const ui32 ref = Blobs.size();
            const TLogoBlobID fake(0, 0, 0, Room->Blobs, blob.size(), ref);

            Blobs.emplace_back(TGlobId{ fake, 0 }, std::move(blob));

            return ref;
        }

        bool Lookup(ui32 table) noexcept
        {
            if (std::exchange(Table, table) != table) {
                Family = Scheme.DefaultFamilyFor(Table);
                Room = Scheme.DefaultRoomFor(Table);

                Y_VERIFY(bool(Family) == bool(Room));
            }

            return nullptr != Family; /* table may be created with data tx */
        }

    private:
        const TScheme &Scheme;
        TVector<NPageCollection::TMemGlob> Blobs;

        /*_ Simple table info lookup cache */
        ui32 Table = Max<ui32>();
        const TScheme::TFamily *Family = nullptr;
        const TScheme::TRoom *Room = nullptr;
     };
}
}
