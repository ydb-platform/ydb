#pragma once

#include "flat_page_iface.h"

namespace NKikimr {
namespace NTable {

    /**
     * Placement of part pages over page collections, stored in TLayout of each part
     *
     * By default all the descriptor pages (root, bloom filters, frames, globs, stats)
     * and indexes of all groups share the page collection with data pages of the main group.
     * A separate descriptor page collection lets data and indexes live in other channels
     * while the descriptor pages stay in the channel of the main group family.
     */
    struct TPartLayout {
        bool SeparateDescriptorCollection = false;

        // With a separate descriptor collection, keep the main group index in it
        bool HotPKIndex = false;

        // With a separate descriptor collection, keep indexes of other groups in it
        bool HotFamilyIndexes = false;

        bool operator==(const TPartLayout&) const = default;
    };

    /**
     * Maps kinds of part pages to page collections (slots) of the part
     *
     * Shared by the part, its writer and its loader, which needs it before the part is created
     */
    class TPartSlots {
    public:
        explicit TPartSlots(ui32 groupsCount, TPartLayout layout = { }) noexcept
            : GroupsCount(groupsCount)
            , Layout(layout)
        { }

        const TPartLayout& GetLayout() const noexcept
        {
            return Layout;
        }

        /**
         * Number of page collections except the outer one
         */
        ui32 SlotsCount() const noexcept
        {
            return GroupsCount + (Layout.SeparateDescriptorCollection ? 1 : 0);
        }

        /**
         * Slot with the part root (Schem2), bloom filters, frames, globs and stats pages
         */
        ui32 DescriptorSlot() const noexcept
        {
            return 0;
        }

        /**
         * Slot with data pages of the column group, historic data pages share the slot of their group
         */
        ui32 DataSlot(NPage::TGroupId groupId) const noexcept
        {
            return groupId.Index + (Layout.SeparateDescriptorCollection ? 1 : 0);
        }

        /**
         * Slot with index pages (flat or b-tree) of the column group, historic ones included
         */
        ui32 IndexSlot(NPage::TGroupId groupId) const noexcept
        {
            if (!Layout.SeparateDescriptorCollection) {
                return 0;
            }
            const bool hot = groupId.Index == 0 ? Layout.HotPKIndex : Layout.HotFamilyIndexes;
            return hot ? DescriptorSlot() : DataSlot(groupId);
        }

        /**
         * Slot with outer (packed) values
         */
        ui32 OuterSlot() const noexcept
        {
            return SlotsCount();
        }

    private:
        ui32 GroupsCount;
        TPartLayout Layout;
    };

}
}
