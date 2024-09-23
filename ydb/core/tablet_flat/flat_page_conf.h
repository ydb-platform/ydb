#pragma once

#include "flat_page_iface.h"
#include "flat_row_eggs.h"

#include <ydb/core/scheme/scheme_tablecell.h>

#include <util/generic/ylimits.h>

namespace NKikimr {
namespace NTable {
namespace NPage {

    /**
     * Used for checking if keys belong to a key space
     */
    class IKeySpace {
    public:
        virtual ~IKeySpace() = default;

        /**
         * Resets this instance back to the beginning
         */
        virtual void Reset() noexcept = 0;

        /**
         * Returns true if the specified key intersects with the key space
         *
         * NOTE: consecutive calls to HasKey must be in an increasing key order
         */
        virtual bool HasKey(TCellsRef key) noexcept = 0;
    };

    /**
     * Used for checking when keys cross a split boundary
     */
    class ISplitKeys {
    public:
        virtual ~ISplitKeys() = default;

        /**
         * Resets this instance back to the beginning
         */
        virtual void Reset() noexcept = 0;

        /**
         * Returns true if key crosses a split boundary
         *
         * NOTE: consecutive calls to ShouldSplit must be in an increasing key order
         */
        virtual bool ShouldSplit(TCellsRef key) noexcept = 0;
    };

    /**
     * Config for generating data pages in a column group
     */
    struct TGroupConf {
        TGroupConf() { }

        bool ForceCompression = false;  /* Do not leave uncompressed    */
        ECodec Codec = ECodec::Plain;   /* Data page encoding method    */
        ui32 PageSize = 7 * 1024;       /* Data page target size        */
        ui32 PageRows = Max<ui32>();    /* Max rows per page, for UTs   */
        ui32 IndexMin = 32 * 1024;      /* Index initial buffer size    */

        ui32 BTreeIndexNodeTargetSize = 7 * 1024; /* 1 GB of (up to) 140B keys leads to 3-level B-Tree index */
        ui32 BTreeIndexNodeKeysMin = 6;           /* 1 GB of 7KB keys leads to 6-level B-Tree index (node size - ~42KB) */
        ui32 BTreeIndexNodeKeysMax = Max<ui32>(); /* for UTs */
    };

    struct TConf {
        TConf() {
            // There should always be at least 1 group
            Groups.emplace_back();
        }

        TConf(bool fin, ui32 page, ui32 large = Max<ui32>())
            : Final(fin)
            , LargeEdge(large)
        {
            Group(0).PageSize = page;
        }

        bool Final = true;
        bool CutIndexKeys = true;
        bool WriteBTreeIndex = true;
        bool WriteFlatIndex = true;
        ui32 MaxLargeBlob = 8 * 1024 * 1024 - 8; /* Maximum large blob size */
        ui32 LargeEdge = Max<ui32>();   /* External blob edge size      */
        ui32 SmallEdge = Max<ui32>();   /* Outer blobs edge bytes limit */
        bool ByKeyFilter = false;       /* Per-part bloom filter        */
        ui64 MaxRows = 0;               /* Used to set up bloom filter size */
        ui64 SliceSize = Max<ui64>();   /* Data size for slice creation */
        ui64 MainPageCollectionEdge = Max<ui64>();
        ui64 SmallPageCollectionEdge = Max<ui64>();
        IKeySpace* UnderlayMask = nullptr;
        ISplitKeys* SplitKeys = nullptr;
        TRowVersion MinRowVersion = TRowVersion::Min();

        TVector<TGroupConf> Groups;

        TGroupConf& Group(size_t index) {
            if (Groups.size() <= index) {
                Groups.resize(index + 1);
            }
            return Groups[index];
        }
    };

}
}
}
