#pragma once
#include "granule_view.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

struct TReadStatsMetadata: public TReadMetadataBase {
private:
    using TBase = TReadMetadataBase;
public:
    using TConstPtr = std::shared_ptr<const TReadStatsMetadata>;

    const ui64 TabletId;
    std::vector<ui32> ReadColumnIds;
    std::vector<ui32> ResultColumnIds;
    std::deque<TGranuleMetaView> IndexGranules;

    explicit TReadStatsMetadata(const std::shared_ptr<TVersionedIndex>& info, ui64 tabletId, const ESorting sorting,
        const TProgramContainer& ssaProgram, const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot)
        : TBase(info, sorting, ssaProgram, schema, requestSnapshot)
        , TabletId(tabletId) {
    }
};

}
