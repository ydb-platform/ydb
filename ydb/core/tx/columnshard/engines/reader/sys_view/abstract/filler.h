#pragma once
#include "granule_view.h"
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

class IMetadataFiller {
private:
    virtual TConclusionStatus DoFillMetadata(const NColumnShard::TColumnShard* shard, const std::shared_ptr<TReadMetadataBase>& metadata, const TReadDescription& read) const = 0;

    virtual NAbstract::TGranuleMetaView DoBuildGranuleView(const TGranuleMeta& granule, const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool reverse, const TSnapshot& reqSnapshot) const {
        return NAbstract::TGranuleMetaView(granule, schemeShardLocalPathId, reverse, reqSnapshot);
    }
public:
    virtual ~IMetadataFiller() = default;

    TConclusionStatus FillMetadata(const NColumnShard::TColumnShard* shard, const std::shared_ptr<TReadMetadataBase>& metadata, const TReadDescription& read) const {
        return DoFillMetadata(shard, metadata, read);
    }

    NAbstract::TGranuleMetaView BuildGranuleView(const TGranuleMeta& granule, const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool reverse, const TSnapshot& reqSnapshot) const {
        return DoBuildGranuleView(granule, schemeShardLocalPathId, reverse, reqSnapshot);
    }

};

class TMetadataFromStore: public IMetadataFiller {
protected:
    virtual TConclusionStatus DoFillMetadata(const NColumnShard::TColumnShard* shard, const std::shared_ptr<TReadMetadataBase>& metadata, const TReadDescription& read) const override;
public:
};

class TMetadataFromTable: public IMetadataFiller {
protected:
    virtual TConclusionStatus DoFillMetadata(const NColumnShard::TColumnShard* shard, const std::shared_ptr<TReadMetadataBase>& metadata, const TReadDescription& read) const override;
public:

};

}