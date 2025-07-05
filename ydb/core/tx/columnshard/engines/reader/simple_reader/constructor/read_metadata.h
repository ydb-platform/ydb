#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TReadMetadata: public NCommon::TReadMetadata {
    using TBase = NCommon::TReadMetadata;

public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;
    using TBase::TBase;

    virtual std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<TReadContext>& context) const override;
    virtual TConclusionStatus DoInitCustom(const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription) override;

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;
};

}   // namespace NKikimr::NOlap::NReader::NSimple
