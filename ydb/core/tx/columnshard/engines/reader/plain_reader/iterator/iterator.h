#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/iterator.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TColumnShardScanIterator: public NCommon::TColumnShardScanIterator {
private:
    using TBase = NCommon::TColumnShardScanIterator;
    virtual void FillReadyResults() override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NReader::NPlain
