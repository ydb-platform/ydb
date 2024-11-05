#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TIndexScannerConstructor: public IScannerConstructor {
private:
    using TBase = IScannerConstructor;
protected:
    virtual TConclusion<std::shared_ptr<TReadMetadataBase>> DoBuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;
public:
    using TBase::TBase;
    virtual TConclusionStatus ParseProgram(const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const override;
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const override;
};

}