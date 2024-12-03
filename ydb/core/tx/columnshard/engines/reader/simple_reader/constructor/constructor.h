#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TIndexScannerConstructor: public IScannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "SIMPLE";
    }

private:
    using TBase = IScannerConstructor;
    static const inline TFactory::TRegistrator<TIndexScannerConstructor> Registrator =
        TFactory::TRegistrator<TIndexScannerConstructor>(GetClassNameStatic());
    virtual std::shared_ptr<IScanCursor> DoBuildCursor() const override {
        return std::make_shared<TSimpleScanCursor>();
    }

protected:
    virtual TConclusion<std::shared_ptr<TReadMetadataBase>> DoBuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;
public:
    using TBase::TBase;
    virtual TConclusionStatus ParseProgram(const TVersionedIndex* vIndex, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const override;
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const override;
};

}