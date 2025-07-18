#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TIndexScannerConstructor: public IScannerConstructor {
public:
    static TString GetClassNameStatic() {
        return "PLAIN";
    }
private:
    using TBase = IScannerConstructor;
    static const inline TFactory::TRegistrator<TIndexScannerConstructor> Registrator =
        TFactory::TRegistrator<TIndexScannerConstructor>(GetClassNameStatic());

    virtual std::shared_ptr<IScanCursor> DoBuildCursor() const override;

protected:
    virtual TConclusion<std::shared_ptr<TReadMetadataBase>> DoBuildReadMetadata(const NColumnShard::TColumnShard* self, const TReadDescription& read) const override;
public:
    virtual TConclusionStatus ParseProgram(
        const TProgramParsingContext& context, const NKikimrTxDataShard::TEvKqpScan& proto, TReadDescription& read) const override;
    virtual std::vector<TNameTypeInfo> GetPrimaryKeyScheme(const NColumnShard::TColumnShard* self) const override;
    using TBase::TBase;
};

}