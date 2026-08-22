#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

namespace NKikimr::NColumnShard {
class TLockSharingInfo;
}

namespace NKikimr::NOlap::NReader::NPlain {

// Holds all metadata that is needed to perform read/scan
class TReadMetadata: public NCommon::TReadMetadata {
private:
    using TBase = NCommon::TReadMetadata;
    virtual TConclusionStatus DoInitCustom(const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription) override;

public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;
    using TBase::TBase;

    virtual std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<TReadContext>& context) const override;
    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;

    virtual TString DebugString() const override {
        return TBase::DebugString();
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain
