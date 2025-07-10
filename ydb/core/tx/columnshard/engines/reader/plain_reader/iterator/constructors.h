#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NReader::NPlain {

class TPortionSources: public NCommon::ISourcesConstructor {
private:
    std::deque<std::shared_ptr<TPortionInfo>> Sources;
    ui32 SourceIdx = 0;

    virtual void DoFillReadStats(TReadStats& stats) const override {
        ui64 compactedPortionsBytes = 0;
        ui64 insertedPortionsBytes = 0;
        ui64 committedPortionsBytes = 0;
        for (auto&& i : Sources) {
            if (i->GetPortionType() == EPortionType::Compacted) {
                compactedPortionsBytes += i->GetTotalBlobBytes();
            } else if (i->GetProduced() == NPortion::EProduced::INSERTED) {
                insertedPortionsBytes += i->GetTotalBlobBytes();
            } else {
                committedPortionsBytes += i->GetTotalBlobBytes();
            }
        }
        stats.IndexPortions = Sources.size();
        stats.InsertedPortionsBytes = insertedPortionsBytes;
        stats.CompactedPortionsBytes = compactedPortionsBytes;
        stats.CommittedPortionsBytes = committedPortionsBytes;
    }

    virtual TString DoDebugString() const override {
        return "{" + ::ToString(Sources.size()) + "}";
    }

    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
    }

    virtual void DoClear() override {
        Sources.clear();
    }
    virtual void DoAbort() override {
        Sources.clear();
    }
    virtual bool DoIsFinished() const override {
        return Sources.empty();
    }
    virtual std::shared_ptr<NCommon::IDataSource> DoExtractNext(const std::shared_ptr<NCommon::TSpecialReadContext>& context) override;

public:
    TPortionSources(std::vector<std::shared_ptr<TPortionInfo>>&& sources)
        : Sources(sources.begin(), sources.end()) {
    }

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const override;
};

}   // namespace NKikimr::NOlap::NReader::NPlain
