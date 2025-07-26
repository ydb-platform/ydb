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
    std::vector<TInsertWriteId> Uncommitted;

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
    virtual std::shared_ptr<NCommon::IDataSource> DoTryExtractNext(
        const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 inFlightCurrentLimit) override;

public:
    TPortionSources(std::vector<std::shared_ptr<TPortionInfo>>&& sources, std::vector<TInsertWriteId>&& uncommitted)
        : Sources(sources.begin(), sources.end())
        , Uncommitted(std::move(uncommitted))
    {
    }

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const override;

    virtual NCommon::TPortionIntervalTree GetPortionIntervals() const override {
        Y_ABORT("unimplemented");
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain
