#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NOlap::NReader::NPlain {

class TPortionSources: public NCommon::ISourcesConstructor {
private:
    std::deque<IColumnEngine::TSelectedPortionInfo> Sources;
    ui32 SourceIdx = 0;

    virtual void DoFillReadStats(TReadStats& stats) const override {
        ui64 compactedPortionsBytes = 0;
        ui64 insertedPortionsBytes = 0;
        ui64 committedPortionsBytes = 0;
        for (auto&& i : Sources) {
            TPortionInfo::TConstPtr p = i.GetPortion();
            if (p->GetPortionType() == EPortionType::Compacted) {
                compactedPortionsBytes += p->GetTotalBlobBytes();
            } else if (p->GetProduced() == NPortion::EProduced::INSERTED) {
                insertedPortionsBytes += p->GetTotalBlobBytes();
            } else {
                committedPortionsBytes += p->GetTotalBlobBytes();
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
    TPortionSources(std::vector<IColumnEngine::TSelectedPortionInfo>&& sources)
        : Sources(sources.begin(), sources.end())
    {
    }

    static std::unique_ptr<TPortionSources> BuildEmpty() {
        return std::make_unique<TPortionSources>(std::vector<IColumnEngine::TSelectedPortionInfo>());
    }

    virtual std::vector<TPortionInfo::TConstPtr> GetConflictingPortions() const override;
};

}   // namespace NKikimr::NOlap::NReader::NPlain
