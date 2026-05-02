#pragma once
#include <ydb/core/tx/columnshard/engines/reader/common/conveyor_task.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>
#include <ydb/core/tx/columnshard/engines/reader/common/stats.h>

namespace NKikimr::NOlap::NReader {

class TScanIteratorBase {
protected:
    virtual void DoOnSentDataFromInterval(const TPartialSourceAddress& /*intervalAddress*/) {

    }
    virtual void DoOnStreamingPageSent(const TPartialSourceAddress& /*pageAddress*/) {

    }
public:
    virtual ~TScanIteratorBase() = default;

    virtual void Apply(const std::shared_ptr<IApplyAction>& /*task*/) {

    }

    virtual TConclusionStatus Start() = 0;

    virtual const TReadStats& GetStats() const;

    // Invoked after a result has been delivered to the client when the source
    // still has more chunks/pages to read; routes the ack to the sync point's
    // Continue() callback.  No-op when the source had nothing left to do.
    void OnSentDataFromInterval(const std::optional<TPartialSourceAddress>& intervalAddress) {
        if (intervalAddress) {
            DoOnSentDataFromInterval(*intervalAddress);
        }
    }

    // Invoked after a streaming page has been delivered to the client; pairs
    // with ISourcesCollection::OnPageCreated() to drive the per-page
    // backpressure counter.  Set independently of OnSentDataFromInterval —
    // the final streaming page acks here but not there.
    void OnStreamingPageSent(const std::optional<TPartialSourceAddress>& pageAddress) {
        if (pageAddress) {
            DoOnStreamingPageSent(*pageAddress);
        }
    }

    virtual std::optional<ui32> GetAvailableResultsCount() const {
        return {};
    }
    virtual bool Finished() const = 0;
    virtual TConclusion<std::unique_ptr<TPartialReadResult>> GetBatch() = 0;
    virtual void PrepareResults() {

    }
    virtual TConclusion<bool> ReadNextInterval() { return false; }
    virtual TString DebugString(const bool verbose = false) const {
        Y_UNUSED(verbose);
        return "NO_DATA";
    }
};

}
