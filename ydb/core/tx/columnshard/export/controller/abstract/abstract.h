#pragma once
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap::NExport {

class IController {
private:
    enum class EStage {
        Initialization,
        InProgress,
        Finished
    };
    EStage Stage = EStage::Initialization;
protected:
    virtual std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> DoBuildRequestInitiator() const = 0;
    virtual std::vector<TString> DoBatchToBlobs(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;
public:
    std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> BuildRequestInitiator() const {
        return DoBuildRequestInitiator();
    }
    void OnScanStarted() {
        AFL_VERIFY(Stage == EStage::Initialization);
        Stage = EStage::InProgress;
    }

    std::vector<TString> BatchToBlobs(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        if (!batch || batch->num_rows() == 0) {
            return {};
        } else {
            return DoBatchToBlobs(batch);
        }
    }
};

}