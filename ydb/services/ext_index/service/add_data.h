#pragma once
#include <ydb/services/ext_index/metadata/object.h>
#include <ydb/services/ext_index/common/service.h>
#include <ydb/services/metadata/ds_table/scheme_describe.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include "add_index.h"

namespace NKikimr::NCSIndex {

class TDataUpserter:
    public IIndexUpsertController,
    public NMetadata::NProvider::ISchemeDescribeController {
private:
    mutable std::shared_ptr<TDataUpserter> SelfContainer;
    std::vector<NMetadata::NCSIndex::TObject> Indexes;
    mutable TAtomicCounter AtomicCounter = 0;
    IDataUpsertController::TPtr ExternalController;
    std::shared_ptr<arrow::RecordBatch> Data;
public:
    TDataUpserter(std::vector<NMetadata::NCSIndex::TObject>&& indexes,
        IDataUpsertController::TPtr externalController, std::shared_ptr<arrow::RecordBatch> data)
        : Indexes(std::move(indexes))
        , ExternalController(externalController)
        , Data(data)
    {

    }

    virtual void OnIndexUpserted() override {
        if (SelfContainer && AtomicCounter.Dec() == 0) {
            ExternalController->OnAllIndexesUpserted();
            SelfContainer = nullptr;
        }
    }
    virtual void OnIndexUpsertionFailed(const TString& errorMessage) override {
        if (SelfContainer) {
            ExternalController->OnAllIndexesUpsertionFailed("IndexUpsertion:" + errorMessage);
            SelfContainer = nullptr;
        }
    }

    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& /*requestId*/) override {
        if (SelfContainer) {
            ExternalController->OnAllIndexesUpsertionFailed("SchemeDescription:" + errorMessage);
            SelfContainer = nullptr;
        }
    }

    virtual void OnDescriptionSuccess(NMetadata::NProvider::TTableInfo&& result, const TString& requestId) override;

    void Start(std::shared_ptr<TDataUpserter> selfContainer);
};

}
