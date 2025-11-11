#pragma once
#include <ydb/services/ext_index/metadata/object.h>
#include <ydb/services/metadata/ds_table/scheme_describe.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/ext_index/common/service.h>

namespace NKikimr::NCSIndex {

class IIndexUpsertController {
public:
    using TPtr = std::shared_ptr<IIndexUpsertController>;
    virtual void OnIndexUpserted() = 0;
    virtual void OnIndexUpsertionFailed(const TString& errorMessage) = 0;
};

class TIndexUpsertActor: public NActors::TActorBootstrapped<TIndexUpsertActor> {
private:
    std::shared_ptr<arrow::RecordBatch> Data;
    const NMetadata::NCSIndex::TObject IndexInfo;
    std::vector<TString> PKFields;
    TString IndexTablePath;
    IIndexUpsertController::TPtr ExternalController;
protected:
    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev);

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            default:
                break;
        }
    }

public:
    void Bootstrap();

    TIndexUpsertActor(const std::shared_ptr<arrow::RecordBatch>& data, const NMetadata::NCSIndex::TObject& indexInfo,
        const std::vector<TString>& pKFields, const TString& indexTablePath, IIndexUpsertController::TPtr externalController)
        : Data(data)
        , IndexInfo(indexInfo)
        , PKFields(pKFields)
        , IndexTablePath(indexTablePath)
        , ExternalController(externalController)
    {

    }

};

}
