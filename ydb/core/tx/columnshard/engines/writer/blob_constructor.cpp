#include "blob_constructor.h"

namespace NKikimr::NOlap {

TBlobWriteInfo::TBlobWriteInfo(const TString& data, const std::shared_ptr<IBlobsWritingAction>& writeOperator, const std::optional<TUnifiedBlobId>& customBlobId)
    : Data(data)
    , WriteOperator(writeOperator)
{
    Y_ABORT_UNLESS(WriteOperator);
    BlobId = WriteOperator->AddDataForWrite(data, customBlobId);
}

NKikimr::NOlap::TBlobWriteInfo TBlobWriteInfo::BuildWriteTask(const TString& data, const std::shared_ptr<IBlobsWritingAction>& writeOperator, const std::optional<TUnifiedBlobId>& customBlobId /*= {}*/) {
    return TBlobWriteInfo(data, writeOperator, customBlobId);
}

}
