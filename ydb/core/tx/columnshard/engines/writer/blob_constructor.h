#pragma once

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/actors/core/event.h>

namespace NKikimr {

struct TAppData;

namespace NColumnShard {

class TBlobBatch;
struct TUsage;

}

namespace NOlap {

class TBlobWriteInfo {
private:
    YDB_READONLY_DEF(TUnifiedBlobId, BlobId);
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY_DEF(std::shared_ptr<IBlobsWritingAction>, WriteOperator);

    TBlobWriteInfo(const TString& data, const std::shared_ptr<IBlobsWritingAction>& writeOperator)
        : Data(data)
        , WriteOperator(writeOperator) {
        Y_VERIFY(WriteOperator);
        BlobId = WriteOperator->AddDataForWrite(data);
    }
public:
    static TBlobWriteInfo BuildWriteTask(const TString& data, const std::shared_ptr<IBlobsWritingAction>& writeOperator) {
        return TBlobWriteInfo(data, writeOperator);
    }
};

}
}
