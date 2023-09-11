#pragma once

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract.h>
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/actors/core/event.h>

namespace NKikimr {

struct TAppData;

namespace NColumnShard {

class TBlobBatch;
struct TUsage;

}

namespace NOlap {

class TUnifiedBlobId;

class TBlobWriteInfo {
private:
    YDB_READONLY_DEF(TUnifiedBlobId, BlobId);
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY_DEF(std::shared_ptr<IBlobsAction>, WriteOperator);

    TBlobWriteInfo(const TString& data, const std::shared_ptr<IBlobsAction>& writeOperator)
        : Data(data)
        , WriteOperator(writeOperator) {
        Y_VERIFY(WriteOperator);
        BlobId = WriteOperator->AllocateNextBlobId(data);
    }
public:
    static TBlobWriteInfo BuildWriteTask(const TString& data, const std::shared_ptr<IBlobsAction>& writeOperator) {
        return TBlobWriteInfo(data, writeOperator);
    }
};

}
}
