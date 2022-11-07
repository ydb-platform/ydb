#pragma once
#include "common_helper.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::Tests::NCS {

class THelper: public NCommon::THelper {
private:
    using TBase = NCommon::THelper;

    std::shared_ptr<arrow::Schema> GetArrowSchema();
    std::shared_ptr<arrow::RecordBatch> TestArrowBatch(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount);

public:
    using TBase::TBase;
    void CreateTestOlapStore(TActorId sender, TString scheme);
    void CreateTestOlapTable(TActorId sender, TString storeName, TString scheme);
    TString TestBlob(ui64 pathIdBegin, ui64 tsBegin, size_t rowCount);
    void SendDataViaActorSystem(TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount);
};

}
