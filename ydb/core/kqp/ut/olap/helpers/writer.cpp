#include "writer.h"
#include "local.h"

namespace NKikimr::NKqp {

void WriteTestData(TKikimrRunner& kikimr, TString testTable, ui64 pathIdBegin, ui64 tsBegin, size_t rowCount, bool withSomeNulls /*= false*/,
    ui64 tsStepUs /*= 1*/) {
    UNIT_ASSERT(testTable != "/Root/benchTable"); // TODO: check schema instead
    TLocalHelper lHelper(kikimr);
    if (withSomeNulls) {
        lHelper.WithSomeNulls();
    }
    auto batch = lHelper.TestArrowBatch(pathIdBegin, tsBegin, rowCount, tsStepUs);
    lHelper.SendDataViaActorSystem(testTable, batch);
}
}