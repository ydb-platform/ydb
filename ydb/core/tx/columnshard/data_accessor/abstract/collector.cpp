#include "collector.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NDataAccessorControl {

void IGranuleDataAccessor::AskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
    AFL_VERIFY(request);
    AFL_VERIFY(request->HasSubscriber());
    return DoAskData(request);
}

}   // namespace NKikimr::NOlap::NDataAccessorControl
