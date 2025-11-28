#include "datashard_failpoints.h"

namespace NKikimr {
namespace NDataShard {

TCancelTxFailPoint gCancelTxFailPoint;
TSkipRepliesFailPoint gSkipRepliesFailPoint;
TSkipReadIteratorResultFailPoint gSkipReadIteratorResultFailPoint;
TBlockOperationsFailPoint gBlockOperationsFailPoint;

TBlockOperationsFailPoint::TGuard::TGuard()
    : Prev(gBlockOperationsFailPoint.Chain.exchange(this, std::memory_order_acq_rel))
{}

TBlockOperationsFailPoint::TGuard::TGuard(std::function<bool(ui64, ui64)> callback)
    : Callback(std::move(callback))
    , Prev(gBlockOperationsFailPoint.Chain.exchange(this, std::memory_order_acq_rel))
{}

TBlockOperationsFailPoint::TGuard::~TGuard() {
    IBlockOperations* expected = this;
    bool success = gBlockOperationsFailPoint.Chain.compare_exchange_strong(expected, Prev, std::memory_order_release);
    Y_ABORT_UNLESS(success, "TBlockOperationsFailPoint::TGuard construction/destruction order mismatch");
}

}}
