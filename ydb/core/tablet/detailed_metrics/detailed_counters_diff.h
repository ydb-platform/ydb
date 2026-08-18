#pragma once

#include <ydb/core/protos/sys_view.pb.h>

namespace NKikimr {

/**
 * Fill diff with the delta of current against prev: Simple copied absolute
 * (stateful, the SwapStatefulCounters convention), Cumulative/Histogram values
 * copied as the delta since prev (only non-zero index-value pairs are stored,
 * CumulativeCount records the full index space so the receiver can tell a
 * sparse diff from a shrunk counter set).
 *
 * @param[out] diff The freshly filled delta, ready to ship on the wire
 * @param[in] current The current absolute values
 * @param[in,out] prev The previous absolute values (the diff baseline). Resized
 *                       in place to current's shape if it does not already
 *                       match it (a first call, or a counter set that grew).
 *
 * @note Verbatim port of the private static function of the same name and
 *       signature in ydb/core/sys_view/service/sysview_service.cpp:69-136 (minus
 *       the SVLOG_CRIT logging, which has no equivalent logging service here).
 *       Unifying the two copies into one is a separate change: this one feeds
 *       the detailed metrics node-to-processor transport (step 08+), the other
 *       feeds the pre-existing SysView Service tick, and they are not on the
 *       same release cadence.
 */
void CalculateCountersDiff(
    NKikimrSysView::TDbCounters* diff,
    const NKikimrSysView::TDbCounters& current,
    NKikimrSysView::TDbCounters& prev
);

/**
 * The TDbTabletCounters-level overload: ExecutorCounters/AppCounters diffed the
 * same way as the TDbCounters overload above (Simple absolute, Cumulative/
 * Histogram delta); MaxExecutorCounters/MaxAppCounters copied absolute (the
 * existing CopyCounters()'s treatment of the Max* pair, since a max has no
 * meaningful delta); Type copied as is.
 *
 * @param[out] diff The freshly filled delta, ready to ship on the wire
 * @param[in] current The current absolute values
 * @param[in,out] prev The previous absolute values (the diff baseline)
 */
void CalculateCountersDiff(
    NKikimrSysView::TDbTabletCounters* diff,
    const NKikimrSysView::TDbTabletCounters& current,
    NKikimrSysView::TDbTabletCounters& prev
);

} // namespace NKikimr
