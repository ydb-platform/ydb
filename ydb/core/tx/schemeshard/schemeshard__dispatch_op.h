#pragma once

#include "schemeshard__op_traits.h"

#include <ydb/core/tx/schemeshard/generated/dispatch_op.h>

namespace NKikimr::NSchemeShard {

template <class TFn>
constexpr auto DispatchOp(const TTxTransaction& tx, TFn fn) {
    return NGenerated::DispatchOp<TSchemeTxTraits, TSchemeTxTraitsFallback, TFn>(tx, std::forward<TFn>(fn));
}

} // namespace NKikimr::NSchemeShard
