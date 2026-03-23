#pragma once

#include "schemeshard_build_index.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"
#include "schemeshard_impl.h"
#include "schemeshard_index_utils.h"

namespace NKikimr {
namespace NSchemeShard {


THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose
    (TSchemeShard* ss, const TIndexBuildInfo& buildInfo, TTxId txId, const TPath& path);

} // namespace NSchemeShard
} // namespace NKikimr
