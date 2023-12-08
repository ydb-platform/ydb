#pragma once
#include "defs.h"

#include <ydb/core/protos/config.pb.h>

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NConsole {

NTabletPipe::TClientRetryPolicy FastConnectRetryPolicy();

TString KindsToString(const TDynBitMap &kinds);

TString KindsToString(const THashSet<ui32> &kinds);

TString KindsToString(const TVector<ui32> &kinds);

TDynBitMap KindsToBitMap(const TVector<ui32> &kinds);

TDynBitMap KindsToBitMap(const THashSet<ui32> &kinds);

/**
 * Replace 'kinds' in 'to' from 'from'
 * repeated items are removed
 */
void ReplaceConfigItems(
    const NKikimrConfig::TAppConfig &from,
    NKikimrConfig::TAppConfig &to,
    const TDynBitMap &kinds,
    const NKikimrConfig::TAppConfig &fallback = {},
    bool cleanupFallback = true);

bool CompareConfigs(const NKikimrConfig::TAppConfig &lhs, const NKikimrConfig::TAppConfig &rhs);

/**
 * Compares only fields in specified kinds
 * repeated items are ignored
 */
bool CompareConfigs(const NKikimrConfig::TAppConfig &lhs, const NKikimrConfig::TAppConfig &rhs, const TDynBitMap &kinds);

/**
 * Extracts versions for specified kinds
 */
NKikimrConfig::TConfigVersion FilterVersion(const NKikimrConfig::TConfigVersion &version, const TDynBitMap &kinds);

} // namespace NKikimr::NConsole
