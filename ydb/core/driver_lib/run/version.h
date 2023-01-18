#pragma once

#include <library/cpp/actors/interconnect/interconnect_common.h>
#include <ydb/core/protos/config.pb.h>

const NKikimrConfig::TCurrentCompatibilityInfo* GetCurrentCompatibilityInfo();
const NKikimrConfig::TStoredCompatibilityInfo* GetUnknownYdbRelease();

NKikimrConfig::TStoredCompatibilityInfo MakeStoredCompatibilityInfo(ui32 componentId,
        const NKikimrConfig::TCurrentCompatibilityInfo* current);

NKikimrConfig::TStoredCompatibilityInfo MakeStoredCompatibilityInfo(ui32 componentId);

bool CheckVersionCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
        const NKikimrConfig::TStoredCompatibilityInfo* stored,
        ui32 componentId, TString& errorReason);

bool CheckVersionCompatibility(const NKikimrConfig::TStoredCompatibilityInfo* stored,
        ui32 componentId, TString& errorReason);

// obsolete version control
// TODO: remove in the next major release
extern TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION;

void CheckVersionTag();
TString GetBranchName(TString url);
