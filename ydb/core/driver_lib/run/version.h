#pragma once

#include <library/cpp/actors/interconnect/interconnect_common.h>
#include <ydb/core/protos/config.pb.h>

const NKikimrConfig::TCurrentCompatibilityInformation* GetCurrentCompatibilityInformation();
const NKikimrConfig::TStoredCompatibilityInformation* GetUnknownYdbRelease();

NKikimrConfig::TStoredCompatibilityInformation MakeStoredCompatibiltyInformation(ui32 componentId,
        const NKikimrConfig::TCurrentCompatibilityInformation* current);

NKikimrConfig::TStoredCompatibilityInformation MakeStoredCompatibiltyInformation(ui32 componentId);

bool CheckVersionCompatibility(const NKikimrConfig::TCurrentCompatibilityInformation* current,
        const NKikimrConfig::TStoredCompatibilityInformation* stored,
        ui32 componentId, TString& errorReason);

bool CheckVersionCompatibility(const NKikimrConfig::TStoredCompatibilityInformation* stored,
        ui32 componentId, TString& errorReason);

// obsolete version control
// TODO: remove in the next major release
extern TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION;

void CheckVersionTag();
TString GetBranchName(TString url);
