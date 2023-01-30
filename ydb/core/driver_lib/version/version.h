#pragma once

#include <library/cpp/actors/interconnect/interconnect_common.h>
#include <ydb/core/protos/config.pb.h>

class TCompatibilityInfo {
    friend class TCompatibilityInfoTest;
    using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;

public:
    TCompatibilityInfo() = delete;
    static const NKikimrConfig::TCurrentCompatibilityInfo* GetCurrent();
    static const NKikimrConfig::TStoredCompatibilityInfo* GetUnknown();

    static NKikimrConfig::TStoredCompatibilityInfo MakeStored(NKikimrConfig::TCompatibilityRule::EComponentId componentId);

    static bool CheckCompatibility(const NKikimrConfig::TStoredCompatibilityInfo* stored,
            ui32 componentId, TString& errorReason);
    static bool CheckCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
            const NKikimrConfig::TStoredCompatibilityInfo* stored, ui32 componentId, TString& errorReason);

    static bool CheckCompatibility(const TOldFormat& stored, ui32 componentId, TString& errorReason);
    static bool CheckCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
            const TOldFormat& stored, ui32 componentId, TString& errorReason);

    static NKikimrConfig::TStoredCompatibilityInfo MakeStored(ui32 componentId,
            const NKikimrConfig::TCurrentCompatibilityInfo* current);

private:
    static TSpinLock LockCurrent;
    static NKikimrConfig::TCurrentCompatibilityInfo* CompatibilityInfo;
    static NKikimrConfig::TStoredCompatibilityInfo* UnknownYdbRelease;

    // functions that modify compatibility information are only accessible from friend classes
    static void Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent);
};

// obsolete version control
// TODO: remove in the next major release
extern TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION;

void CheckVersionTag();
TString GetBranchName(TString url);
