#pragma once

#include <library/cpp/actors/interconnect/interconnect_common.h>
#include <ydb/core/protos/config.pb.h>

namespace NKikimr {

class TCompatibilityInfo {
    friend class TCompatibilityInfoTest;
    using TOldFormat = NActors::TInterconnectProxyCommon::TVersionInfo;
    using TComponentId = NKikimrConfig::TCompatibilityRule::EComponentId;

public:
    struct TProtoConstructor {
        TProtoConstructor() = delete;

        struct TYdbVersion {
            std::optional<ui32> Year;
            std::optional<ui32> Major;
            std::optional<ui32> Minor;
            std::optional<ui32> Hotfix;

            NKikimrConfig::TYdbVersion ToPB() {
                NKikimrConfig::TYdbVersion res;
                if (Year) {
                    res.SetYear(*Year);
                }
                if (Major) {
                    res.SetMajor(*Major);
                }
                if (Minor) {
                    res.SetMinor(*Minor);
                }
                if (Hotfix) {
                    res.SetHotfix(*Hotfix);
                }

                return res;
            }
        };

        struct TCompatibilityRule {
            std::optional<std::string> Build;
            std::optional<TYdbVersion> LowerLimit;
            std::optional<TYdbVersion> UpperLimit;
            std::optional<ui32> ComponentId;
            std::optional<bool> Forbidden;

            NKikimrConfig::TCompatibilityRule ToPB() {
                NKikimrConfig::TCompatibilityRule res;
                if (Build) {
                    res.SetBuild(Build->data());
                }
                if (LowerLimit) {
                    res.MutableLowerLimit()->CopyFrom(LowerLimit->ToPB());
                }
                if (UpperLimit) {
                    res.MutableUpperLimit()->CopyFrom(UpperLimit->ToPB());
                }
                if (ComponentId) {
                    res.SetComponentId(*ComponentId);
                }
                if (Forbidden) {
                    res.SetForbidden(*Forbidden);
                }

                return res;
            }
        };

        struct TCurrentCompatibilityInfo {
            std::optional<std::string> Build;
            std::optional<TYdbVersion> YdbVersion;
            std::vector<TCompatibilityRule> CanLoadFrom;
            std::vector<TCompatibilityRule> StoresReadableBy;

            NKikimrConfig::TCurrentCompatibilityInfo ToPB() {
                NKikimrConfig::TCurrentCompatibilityInfo res;
                Y_VERIFY(Build);
                res.SetBuild(Build->data());
                if (YdbVersion) {
                    res.MutableYdbVersion()->CopyFrom(YdbVersion->ToPB());
                }

                for (auto canLoadFrom : CanLoadFrom) {
                    res.AddCanLoadFrom()->CopyFrom(canLoadFrom.ToPB());
                }
                for (auto storesReadableBy : StoresReadableBy) {
                    res.AddStoresReadableBy()->CopyFrom(storesReadableBy.ToPB());
                }

                return res;
            }
        };

        struct TStoredCompatibilityInfo {
            std::optional<std::string> Build;
            std::optional<TYdbVersion> YdbVersion;
            std::vector<TCompatibilityRule> ReadableBy;

            NKikimrConfig::TStoredCompatibilityInfo ToPB() {
                NKikimrConfig::TStoredCompatibilityInfo res;
                Y_VERIFY(Build);

                res.SetBuild(Build->data());
                if (YdbVersion) {
                    res.MutableYdbVersion()->CopyFrom(YdbVersion->ToPB());
                }

                for (auto readableBy : ReadableBy) {
                    res.AddReadableBy()->CopyFrom(readableBy.ToPB());
                }

                return res;
            }
        };
    };

public:
    TCompatibilityInfo() = delete;

    static void Initialize();
    static const NKikimrConfig::TCurrentCompatibilityInfo* GetCurrent();
    static const NKikimrConfig::TStoredCompatibilityInfo* GetDefault(TComponentId componentId);

    // pass nullptr if stored CompatibilityInfo is absent
    static bool CheckCompatibility(const NKikimrConfig::TStoredCompatibilityInfo* stored,
            TComponentId componentId, TString& errorReason);
    static bool CheckCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
            const NKikimrConfig::TStoredCompatibilityInfo* stored, TComponentId componentId,
            TString& errorReason);

    static bool CheckCompatibility(const TOldFormat& stored, TComponentId componentId, TString& errorReason);
    static bool CheckCompatibility(const NKikimrConfig::TCurrentCompatibilityInfo* current,
            const TOldFormat& stored, TComponentId componentId, TString& errorReason);

    static bool CompleteFromTag(NKikimrConfig::TCurrentCompatibilityInfo& current);

    static NKikimrConfig::TStoredCompatibilityInfo MakeStored(TComponentId componentId);
    static NKikimrConfig::TStoredCompatibilityInfo MakeStored(TComponentId componentId,
            const NKikimrConfig::TCurrentCompatibilityInfo* current);

private:
    static NKikimrConfig::TCurrentCompatibilityInfo CompatibilityInfo;

    using TDefaultCompatibilityInfo = std::array<std::optional<NKikimrConfig::TStoredCompatibilityInfo>,
            NKikimrConfig::TCompatibilityRule::ComponentsCount>;
    static TDefaultCompatibilityInfo DefaultCompatibilityInfo;

    // functions that modify compatibility information are only accessible from friend classes
    // Reset() is not thread-safe!
    static void Reset(NKikimrConfig::TCurrentCompatibilityInfo* newCurrent);
};

// obsolete version control
// TODO: remove in the next major release
extern TMaybe<NActors::TInterconnectProxyCommon::TVersionInfo> VERSION;

void CheckVersionTag();
TString GetBranchName(TString url);

} // namespace NKikimr
