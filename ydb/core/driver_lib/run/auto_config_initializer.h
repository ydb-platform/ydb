#pragma once

#include <util/system/types.h>
#include <util/generic/fwd.h>
#include <util/generic/map.h>

#include <vector>

namespace NKikimrConfig {

    class TActorSystemConfig;
    class TGRpcConfig;

}

namespace NKikimr::NAutoConfigInitializer {

    struct TASPools {
        static constexpr auto CommonPoolName = "Common";
        static constexpr auto SystemPoolName = "System";
        static constexpr auto UserPoolName = "User";
        static constexpr auto BatchPoolName = "Batch";
        static constexpr auto IOPoolName = "IO";
        static constexpr auto ICPoolName = "IC";

        ui8 SystemPoolId = 0;
        ui8 UserPoolId = 1;
        ui8 BatchPoolId = 2;
        ui8 IOPoolId = 3;
        ui8 ICPoolId = 4;

        ui8 GetRealPoolCount() const {
            return 1 + Max(SystemPoolId, UserPoolId, BatchPoolId, IOPoolId, ICPoolId);
        }

        std::vector<ui8> GetIndeces() const {
            return {
                SystemPoolId,
                UserPoolId,
                BatchPoolId,
                IOPoolId,
                ICPoolId
            };
        }

        std::vector<TString> GetRealPoolNames() const {
            switch (GetRealPoolCount()) {
            case 1:
                return {CommonPoolName};
            case 2:
                return {CommonPoolName, IOPoolName};
            case 3:
                return {CommonPoolName, BatchPoolName, IOPoolName};
            case 4:
                return {CommonPoolName, BatchPoolName, IOPoolName, ICPoolName};
            default:
                return {SystemPoolName, UserPoolName, BatchPoolName, IOPoolName, ICPoolName};
            }
        }

        std::vector<ui8> GetPriorities() const {
            switch (GetRealPoolCount()) {
            case 1:
                return {40};
            case 2:
                return {40, 0};
            case 3:
                return {40, 10, 0,};
            case 4:
                return {30, 10, 0, 40};
            default:
                return {30, 20, 10, 0, 40};
            }
        }
    };

    TASPools GetASPools(i16 cpuCount = 0);

    TASPools GetASPools(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig);

    TMap<TString, ui32> GetServicePools(const NKikimrConfig::TActorSystemConfig &config, bool useAutoConfig);

    void ApplyAutoConfig(NKikimrConfig::TActorSystemConfig *config, bool isDynamicNode);

    void ApplyAutoConfig(NKikimrConfig::TGRpcConfig *config, const NKikimrConfig::TActorSystemConfig &asConfig);

} // NKikimr::NActorSystemInitializer

namespace NKikimr {
    bool NeedToUseAutoConfig(const NKikimrConfig::TActorSystemConfig& config);
}
