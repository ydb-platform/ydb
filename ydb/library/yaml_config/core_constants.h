#pragma once

#include <util/generic/string.h>

#include <utility>
#include <array>
#include <map>

namespace NKikimr::NYaml {

constexpr ui32 DEFAULT_INTERCONNECT_PORT = 19001;

constexpr inline TStringBuf DEFAULT_ROOT_USERNAME = "root";

constexpr inline TStringBuf COMBINED_DISK_INFO_PATH = "/blob_storage_config/service_set/groups/*/rings/*/fail_domains/*/vdisk_locations/*";
constexpr inline TStringBuf GROUP_PATH = "/blob_storage_config/service_set/groups/*";
constexpr inline TStringBuf DISABLE_BUILTIN_SECURITY_PATH = "/domains_config/disable_builtin_security";
constexpr inline TStringBuf DEFAULT_GROUPS_PATH = "/domains_config/default_groups";
constexpr inline TStringBuf DEFAULT_ACCESS_PATH = "/domains_config/default_access";
constexpr inline TStringBuf POOL_CONFIG_PATH = "/domains_config/domains/*/storage_pool_types/*/pool_config";

constexpr inline TStringBuf ERASURE_SPECIES_FIELD = "erasure_species";

const inline std::array<std::pair<TString, ui32>, 10> DEFAULT_TABLETS{
    std::pair{TString{"FlatHive"}, 1},
    std::pair{TString{"FlatBsController"}, 1},
    std::pair{TString{"FlatSchemeshard"}, 1},
    std::pair{TString{"FlatTxCoordinator"}, 3},
    std::pair{TString{"TxMediator"}, 3},
    std::pair{TString{"TxAllocator"}, 3},
    std::pair{TString{"Cms"}, 1},
    std::pair{TString{"NodeBroker"}, 1},
    std::pair{TString{"TenantSlotBroker"}, 1},
    std::pair{TString{"Console"}, 1},
};

const inline std::map<TString, ui64> GetTablets(ui64 idx) {
    return {
        {TString{"FlatHive"}, 72057594037968897},
        {TString{"FlatBsController"}, 72057594037932033},
        {TString{"FlatSchemeshard"}, 72057594046678944},
        {TString{"Cms"}, 72057594037936128},
        {TString{"NodeBroker"}, 72057594037936129},
        {TString{"TenantSlotBroker"}, 72057594037936130},
        {TString{"Console"}, 72057594037936131},
        {TString{"TxAllocator"}, TDomainsInfo::MakeTxAllocatorIDFixed(idx)},
        {TString{"FlatTxCoordinator"}, TDomainsInfo::MakeTxCoordinatorIDFixed(idx)},
        {TString{"TxMediator"}, TDomainsInfo::MakeTxMediatorIDFixed(idx)},
    };
}

const inline std::array<std::pair<TString, TString>, 4> DEFAULT_EXECUTORS{
    std::pair{TString{"IoExecutor"}, TString{"IO"}},
    std::pair{TString{"SysExecutor"}, TString{"SYSTEM"}},
    std::pair{TString{"UserExecutor"}, TString{"USER"}},
    std::pair{TString{"BatchExecutor"}, TString{"BATCH"}},
};


const inline std::array<std::pair<TString, TString>, 3> EXPLICIT_TABLETS{
    std::pair{TString{"ExplicitCoordinators"}, TString{"FlatTxCoordinator"}},
    std::pair{TString{"ExplicitAllocators"}, TString{"TxAllocator"}},
    std::pair{TString{"ExplicitMediators"}, TString{"TxMediator"}},
};


} // namespace NKikimr::NYaml
