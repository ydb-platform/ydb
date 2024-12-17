#pragma once

#include <ydb/core/protos/config.pb.h>

#include <vector>

namespace NKikimrProto {

class TAuthConfig;

} // NKikimrProto

namespace NKikimr::NConfig {

enum class EValidationResult {
    Ok,
    Warn,
    Error,
};

struct TPDiskKey {
    ui32 NodeId;
    ui32 PDiskId;

    auto operator<=>(const TPDiskKey&) const = default;
};

struct TVDiskKey {
    ui32 NodeId;
    ui32 PDiskId;
    ui32 VDiskSlotId;

    auto operator<=>(const TVDiskKey&) const = default;
};

EValidationResult ValidateStaticGroup(
    const NKikimrConfig::TAppConfig& current,
    const NKikimrConfig::TAppConfig& proposed,
    std::vector<TString>& msg);

EValidationResult ValidateAuthConfig(
    const NKikimrProto::TAuthConfig& authConfig,
    std::vector<TString>& msg);

EValidationResult ValidateConfig(
    const NKikimrConfig::TAppConfig& config,
    std::vector<TString>& msg);

} // namespace NKikimr::NConfig
