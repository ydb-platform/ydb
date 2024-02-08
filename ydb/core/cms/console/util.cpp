#include "util.h"

#include <util/random/random.h>

#include <ydb/core/protos/console_config.pb.h>

namespace NKikimr::NConsole {

NTabletPipe::TClientRetryPolicy FastConnectRetryPolicy() {
    auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
    policy.MaxRetryTime = TDuration::MilliSeconds(1000 + RandomNumber<ui32>(1000)); // 1-2 seconds
    return policy;
}

TString KindsToString(const TDynBitMap &kinds) {
    TStringStream ss;
    bool first = true;
    Y_FOR_EACH_BIT(kind, kinds) {
        ss << (first ? "" : ", ") << static_cast<NKikimrConsole::TConfigItem::EKind>(kind);
        first = false;
    }
    return ss.Str();
}

TString KindsToString(const THashSet<ui32> &kinds) {
    TStringStream ss;
    bool first = true;
    for (auto kind : kinds) {
        ss << (first ? "" : ", ") << static_cast<NKikimrConsole::TConfigItem::EKind>(kind);
        first = false;
    }
    return ss.Str();
}

TString KindsToString(const TVector<ui32> &kinds) {
    TStringStream ss;
    bool first = true;
    for (auto kind : kinds) {
        ss << (first ? "" : ", ") << static_cast<NKikimrConsole::TConfigItem::EKind>(kind);
        first = false;
    }
    return ss.Str();
}

TDynBitMap KindsToBitMap(const TVector<ui32> &kinds)
{
    TDynBitMap result;
    for (auto &kind : kinds)
        result.Set(kind);

    return result;
}

TDynBitMap KindsToBitMap(const THashSet<ui32> &kinds)
{
    TDynBitMap result;
    for (auto &kind : kinds)
        result.Set(kind);

    return result;
}

void ReplaceConfigItems(
    const NKikimrConfig::TAppConfig &from,
    NKikimrConfig::TAppConfig &to,
    const TDynBitMap &kinds,
    const NKikimrConfig::TAppConfig &fallback,
    bool cleanupFallback)
{
    NKikimrConfig::TAppConfig fromCopy = from;
    NKikimrConfig::TAppConfig fallbackCopy = fallback;

    auto *desc = to.GetDescriptor();
    auto *reflection = to.GetReflection();

    for (int i = 0; i < desc->field_count(); i++) {
        auto *field = desc->field(i);
        auto tag = field->number();
        if (field && !field->is_repeated()) {
            if (kinds.Test(tag)) {
                if (reflection->HasField(to, field)) {
                    reflection->ClearField(&to, field);
                }
                if (reflection->HasField(fromCopy, field)) {
                    reflection->ClearField(&fallbackCopy, field);
                }
            } else {
                if (reflection->HasField(fromCopy, field)) {
                    reflection->ClearField(&fromCopy, field);
                }
                if (cleanupFallback) {
                    reflection->ClearField(&fallbackCopy, field);
                }
            }
        } else {
            reflection->ClearField(&to, field);
            reflection->ClearField(&fromCopy, field);
        }
    }

    to.MergeFrom(fromCopy);
    to.MergeFrom(fallbackCopy);
}

bool CompareConfigs(const NKikimrConfig::TAppConfig &lhs, const NKikimrConfig::TAppConfig &rhs)
{
    return lhs.SerializeAsString() == rhs.SerializeAsString();
}

bool CompareConfigs(const NKikimrConfig::TAppConfig &lhs, const NKikimrConfig::TAppConfig &rhs, const TDynBitMap &kinds)
{
    NKikimrConfig::TAppConfig lhsTrunc;
    ReplaceConfigItems(lhs, lhsTrunc, kinds);

    NKikimrConfig::TAppConfig rhsTrunc;
    ReplaceConfigItems(rhs, rhsTrunc, kinds);

    return CompareConfigs(rhsTrunc, lhsTrunc);
}

NKikimrConfig::TConfigVersion FilterVersion(
    const NKikimrConfig::TConfigVersion &version,
    const TDynBitMap &kinds)
{
    NKikimrConfig::TConfigVersion result;

    for (auto &item : version.GetItems()) {
        if (kinds.Test(item.GetKind())) {
            result.AddItems()->CopyFrom(item);
        }
    }

    return result;
}

} // namespace NKikimr::NConsole
