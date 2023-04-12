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

} // namespace NKikimr::NConsole
