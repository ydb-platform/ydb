#include "scan_factory.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/events/scan.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NKikimr::NSysView {
namespace {

class TUnsupportedSystemViewScan : public NActors::TActorBootstrapped<TUnsupportedSystemViewScan> {
public:
    explicit TUnsupportedSystemViewScan(const NActors::TActorId& ownerId)
        : OwnerId(ownerId)
    {}

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    void Bootstrap() {
        auto error = MakeHolder<NKqp::TEvKqpCompute::TEvScanError>();
        error->Record.SetStatus(Ydb::StatusIds::UNSUPPORTED);
        NYql::IssuesToMessage(
            NYql::TIssues({NYql::TIssue("System view scans are not supported by this server build")}),
            error->Record.MutableIssues());
        Send(OwnerId, error.Release());
        PassAway();
    }

private:
    const NActors::TActorId OwnerId;
};

} // namespace

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
    const TTableId& tableId, const TString& tablePath,
    TVector<TSerializedTableRange> ranges, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse)
{
    if (const auto* factory = AppData()->SystemViewScanFactory) {
        return factory->Create(ownerId, scanId, database, sysViewInfo, tableId, tablePath,
            std::move(ranges), columns, std::move(userToken), reverse);
    }
    return MakeHolder<TUnsupportedSystemViewScan>(ownerId);
}

THolder<NActors::IActor> CreateSystemViewScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const TMaybe<NKikimrSysView::TSysViewDescription>& sysViewInfo,
    const TTableId& tableId, const TString& tablePath,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse)
{
    if (const auto* factory = AppData()->SystemViewScanFactory) {
        return factory->Create(ownerId, scanId, database, sysViewInfo, tableId, tablePath,
            tableRange, columns, std::move(userToken), reverse);
    }
    return MakeHolder<TUnsupportedSystemViewScan>(ownerId);
}

} // namespace NKikimr::NSysView
