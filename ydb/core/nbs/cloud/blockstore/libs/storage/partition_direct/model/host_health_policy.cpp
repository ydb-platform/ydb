#include "host_health_policy.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

class TDefaultHostHealthPolicy: public IHostHealthPolicy
{
public:
    explicit TDefaultHostHealthPolicy(TOracleConfigPtr config);

    [[nodiscard]] EHostHealth GetNewHealth(
        EHostHealth health,
        const THostErrorsInfo& stats,
        ui64 errorsTotalSize) const override;

private:
    [[nodiscard]] EHostHealth NewHealthFromOnline(
        const THostErrorsInfo& stats,
        ui64 errorsTotalSize) const;

    [[nodiscard]] EHostHealth NewHealthFromTemporaryOffline(
        const THostErrorsInfo& stats,
        ui64 errorsTotalSize) const;

    [[nodiscard]] bool HasRecovered(const THostErrorsInfo& stats) const;

    [[nodiscard]] bool IsDownByStats(
        const THostErrorsInfo& stats,
        ui64 errorsTotalSize) const;

    const TOracleConfigPtr Config;
};

TDefaultHostHealthPolicy::TDefaultHostHealthPolicy(TOracleConfigPtr config)
    : Config(config)
{}

EHostHealth TDefaultHostHealthPolicy::GetNewHealth(
    const EHostHealth health,
    const THostErrorsInfo& stats,
    const ui64 errorsTotalSize) const
{
    switch (health) {
        case EHostHealth::Online:
        case EHostHealth::Sufferer:
            return NewHealthFromOnline(stats, errorsTotalSize);
        case EHostHealth::TemporaryOffline:
            return NewHealthFromTemporaryOffline(stats, errorsTotalSize);
        case EHostHealth::Offline:
            return HasRecovered(stats) ? EHostHealth::Online
                                       : EHostHealth::Offline;
        case EHostHealth::Broken:
            // Broken state is irrecoverable
            return EHostHealth::Broken;
    }
}

EHostHealth TDefaultHostHealthPolicy::NewHealthFromOnline(
    const THostErrorsInfo& stats,
    const ui64 errorsTotalSize) const
{
    if (IsDownByStats(stats, errorsTotalSize)) {
        return stats.FromFirstError > Config->GetMaxDurationBeforeGoingOffline()
                   ? EHostHealth::Offline
                   : EHostHealth::TemporaryOffline;
    }

    return stats.ConsecutiveErrorCount > 0 ? EHostHealth::Sufferer
                                           : EHostHealth::Online;
}

EHostHealth TDefaultHostHealthPolicy::NewHealthFromTemporaryOffline(
    const THostErrorsInfo& stats,
    const ui64 errorsTotalSize) const
{
    if (HasRecovered(stats)) {
        return EHostHealth::Online;
    }
    if (IsDownByStats(stats, errorsTotalSize) &&
        stats.FromFirstError > Config->GetMaxDurationBeforeGoingOffline())
    {
        return EHostHealth::Offline;
    }
    return EHostHealth::TemporaryOffline;
}

bool TDefaultHostHealthPolicy::HasRecovered(const THostErrorsInfo& stats) const
{
    return stats.ConsecutiveSuccessCount >=
               Config->GetMinSuccessesCountBeforeReturningOnline() &&
           stats.FromFirstSuccess >
               Config->GetMaxDurationBeforeReturningOnline();
}

bool TDefaultHostHealthPolicy::IsDownByStats(
    const THostErrorsInfo& stats,
    const ui64 errorsTotalSize) const
{
    const bool softDowntimeByErrors =
        stats.ConsecutiveErrorCount >=
        Config->GetMinErrorsCountBeforeGoingOffline();

    const bool temporaryOfflineDelayOver =
        stats.FromFirstError >
        Config->GetMaxDurationBeforeGoingTemporaryOffline();

    const bool hardDowntimeByErrors =
        stats.ConsecutiveErrorCount >= Config->GetErrorsCountForGoingOffline();

    const bool hardDowntimeByErrorsTotalSize =
        errorsTotalSize >= Config->GetErrorsTotalSizeForGoingOffline();

    return hardDowntimeByErrorsTotalSize || hardDowntimeByErrors ||
           (softDowntimeByErrors && temporaryOfflineDelayOver);
}

std::unique_ptr<IHostHealthPolicy> CreateDefaultHostHealthPolicy(
    TOracleConfigPtr config)
{
    return std::make_unique<TDefaultHostHealthPolicy>(config);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
