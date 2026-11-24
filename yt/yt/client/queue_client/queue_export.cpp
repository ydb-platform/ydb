#include "queue_export.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

TQueueExportSchedule::TQueueExportSchedule(TQueueExportCronExpression cronExpression)
    : Schedule_(std::move(cronExpression))
{ }

TQueueExportSchedule::TQueueExportSchedule(TDuration period)
    : Schedule_(period)
{ }

EQueueExportScheduleType TQueueExportSchedule::GetType() const
{
    return std::holds_alternative<TQueueExportCronExpression>(Schedule_)
        ? EQueueExportScheduleType::CronExpression
        : EQueueExportScheduleType::Period;
}

TQueueExportCronExpression TQueueExportSchedule::GetCronExpression() const
{
    return std::get<TQueueExportCronExpression>(Schedule_);
}

TDuration TQueueExportSchedule::GetPeriod() const
{
    return std::get<TDuration>(Schedule_);
}

void FormatValue(TStringBuilderBase* builder, const TQueueExportSchedule& schedule, TStringBuf /*spec*/)
{
    auto type = schedule.GetType();
    switch (type) {
        case EQueueExportScheduleType::CronExpression:
            builder->AppendFormat("{Type: %v, CronExpression: %v}", type, schedule.GetCronExpression());
            break;
        case EQueueExportScheduleType::Period:
            builder->AppendFormat("{Type: %v, Period: %v}", type, schedule.GetPeriod());
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
