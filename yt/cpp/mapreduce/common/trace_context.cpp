#include "trace_context.h"

#include <yt/cpp/mapreduce/interface/config.h>

namespace NYT::NTracing {

TTraceContextWrapperPtr CreateTraceContext(const std::string& name, const TConfigPtr& config)
{
    return std::make_shared<NTracing::TTraceContextWrapper>(
        config->EnableClientTracing
        ? NTracing::CreateTraceContextFromCurrent(name)
        : nullptr);
}

} // namespace NYT::NTracing
