#include <library/cpp/threading/future/core/future.h>

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/job_factory/interface/yql_yt_job_factory.h>

namespace NYql::NFmr {

class IFmrWorker: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrWorker>;

    virtual ~IFmrWorker() = default;

    virtual void Start() = 0;

    virtual void Stop() = 0;
};

} // namespace NYql::NFmr
