#include "kqp_schedulable_work_factory.h"

#include "kqp_schedulable_base.h"
#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

namespace {
    NYql::NDq::TWorkScope ToWorkScope(const NHdrf::TFullPoolId& fullPoolId) {
        return {
            .Namespace = fullPoolId.DatabaseId,
            .Name = fullPoolId.PoolId,
        };
    }

    // Converts TSchedulableBase to the DQ layer
    class TDqSchedulableWork final : public NYql::NDq::IDqSchedulableWork {
    public:
        explicit TDqSchedulableWork(const TSchedulableOptions& options)
            : Impl(options)
        {}

        std::optional<TDuration> TryStartExecution(TMonotonic now) final {
            return Impl.TryStartExecution(now);
        }

        void StopExecution() final {
            Impl.StopExecution();
        }

        void NotifyResumed(bool byScheduler) final {
            Impl.NotifyResumed(byScheduler);
        }

        void RegisterForResume(const NActors::TActorId& actorId) final {
            Impl.RegisterForResume(actorId);
        }

        NYql::NDq::TWorkScope GetWorkScope() const final {
            return ToWorkScope(Impl.GetFullPoolId());
        }

    private:
        TSchedulableBase Impl;
    };

} // namespace

TSchedulableWorkFactory::TSchedulableWorkFactory(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable)
    : Query(std::move(query))
    , IsSchedulable(isSchedulable)
{
    Y_ENSURE(Query);
}

std::unique_ptr<NYql::NDq::IDqSchedulableWork> TSchedulableWorkFactory::CreateSchedulableWork() {
    return std::make_unique<TDqSchedulableWork>(TSchedulableOptions{
        .Query = Query,
        .IsSchedulable = IsSchedulable,
    });
}

NYql::NDq::TWorkScope TSchedulableWorkFactory::GetWorkScope() const {
    return ToWorkScope(Query->GetFullPoolId());
}

} // namespace NKikimr::NKqp::NScheduler
