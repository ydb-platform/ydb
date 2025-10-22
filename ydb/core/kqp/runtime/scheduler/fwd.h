#pragma once

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/stream/output.h>

#include <list>
#include <memory>

namespace NKikimr::NKqp::NScheduler {

    namespace NHdrf {
        using TQueryId = ui64;
        using TPoolId = TString;
        using TDatabaseId = TPoolId;

        using TId = std::variant<TQueryId, TPoolId>;

        struct TStaticAttributes;

        namespace NDynamic {
            class TQuery;
            class TPool;
            class TDatabase;
            class TRoot;

            using TQueryPtr = std::shared_ptr<TQuery>;
            using TPoolPtr = std::shared_ptr<TPool>;
            using TDatabasePtr = std::shared_ptr<TDatabase>;
            using TRootPtr = std::shared_ptr<TRoot>;
        } // namespace NDynamic

        namespace NSnapshot {
            struct TTreeElement;

            class TQuery;
            class TPool;
            class TDatabase;
            class TRoot;

            using TQueryPtr = std::shared_ptr<TQuery>;
            using TPoolPtr = std::shared_ptr<TPool>;
            using TDatabasePtr = std::shared_ptr<TDatabase>;
            using TRootPtr = std::shared_ptr<TRoot>;
        } // namespace NSnapshot
    }

    struct TSchedulableTask;
    using TSchedulableTaskPtr = std::shared_ptr<TSchedulableTask>;
    using TSchedulableTaskList = std::list<std::pair<TSchedulableTaskPtr::weak_type, std::atomic<bool> /* isThrottled */>>;

    // These params are used when calculating delay for schedulable task, but are taken from the scheduler configuration.
    struct TDelayParams {
        const TDuration MaxDelay;
        const TDuration MinDelay;
        const TDuration AttemptBonus;
        const TDuration MaxRandomDelay;
    };

} // namespace NKikimr::NKqp::NScheduler

Y_DECLARE_OUT_SPEC(inline, NKikimr::NKqp::NScheduler::NHdrf::TId, out, id) {
    if (id.index() == 0) {
        out << std::get<NKikimr::NKqp::NScheduler::NHdrf::TQueryId>(id);
    } else {
        out << std::get<NKikimr::NKqp::NScheduler::NHdrf::TPoolId>(id);
    }
}
