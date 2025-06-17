#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr::NKqp::NScheduler {

    namespace NHdrf {
        using TQueryId = ui64;

        struct TStaticAttributes;

        namespace NDynamic {
            struct TTreeElementBase;

            class TQuery;
            class TPool;
            class TDatabase;
            class TRoot;

            using TTreeElementPtr = std::shared_ptr<TTreeElementBase>;
            using TQueryPtr = std::shared_ptr<TQuery>;
            using TPoolPtr = std::shared_ptr<TPool>;
            using TDatabasePtr = std::shared_ptr<TDatabase>;
            using TRootPtr = std::shared_ptr<TRoot>;
        } // namespace NDynamic

        namespace NSnapshot {
            struct TTreeElementBase;

            class TQuery;
            class TPool;
            class TDatabase;
            class TRoot;

            using TTreeElementPtr = std::shared_ptr<TTreeElementBase>;
            using TQueryPtr = std::shared_ptr<TQuery>;
            using TPoolPtr = std::shared_ptr<TPool>;
            using TDatabasePtr = std::shared_ptr<TDatabase>;
            using TRootPtr = std::shared_ptr<TRoot>;
        } // namespace NSnapshot
    }

    struct TSchedulableTask;
    using TSchedulableTaskPtr = THolder<TSchedulableTask>;
    using TSchedulableTaskFactory = std::function<TSchedulableTaskPtr(const NHdrf::TQueryId&)>;

} // namespace NKikimr::NKqp::NScheduler
