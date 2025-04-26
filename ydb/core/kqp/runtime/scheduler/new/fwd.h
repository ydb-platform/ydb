#pragma once

#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr::NKqp::NScheduler {

    namespace NHdrf {
        struct TStaticAttributes;
        struct TTreeElementBase;

        using TQueryId = ui64;

        class TQuery;
        class TPool;
        class TDatabase;
        class TRoot;

        using TTreeElementPtr = std::shared_ptr<TTreeElementBase>;
        using TQueryPtr = std::shared_ptr<TQuery>;
        using TPoolPtr = std::shared_ptr<TPool>;
        using TDatabasePtr = std::shared_ptr<TDatabase>;
        using TRootPtr = std::shared_ptr<TRoot>;
    }

    struct TSchedulableTask;
    using TSchedulableTaskPtr = THolder<TSchedulableTask>;
    using TSchedulableTaskFactory = std::function<TSchedulableTaskPtr(const NHdrf::TQueryId&)>;

} // namespace NKikimr::NKqp::NScheduler
