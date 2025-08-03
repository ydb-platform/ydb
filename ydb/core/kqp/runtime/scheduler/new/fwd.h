#pragma once

#include <util/generic/ptr.h>
#include <util/stream/output.h>

#include <memory>

namespace NKikimr::NKqp::NScheduler {

    namespace NHdrf {
        using TQueryId = ui64;
        using TPoolId = TString;
        using TDatabaseId = TPoolId;

        using TId = std::variant<TQueryId, TPoolId>;

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

} // namespace NKikimr::NKqp::NScheduler

Y_DECLARE_OUT_SPEC(inline, NKikimr::NKqp::NScheduler::NHdrf::TId, out, id) {
    if (id.index() == 0) {
        out << std::get<NKikimr::NKqp::NScheduler::NHdrf::TQueryId>(id);
    } else {
        out << std::get<NKikimr::NKqp::NScheduler::NHdrf::TPoolId>(id);
    }
}
