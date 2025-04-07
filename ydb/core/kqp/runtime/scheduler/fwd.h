#pragma once

#include <memory>

namespace NKikimr::NKqp::NScheduler {

    struct TTreeElementBase;
    class TQuery;
    class TPool2;
    class TRoot;
    class TTree;

    using TTreeElementPtr = std::shared_ptr<TTreeElementBase>;
    using TQueryPtr = std::shared_ptr<TQuery>;
    using TPoolPtr = std::shared_ptr<TPool2>;
    using TRootPtr = std::shared_ptr<TRoot>;
    using TTreePtr = std::shared_ptr<TTree>;

} // namespace NKikimr::NKqp::NScheduler
