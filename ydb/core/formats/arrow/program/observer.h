#pragma once

#include "abstract.h"

#include <ydb/core/formats/arrow/accessor/abstract/decode_observer.h>

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {

    class IExecutionObserver {
    public:
        virtual ~IExecutionObserver() = default;
        virtual NAccessor::IDecodeObserver* GetDecodeObserver() const noexcept = 0;

        virtual void OnEnter(ui64, const IResourceProcessor&, const TAccessorsCollection&) {
        }

        virtual void OnExecute(ui64, const TAccessorsCollection&) {
        }

        virtual void OnResult(ui64, const TAccessorsCollection&, const TConclusion<TExecutionResult>&) {
        }

        virtual void OnExit(ui64, const TAccessorsCollection&, bool) {
        }

        virtual void OnException(ui64) {
        }
    };

} // namespace NKikimr::NArrow::NSSA::NGraph::NExecution
