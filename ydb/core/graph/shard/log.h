#pragma once

#include <util/generic/string.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NGraph {

TString GetLogPrefix();

template <typename T>
class TTransactionBase : public NKikimr::NTabletFlatExecutor::TTransactionBase<T> {
protected:
    using TSelf = T;
    using TBase = TTransactionBase<T>;

public:
    TTransactionBase(T* self)
        : NKikimr::NTabletFlatExecutor::TTransactionBase<T>(self)
    {}

    TString GetLogPrefix() const {
        return NKikimr::NTabletFlatExecutor::TTransactionBase<T>::Self->GetLogPrefix();
    }
};

}
}

#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { ALOG_ERROR(NKikimrServices::GRAPH, GetLogPrefix() << "Failed condition \"" << #cond << "\" " << stream); }
