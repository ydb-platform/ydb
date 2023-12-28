#pragma once

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

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

#define BLOG_D(stream) ALOG_DEBUG(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define BLOG_I(stream) ALOG_INFO(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define BLOG_W(stream) ALOG_WARN(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define BLOG_NOTICE(stream) ALOG_NOTICE(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define BLOG_ERROR(stream) ALOG_ERROR(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define BLOG_CRIT(stream) ALOG_CRIT(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define BLOG_TRACE(stream) ALOG_TRACE(NKikimrServices::GRAPH, GetLogPrefix() << stream)
#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { BLOG_ERROR("Failed condition \"" << #cond << "\" " << stream); }
