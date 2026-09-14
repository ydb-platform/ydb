#include "trace_helpers.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TEndSpanWithError::TEndSpanWithError(
    std::shared_ptr<NWilson::TSpan> span,
    const NProto::TError& error)
    : Span(std::move(span))
    , ErrorMessage(Span && *Span ? FormatError(error) : TString())
{}

TEndSpanWithError::~TEndSpanWithError()
{
    if (Span) {
        Span->EndError(std::move(ErrorMessage));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
