#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>
#include <ydb/core/nbs/cloud/storage/core/protos/error.pb.h>

#include <ydb/library/actors/wilson/wilson_span.h>

#include <library/cpp/threading/future/core/future.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TEndSpanWithError: public TDisableCopy
{
public:
    TEndSpanWithError(
        std::shared_ptr<NWilson::TSpan> span,
        const NProto::TError& error);

    TEndSpanWithError(TEndSpanWithError&&) = default;
    TEndSpanWithError& operator=(TEndSpanWithError&&) = default;

    ~TEndSpanWithError();

private:
    std::shared_ptr<NWilson::TSpan> Span;
    TString ErrorMessage;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TTracedPromise
{
public:
    explicit TTracedPromise(std::shared_ptr<NWilson::TSpan> span, ui8 verbosity)
        : Span(std::move(span))
        , Verbosity(verbosity)
    {}

    NThreading::TFuture<T> GetFuture() const
    {
        return Promise.GetFuture();
    }

    void SetValue(T&& value)
    {
        auto span =
            Span->CreateChild(Verbosity, "Reply", NWilson::EFlags::AUTO_END);
        Promise.SetValue(std::move(value));
    }

private:
    std::shared_ptr<NWilson::TSpan> Span;
    NThreading::TPromise<T> Promise = NThreading::NewPromise<T>();
    ui8 Verbosity;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
