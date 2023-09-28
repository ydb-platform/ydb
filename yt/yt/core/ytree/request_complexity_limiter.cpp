#include "request_complexity_limiter.h"

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TReadRequestComplexityLimiter::TReadRequestComplexityLimiter(TReadRequestComplexity limits) noexcept
    : Limits_(limits)
{ }

void TReadRequestComplexityLimiter::Charge(TReadRequestComplexity usage) noexcept
{
    Usage_.NodeCount += usage.NodeCount;
    Usage_.ResultSize += usage.ResultSize;
}

TError TReadRequestComplexityLimiter::CheckOverdraught() const noexcept
{
    TError error;

    auto doCheck = [&] (TStringBuf fieldName, i64 limit, i64 usage) {
        if (limit < usage) {
            error.SetCode(NYT::EErrorCode::Generic);
            error = error
                << TErrorAttribute(Format("%v_usage", fieldName), usage)
                << TErrorAttribute(Format("%v_limit", fieldName), limit);
            error.SetMessage("Read request complexity limits exceeded");
        }
    };

    doCheck("node_count", Limits_.NodeCount, Usage_.NodeCount);
    doCheck("result_size", Limits_.ResultSize, Usage_.ResultSize);

    return error;
}

void TReadRequestComplexityLimiter::ThrowIfOverdraught() const
{
    if (auto error = CheckOverdraught(); !error.IsOK()) {
        THROW_ERROR error;
    }
}

////////////////////////////////////////////////////////////////////////////////

TLimitedAsyncYsonWriter::TLimitedAsyncYsonWriter(TReadRequestComplexityLimiterPtr complexityLimiter)
    : ComplexityLimiter_(std::move(complexityLimiter))
{ }

template <bool CountNodes, class... Args>
void DoOnSomething(
    TWeakPtr<TReadRequestComplexityLimiter> weakLimiter,
    TAsyncYsonWriter& writer,
    void (TAsyncYsonWriter::*onSomething)(Args...),
    Args... values)
{
    auto writtenBefore = writer.GetTotalWrittenSize();
    (writer.*onSomething)(Args(values)...);
    if (auto limiter = weakLimiter.Lock()) {
        limiter->Charge({
            .NodeCount = CountNodes ? 1 : 0,
            .ResultSize = writer.GetTotalWrittenSize() - writtenBefore,
        });
        limiter->ThrowIfOverdraught();
    }
}

void TLimitedAsyncYsonWriter::OnStringScalar(TStringBuf value)
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnStringScalar, value);
}

void TLimitedAsyncYsonWriter::OnInt64Scalar(i64 value)
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnInt64Scalar, value);
}

void TLimitedAsyncYsonWriter::OnUint64Scalar(ui64 value)
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnUint64Scalar, value);
}

void TLimitedAsyncYsonWriter::OnDoubleScalar(double value)
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnDoubleScalar, value);
}

void TLimitedAsyncYsonWriter::OnBooleanScalar(bool value)
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnBooleanScalar, value);
}

void TLimitedAsyncYsonWriter::OnEntity()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnEntity);
}

void TLimitedAsyncYsonWriter::OnBeginList()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnBeginList);
}

void TLimitedAsyncYsonWriter::OnListItem()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnListItem);
}

void TLimitedAsyncYsonWriter::OnEndList()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnEndList);
}

void TLimitedAsyncYsonWriter::OnBeginMap()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnBeginMap);
}

void TLimitedAsyncYsonWriter::OnKeyedItem(TStringBuf value)
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnKeyedItem, value);
}

void TLimitedAsyncYsonWriter::OnEndMap()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnEndMap);
}

void TLimitedAsyncYsonWriter::OnBeginAttributes()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnBeginAttributes);
}

void TLimitedAsyncYsonWriter::OnEndAttributes()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnEndAttributes);
}

void TLimitedAsyncYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnRaw, std::move(yson), type);
}

void TLimitedAsyncYsonWriter::OnRaw(TFuture<TYsonString> asyncStr)
{
    if (ComplexityLimiter_) {
        ComplexityLimiter_->ThrowIfOverdraught();
        asyncStr.Subscribe(BIND(
            [weakLimiter = TWeakPtr(ComplexityLimiter_)] (const TErrorOr<TYsonString>& str) {
                if (str.IsOK()) {
                    if (auto limiter = weakLimiter.Lock()) {
                        limiter->Charge({
                            .NodeCount = 0,
                            .ResultSize = std::ssize(str.Value().AsStringBuf())
                        });
                    }
                }
            }));
    }
    UnderlyingWriter_.OnRaw(std::move(asyncStr));
}

TFuture<TYsonString> TLimitedAsyncYsonWriter::Finish()
{
    return UnderlyingWriter_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
