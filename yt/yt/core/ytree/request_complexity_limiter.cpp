#include "request_complexity_limiter.h"

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void VerifyNonNegative(const TReadRequestComplexity& complexity) noexcept
{
    YT_VERIFY(complexity.NodeCount.value_or(0) >= 0);
    YT_VERIFY(complexity.ResultSize.value_or(0) >= 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TReadRequestComplexity::TReadRequestComplexity(std::optional<i64> nodeCount, std::optional<i64> resultSize) noexcept
    : NodeCount(std::move(nodeCount))
    , ResultSize(std::move(resultSize))
{
    VerifyNonNegative(*this);
}

void TReadRequestComplexity::Sanitize(const TReadRequestComplexity& max) noexcept
{
    auto applyMax = [] (std::optional<i64> value, std::optional<i64> maxValue) {
        return (value.has_value() && maxValue.has_value())
            ? std::optional(std::min(*value, *maxValue))
            : value;
    };
    NodeCount = applyMax(NodeCount, max.NodeCount);
    ResultSize = applyMax(ResultSize, max.ResultSize);
}

////////////////////////////////////////////////////////////////////////////////

TReadRequestComplexityUsage::TReadRequestComplexityUsage() noexcept
    : TReadRequestComplexity(0, 0)
{ }

TReadRequestComplexityUsage::TReadRequestComplexityUsage(const TReadRequestComplexity& usage) noexcept
    : TReadRequestComplexity(usage)
{
    VerifyNonNegative(usage);
}

TReadRequestComplexityUsage& TReadRequestComplexityUsage::operator+=(const TReadRequestComplexityUsage& that) noexcept
{
    auto add = [] (std::optional<i64> target, std::optional<i64> source) {
        if (source.has_value()) {
            target = target.value_or(0) + source.value();
        }
        return target;
    };

    NodeCount = add(NodeCount, that.NodeCount);
    ResultSize = add(ResultSize, that.ResultSize);
    return *this;
}

const TReadRequestComplexity& TReadRequestComplexityUsage::AsComplexity() const noexcept
{
    return static_cast<const TReadRequestComplexity&>(*this);
}

////////////////////////////////////////////////////////////////////////////////

TReadRequestComplexityLimits::TReadRequestComplexityLimits(
    const TReadRequestComplexity& limits) noexcept
    : TReadRequestComplexity(limits)
{
    VerifyNonNegative(limits);
}

TError TReadRequestComplexityLimits::CheckOverdraught(
    const TReadRequestComplexityUsage& usage) const noexcept
{
    TError error;

    auto checkField = [&] (TStringBuf fieldName, std::optional<i64> limit, std::optional<i64> usage) {
        if (limit.has_value() && usage.value_or(0) > *limit) {
            error.SetCode(NYT::EErrorCode::Generic);
            error = error << TErrorAttribute(Format("%v_usage", fieldName), *usage);
            error = error << TErrorAttribute(Format("%v_limit", fieldName), *limit);
        }
    };

    checkField("node_count", NodeCount, usage.NodeCount);
    checkField("result_size", ResultSize, usage.ResultSize);

    if (!error.IsOK()) {
        error.SetMessage("Read complexity limit exceeded");
    }

    return error;
}

////////////////////////////////////////////////////////////////////////////////

TReadRequestComplexityLimiter::TReadRequestComplexityLimiter() noexcept = default;

void TReadRequestComplexityLimiter::Reconfigure(const TReadRequestComplexity& limits) noexcept
{
    Limits_ = TReadRequestComplexityLimits(limits);
}

void TReadRequestComplexityLimiter::Charge(const TReadRequestComplexityUsage& usage) noexcept
{
    Usage_ += usage;
}

TError TReadRequestComplexityLimiter::CheckOverdraught() const noexcept
{
    return Limits_.CheckOverdraught(Usage_);
}

void TReadRequestComplexityLimiter::ThrowIfOverdraught() const
{
    if (auto error = CheckOverdraught(); !error.IsOK()) {
        THROW_ERROR error;
    }
}

TReadRequestComplexity TReadRequestComplexityLimiter::GetUsage() const noexcept
{
    return Usage_.AsComplexity();
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
    if (auto limiter = weakLimiter.Lock()) {
        auto writtenBefore = writer.GetTotalWrittenSize();
        (writer.*onSomething)(values...);
        if (limiter) {
            limiter->Charge(TReadRequestComplexityUsage({
                /*nodeCount*/ CountNodes ? 1 : 0,
                /*resultSize*/ writer.GetTotalWrittenSize() - writtenBefore,
            }));
            limiter->ThrowIfOverdraught();
        }
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
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnEntity);
}

void TLimitedAsyncYsonWriter::OnBeginList()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnBeginList);
}

void TLimitedAsyncYsonWriter::OnListItem()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnListItem);
}

void TLimitedAsyncYsonWriter::OnEndList()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnEndList);
}

void TLimitedAsyncYsonWriter::OnBeginMap()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnBeginMap);
}

void TLimitedAsyncYsonWriter::OnKeyedItem(TStringBuf value)
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnKeyedItem, value);
}

void TLimitedAsyncYsonWriter::OnEndMap()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnEndMap);
}

void TLimitedAsyncYsonWriter::OnBeginAttributes()
{
    DoOnSomething<true>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnBeginAttributes);
}

void TLimitedAsyncYsonWriter::OnEndAttributes()
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_,&TAsyncYsonWriter::OnEndAttributes);
}

void TLimitedAsyncYsonWriter::OnRaw(TStringBuf yson, EYsonType type)
{
    DoOnSomething<false>(ComplexityLimiter_, UnderlyingWriter_, &TAsyncYsonWriter::OnRaw, std::move(yson), type);
}

void TLimitedAsyncYsonWriter::OnRaw(TFuture<TYsonString> asyncStr)
{
    ComplexityLimiter_->ThrowIfOverdraught();
    asyncStr.Subscribe(BIND(
        [weakLimiter = TWeakPtr(ComplexityLimiter_)] (const TErrorOr<TYsonString>& str) {
            if (auto limiter = weakLimiter.Lock()) {
                if (str.IsOK()) {
                    limiter->Charge(TReadRequestComplexityUsage({
                        /*nodeCount*/ 1,
                        /*resultSize*/ str.Value().AsStringBuf().size()
                    }));
                }
            }
        }));
    UnderlyingWriter_.OnRaw(std::move(asyncStr));
}


TFuture<TYsonString> TLimitedAsyncYsonWriter::Finish()
{
    return UnderlyingWriter_.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
