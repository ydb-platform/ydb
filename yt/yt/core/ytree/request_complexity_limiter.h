#pragma once

#include "request_complexity_limits.h"

#include <yt/yt/core/yson/async_writer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TReadRequestComplexityLimiter
    : public TRefCounted
    , public TNonCopyable
{
public:
    explicit TReadRequestComplexityLimiter(TReadRequestComplexity limits) noexcept;

    void Charge(TReadRequestComplexity usage) noexcept;

    TError CheckOverdraught() const noexcept;
    void ThrowIfOverdraught() const;

    struct TReadRequestComplexityUsage
    {
        std::atomic<i64> NodeCount;
    };
    DEFINE_BYVAL_RO_PROPERTY(TReadRequestComplexity, Usage);

private:
    const TReadRequestComplexity Limits_;
};

DEFINE_REFCOUNTED_TYPE(TReadRequestComplexityLimiter)

////////////////////////////////////////////////////////////////////////////////

class TLimitedAsyncYsonWriter
    : public NYson::IAsyncYsonConsumer
    , private TNonCopyable
{
public:
    explicit TLimitedAsyncYsonWriter(TReadRequestComplexityLimiterPtr complexityLimiter);

    void OnStringScalar(TStringBuf value) override;
    void OnInt64Scalar(i64 value) override;
    void OnUint64Scalar(ui64 value) override;
    void OnDoubleScalar(double value) override;
    void OnBooleanScalar(bool value) override;
    void OnEntity() override;
    void OnBeginList() override;
    void OnListItem() override;
    void OnEndList() override;
    void OnBeginMap() override;
    void OnKeyedItem(TStringBuf key) override;
    void OnEndMap() override;
    void OnBeginAttributes() override;
    void OnEndAttributes() override;
    void OnRaw(TStringBuf yson, NYson::EYsonType type) override;
    void OnRaw(TFuture<NYson::TYsonString> asyncStr) override;

    TFuture<NYson::TYsonString> Finish();

protected:
    NYson::TAsyncYsonWriter UnderlyingWriter_;
    TReadRequestComplexityLimiterPtr const ComplexityLimiter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
