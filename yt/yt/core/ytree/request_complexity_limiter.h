#pragma once

#include <yt/yt/core/yson/async_writer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TReadRequestComplexity
{
    std::optional<i64> NodeCount;
    std::optional<i64> ResultSize;

    TReadRequestComplexity() noexcept = default;
    TReadRequestComplexity(std::optional<i64> nodeCount, std::optional<i64> resultSize) noexcept;
    void Sanitize(const TReadRequestComplexity& max) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

class TReadRequestComplexityUsage
    : private TReadRequestComplexity
{
public:
    friend class TReadRequestComplexityLimits;

    TReadRequestComplexityUsage() noexcept;
    explicit TReadRequestComplexityUsage(const TReadRequestComplexity& usage) noexcept;

    TReadRequestComplexityUsage& operator+=(const TReadRequestComplexityUsage& that) noexcept;

    const TReadRequestComplexity& AsComplexity() const noexcept;
};

////////////////////////////////////////////////////////////////////////////////

class TReadRequestComplexityLimits
    : private TReadRequestComplexity
{
public:
    TReadRequestComplexityLimits() noexcept = default;

    // NB: Limits must be non-negative.
    explicit TReadRequestComplexityLimits(const TReadRequestComplexity& limits) noexcept;

    TError CheckOverdraught(const TReadRequestComplexityUsage& usage) const noexcept;
};

////////////////////////////////////////////////////////////////////////////////

class TReadRequestComplexityLimiter
    : public TRefCounted
    , public TNonCopyable
{
public:
    TReadRequestComplexityLimiter() noexcept;

    void Reconfigure(const TReadRequestComplexity& limits) noexcept;

    void Charge(const TReadRequestComplexityUsage& usage) noexcept;

    TError CheckOverdraught() const noexcept;
    void ThrowIfOverdraught() const;

    TReadRequestComplexity GetUsage() const noexcept;

private:
    TReadRequestComplexityUsage Usage_;
    TReadRequestComplexityLimits Limits_;
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
