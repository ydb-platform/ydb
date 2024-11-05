#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {
namespace NDetail {

class TCountingRawTableReader
    final : public TRawTableReader
{
public:
    TCountingRawTableReader(::TIntrusivePtr<TRawTableReader> reader)
        : Reader_(std::move(reader))
    { }

    bool Retry(
        const TMaybe<ui32>& rangeIndex,
        const TMaybe<ui64>& rowIndex,
        const std::exception_ptr& error) override;
    void ResetRetries() override;
    bool HasRangeIndices() const override;

    size_t GetReadByteCount() const;

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    ::TIntrusivePtr<TRawTableReader> Reader_;
    size_t ReadByteCount_ = 0;
};

} // namespace NDetail
} // namespace NYT
