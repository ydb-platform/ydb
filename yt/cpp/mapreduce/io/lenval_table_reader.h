#pragma once

#include "counting_raw_reader.h"

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TLenvalTableReader
{
public:
    explicit TLenvalTableReader(::TIntrusivePtr<TRawTableReader> input);
    virtual ~TLenvalTableReader();

protected:
    bool IsValid() const;
    void Next();
    ui32 GetTableIndex() const;
    ui32 GetRangeIndex() const;
    ui64 GetRowIndex() const;
    void NextKey();
    TMaybe<size_t> GetReadByteCount() const;
    bool IsEndOfStream() const;
    bool IsRawReaderExhausted() const;

    void CheckValidity() const;

    bool Retry(const std::exception_ptr& error);

    template <class T>
    bool ReadInteger(T* result, bool acceptEndOfStream = false)
    {
        size_t count = Input_.Load(result, sizeof(T));
        if (acceptEndOfStream && count == 0) {
            Finished_ = true;
            Valid_ = false;
            return false;
        }
        Y_ENSURE(count == sizeof(T), "Premature end of stream");
        return true;
    }

    virtual void SkipRow() = 0;

protected:
    NDetail::TCountingRawTableReader Input_;

    bool Valid_ = true;
    bool Finished_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    ui32 RangeIndexShift_ = 0;
    TMaybe<ui64> TabletIndex_;
    bool IsEndOfStream_ = false;
    bool AtStart_ = true;
    bool RowTaken_ = true;
    ui32 Length_ = 0;

private:
    bool PrepareRetry(const std::exception_ptr& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
