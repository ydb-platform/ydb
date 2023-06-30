#pragma once

#include "lenval_table_reader.h"

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

class TRawTableReader;
struct TClientContext;

////////////////////////////////////////////////////////////////////////////////

class TYaMRTableReader
    : public IYaMRReaderImpl
    , public TLenvalTableReader
{
public:
    explicit TYaMRTableReader(::TIntrusivePtr<TRawTableReader> input);
    ~TYaMRTableReader() override;

    const TYaMRRow& GetRow() const override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsEndOfStream() const override;
    bool IsRawReaderExhausted() const override;

private:
    void ReadField(TString* result, i32 length);

    void ReadRow();
    void SkipRow() override;

    TYaMRRow Row_;
    TString Key_;
    TString SubKey_;
    TString Value_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
