#pragma once

#include "counting_raw_reader.h"

#include <yt/cpp/mapreduce/interface/io.h>

#include <library/cpp/yson/public.h>

#include <util/stream/input.h>
#include <util/generic/buffer.h>
#include <util/system/event.h>
#include <util/system/thread.h>

#include <atomic>

namespace NYT {

class TRawTableReader;
class TRowBuilder;

////////////////////////////////////////////////////////////////////////////////

struct TRowElement
{
    TNode Node;
    size_t Size = 0;

    void Reset()
    {
        Node = TNode();
        Size = 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNodeTableReader
    : public INodeReaderImpl
{
public:
    explicit TNodeTableReader(::TIntrusivePtr<TRawTableReader> input);
    ~TNodeTableReader() override;

    const TNode& GetRow() const override;
    void MoveRow(TNode* result) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    i64 GetTabletIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsEndOfStream() const override;
    bool IsRawReaderExhausted() const override;

private:
    void NextImpl();
    void OnStreamError(std::exception_ptr exception, TString error);
    void CheckValidity() const;
    void PrepareParsing();
    void ParseListFragmentItem();
    void ParseFirstListFragmentItem();

private:
    NDetail::TCountingRawTableReader Input_;

    bool Valid_ = true;
    bool Finished_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    ui32 RangeIndexShift_ = 0;
    TMaybe<i64> TabletIndex_;
    bool IsEndOfStream_ = false;
    bool AtStart_ = true;

    TMaybe<TRowElement> Row_;
    TMaybe<TRowElement> NextRow_;

    THolder<TRowBuilder> Builder_;
    THolder<::NYson::TYsonListParser> Parser_;

    std::exception_ptr Exception_;
    bool NeedParseFirst_ = true;
    bool IsLast_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
