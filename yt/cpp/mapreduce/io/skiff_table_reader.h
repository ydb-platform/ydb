#pragma once

#include "counting_raw_reader.h"

#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/cpp/mapreduce/skiff/wire_type.h>
#include <yt/cpp/mapreduce/skiff/unchecked_parser.h>

#include <util/stream/buffered.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TSkiffTableReader
    : public INodeReaderImpl
{
public:
    TSkiffTableReader(
        ::TIntrusivePtr<TRawTableReader> input,
        const std::shared_ptr<NSkiff::TSkiffSchema>& schema);
    ~TSkiffTableReader() override;

    virtual const TNode& GetRow() const override;
    virtual void MoveRow(TNode* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;
    TMaybe<size_t> GetReadByteCount() const override;
    bool IsRawReaderExhausted() const override;

private:
    struct TSkiffTableSchema;

private:
    void EnsureValidity() const;
    void ReadRow();
    static TVector<TSkiffTableSchema> CreateSkiffTableSchemas(const std::shared_ptr<NSkiff::TSkiffSchema>& schema);

private:
    NDetail::TCountingRawTableReader Input_;
    TBufferedInput BufferedInput_;
    std::optional<NSkiff::TUncheckedSkiffParser> Parser_;
    TVector<TSkiffTableSchema> Schemas_;

    TNode Row_;

    bool Valid_ = true;
    bool AfterKeySwitch_ = false;
    bool Finished_ = false;
    TMaybe<ui64> RangeIndex_;
    ui64 RangeIndexShift_ = 0;
    TMaybe<ui64> RowIndex_;
    ui32 TableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
