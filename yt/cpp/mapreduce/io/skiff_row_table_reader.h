#pragma once

#include "counting_raw_reader.h"

#include <yt/cpp/mapreduce/client/skiff.h>

#include <yt/cpp/mapreduce/interface/io.h>

#include <yt/cpp/mapreduce/skiff/unchecked_parser.h>

#include <util/stream/buffered.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSkiffRowTableReader
    : public ISkiffRowReaderImpl
{
public:
    explicit TSkiffRowTableReader(
        ::TIntrusivePtr<TRawTableReader> input,
        const NSkiff::TSkiffSchemaPtr& schema,
        TVector<ISkiffRowSkipperPtr>&& skippers,
        NDetail::TCreateSkiffSchemaOptions&& options);

    ~TSkiffRowTableReader() override;

    void ReadRow(const ISkiffRowParserPtr& parser) override;

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
    bool Retry(const std::exception_ptr& error);
    void SkipRow();
    void CheckValidity() const;
    bool PrepareRetry(const std::exception_ptr& error);

private:
    NDetail::TCountingRawTableReader Input_;
    TBufferedInput BufferedInput_;
    std::optional<NSkiff::TCheckedInDebugSkiffParser> Parser_;
    TVector<ISkiffRowSkipperPtr> Skippers_;
    NDetail::TCreateSkiffSchemaOptions Options_;

    bool RowTaken_ = true;
    bool Valid_ = true;
    bool Finished_ = false;
    bool AfterKeySwitch_ = false;
    bool IsEndOfStream_ = false;

    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    ui32 RangeIndexShift_ = 0;
    ui32 TableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
