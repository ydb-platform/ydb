#pragma once
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yql/essentials/providers/common/codec/yql_codec_buf.h>

#include <util/generic/ptr.h>

namespace NYql::NFmr {

struct TYtBlockIteratorSettings {
    ui64 BlockCount = 1; // For async reading
    ui64 BlockSize = 1ULL << 20;                    // 20b
    ui64 MaxBlockReserveBytes = 8ULL * 1024 * 1024; // 8Mb
    ui64 YsonRowReserveBytes = 64ULL * 1024;        // 64Kb
    char RowSeparator = ';';
};

class TYtBlockIterator final: public IBlockIterator {
public:
    using TPtr = TIntrusivePtr<TYtBlockIterator>;

    explicit TYtBlockIterator(
        TVector<NYT::TRawTableReaderPtr> partReaders,
        TVector<TString> keyColumns,
        TYtBlockIteratorSettings settings,
        TVector<ESortOrder> sortOrders = {},
        TMaybe<bool> isFirstRowKeysInclusive = Nothing(),
        TMaybe<TString> firstRowKeys = Nothing(),
        TMaybe<TString> lastRowKeys = Nothing()
    );

    ~TYtBlockIterator() final;

    bool NextBlock(TIndexedBlock& out) final;

private:
    bool RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const;
    TVector<TRowIndexMarkup> FilterRowsInKeyBounds(const TString& blob, const TVector<TRowIndexMarkup>& rows) const;

private:
    const TVector<NYT::TRawTableReaderPtr> PartReaders_;
    const TVector<TString> KeyColumns_;
    const TYtBlockIteratorSettings Settings_;
    TVector<ESortOrder> SortOrders_;
    ui64 CurrentPart_ = 0;

    // Streaming state for current part.
    THolder<NYql::NCommon::IBlockReader> BlockReader_;
    THolder<NYql::NCommon::TInputBuf> InputBuf_;

    TMaybe<TFmrTableKeysBoundary> FirstBound_;
    TMaybe<TFmrTableKeysBoundary> LastBound_;
    TMaybe<bool> IsFirstBoundInclusive_;
};

} // namespace NYql::NFmr
