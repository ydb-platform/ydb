#include "yql_yt_yson_yt_block_iterator.h"

#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/codec/yt_codec_io.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>

namespace NYql::NFmr {

TYtBlockIterator::TYtBlockIterator(
    std::vector<NYT::TRawTableReaderPtr> partReaders,
    std::vector<TString> keyColumns,
    TYtBlockIteratorSettings settings,
    std::vector<ESortOrder> sortOrders,
    TMaybe<bool> isFirstRowKeysInclusive,
    TMaybe<TString> firstRowKeys,
    TMaybe<TString> lastRowKeys
)
    : PartReaders_(std::move(partReaders))
    , KeyColumns_(std::move(keyColumns))
    , Settings_(std::move(settings))
{
    if (sortOrders.empty()) {
        sortOrders.assign(KeyColumns_.size(), ESortOrder::Ascending);
    }
    if (sortOrders.size() != KeyColumns_.size()) {
        sortOrders.assign(KeyColumns_.size(), ESortOrder::Ascending);
    }
    SortOrders_ = std::move(sortOrders);

    if (firstRowKeys) {
        FirstBound_ = TFmrTableKeysBoundary(*firstRowKeys, KeyColumns_, SortOrders_);
        Y_ENSURE(isFirstRowKeysInclusive.Defined(), "isFirstRowKeysInclusive must be defined for First Bound");
        IsFirstBoundInclusive_ = *isFirstRowKeysInclusive;
    }
    if (lastRowKeys) {
        LastBound_ = TFmrTableKeysBoundary(*lastRowKeys, KeyColumns_, SortOrders_);
    }
}

TYtBlockIterator::~TYtBlockIterator() = default;

bool TYtBlockIterator::RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const {
    if (FirstBound_) {
        int c = CompareKeyRowsAcrossYsonBlocks(
            blob,
            row,
            FirstBound_->Row,
            FirstBound_->Markup,
            SortOrders_
        );
        if (c < 0) { // if row < first bound
            return false;
        } else if (!IsFirstBoundInclusive_ && c == 0) { // if row == first bound
            return false;
        }
    }
    if (LastBound_) {
        int c = CompareKeyRowsAcrossYsonBlocks(
            blob,
            row,
            LastBound_->Row,
            LastBound_->Markup,
            SortOrders_
        );
        if (c > 0) { // if row > last bound
            return false;
        }
    }
    return true;
}

std::vector<TRowIndexMarkup> TYtBlockIterator::FilterRowsInKeyBounds(const TString& blob, const std::vector<TRowIndexMarkup>& rows) const {
    if (!(FirstBound_ || LastBound_)) {
        return rows;
    }
    std::vector<TRowIndexMarkup> filtered;
    filtered.reserve(rows.size());
    for (const auto& r : rows) {
        if (RowInKeyBounds(blob, r)) {
            filtered.push_back(r);
        }
    }
    return filtered;
}

bool TYtBlockIterator::NextBlock(TIndexedBlock& out) {
    out = {};

    TString blockData;
    blockData.reserve(Settings_.MaxBlockReserveBytes);

    TVector<char> rowBytes;
    rowBytes.reserve(Settings_.YsonRowReserveBytes);

    while (true) {
        while (!InputBuf_) {
            if (CurrentPart_ >= PartReaders_.size()) {
                return false;
            }
            auto& reader = PartReaders_[CurrentPart_];
            BlockReader_ = NYql::MakeBlockReader(*reader, Settings_.BlockCount, Settings_.BlockSize);
            InputBuf_ = MakeHolder<NYql::NCommon::TInputBuf>(*BlockReader_, nullptr);
        }

        char cmd = 0;
        if (!InputBuf_->TryRead(cmd)) {
            InputBuf_.Reset();
            BlockReader_.Reset();
            ++CurrentPart_;

            if (!blockData.empty()) {
                TParserFragmentListIndex parser(blockData, KeyColumns_);
                parser.Parse();
                out.Data = std::move(blockData);
                const auto& rows = parser.GetRows();
                out.Rows = FilterRowsInKeyBounds(out.Data, rows);
                return true;
            }
            continue;
        }

        rowBytes.clear();

        NYql::NCommon::CopyYson(cmd, *InputBuf_, rowBytes);

        bool needBreak = false;
        if (!InputBuf_->TryRead(cmd)) {
            needBreak = true;
        } else {
            Y_ENSURE(cmd == Settings_.RowSeparator);
            rowBytes.emplace_back(cmd);
        }

        blockData.append(rowBytes.data(), rowBytes.size());

        if (blockData.size() >= Settings_.MaxBlockReserveBytes || needBreak) {
            if (needBreak) {
                InputBuf_.Reset();
                BlockReader_.Reset();
                ++CurrentPart_;
            }

            TParserFragmentListIndex parser(blockData, KeyColumns_);
            parser.Parse();
            out.Data = std::move(blockData);
            const auto& rows = parser.GetRows();
            out.Rows = FilterRowsInKeyBounds(out.Data, rows);
            return true;
        }
    }
}

} // namespace NYql::NFmr
