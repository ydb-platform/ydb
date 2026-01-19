#include "yql_yt_yson_yt_block_iterator.h"

#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/codec/yt_codec_io.h>
#include <yql/essentials/providers/common/codec/yql_codec.h>

namespace NYql::NFmr {

TYtBlockIterator::TYtBlockIterator(
    TVector<NYT::TRawTableReaderPtr> partReaders,
    TVector<TString> keyColumns,
    TYtBlockIteratorSettings settings = {}
)
    : PartReaders_(std::move(partReaders))
    , KeyColumns_(std::move(keyColumns))
    , Settings_(std::move(settings))
{
}

TYtBlockIterator::~TYtBlockIterator() = default;

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
                out.Rows = parser.GetRows();
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
            out.Rows = parser.GetRows();
            return true;
        }
    }
}

} // namespace NYql::NFmr
