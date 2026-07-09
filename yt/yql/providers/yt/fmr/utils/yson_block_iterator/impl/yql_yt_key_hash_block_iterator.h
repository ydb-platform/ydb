#pragma once

#include <library/cpp/yson/zigzag.h>
#include <util/generic/buffer.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/utils/hasher/yql_yt_binary_yson_hasher.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

namespace NYql::NFmr {

namespace NDetail {

inline void WriteVarint64(TBuffer& buf, ui64 value) {
    do {
        ui8 byte = value & 0x7F;
        value >>= 7;
        if (value != 0) {
            byte |= 0x80;
        }
        buf.Append(static_cast<char>(byte));
    } while (value != 0);
}

inline void WriteStringLengthVarint(TBuffer& buf, i32 length) {
    ui32 encoded = NYson::ZigZagEncode32(length);
    do {
        ui8 byte = encoded & 0x7F;
        encoded >>= 7;
        if (encoded != 0) {
            byte |= 0x80;
        }
        buf.Append(static_cast<char>(byte));
    } while (encoded != 0);
}

} // namespace NDetail

// Build binary YSON row with _yql_key_hash prepended: { _yql_key_hash=<hash>; <original contents> }
// originalRowBytes must start with '{' (BeginMapSymbol).
inline TBuffer BuildRowWithKeyHash(TStringBuf originalRowBytes, ui64 hash) {
    using namespace NYson::NDetail;
    Y_ENSURE(!originalRowBytes.empty() && originalRowBytes[0] == BeginMapSymbol,
             "Expected binary YSON map row starting with '{'");

    TBuffer row;
    row.Append(BeginMapSymbol);

    row.Append(StringMarker);
    NDetail::WriteStringLengthVarint(row, static_cast<i32>(YqlKeyHashColumn.size()));
    row.Append(YqlKeyHashColumn.data(), YqlKeyHashColumn.size());
    row.Append(KeyValueSeparatorSymbol);
    row.Append(Uint64Marker);
    NDetail::WriteVarint64(row, hash);

    TStringBuf rest = originalRowBytes.substr(1);
    if (!rest.empty() && rest[0] != EndMapSymbol) {
        row.Append(ListItemSeparatorSymbol);
    }
    row.Append(rest.data(), rest.size());

    return row;
}

// Wraps an IBlockIterator and inserts the computed _yql_key_hash column into each row.
// FullSortColumns must start with _yql_key_hash; the remaining columns are the reduce key columns
// used to compute the hash.
class TKeyHashAddingBlockIterator final: public IBlockIterator {
public:
    TKeyHashAddingBlockIterator(
        IBlockIterator::TPtr inner,
        std::vector<TString> fullSortColumns,
        std::vector<ESortOrder> sortOrders
    )
        : Inner_(std::move(inner))
        , FullSortColumns_(std::move(fullSortColumns))
        , SortOrders_(std::move(sortOrders))
    {
        Y_ENSURE(!FullSortColumns_.empty() && FullSortColumns_[0] == TString(YqlKeyHashColumn),
                 "_yql_key_hash must be the first sort column");
        NumReduceKeyColumns_ = FullSortColumns_.size() - 1;
    }

    bool NextBlock(TIndexedBlock& out) final {
        TIndexedBlock raw;
        if (!Inner_->NextBlock(raw)) {
            return false;
        }
        if (raw.Rows.empty()) {
            out = std::move(raw);
            return true;
        }

        TBuffer newData;
        newData.Reserve(raw.Data.size() + raw.Rows.size() * 20);

        for (size_t rowIdx = 0; rowIdx < raw.Rows.size(); ++rowIdx) {
            const TRowIndexMarkup& markup = raw.Rows[rowIdx];
            const TColumnOffsetRange& rowRange = markup.back();
            TStringBuf rowBytes(raw.Data.data() + rowRange.StartOffset,
                                rowRange.EndOffset - rowRange.StartOffset);

            ui64 hash = HashKeyColumns(raw.Data, markup, NumReduceKeyColumns_);
            TBuffer newRow = BuildRowWithKeyHash(rowBytes, hash);

            newData.Append(newRow.Data(), newRow.Size());
            newData.Append(NYson::NDetail::ListItemSeparatorSymbol);
        }

        out.Data = TString(newData.Data(), newData.Size());
        TParserFragmentListIndex parser(out.Data, FullSortColumns_);
        parser.Parse();
        out.Rows = parser.GetRows();

        return true;
    }

    std::vector<ESortOrder> GetSortOrder() final {
        return SortOrders_;
    }

private:
    IBlockIterator::TPtr Inner_;
    std::vector<TString> FullSortColumns_;
    std::vector<ESortOrder> SortOrders_;
    size_t NumReduceKeyColumns_ = 0;
};

} // namespace NYql::NFmr
