
#pragma once
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>

namespace NYql::NFmr {

struct TIndexedBlock {
    TString Data;
    std::vector<TRowIndexMarkup> Rows;

    Y_FORCE_INLINE TStringBuf GetRowBytes(ui64 rowIndex) const {
        const auto& markup = Rows[rowIndex];
        auto boundary = markup.back();
        if (boundary.EndOffset < Data.size() && Data[boundary.EndOffset] == ';') {
            ++boundary.EndOffset;
        }
        return SliceRange(Data, boundary);
    }
};

class IBlockIterator: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IBlockIterator>;

    virtual ~IBlockIterator() = default;

    virtual bool NextBlock(TIndexedBlock& out) = 0;

    virtual std::vector<ESortOrder> GetSortOrder() = 0;
};

} // namespace NYql::NFmr
