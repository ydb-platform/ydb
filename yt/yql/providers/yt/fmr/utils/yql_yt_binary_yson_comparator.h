
// yql_yt_binary_yson_comparator.h
#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>

namespace NYql::NFmr {

enum class ESortOrder {
    Ascending = 0,
    Descending = 1
};


class TBinaryYsonComparator {
public:
    TBinaryYsonComparator(
        TStringBuf blobData,
        TVector<ESortOrder> sortOrders
    )
        : BlobData_(blobData)
        , SortOrders_(std::move(sortOrders))
    {}

    int CompareYsonValues(
        TColumnOffsetRange lhsOffsetRange,
        TColumnOffsetRange rhsOffsetRange
    ) const;

    int CompareRows(
        const TRowIndexMarkup& lhsRow,
        const TRowIndexMarkup& rhsRow
    ) const;

private:
    TStringBuf BlobData_;
    TVector<ESortOrder> SortOrders_;


    struct TYsonReader {
        TStringBuf Data;
        const char* Pos;

        explicit TYsonReader(TStringBuf data)
            : Data(data)
            , Pos(data.data())
        {}

        bool HasData() const {
            return Pos < Data.data() + Data.size();
        }

        ui64 Remaining() const {
            return Data.data() + Data.size() - Pos;
        }

        char PeekByte() const {
            Y_ENSURE(HasData(), "Unexpected end of YSON data");
            return *Pos;
        }

        char ReadByte() {
            Y_ENSURE(HasData(), "Unexpected end of YSON data");
            return *Pos++;
        }

        void Skip(ui64 bytes) {
            Y_ENSURE(Remaining() >= bytes, "Not enough data to skip");
            Pos += bytes;
        }

        TStringBuf GetView(ui64 length) {
            Y_ENSURE(Remaining() >= length, "Not enough data");
            TStringBuf result(Pos, length);
            Pos += length;
            return result;
        }

        ui64 ReadVarUint64();
        i64 ReadVarInt64();
        ui32 ReadVarUint32();
        TStringBuf ReadString();
        double ReadDouble();
    };

    int CompareYsonValuesImpl(TYsonReader& lhs, TYsonReader& rhs) const;
    int CompareYsonScalars(char type, TYsonReader& lhs, TYsonReader& rhs) const;

    TYsonReader GetColumnReader(const TColumnOffsetRange& range) const {
        Y_ENSURE(range.IsValid(), "Invalid column offset range");
        Y_ENSURE(range.EndOffset <= BlobData_.size(), "Offset out of bounds");
        auto buf = TStringBuf(
            BlobData_.data() + range.StartOffset,
            range.EndOffset - range.StartOffset
        );
        return TYsonReader(buf);
    }
};

} // namespace NYql::NFmr
