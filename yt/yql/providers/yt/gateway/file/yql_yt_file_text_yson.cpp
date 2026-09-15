#include "yql_yt_file_text_yson.h"
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/stream/buffer.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYql::NFile {

// When numberOfParts is Nothing(), reads from a single file.
// When set to N, reads all .part.0 ... .part.N-1 files in order into a single stream.
class TTextYsonInput: public NYT::TRawTableReader {
public:
    TTextYsonInput(const TString& file, const TColumnsInfo& columnsInfo, bool addRowIndex,
                   TMaybe<i64> numberOfParts = Nothing()) {
        TBinaryYsonWriter writer(&Input_, ::NYson::EYsonType::ListFragment);
        if (addRowIndex) {
            writer.OnBeginAttributes();
            writer.OnKeyedItem("row_index");
            writer.OnInt64Scalar(0);
            writer.OnEndAttributes();
            writer.OnEntity();
            writer.OnListItem();
        }
        NYT::NYson::IYsonConsumer* consumer = &writer;
        THolder<TColumnFilteringConsumer> filter;
        if (columnsInfo.Columns || columnsInfo.RenameColumns) {
            TMaybe<TSet<TStringBuf>> columns;
            TMaybe<THashMap<TStringBuf, TStringBuf>> renames;
            if (columnsInfo.Columns) {
                columns.ConstructInPlace(columnsInfo.Columns->Parts_.begin(), columnsInfo.Columns->Parts_.end());
            }
            if (columnsInfo.RenameColumns) {
                renames.ConstructInPlace(columnsInfo.RenameColumns->begin(), columnsInfo.RenameColumns->end());
            }
            filter.Reset(new TColumnFilteringConsumer(consumer, columns, renames));
            consumer = filter.Get();
        }
        if (numberOfParts.Defined()) {
            for (i64 i = 0; i < *numberOfParts; ++i) {
                TIFStream in(file + ".part." + ToString(i));
                NYson::TYsonParser parser(consumer, &in, ::NYson::EYsonType::ListFragment);
                parser.Parse();
            }
        } else {
            TIFStream in(file);
            NYson::TYsonParser parser(consumer, &in, ::NYson::EYsonType::ListFragment);
            parser.Parse();
        }
    }

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override {
        return false;
    }
    void ResetRetries() override {}
    bool HasRangeIndices() const override { return false; }

protected:
    size_t DoRead(void* buf, size_t len) override {
        return Input_.Read(buf, len);
    }

private:
    TBufferStream Input_;
};

i64 ReadSplittedPartsCount(const TString& filePath) {
    const TString attrPath = filePath + ".attr";
    if (!NFs::Exists(attrPath)) {
        return 0;
    }
    const auto attrs = NYT::NodeFromYsonString(TIFStream(attrPath).ReadAll());
    if (!attrs.HasKey("splitted")) {
        return 0;
    }
    return attrs["splitted"].AsInt64();
}

TVector<NYT::TRawTableReaderPtr> MakeTextYsonInputs(const TVector<std::pair<TString, TColumnsInfo>>& files, bool addRowIndex) {
    TVector<NYT::TRawTableReaderPtr> rawReaders;
    for (auto& file: files) {
        if (!NFs::Exists(file.first)) {
            rawReaders.emplace_back(nullptr);
            continue;
        }
        const i64 numParts = ReadSplittedPartsCount(file.first);
        TMaybe<i64> numberOfParts = numParts > 0 ? TMaybe<i64>(numParts) : Nothing();
        rawReaders.emplace_back(MakeIntrusive<TTextYsonInput>(file.first, file.second, addRowIndex, numberOfParts));
    }
    return rawReaders;
}

} // namespace NYql::NFile
