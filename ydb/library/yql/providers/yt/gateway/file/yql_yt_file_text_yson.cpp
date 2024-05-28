#include "yql_yt_file_text_yson.h"
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <library/cpp/yson/parser.h>
#include <util/stream/buffer.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYql::NFile {

class TTextYsonInput: public NYT::TRawTableReader {
public:
    TTextYsonInput(const TString& file, const TColumnsInfo& columnsInfo) {
        TIFStream in(file);

        TBinaryYsonWriter writer(&Input_, ::NYson::EYsonType::ListFragment);
        writer.OnBeginAttributes();
        writer.OnKeyedItem("row_index");
        writer.OnInt64Scalar(0);
        writer.OnEndAttributes();
        writer.OnEntity();
        writer.OnListItem();
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
        NYson::TYsonParser parser(consumer, &in, ::NYson::EYsonType::ListFragment);
        parser.Parse();
    }

    bool Retry(const TMaybe<ui32>& /* rangeIndex */, const TMaybe<ui64>& /* rowIndex */, const std::exception_ptr& /* error */) override {
        return false;
    }

    void ResetRetries() override {
    }

    bool HasRangeIndices() const override {
        return false;
    }

protected:
    size_t DoRead(void* buf, size_t len) override {
        return Input_.Read(buf, len);
    }

private:
    TBufferStream Input_;
};

TVector<NYT::TRawTableReaderPtr> MakeTextYsonInputs(const TVector<std::pair<TString, TColumnsInfo>>& files) {
    TVector<NYT::TRawTableReaderPtr> rawReaders;
    for (auto& file: files) {
        if (!NFs::Exists(file.first)) {
            rawReaders.emplace_back(nullptr);
            continue;
        }
        rawReaders.emplace_back(MakeIntrusive<TTextYsonInput>(file.first, file.second));
    }
    return rawReaders;
}

} //namespace NYql::NFile