#include "yql_yt_file_yt_job_service.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_row_count.h>
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/node/node_visitor.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>
#include <util/stream/length.h>
#include <util/system/file.h>

namespace NYql::NFmr {

namespace {

// Reads a byte range [byteStart, byteEnd) from a YSON text file.
// Both offsets are \n-aligned (computed by the coordinator), so the slice is a
// valid YSON list fragment. The file is seeked once and the byte range is
// streamed directly into the YSON parser via TLengthLimitedInput.
class TTextYsonRangedInput: public NYT::TRawTableReader {
public:
    TTextYsonRangedInput(
        const TString& filePath,
        const NFile::TColumnsInfo& columnsInfo,
        i64 byteStart,
        i64 byteEnd
    ) {
        YQL_ENSURE(byteStart >= 0 && byteEnd > byteStart);

        TFile file(filePath, OpenExisting | RdOnly);
        file.Seek(byteStart, sSet);
        TFileInput fileIn(file);
        TLengthLimitedInput limited(&fileIn, byteEnd - byteStart);

        TBinaryYsonWriter writer(&Input_, ::NYson::EYsonType::ListFragment);
        NYT::NYson::IYsonConsumer* consumer = &writer;
        THolder<TColumnFilteringConsumer> filter;
        if (columnsInfo.Columns || columnsInfo.RenameColumns) {
            TMaybe<TSet<TStringBuf>> columns;
            TMaybe<THashMap<TStringBuf, TStringBuf>> renames;
            if (columnsInfo.Columns) {
                columns.ConstructInPlace(
                    columnsInfo.Columns->Parts_.begin(),
                    columnsInfo.Columns->Parts_.end());
            }
            if (columnsInfo.RenameColumns) {
                renames.ConstructInPlace(
                    columnsInfo.RenameColumns->begin(),
                    columnsInfo.RenameColumns->end());
            }
            filter.Reset(new TColumnFilteringConsumer(consumer, columns, renames));
            consumer = filter.Get();
        }

        NYson::TYsonParser parser(consumer, &limited, ::NYson::EYsonType::ListFragment);
        parser.Parse();
    }

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override {
        return false;
    }

    void ResetRetries() override {}

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

class TFileYtTableWriter: public NYT::TRawTableWriter {
public:
    TFileYtTableWriter(const TString& filePath): FilePath_(filePath) {}

    void NotifyRowEnd() override {
        ++RowCount_;
    }

private:
    void DoWrite(const void* buf, size_t len) override {
        Buffer_.Append(static_cast<const char*>(buf), len);
    }

    void DoFlush() override {
        TMemoryInput input(Buffer_.data(), Buffer_.size());
        TFile outputFile(FilePath_, OpenAlways | WrOnly | ForAppend);
        TFileOutput outputFileStream(outputFile);
        TDoubleHighPrecisionYsonWriter writer(&outputFileStream, ::NYson::EYsonType::ListFragment);
        NYson::TYsonParser parser(&writer, &input, ::NYson::EYsonType::ListFragment);
        parser.Parse();
        Buffer_.Clear();
        NFile::SaveRowCountToAttr(FilePath_, RowCount_);
    }

    TString FilePath_;
    TBuffer Buffer_;
    ui64 RowCount_ = 0;
};

class TFileWriteDistributedSession: public IWriteDistributedSession {
public:
    TFileWriteDistributedSession(const TString& filePath)
        : SessionId_(CreateGuidAsString())
        , FilePath_(filePath)
    {
    }

    TString GetId() const override {
        return SessionId_;
    }

    std::vector<TString> GetCookies() const override {
        return std::vector<TString>{FilePath_};
    }

    void Finish(const std::vector<TString>& fragmentResultsYson) override {
        Y_ENSURE(fragmentResultsYson.size() == 1);

        // Fragment results are double-encoded: NodeToYsonString(TString(responseBody))
        // produces a YSON string node; unwrap it to get the actual map.
        NYT::TNode outer = NYT::NodeFromYsonString(fragmentResultsYson[0]);
        NYT::TNode fragmentNode = NYT::NodeFromYsonString(outer.AsString());
        if (fragmentNode.HasKey("row_count")) {
            NFile::SaveRowCountToAttr(FilePath_, static_cast<ui64>(fragmentNode["row_count"].AsInt64()));
        }
    }

    bool HasPingError() const override {
        return false;
    }

    TString GetPingError() const override {
        return {};
    }

private:
    TString SessionId_;
    TString FilePath_;
};

class TFileDistributedOutputStream: public NYT::IOutputStreamWithResponse {
public:
    TFileDistributedOutputStream(const TString& filePath)
        : FilePath_(filePath)
    {}

    void DoWrite(const void* buf, size_t len) {
        Buffer_.Append(static_cast<const char*>(buf), len);
    }

    // Returns a YSON map {"row_count": N} for the gateway to collect after all fragments.
    TString GetResponse() const {
        NYT::TNode result;
        result["row_count"] = static_cast<i64>(RowCount_);
        return NYT::NodeToYsonString(result);
    }

    void DoFlush() {
        TMemoryInput input(Buffer_.data(), Buffer_.size());
        TFile outputFile(FilePath_, OpenAlways | WrOnly | ForAppend);
        TFileOutput outputFileStream(outputFile);
        TDoubleHighPrecisionYsonWriter writer(&outputFileStream, ::NYson::EYsonType::ListFragment);
        auto counting = NFile::MakeRowCountingYsonConsumer(&writer, RowCount_);
        NYson::TYsonParser parser(counting.get(), &input, ::NYson::EYsonType::ListFragment);
        parser.Parse();
        Buffer_.Clear();
    }

    void Finish() {
        DoFlush();
    }

    NYT::TWriteFileFragmentResult GetWriteFragmentResult() const {
        return NYT::TWriteFileFragmentResult{};
    }

private:
    TString FilePath_;
    TBuffer Buffer_;
    ui64 RowCount_ = 0;
};

class TFileYtJobService: public NYql::NFmr::IYtJobService {
public:
    NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTablePart,
        const TClusterConnection& /*clusterConnection*/,
        const TYtReaderSettings& /*readerSettings*/
    ) override {
        TMaybe<TString> filePath = ytTablePart.FilePath;
        YQL_ENSURE(filePath.Defined(), "File path should be set for file yt reader");

        NFile::TColumnsInfo columnsInfo;
        auto ytPath = ytTablePart.RichPath;
        if (!ytPath.Columns_.Empty()) {
            TVector<TString> columns(ytPath.Columns_->Parts_.begin(), ytPath.Columns_->Parts_.end());
            columnsInfo.Columns = NYT::TSortColumns(columns);
        }

        const auto& ranges = ytPath.GetRanges();
        if (ranges.Defined() && !ranges->empty()) {
            // The coordinator stores byte offsets in the RowIndex fields of TReadRange.
            const auto& range = ranges->front();
            YQL_ENSURE(range.LowerLimit_.RowIndex_.Defined() && range.UpperLimit_.RowIndex_.Defined(),
                "File byte-range partition must have both lower and upper RowIndex set");
            const i64 byteStart = *range.LowerLimit_.RowIndex_;
            const i64 byteEnd   = *range.UpperLimit_.RowIndex_;
            return MakeIntrusive<TTextYsonRangedInput>(*filePath, columnsInfo, byteStart, byteEnd);
        }

        auto textYsonInputs = NFile::MakeTextYsonInputs({{*filePath, columnsInfo}}, false);
        return textYsonInputs[0];
    }

    NYT::TRawTableWriterPtr MakeWriter(
        const TYtTableRef& ytTable,
        const TClusterConnection& /*clusterConnection*/,
        const TYtWriterSettings& /*writerSettings*/
    ) override {
        YQL_ENSURE(ytTable.FilePath.Defined());
        return MakeIntrusive<TFileYtTableWriter>(*ytTable.FilePath);
    }

    IWriteDistributedSession::TPtr StartDistributedWriteSession(
        const TYtTableRef& ytTable,
        ui64 cookieCount,
        const TClusterConnection& clusterConnection,
        const TStartDistributedWriteOptions& options
    ) override {
        Y_UNUSED(clusterConnection);
        Y_UNUSED(options);
        Y_ENSURE(cookieCount == 1, "File distributed writer session supports only cookieCount=1 (single uploader). "
            << "Got cookieCount=" << cookieCount << "; set partition max_parts=1 for SortedUpload in file gateway.");
        TMaybe<TString> filePath = ytTable.FilePath;
        YQL_ENSURE(filePath.Defined(), "File path should be set for file distributed writer session");
        return MakeIntrusive<TFileWriteDistributedSession>(*filePath);
    }

    std::unique_ptr<NYT::IOutputStreamWithResponse> GetDistributedWriter(
        const TString& cookieYson,
        const TClusterConnection& clusterConnection
    ) override {
        Y_UNUSED(clusterConnection);
        return std::make_unique<TFileDistributedOutputStream>(cookieYson);
    }

    void Create(
        const TYtTableRef& ytTable,
        const TClusterConnection& /*clusterConnection*/,
        const NYT::TNode& attributes
    ) override {
        YQL_ENSURE(ytTable.FilePath.Defined());
        const TString& filePath = *ytTable.FilePath;
        if (!TFsPath(filePath).Exists()) {
            YQL_CLOG(DEBUG, FastMapReduce) << "File path " << filePath << " doesn't exist, creating it.";
            YQL_ENSURE(NFs::MakeDirectoryRecursive(TFsPath(filePath).Parent()));
            TFsPath(filePath).Touch();
        }
        if (!attributes.IsUndefined() && !attributes.AsMap().empty()) {
            TOFStream ofAttr(filePath + ".attr");
            NYson::TYsonWriter writer(&ofAttr, NYson::EYsonFormat::Pretty, ::NYson::EYsonType::Node);
            NYT::TNodeVisitor visitor(&writer);
            visitor.Visit(attributes);
        }
        // TODO - delete created files in DropTables() / CloseSession()
    }
};

} // namespace

IYtJobService::TPtr MakeFileYtJobService() {
    return MakeIntrusive<TFileYtJobService>();
}

} // namespace NYql::NFmr
