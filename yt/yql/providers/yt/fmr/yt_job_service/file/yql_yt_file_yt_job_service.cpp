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
#include <util/string/cast.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>

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

// Shared state for all concurrent writers targeting the same output path.
// The mutex covers only the brief .attr update; data writes to .part.N run in parallel.
struct TFileOutputState: public TThrRefBase {
    TMutex AttrMutex;
    TString AttrPath;
    std::atomic<ui64> NextPartIndex{0};
};

class TFileYtTableWriter: public NYT::TRawTableWriter {
public:
    TFileYtTableWriter(const TString& partPath, TIntrusivePtr<TFileOutputState> state)
        : PartPath_(partPath)
        , State_(std::move(state))
    {}

    void NotifyRowEnd() override {
        ++RowCount_;
    }

private:
    void DoWrite(const void* buf, size_t len) override {
        Buffer_.Append(static_cast<const char*>(buf), len);
    }

    void DoFlush() override {
        // Write data to the exclusive part file — no locking needed.
        {
            TMemoryInput input(Buffer_.data(), Buffer_.size());
            TFile outputFile(PartPath_, OpenAlways | WrOnly | ForAppend);
            TFileOutput outputFileStream(outputFile);
            TDoubleHighPrecisionYsonWriter writer(&outputFileStream, ::NYson::EYsonType::ListFragment);
            NYson::TYsonParser parser(&writer, &input, ::NYson::EYsonType::ListFragment);
            parser.Parse();
            Buffer_.Clear();
        }

        // Briefly lock only to accumulate row_count and splitted in the shared .attr.
        const ui64 flushRows = RowCount_;
        RowCount_ = 0;
        const bool firstFlush = !HasContributedPart_;
        HasContributedPart_ = true;

        TGuard<TMutex> guard(State_->AttrMutex);
        NYT::TNode attrs;
        if (NFs::Exists(State_->AttrPath)) {
            attrs = NYT::NodeFromYsonString(TFileInput(State_->AttrPath).ReadAll());
        } else {
            attrs = NYT::TNode::CreateMap();
        }
        const i64 prevRows = attrs.HasKey("row_count") ? attrs["row_count"].AsInt64() : 0;
        attrs["row_count"] = prevRows + static_cast<i64>(flushRows);
        if (firstFlush) {
            const i64 prevSplitted = attrs.HasKey("splitted") ? attrs["splitted"].AsInt64() : 0;
            attrs["splitted"] = prevSplitted + 1;
        }
        TFileOutput ofAttr(State_->AttrPath);
        ofAttr << NYT::NodeToYsonString(attrs, NYT::NYson::EYsonFormat::Pretty);
    }

    TString PartPath_;
    TIntrusivePtr<TFileOutputState> State_;
    TBuffer Buffer_;
    ui64 RowCount_ = 0;
    bool HasContributedPart_ = false;
};


class TFileWriteDistributedSession: public IWriteDistributedSession {
public:
    TFileWriteDistributedSession(const TString& filePath, ui64 cookieCount)
        : SessionId_(CreateGuidAsString())
        , FilePath_(filePath)
        , CookieCount_(cookieCount)
    {
    }

    TString GetId() const override {
        return SessionId_;
    }

    // Cookies encode "{filePath}:{partIndex}"
    std::vector<TString> GetCookies() const override {
        std::vector<TString> cookies;
        cookies.reserve(CookieCount_);
        for (ui64 i = 0; i < CookieCount_; ++i) {
            cookies.emplace_back(FilePath_ + ":" + ToString(i));
        }
        return cookies;
    }

    void Finish(const std::vector<TString>& fragmentResultsYson) override {
        Y_ENSURE(fragmentResultsYson.size() == CookieCount_);

        // Fragment results are double-encoded: NodeToYsonString(TString(responseBody))
        // produces a YSON string node; unwrap it to get the actual map.
        ui64 totalRowCount = 0;
        for (const auto& fragmentYson : fragmentResultsYson) {
            NYT::TNode outer = NYT::NodeFromYsonString(fragmentYson);
            NYT::TNode fragmentNode = NYT::NodeFromYsonString(outer.AsString());
            if (fragmentNode.HasKey("row_count")) {
                totalRowCount += static_cast<ui64>(fragmentNode["row_count"].AsInt64());
            }
        }

        // Write row_count and splitted=N together in a single attr read-modify-write.
        const TString attrPath = FilePath_ + ".attr";
        NYT::TNode attrs;
        if (NFs::Exists(attrPath)) {
            attrs = NYT::NodeFromYsonString(TFileInput(attrPath).ReadAll());
        } else {
            attrs = NYT::TNode::CreateMap();
        }
        attrs["row_count"] = static_cast<i64>(totalRowCount);
        attrs["splitted"] = static_cast<i64>(CookieCount_);
        TFileOutput ofAttr(attrPath);
        ofAttr << NYT::NodeToYsonString(attrs, NYT::NYson::EYsonFormat::Pretty);
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
    ui64 CookieCount_;
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

        // If FileName is set in the rich path, it encodes the part number of a splitted file
        // (set by the file coordinator). Derive the actual part file path from the base path
        // and fall through to apply any byte ranges to that specific part file.
        if (ytPath.FileName_.Defined()) {
            filePath = TMaybe<TString>(*filePath + ".part." + *ytPath.FileName_);
        }
        // For the no-FileName path, NFile::MakeTextYsonInputs (called below) transparently
        // handles splitted files by reading all part files in sequence.

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
        const TString& basePath = *ytTable.FilePath;

        TIntrusivePtr<TFileOutputState> state;
        ui64 partIndex;
        {
            TGuard<TMutex> guard(OutputsMutex_);
            auto it = Outputs_.find(basePath);
            if (it == Outputs_.end()) {
                auto s = MakeIntrusive<TFileOutputState>();
                s->AttrPath = basePath + ".attr";
                it = Outputs_.emplace(basePath, s).first;
            }
            state = it->second;
            partIndex = state->NextPartIndex.fetch_add(1);
        }
        return MakeIntrusive<TFileYtTableWriter>(
            basePath + ".part." + ToString(partIndex),
            std::move(state));
    }

    IWriteDistributedSession::TPtr StartDistributedWriteSession(
        const TYtTableRef& ytTable,
        ui64 cookieCount,
        const TClusterConnection& clusterConnection,
        const TStartDistributedWriteOptions& options
    ) override {
        Y_UNUSED(clusterConnection);
        Y_UNUSED(options);
        TMaybe<TString> filePath = ytTable.FilePath;
        YQL_ENSURE(filePath.Defined(), "File path should be set for file distributed writer session");
        return MakeIntrusive<TFileWriteDistributedSession>(*filePath, cookieCount);
    }

    std::unique_ptr<NYT::IOutputStreamWithResponse> GetDistributedWriter(
        const TString& cookieYson,
        const TClusterConnection& clusterConnection
    ) override {
        Y_UNUSED(clusterConnection);
        // Cookie format: "{filePath}:{partIndex}"
        const auto colonPos = cookieYson.rfind(':');
        YQL_ENSURE(colonPos != TString::npos, "Unexpected cookie format: " << cookieYson);
        const TString basePath = cookieYson.substr(0, colonPos);
        const ui64 partIndex = FromString<ui64>(cookieYson.substr(colonPos + 1));
        return std::make_unique<TFileDistributedOutputStream>(basePath + ".part." + ToString(partIndex));
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

private:
    TMutex OutputsMutex_;
    THashMap<TString, TIntrusivePtr<TFileOutputState>> Outputs_;
};

} // namespace

IYtJobService::TPtr MakeFileYtJobService() {
    return MakeIntrusive<TFileYtJobService>();
}

} // namespace NYql::NFmr
