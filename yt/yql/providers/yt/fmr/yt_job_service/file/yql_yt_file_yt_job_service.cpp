#include "yql_yt_file_yt_job_service.h"
#include <library/cpp/yson/parser.h>
#include <util/stream/file.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_text_yson.h>
#include <yt/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>

namespace NYql::NFmr {

namespace {

class TFileYtTableWriter: public NYT::TRawTableWriter {
public:
    TFileYtTableWriter(const TString& filePath): FilePath_(filePath) {}

    void NotifyRowEnd() override {
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
    }

    TString FilePath_;
    TBuffer Buffer_;
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

    virtual std::vector<TString> GetCookies() const override {
        return std::vector<TString>{FilePath_};
    }

    void Finish(const std::vector<TString>& fragmentResultsYson) override {
        Y_ENSURE(fragmentResultsYson.size() == 1);
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

    TString GetResponse() const {
        return TString();
    };

    void DoFlush() {
        TMemoryInput input(Buffer_.data(), Buffer_.size());
        TFile outputFile(FilePath_, OpenAlways | WrOnly | ForAppend);
        TFileOutput outputFileStream(outputFile);
        TDoubleHighPrecisionYsonWriter writer(&outputFileStream, ::NYson::EYsonType::ListFragment);
        NYson::TYsonParser parser(&writer, &input, ::NYson::EYsonType::ListFragment);
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
        Y_ENSURE(cookieCount == 1);
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
        const NYT::TNode& /*attributes*/
    ) override {
        YQL_ENSURE(ytTable.FilePath.Defined());
        TFsPath filePath(*ytTable.FilePath);
        if (!filePath.Exists()) {
            YQL_CLOG(DEBUG, FastMapReduce) << "File path " << filePath.GetPath() << " doesn't exist, creating it.";
            TFsPath parentDir = filePath.Parent();
            YQL_ENSURE(NFs::MakeDirectoryRecursive(parentDir));
            filePath.Touch();
        }
        // TODO - delete created files in DropTables() / CloseSession()
    }
};

} // namespace

IYtJobService::TPtr MakeFileYtJobService() {
    return MakeIntrusive<TFileYtJobService>();
}

} // namespace NYql::NFmr
