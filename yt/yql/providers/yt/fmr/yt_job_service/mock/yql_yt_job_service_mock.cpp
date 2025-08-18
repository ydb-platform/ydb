#include "yql_yt_job_service_mock.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

class TMockYtTableReader: public NYT::TRawTableReader {
public:
    TMockYtTableReader(const TString& tableContent) {
        TStringStream textInputYsonStream(tableContent);
        NYson::ReformatYsonStream(&textInputYsonStream, &Stream_, NYson::EYsonFormat::Binary, ::NYson::EYsonType::ListFragment);
    }

    bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) override {
        return false;
    }

    void ResetRetries() override { }

    bool HasRangeIndices() const override {
        return false;
    }

private:
    size_t DoRead(void* buf, size_t len) override {
        return Stream_.Read(buf, len);
    }

    TStringStream Stream_;
};

class TMockYtTableWriter: public NYT::TRawTableWriter {
public:
    TMockYtTableWriter(TString& tableConent): TableContent_(tableConent) {}

    void NotifyRowEnd() override {
        DoFlush();
    }

private:
    void DoWrite(const void* buf, size_t len) override {
        Buffer_.Append(static_cast<const char*>(buf), len);
    }

    void DoFlush() override {
        TStringStream binaryYsonOutputStream;
        TStringStream textYsonOutputStream;
        binaryYsonOutputStream << TString(Buffer_.Data(), Buffer_.Size());
        NYson::ReformatYsonStream(&binaryYsonOutputStream, &textYsonOutputStream, NYson::EYsonFormat::Text, ::NYson::EYsonType::ListFragment);
        TableContent_ += textYsonOutputStream.ReadAll();
        Buffer_.Clear();
    }

    TString& TableContent_;
    TBuffer Buffer_;
};

class TMockYtJobService: public NYql::NFmr::IYtJobService {
public:
    TMockYtJobService(const std::unordered_map<TString, TString>& inputTables, std::unordered_map<TString, TString>& outputTables)
        : InputTables_(inputTables), OutputTables_(outputTables) {}

    virtual NYT::TRawTableReaderPtr MakeReader(
        const TYtTableRef& ytTablePart,
        const TClusterConnection& /*clusterConnection*/,
        const TYtReaderSettings& /*settings*/
    ) override {
        auto richPath = ytTablePart.RichPath;
        TString richPathStr = NYT::NodeToCanonicalYsonString(NYT::PathToNode(richPath));
        YQL_ENSURE(InputTables_.contains(richPathStr));
        return {MakeIntrusive<TMockYtTableReader>(InputTables_[richPathStr])};
    }

    NYT::TRawTableWriterPtr MakeWriter(const TYtTableRef& ytTableRef, const TClusterConnection&, const TYtWriterSettings&) override {
        TString serializedRichPath = SerializeRichPath(ytTableRef.RichPath);
        if (!OutputTables_.contains(serializedRichPath)) {
            OutputTables_.emplace(serializedRichPath, TString());
        }
        return MakeIntrusive<TMockYtTableWriter>(OutputTables_[serializedRichPath]);
    }

private:
    std::unordered_map<TString, TString> InputTables_; // serialized rich yt path in string form -> total textYsonContent of it
    std::unordered_map<TString, TString>& OutputTables_;
};

} // namespace

IYtJobService::TPtr MakeMockYtJobService(const std::unordered_map<TString, TString>& inputTables, std::unordered_map<TString, TString>& outputTables) {
    return MakeIntrusive<TMockYtJobService>(inputTables, outputTables);
}

} // namespace NYql::NFmr
