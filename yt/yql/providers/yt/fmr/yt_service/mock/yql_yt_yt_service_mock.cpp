#include "yql_yt_yt_service_mock.h"

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

class TMockYtService: public NYql::NFmr::IYtService {
public:
    TMockYtService(const std::unordered_map<TYtTableRef, TString>& inputTables, std::unordered_map<TYtTableRef, TString>& outputTables)
        : InputTables_(inputTables), OutputTables_(outputTables) {}

    NYT::TRawTableReaderPtr MakeReader(const TYtTableRef& ytTableRef, const TClusterConnection&, const TYtReaderSettings&) override {
        YQL_ENSURE(InputTables_.contains(ytTableRef));
        return MakeIntrusive<TMockYtTableReader>(InputTables_[ytTableRef]);
    }

    NYT::TRawTableWriterPtr MakeWriter(const TYtTableRef& ytTableRef, const TClusterConnection&, const TYtWriterSettings&) override {
        if (!OutputTables_.contains(ytTableRef)) {
            OutputTables_.emplace(ytTableRef, TString());
        }
        return MakeIntrusive<TMockYtTableWriter>(OutputTables_[ytTableRef]);
    }

private:
    std::unordered_map<TYtTableRef, TString> InputTables_; // table -> textYsonContent
    std::unordered_map<TYtTableRef, TString>& OutputTables_;
};

} // namespace

IYtService::TPtr MakeMockYtService(const std::unordered_map<TYtTableRef, TString>& inputTables, std::unordered_map<TYtTableRef, TString>& outputTables) {
    return MakeIntrusive<TMockYtService>(inputTables, outputTables);
}

} // namespace NYql::NFmr
