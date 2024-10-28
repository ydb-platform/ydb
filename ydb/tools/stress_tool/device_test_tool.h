#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/tools/stress_tool/proto/device_perf_test.pb.h>
#include <ydb/library/actors/core/probes.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/map.h>
#include <util/stream/str.h>


#include <cstdlib>

namespace NKikimr {

static bool IsVerbose = true;

#define VERBOSE_COUT(str) \
do { \
    if (NKikimr::IsVerbose) { \
        Cerr << str << Endl; \
    } \
} while(false)

#define TEST_COUT(str) \
do { \
    Cout << str; \
} while(false)

#define TEST_COUT_LN(str) \
    TEST_COUT(str << Endl)

class IResultPrinter : public TThrRefBase {
public:
    template<typename T>
    void AddResult(const TString& name, const T& value) {
        TStringStream str;
        str << value;
        AddResult(name, str.Str());
    }

    virtual void AddResult(const TString& name, const TString& value) = 0;
    virtual void AddGlobalParam(const TString& name, const TString& value) = 0;

    template<typename T>
    void AddGlobalParam(const TString& name, const T& value) {
        TStringStream str;
        str << value;
        AddGlobalParam(name, str.Str());
    }

    virtual void PrintResults() = 0;
    virtual void EndTest() = 0;
    virtual ~IResultPrinter() = default;
};


struct TStringPair {
    TString Name;
    TString Value;

    TStringPair(const TString& name, const TString& value) {
        Name = name;
        Value = value;
    }
};

class TResultPrinter : public IResultPrinter {
public:
    enum EOutputFormat {
        OUTPUT_FORMAT_WIKI,
        OUTPUT_FORMAT_HUMAN,
        OUTPUT_FORMAT_JSON,
    };

    EOutputFormat OutputFormat = OUTPUT_FORMAT_WIKI;
    TVector<TStringPair> GlobalParams;
    TVector<TStringPair> Results;
    bool HeaderPrinted = false;
    NJson::TJsonWriter JsonWriter;

    void PrintGlobalParams() const {
        if (GlobalParams) {
            TEST_COUT("GlobalParams# {");
            bool needComma = false;
            for (const auto& param : GlobalParams) {
                TEST_COUT((needComma ? ", " : "") << param.Name << "# " << param.Value);
                needComma = true;
            }
            TEST_COUT_LN("}");
        }
    }

    void PrintResultsWikiFormat() {
        if (!HeaderPrinted) {
            HeaderPrinted = true;
            PrintGlobalParams();
            TEST_COUT_LN("#|");
            TEST_COUT("||");
            for (const auto& counter : Results) {
                TEST_COUT(" " << counter.Name << " |");
            }
            TEST_COUT_LN("|");
        }
        TEST_COUT("||");
        for (const auto& counter : Results) {
            TEST_COUT(" " << counter.Value << " |");
        }
        TEST_COUT_LN("|");
    }

    void PrintResultsHumanFormat() {
        const ui32 screenWidth = 140;
        const ui32 columnWidth = screenWidth / Results.size() - 3; // 3 symbols is additional 2 spaces and | sign
        TStringStream formatStr;
        formatStr << " %" << columnWidth << "s |";
        const char *format = formatStr.Str().c_str();
        if (!HeaderPrinted) {
            HeaderPrinted = true;
            PrintGlobalParams();
            TEST_COUT("|");
            for (const auto& counter : Results) {
                TEST_COUT(Sprintf(format, counter.Name.c_str()));
            }
            TEST_COUT_LN("");
        }
        TEST_COUT("|");
        for (const auto& counter : Results) {
            TEST_COUT(Sprintf(format, counter.Value.c_str()));
        }
        TEST_COUT_LN("");
    }

    void PrintResultsJsonFormat() {
        NJson::TJsonValue value;
        for (const auto& param : GlobalParams) {
            value[param.Name] = param.Value;
        }
        for (const auto& counter : Results) {
            value[counter.Name] = counter.Value;
        }
        if (value.IsDefined()) {
            JsonWriter.Write(value);
        }
    }

public:
    TResultPrinter(const EOutputFormat outputFormat)
        : OutputFormat(outputFormat)
        , JsonWriter(&Cout, true)
    {
        if (OutputFormat == OUTPUT_FORMAT_JSON) {
            JsonWriter.OpenArray();
        }
    }

    void AddResult(const TString& name, const TString& value) override {
        Results.emplace_back(name, value);
    }
    void AddGlobalParam(const TString& name, const TString& value) override {
        GlobalParams.emplace_back(name, value);
    }

    void PrintResults() override {
        if (Results.empty()) {
            return;
        }

        switch(OutputFormat) {
        case OUTPUT_FORMAT_WIKI:
            PrintResultsWikiFormat();
            break;
        case OUTPUT_FORMAT_HUMAN:
            PrintResultsHumanFormat();
            break;
        case OUTPUT_FORMAT_JSON:
            PrintResultsJsonFormat();
            break;
        }
        Results.clear();
    }

    void EndTest() override {
        if (OutputFormat == OUTPUT_FORMAT_WIKI && HeaderPrinted) {
            TEST_COUT_LN("|#");
        }
        if (OutputFormat == OUTPUT_FORMAT_JSON) {
            JsonWriter.Flush();
        }
        HeaderPrinted = false;
        PrintResults();
    }

    ~TResultPrinter() {
        if (OutputFormat == OUTPUT_FORMAT_JSON) {
            JsonWriter.CloseArray();
            JsonWriter.Flush();
            Cout << Endl;
        }
    }
};



struct TPerfTestConfig {
    const TString Path;
    const TString Name;
    ui16 MonPort;
    NPDisk::EDeviceType DeviceType;
    TResultPrinter::EOutputFormat OutputFormat;
    bool DoLockFile;

    TMap<const TString, NPDisk::EDeviceType> DeviceStrToType {
        {"ROT",  NPDisk::DEVICE_TYPE_ROT},
        {"SSD",  NPDisk::DEVICE_TYPE_SSD},
        {"NVME", NPDisk::DEVICE_TYPE_NVME},
    };
    TMap<const TString, TResultPrinter::EOutputFormat> OutputFormatStrToType {
        {"wiki",  TResultPrinter::OUTPUT_FORMAT_WIKI},
        {"human", TResultPrinter::OUTPUT_FORMAT_HUMAN},
        {"json", TResultPrinter::OUTPUT_FORMAT_JSON},
    };

    TPerfTestConfig(const TString path, const TString name, const TString type, const TString outputFormatName,
            const TString monPort, bool doLockFile)
        : Path(path)
        , Name(name)
        , MonPort(std::strtol(monPort.c_str(), nullptr, 10))
        , DoLockFile(doLockFile)
    {
        auto it_type = DeviceStrToType.find(type);
        if (it_type != DeviceStrToType.end()) {
            DeviceType = it_type->second;
        } else {
            DeviceType = NPDisk::DEVICE_TYPE_ROT;
        }
        auto it_format = OutputFormatStrToType.find(outputFormatName);
        if (it_format != OutputFormatStrToType.end()) {
            OutputFormat = it_format->second;
        } else {
            OutputFormat = TResultPrinter::OUTPUT_FORMAT_WIKI;
        }
    }

    bool IsSolidState() const {
        return TPDiskCategory(DeviceType, 0).IsSolidState();
    }
};

class IPerfTest {
public:
    virtual void Init() = 0;

    virtual void Run() = 0;

    virtual void Finish() = 0;

    virtual ~IPerfTest() {
    }
};

class TPerfTest : IPerfTest {
protected:
    const TPerfTestConfig& Cfg;
    TIntrusivePtr<IResultPrinter> Printer;
    NMonitoring::TMonService2 MonService;

    TPerfTest(const TPerfTestConfig& cfg)
        : Cfg(cfg)
        , MonService(Cfg.MonPort, "Monitoring page")
    {
        if (cfg.MonPort) {
            NLwTraceMonPage::RegisterPages(MonService.GetRoot());
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(BLOBSTORAGE_PROVIDER));
            NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(ACTORLIB_PROVIDER));
            bool success = MonService.Start();
            if (!success) {
                Cerr << "Error! Unable to run monitoring http server on port# " << Cfg.MonPort << Endl;
            }
        }
    }

public:
    void SetPrinter(const TIntrusivePtr<IResultPrinter>& printer) {
        Printer = printer;
    }

    void RunTest() {
        Init();
        Run();
        Finish();
    }
};

}
