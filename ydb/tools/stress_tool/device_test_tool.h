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

#include <algorithm>
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

struct TSpeedAndIops {
    double SpeedMBps = 0.0;
    double Iops = 0.0;

    TSpeedAndIops() = default;
    TSpeedAndIops(double speed, double iops)
        : SpeedMBps(speed)
        , Iops(iops)
    {}
};

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
    virtual void AddSpeedAndIops(const TSpeedAndIops& data) = 0;

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
    ui32 RunCount = 1;
    TVector<TStringPair> GlobalParams;
    TVector<TStringPair> Results;
    TVector<TVector<TStringPair>> AllRuns; // All runs collected for statistics
    TSpeedAndIops CurrentSpeedAndIops; // Current run's speed and IOPS
    TVector<TSpeedAndIops> AllSpeedAndIops; // All runs' speed and IOPS for statistics
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

    static double GetMedian(TVector<double>& values) {
        if (values.empty()) {
            return 0.0;
        }
        std::sort(values.begin(), values.end());
        size_t n = values.size();
        if (n % 2 == 0) {
            return (values[n / 2 - 1] + values[n / 2]) / 2.0;
        }
        return values[n / 2];
    }

    void PrintResultsWikiFormatSingleRow(const TVector<TStringPair>& row) {
        TEST_COUT("||");
        for (const auto& counter : row) {
            TEST_COUT(" " << counter.Value << " |");
        }
        TEST_COUT_LN("|");
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
        PrintResultsWikiFormatSingleRow(Results);
    }

    void PrintResultsHumanFormatSingleRow(const TVector<TStringPair>& row, const TVector<ui32>& columnWidths) {
        TEST_COUT("|");
        for (size_t i = 0; i < row.size(); ++i) {
            const auto& value = row[i].Value;
            ui32 width = columnWidths[i];
            TEST_COUT(std::format(" {:>{}.{}} |", value.c_str(), width, width));
        }
        TEST_COUT_LN("");
    }

    TVector<ui32> CalculateColumnWidths(const TVector<TStringPair>& row) const {
        const ui32 screenWidth = 250;
        const ui32 maxColumnWidthLimit = row.size() > 0 ? screenWidth / row.size() - 3 : 10;

        TVector<ui32> columnWidths;
        columnWidths.reserve(row.size());
        for (const auto& counter : row) {
            ui32 maxWidth = std::max(counter.Name.size(), counter.Value.size());
            columnWidths.push_back(std::min<ui32>(maxWidth, maxColumnWidthLimit));
        }
        return columnWidths;
    }

    void PrintResultsHumanFormat() {
        TVector<ui32> columnWidths = CalculateColumnWidths(Results);

        if (!HeaderPrinted) {
            HeaderPrinted = true;
            PrintGlobalParams();
            TEST_COUT("|");
            for (size_t i = 0; i < Results.size(); ++i) {
                const auto& name = Results[i].Name;
                ui32 width = columnWidths[i];
                TEST_COUT(std::format(" {:>{}.{}} |", name.c_str(), width, width));
            }
            TEST_COUT_LN("");
        }

        PrintResultsHumanFormatSingleRow(Results, columnWidths);
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

    void PrintStatisticsRow(const TString& statName, const TVector<TStringPair>& templateRow,
                           double speed, double iops, size_t speedCol, size_t iopsCol) {
        TVector<TStringPair> statsRow;
        for (size_t i = 0; i < templateRow.size(); ++i) {
            TStringPair pair(templateRow[i].Name, "");
            if (i == 0) {
                pair.Value = statName;
            } else if (i == speedCol) {
                pair.Value = Sprintf("%.1f MB/s", speed);
            } else if (i == iopsCol) {
                pair.Value = Sprintf("%.1f", iops);
            } else {
                pair.Value = "-";
            }
            statsRow.push_back(pair);
        }

        switch (OutputFormat) {
        case OUTPUT_FORMAT_WIKI:
            PrintResultsWikiFormatSingleRow(statsRow);
            break;
        case OUTPUT_FORMAT_HUMAN: {
            TVector<ui32> columnWidths = CalculateColumnWidths(statsRow);
            PrintResultsHumanFormatSingleRow(statsRow, columnWidths);
            break;
        }
        case OUTPUT_FORMAT_JSON: {
            NJson::TJsonValue value;
            value["_stat"] = statName;
            for (const auto& param : GlobalParams) {
                value[param.Name] = param.Value;
            }
            for (const auto& pair : statsRow) {
                if (!pair.Value.empty() && pair.Value != "-") {
                    value[pair.Name] = pair.Value;
                }
            }
            JsonWriter.Write(value);
            break;
        }
        }
    }

    void PrintAllRunsWithStatistics() {
        if (AllRuns.empty()) {
            return;
        }

        // Find column indices for Speed and IOPS
        size_t speedCol = SIZE_MAX;
        size_t iopsCol = SIZE_MAX;
        const auto& firstRun = AllRuns[0];
        for (size_t i = 0; i < firstRun.size(); ++i) {
            TString nameLower = firstRun[i].Name;
            nameLower.to_lower();
            if (nameLower == "speed") {
                speedCol = i;
            } else if (nameLower == "iops") {
                iopsCol = i;
            }
        }

        // Print header once
        if (!HeaderPrinted && !firstRun.empty()) {
            HeaderPrinted = true;
            PrintGlobalParams();
            if (OutputFormat == OUTPUT_FORMAT_WIKI) {
                TEST_COUT_LN("#|");
                TEST_COUT("||");
                for (const auto& counter : firstRun) {
                    TEST_COUT(" " << counter.Name << " |");
                }
                TEST_COUT_LN("|");
            } else if (OutputFormat == OUTPUT_FORMAT_HUMAN) {
                TVector<ui32> columnWidths = CalculateColumnWidths(firstRun);
                TEST_COUT("|");
                for (size_t i = 0; i < firstRun.size(); ++i) {
                    const auto& name = firstRun[i].Name;
                    ui32 width = columnWidths[i];
                    TEST_COUT(std::format(" {:>{}.{}} |", name.c_str(), width, width));
                }
                TEST_COUT_LN("");
            }
        }

        // Print each run
        for (const auto& run : AllRuns) {
            switch (OutputFormat) {
            case OUTPUT_FORMAT_WIKI:
                PrintResultsWikiFormatSingleRow(run);
                break;
            case OUTPUT_FORMAT_HUMAN: {
                TVector<ui32> columnWidths = CalculateColumnWidths(run);
                PrintResultsHumanFormatSingleRow(run, columnWidths);
                break;
            }
            case OUTPUT_FORMAT_JSON: {
                NJson::TJsonValue value;
                for (const auto& param : GlobalParams) {
                    value[param.Name] = param.Value;
                }
                for (const auto& counter : run) {
                    value[counter.Name] = counter.Value;
                }
                JsonWriter.Write(value);
                break;
            }
            }
        }

        // Calculate and print statistics using stored raw values
        bool hasSpeedOrIops = (speedCol != SIZE_MAX || iopsCol != SIZE_MAX);
        if (hasSpeedOrIops && AllSpeedAndIops.size() > 1) {
            TVector<double> speeds;
            TVector<double> iops;
            for (const auto& data : AllSpeedAndIops) {
                speeds.push_back(data.SpeedMBps);
                iops.push_back(data.Iops);
            }

            double minSpeed = *std::min_element(speeds.begin(), speeds.end());
            double maxSpeed = *std::max_element(speeds.begin(), speeds.end());
            double medianSpeed = GetMedian(speeds);

            double minIops = *std::min_element(iops.begin(), iops.end());
            double maxIops = *std::max_element(iops.begin(), iops.end());
            double medianIops = GetMedian(iops);

            PrintStatisticsRow("Min", firstRun, minSpeed, minIops, speedCol, iopsCol);
            PrintStatisticsRow("Max", firstRun, maxSpeed, maxIops, speedCol, iopsCol);
            PrintStatisticsRow("Median", firstRun, medianSpeed, medianIops, speedCol, iopsCol);
        }

        AllRuns.clear();
        AllSpeedAndIops.clear();
    }

public:
    TResultPrinter(const EOutputFormat outputFormat, ui32 runCount = 1)
        : OutputFormat(outputFormat)
        , RunCount(runCount)
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
    void AddSpeedAndIops(const TSpeedAndIops& data) override {
        CurrentSpeedAndIops = data;
    }

    void PrintResults() override {
        if (Results.empty()) {
            return;
        }

        if (RunCount == 1) {
            // Original behavior
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
        } else {
            // Collect results for later statistics calculation
            AllRuns.push_back(Results);
            AllSpeedAndIops.push_back(CurrentSpeedAndIops);
        }
        Results.clear();
        CurrentSpeedAndIops = TSpeedAndIops();
    }

    void EndTest() override {
        if (RunCount == 1) {
            // Original behavior
            if (OutputFormat == OUTPUT_FORMAT_WIKI && HeaderPrinted) {
                TEST_COUT_LN("|#");
            }
            if (OutputFormat == OUTPUT_FORMAT_JSON) {
                JsonWriter.Flush();
            }
            HeaderPrinted = false;
            PrintResults();
        } else {
            // Print all runs with statistics
            PrintAllRunsWithStatistics();
            if (OutputFormat == OUTPUT_FORMAT_WIKI && HeaderPrinted) {
                TEST_COUT_LN("|#");
            }
            if (OutputFormat == OUTPUT_FORMAT_JSON) {
                JsonWriter.Flush();
            }
            HeaderPrinted = false;
        }
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
    ui32 RunCount;

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
            const TString monPort, bool doLockFile, const TString runCountStr = "1")
        : Path(path)
        , Name(name)
        , MonPort(std::strtol(monPort.c_str(), nullptr, 10))
        , DoLockFile(doLockFile)
        , RunCount(std::max(1, static_cast<int>(std::strtol(runCountStr.c_str(), nullptr, 10))))
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
