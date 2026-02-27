#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/library/pdisk_io/sector_map.h>
#include <ydb/tools/stress_tool/proto/device_perf_test.pb.h>
#include <ydb/library/actors/core/probes.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/map.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <algorithm>
#include <cmath>
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
    virtual void SetTestType(const TString& testType) = 0;
    virtual void SetInFlight(ui32 inFlight) = 0;
    virtual void SetSkipStatistics(bool skip) { Y_UNUSED(skip); }

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
    TVector<TVector<TStringPair>> AllRuns; // All runs collected for statistics (for flat mode)
    TSpeedAndIops CurrentSpeedAndIops; // Current run's speed and IOPS
    TVector<TSpeedAndIops> AllSpeedAndIops; // All runs' speed and IOPS for statistics (for flat mode)
    TString TestType; // Test type name (e.g., "PDiskWriteLoad")
    ui32 CurrentInFlight = 0; // Current InFlight value
    TMap<ui32, TVector<TVector<TStringPair>>> RunsByInFlight; // Runs grouped by InFlight
    TMap<ui32, TVector<TSpeedAndIops>> SpeedAndIopsByInFlight; // Speed/IOPS grouped by InFlight
    bool HeaderPrinted = false;
    bool SkipStatistics_ = false;
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

    struct TStatistics {
        double Min = 0.0;
        double Max = 0.0;
        double Median = 0.0;
        double StdDev = 0.0;
    };

    static TStatistics GetStatistics(TVector<double>& values) {
        if (values.empty()) {
            return {};
        }
        std::sort(values.begin(), values.end());
        size_t n = values.size();
        double median;
        if (n % 2 == 0) {
            median = (values[n / 2 - 1] + values[n / 2]) / 2.0;
        } else {
            median = values[n / 2];
        }

        // Calculate standard deviation
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        double mean = sum / n;
        double sqDiffSum = 0.0;
        for (double v : values) {
            double diff = v - mean;
            sqDiffSum += diff * diff;
        }
        double stddev = std::sqrt(sqDiffSum / n);

        return {values.front(), values.back(), median, stddev};
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

    TVector<TStringPair> MakeStatisticsRow(const TString& statName, const TVector<TStringPair>& templateRow,
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
        return statsRow;
    }

    void PrintAllRunsStructuredJson() {
        if (RunsByInFlight.empty()) {
            return;
        }

        NJson::TJsonValue root;

        // Set TestType if provided
        if (TestType) {
            root["TestType"] = TestType;
        }

        // Extract Name from first run if available and not a placeholder
        if (!AllRuns.empty()) {
            const auto& firstRun = AllRuns[0];
            for (const auto& pair : firstRun) {
                if (pair.Name == "Name" && pair.Value != "Name") {
                    root["Name"] = pair.Value;
                    break;
                }
            }
        }

        // Params from GlobalParams
        NJson::TJsonValue params;
        for (const auto& param : GlobalParams) {
            params[param.Name] = param.Value;
        }
        root["Params"] = params;

        // InFlights array - each entry has InFlight value, statistics, and runs
        NJson::TJsonValue inFlightsArray;
        inFlightsArray.SetType(NJson::JSON_ARRAY);

        for (const auto& [inFlight, runs] : RunsByInFlight) {
            NJson::TJsonValue inFlightObj;
            inFlightObj["InFlight"] = inFlight;

            // Calculate statistics for this InFlight
            const auto& speedAndIopsVec = SpeedAndIopsByInFlight[inFlight];
            if (speedAndIopsVec.size() > 0) {
                TVector<double> speeds;
                TVector<double> iopsVec;
                for (const auto& data : speedAndIopsVec) {
                    speeds.push_back(data.SpeedMBps);
                    iopsVec.push_back(data.Iops);
                }

                auto speedStats = GetStatistics(speeds);
                auto iopsStats = GetStatistics(iopsVec);

                NJson::TJsonValue speedJson;
                speedJson["min"] = Sprintf("%.1f MB/s", speedStats.Min);
                speedJson["max"] = Sprintf("%.1f MB/s", speedStats.Max);
                speedJson["median"] = Sprintf("%.1f MB/s", speedStats.Median);
                speedJson["stdev"] = Sprintf("%.1f MB/s", speedStats.StdDev);
                inFlightObj["Speed"] = speedJson;

                NJson::TJsonValue iopsJson;
                iopsJson["min"] = Sprintf("%.1f", iopsStats.Min);
                iopsJson["max"] = Sprintf("%.1f", iopsStats.Max);
                iopsJson["median"] = Sprintf("%.1f", iopsStats.Median);
                iopsJson["stdev"] = Sprintf("%.1f", iopsStats.StdDev);
                inFlightObj["IOPS"] = iopsJson;
            }

            // Runs array for this InFlight (without InFlight field since it's at parent level)
            NJson::TJsonValue runsArray;
            runsArray.SetType(NJson::JSON_ARRAY);
            for (const auto& run : runs) {
                NJson::TJsonValue runObj;
                for (const auto& pair : run) {
                    // Skip InFlight in individual runs since it's at the group level
                    if (pair.Name != "InFlight") {
                        runObj[pair.Name] = pair.Value;
                    }
                }
                runsArray.AppendValue(runObj);
            }
            inFlightObj["Runs"] = runsArray;

            inFlightsArray.AppendValue(inFlightObj);
        }
        root["InFlights"] = inFlightsArray;

        NJson::WriteJson(&Cout, &root, true, true);
        Cout << Endl;
    }

    // Calculate column widths based on all rows for consistent alignment
    TVector<ui32> CalculateColumnWidthsForAllRows(const TVector<TVector<TStringPair>>& allRows) const {
        if (allRows.empty()) {
            return {};
        }

        const ui32 screenWidth = 250;
        size_t numCols = allRows[0].size();
        const ui32 maxColumnWidthLimit = numCols > 0 ? screenWidth / numCols - 3 : 10;

        TVector<ui32> columnWidths(numCols, 0);

        // First pass: consider header names
        for (size_t i = 0; i < numCols; ++i) {
            columnWidths[i] = std::max(columnWidths[i], static_cast<ui32>(allRows[0][i].Name.size()));
        }

        // Consider all row values
        for (const auto& row : allRows) {
            for (size_t i = 0; i < row.size() && i < numCols; ++i) {
                columnWidths[i] = std::max(columnWidths[i], static_cast<ui32>(row[i].Value.size()));
            }
        }

        // Apply max width limit
        for (size_t i = 0; i < numCols; ++i) {
            columnWidths[i] = std::min(columnWidths[i], maxColumnWidthLimit);
        }

        return columnWidths;
    }

    void PrintAllRunsWithStatistics() {
        if (AllRuns.empty()) {
            return;
        }

        // For JSON format, use structured output
        if (OutputFormat == OUTPUT_FORMAT_JSON) {
            PrintAllRunsStructuredJson();
            AllRuns.clear();
            AllSpeedAndIops.clear();
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

        // Build statistics rows first (needed for width calculation)
        TVector<TVector<TStringPair>> statsRows;
        bool hasSpeedOrIops = (speedCol != SIZE_MAX || iopsCol != SIZE_MAX);
        if (hasSpeedOrIops && AllSpeedAndIops.size() > 1) {
            TVector<double> speeds;
            TVector<double> iopsVec;
            for (const auto& data : AllSpeedAndIops) {
                speeds.push_back(data.SpeedMBps);
                iopsVec.push_back(data.Iops);
            }

            auto speedStats = GetStatistics(speeds);
            auto iopsStats = GetStatistics(iopsVec);

            statsRows.push_back(MakeStatisticsRow("Min", firstRun, speedStats.Min, iopsStats.Min, speedCol, iopsCol));
            statsRows.push_back(MakeStatisticsRow("Max", firstRun, speedStats.Max, iopsStats.Max, speedCol, iopsCol));
            statsRows.push_back(MakeStatisticsRow("Median", firstRun, speedStats.Median, iopsStats.Median, speedCol, iopsCol));
            statsRows.push_back(MakeStatisticsRow("StdDev", firstRun, speedStats.StdDev, iopsStats.StdDev, speedCol, iopsCol));
        }

        // Calculate column widths based on ALL rows (data + stats) for human format
        TVector<ui32> columnWidths;
        if (OutputFormat == OUTPUT_FORMAT_HUMAN) {
            TVector<TVector<TStringPair>> allRowsForWidth;
            allRowsForWidth.insert(allRowsForWidth.end(), AllRuns.begin(), AllRuns.end());
            allRowsForWidth.insert(allRowsForWidth.end(), statsRows.begin(), statsRows.end());
            columnWidths = CalculateColumnWidthsForAllRows(allRowsForWidth);
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
            case OUTPUT_FORMAT_HUMAN:
                PrintResultsHumanFormatSingleRow(run, columnWidths);
                break;
            case OUTPUT_FORMAT_JSON:
                // Handled above
                break;
            }
        }

        // Print statistics rows
        for (const auto& statsRow : statsRows) {
            switch (OutputFormat) {
            case OUTPUT_FORMAT_WIKI:
                PrintResultsWikiFormatSingleRow(statsRow);
                break;
            case OUTPUT_FORMAT_HUMAN:
                PrintResultsHumanFormatSingleRow(statsRow, columnWidths);
                break;
            case OUTPUT_FORMAT_JSON:
                // Handled above
                break;
            }
        }

        AllRuns.clear();
        AllSpeedAndIops.clear();
        RunsByInFlight.clear();
        SpeedAndIopsByInFlight.clear();
    }

public:
    TResultPrinter(const EOutputFormat outputFormat, ui32 runCount = 1)
        : OutputFormat(outputFormat)
        , RunCount(runCount)
        , JsonWriter(&Cout, true)
    {
        // Only open array for single run JSON (backward compatibility)
        if (OutputFormat == OUTPUT_FORMAT_JSON && RunCount == 1) {
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
    void SetTestType(const TString& testType) override {
        TestType = testType;
    }
    void SetInFlight(ui32 inFlight) override {
        CurrentInFlight = inFlight;
    }
    void SetSkipStatistics(bool skip) override {
        SkipStatistics_ = skip;
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
            AllRuns.push_back(Results);
            RunsByInFlight[CurrentInFlight].push_back(Results);
            if (!SkipStatistics_) {
                SpeedAndIopsByInFlight[CurrentInFlight].push_back(CurrentSpeedAndIops);
                AllSpeedAndIops.push_back(CurrentSpeedAndIops);
            }
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
            // Note: JSON for multiple runs uses NJson::WriteJson in PrintAllRunsStructuredJson()
            HeaderPrinted = false;
        }
    }

    ~TResultPrinter() {
        // Only close array for single run JSON (backward compatibility)
        if (OutputFormat == OUTPUT_FORMAT_JSON && RunCount == 1) {
            JsonWriter.CloseArray();
            JsonWriter.Flush();
            Cout << Endl;
        }
    }
};



struct TPerfTestConfig {
    TVector<TString> Paths;
    const TString Path; // Paths[0] alias for backward compatibility
    TVector<TIntrusivePtr<NPDisk::TSectorMap>> SectorMaps;
    const TString Name;
    ui16 MonPort;
    NPDisk::EDeviceType DeviceType;
    TResultPrinter::EOutputFormat OutputFormat;
    bool DoLockFile;
    ui32 RunCount;
    ui32 InFlightFrom = 0;
    ui32 InFlightTo = 0;
    TIntrusivePtr<NPDisk::TSectorMap> SectorMap; // SectorMaps[0] alias
    bool DisablePDiskDataEncryption;

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

    static TIntrusivePtr<NPDisk::TSectorMap> TryParseSectorMap(const TString& path) {
        if (!path.Contains(":")) {
            return nullptr;
        }
        TVector<TString> splitted;
        Split(path, ":", splitted);
        if (splitted.size() >= 2 && splitted[0] == "SectorMap") {
            ui32 defaultSizeGb = 100;
            ui64 size = (ui64)defaultSizeGb << 30;
            if (splitted.size() >= 3) {
                size = (ui64)FromStringWithDefault<ui32>(splitted[2], defaultSizeGb) << 30;
            }
            auto diskMode = NPDisk::NSectorMap::DM_NONE;
            if (splitted.size() >= 4) {
                diskMode = NPDisk::NSectorMap::DiskModeFromString(splitted[3]);
            }
            auto sectorMap = MakeIntrusive<NPDisk::TSectorMap>(size, diskMode);
            sectorMap->ZeroInit(1000);
            return sectorMap;
        }
        return nullptr;
    }

    TPerfTestConfig(const TString& path, const TString name, const TString type, const TString outputFormatName,
            const TString monPort, bool doLockFile, const TString runCountStr = "1",
            const TString inFlightFromStr = "0", const TString inFlightToStr = "0",
            bool disablePDiskDataEncryption = false)
        : TPerfTestConfig(TVector<TString>{path}, name, type, outputFormatName,
                monPort, doLockFile, runCountStr, inFlightFromStr, inFlightToStr,
                disablePDiskDataEncryption)
    {}

    TPerfTestConfig(const TVector<TString>& paths, const TString name, const TString type, const TString outputFormatName,
            const TString monPort, bool doLockFile, const TString runCountStr = "1",
            const TString inFlightFromStr = "0", const TString inFlightToStr = "0",
            bool disablePDiskDataEncryption = false)
        : Paths(paths)
        , Path(paths.at(0))
        , Name(name)
        , MonPort(std::strtol(monPort.c_str(), nullptr, 10))
        , DoLockFile(doLockFile)
        , RunCount(std::max(1, static_cast<int>(std::strtol(runCountStr.c_str(), nullptr, 10))))
        , InFlightFrom(std::strtol(inFlightFromStr.c_str(), nullptr, 10))
        , InFlightTo(std::strtol(inFlightToStr.c_str(), nullptr, 10))
        , DisablePDiskDataEncryption(disablePDiskDataEncryption)
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

        SectorMaps.resize(Paths.size());
        for (size_t i = 0; i < Paths.size(); ++i) {
            SectorMaps[i] = TryParseSectorMap(Paths[i]);
        }
        SectorMap = SectorMaps[0];
    }

    ui32 NumDevices() const {
        return Paths.size();
    }

    bool HasInFlightOverride() const {
        return InFlightFrom > 0 && InFlightTo > 0;
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
