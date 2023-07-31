#include "util_helpers.h"

#include <library/cpp/ipreg/reader.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/geobase/lookup.hpp>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/string/split.h>
#include <util/string/vector.h>
#include <util/stream/str.h>

namespace NIPREG {
    namespace {
        double FindNearestCoarsedCoeff(double baseValue) {
            using ValueStepPair = std::pair<double, double>;
            static const double fix = 0.01;
            static const TVector<ValueStepPair> limits = {
                { 100.,   20. + fix },
                { 500.,   50. + fix },
                { 2500.,  100. + fix },
                { 10000., 1000. + fix },
                { 50000., 10000. + fix }
            };

            double last_step{};
            for (const auto& pair : limits) {
                last_step = pair.second;
                if (baseValue <= pair.first) {
                    break;
                }
            }
            return last_step;
        }

        double CalcCoarsedValue(double baseValue) {
            if (baseValue < 0.) {
                ythrow yexception() << "negative value detected: " << baseValue;
            }

            // TODO(dieash) some "strange" calculation below
            const auto coarsedCoeff = FindNearestCoarsedCoeff(baseValue);
            const double fixedValue = coarsedCoeff * static_cast<int>((baseValue + coarsedCoeff / 2) / coarsedCoeff);
            return fixedValue;
        }

        const char * const REL_FIELD = "reliability";
        const char * const REG_FIELD = "region_id";

        void CorrectReliability(NJson::TJsonValue& jsonData, const TString& data) {
            jsonData = ParseJsonString(data);
            auto& jsonMap = jsonData.GetMapSafe();

            auto& reliabilityField = jsonMap[REL_FIELD];
            reliabilityField = CalcCoarsedValue(reliabilityField.GetDouble());
        }

        TString SortJson(const TString& data) {
            NJson::TJsonValue json = ParseJsonString(data);
            return SortJsonData(json);
        }

        static TString MergeJsonsData(const TString& data1, const TString& data2, bool sortKeys = false, bool countMerge = false) {
            static const char* MERGE_QTY = "_mrg_qty_";

            auto json1 = ParseJsonString(data1);
            const auto& json2 = ParseJsonString(data2);

            if (countMerge && !json1.Has(MERGE_QTY)) {
                json1.InsertValue(MERGE_QTY, 1);
            }

            for (const auto& item : json2.GetMapSafe()) {
                json1.InsertValue(item.first, item.second);
            }

            if (countMerge) {
                json1.InsertValue(MERGE_QTY, (json1[MERGE_QTY].GetInteger() + 1));
            }

            const auto NoFormat = false;
            return NJson::WriteJson(json1, NoFormat, sortKeys);
        }

        bool IsJsonEquals(const TVector<TString>& excludeFieldsList, const TString& data1, const TString& data2) {
            if (excludeFieldsList.empty()) {
                return data1 == data2;
            }

            auto json1 = ParseJsonString(data1);
            auto json2 = ParseJsonString(data2);

            for (const auto& excludeField : excludeFieldsList) {
                json1.EraseValue(excludeField);
                json2.EraseValue(excludeField);
            }

            return json1 == json2;
        }

        class Patcher {
        public:
            Patcher(TReader& base, TReader& patch, IOutputStream& output, bool sortData)
                : BaseStream(base)
                , PatchStream(patch)
                , Output(output)
                , SortData(sortData)
            {
                GetNext(BaseStream, BaseRangePtr);
                GetNext(PatchStream, PatchRangePtr);
            }

            void Process() {
                while (BaseRangePtr || PatchRangePtr) {
                    if (   CheckPatch()
                        || OnlySecond(BaseRangePtr, PatchRangePtr, PatchStream)
                        || OnlySecond(PatchRangePtr, BaseRangePtr, BaseStream)
                        || Range1BeforeRange2(BaseRangePtr, PatchRangePtr, BaseStream)
                        || Range1BeforeRange2(PatchRangePtr, BaseRangePtr, PatchStream)
                        || FirstEndInSecond(BaseRangePtr, PatchRangePtr)
                        || FirstEndInSecond(PatchRangePtr, BaseRangePtr)
                        || FirstStartInSecond(BaseRangePtr, PatchRangePtr, BaseStream, PatchStream))
                    {
                        continue;
                    }
                }
            }

        private:
            void GetNext(TReader& stream, TAutoPtr<TRange>& rangePtr) {
                if (stream.Next()) {
                    if (rangePtr) {
                        *rangePtr = stream.Get();
                    } else {
                        rangePtr.Reset(new TRange(stream.Get()));
                    }
                }
                else {
                    rangePtr.Reset();
                }
            }

            void Print(const TRange& range) const {
                Output << range;
            }

            void PrintSorted(const TRange& range) const {
                const TRange sortedCopy{range.First, range.Last, SortJson(range.Data)};
                Output << sortedCopy;
            }

            bool CheckPatch() {
                if (PatchRangePtr && PatchRangePtr->First > PatchRangePtr->Last) {
                    GetNext(PatchStream, PatchRangePtr);
                    return true;
                }
                return false;
            }

            bool OnlySecond(TAutoPtr<TRange>& first, TAutoPtr<TRange>& second, TReader& stream) {
                if (!first && second) {
                    Print(*second);
                    GetNext(stream, second);
                    return true;
                }
                return false;
            }

            bool Range1BeforeRange2(TAutoPtr<TRange>& first, TAutoPtr<TRange>& second, TReader& stream) {
                if (first->Last < second->First) {
                    Print(*first);
                    GetNext(stream, first);
                    return true;
                }
                return false;
            }

            bool FirstEndInSecond(TAutoPtr<TRange>& first, TAutoPtr<TRange>& second) {
                if (first->First < second->First) {
                    auto leftBaseRange = *first;
                    leftBaseRange.Last = second->First.Prev();
                    Print(leftBaseRange);

                    first->First = second->First;
                    return true;
                }
                return false;
            }

            bool FirstStartInSecond(TAutoPtr<TRange>& first, TAutoPtr<TRange>& second, TReader& stream1, TReader& stream2) {
                if (first->First >= second->First) {
                    auto leftBaseRange = *first;
                    leftBaseRange.Data = MergeJsonsData(first->Data, second->Data);

                    if (first->Last <= second->Last) {
                        second->First = first->Last.Next();
                        GetNext(stream1, first);
                        if (second->First == TAddress::Highest()) {
                            GetNext(stream2, second);
                        }
                    } else {
                        leftBaseRange.Last = second->Last;
                        first->First = second->Last.Next();
                        GetNext(stream2, second);
                    }

                    SortData ? PrintSorted(leftBaseRange) : Print(leftBaseRange);
                    return true;
                }
                return false;
            }

        private:
            TAutoPtr<TRange> BaseRangePtr;
            TAutoPtr<TRange> PatchRangePtr;

            TReader& BaseStream;
            TReader& PatchStream;
            IOutputStream& Output;
            const bool SortData = false;
        };

        struct IpChecker {
            static void LessOrEqual(const size_t row, const TAddress& lastIp, const TAddress& checkedIp) {
                if (lastIp <= checkedIp) {
                    return;
                }
                GenErr(row, " <= ", lastIp, checkedIp);
            }

            static void Less(const size_t row, const TAddress& lastIp, const TAddress& checkedIp) {
                if (lastIp < checkedIp) {
                    return;
                }
                GenErr(row, " < ", lastIp, checkedIp);
            }

            static void GenErr(const size_t row, const char* msg, const TAddress& lastIp, const TAddress& checkedIp) {
                const TString& errMsg = ">>> row#" + ToString(row) + "; " + lastIp.AsIPv6() + msg + checkedIp.AsIPv6();
                throw std::runtime_error(errMsg.data());
            }
        };

        class MergerBy3 {
        public:
            MergerBy3(const TString& geodataPath, IOutputStream& output)
                : Geobase(geodataPath)
                , Out(output)
            {}

            void Process(TReader& input, bool ByRegsOnly, bool silentMode) {
                while (input.Next()) {
                    Trio.push_back(input.Get());
                    if (3 > Trio.size()) {
                        continue;
                    }

                    auto& range2Data = (++Trio.begin())->Data;
                    if (range2Data.npos != range2Data.find("\"is_placeholder\":1")) {
                        PrintAndDrop1stRange();
                        PrintAndDrop1stRange();
                        continue;
                    }

                    const auto range1RegId = GetRegionId(Trio.begin()->Data);
                    const auto range3RegId = GetRegionId(Trio.rbegin()->Data);
                    if (range1RegId != range3RegId) {
                        PrintAndDrop1stRange();
                        continue;
                    }

                    const auto range2RegId = GetRegionId(range2Data);
                    const auto& parentsIds = Geobase.GetParentsIds(range1RegId);
                    if (parentsIds.end() == std::find(parentsIds.begin() + 1, parentsIds.end(), range2RegId)) {
                        PrintAndDrop1stRange();
                        continue;
                    }

                    if (!ByRegsOnly) {
                        const auto range1Size = Trio.begin()->GetAddrsQty();
                        const auto range2Size = (++Trio.begin())->GetAddrsQty();
                        const auto range3Size = Trio.rbegin()->GetAddrsQty();

                        if (range2Size > (range1Size + range3Size)) {
                            PrintAndDrop1stRange();
                            continue;
                        }
                    }

                    range2Data = SubstRegionId(range2Data, range1RegId);
                    if (!silentMode) {
                        PrintSubstNote(range2RegId, range1RegId);
                    }

                    PrintAndDrop1stRange(); // 1st
                    PrintAndDrop1stRange(); // 2nd
                }

                while (Trio.end() != Trio.begin()) {
                    PrintAndDrop1stRange();
                }
            }
        private:
            void PrintAndDrop1stRange() {
                Out << *Trio.begin();
                Trio.erase(Trio.begin());
            }

            void PrintSubstNote(const int oldId, const int newId) {
                const bool NoData = false;
                Cerr << "s/" << oldId << "/" << newId << "/: [";

                Trio.begin()->DumpTo(Cerr, NoData);
                Cerr << "/" << Trio.begin()->GetAddrsQty() << " | ";

                const auto& range2nd = *(++Trio.begin());
                range2nd.DumpTo(Cerr, NoData);
                Cerr << "/" << range2nd.GetAddrsQty() << " | ";

                Trio.rbegin()->DumpTo(Cerr, NoData);
                Cerr << "/" << Trio.rbegin()->GetAddrsQty() << "]\n";
            }


            static int GetRegionId(const TString& data) {
                const auto& json = ParseJsonString(data);
                auto reg_id = json["region_id"].GetIntegerSafe(0);
                return 99999 == reg_id ? 10000 : reg_id;
            }

            static TString SubstRegionId(const TString& data, const int newId) {
                auto json = ParseJsonString(data);
                json.InsertValue("region_id", newId);
                return SortJsonData(json);
            }

            const NGeobase::TLookup Geobase;
            IOutputStream& Out;
            TList<TRange> Trio;
        };
    } // anon-ns

    void DoCoarsening(IInputStream& input, IOutputStream& output) {
        TString line;
        while (input.ReadLine(line)) {
            TVector<TString> parts;
            StringSplitter(line).Split('\t').AddTo(&parts);

            NJson::TJsonValue jsonData;
            CorrectReliability(jsonData, parts[1]);
            output << parts[0]  << "\t"  << "{\""
                   << REG_FIELD << "\":" << jsonData[REG_FIELD] << ",\""
                   << REL_FIELD << "\":" << Prec(jsonData[REL_FIELD].GetDouble(), PREC_POINT_DIGITS_STRIP_ZEROES, 2)
                   << "}\n";
        }
    }

    void DoMergeEqualsRange(TReader& input, IOutputStream& output) {
        // TODO(dieash@) may be check region for parent/child relation
        // , const TString& geodataPath
        // NGeobase::TLookup geoLookup(geodataPath);

        TVector<TString> rangeDataList;
        TRange lastRange{};

        const char* REG_ID_ATTR = "region_id";
        const char* ORG_NET_ATTR = "orig_net_size";
        const char* HUGE_SIZE_VALUE = "huge";

        const int HUGE_SIZE_COEFF = 100;

        const auto CalcRegionBinding = [&]() {
            if (rangeDataList.empty()) {
                throw std::runtime_error("empty data list");
            }

            if (1 == rangeDataList.size()) {
                return rangeDataList[0];
            }

            size_t maxAmount{};
            NJson::TJsonValue maxData;

            THashMap<NGeobase::TId, size_t> reg2amount;
            for (const auto& data : rangeDataList) {
                const auto& json = ParseJsonString(data);

                const auto id = json[REG_ID_ATTR].GetInteger();
                const auto amount = (json.Has(ORG_NET_ATTR) && HUGE_SIZE_VALUE == json[ORG_NET_ATTR].GetString()) ? HUGE_SIZE_COEFF : FromString<int>(json[ORG_NET_ATTR].GetString());
                reg2amount[id] += amount;

                if (reg2amount[id] > maxAmount) {
                    maxData = json;
                }
            }

            maxData.EraseValue(ORG_NET_ATTR);
            return SortJsonData(maxData);
        };

        const auto PrintRow = [&]() {
            if (rangeDataList.empty()) {
                return;
            }
            lastRange.Data = CalcRegionBinding();
            output << lastRange;
        };

        while (input.Next()) {
            auto currRange = input.Get();
            if (currRange != lastRange) {
                PrintRow();

                lastRange = currRange;
                rangeDataList = {};
            }

            rangeDataList.push_back(currRange.Data);
        }
        PrintRow();
    }

    void DoMerging(TReader& input, IOutputStream& output, const MergeTraits& traits) {
        if (!input.Next()) {
            return; // empty file here
        }

        const bool IsJsonData = traits.ConcatSep.empty();

        TRange joinedRange = input.Get();
        if (traits.SortData) {
            joinedRange.Data = SortJson(joinedRange.Data);
        }

        while (input.Next()) {
            auto currRange = input.Get();
            if (traits.SortData) {
                currRange.Data = SortJson(currRange.Data);
            }

            if (currRange.Contains(joinedRange) && joinedRange.Data == currRange.Data) {
                joinedRange = currRange;
                continue;
            }

            if (traits.JoinNestedRanges && joinedRange.Contains(currRange) && joinedRange.Data == currRange.Data) {
                continue;
            }

            if (   currRange.First != joinedRange.Last.Next()
                || ( IsJsonData && !IsJsonEquals(traits.ExcludeFieldsList, currRange.Data, joinedRange.Data))
                || (!IsJsonData && currRange.Data != joinedRange.Data))
            {
                output << joinedRange;
                joinedRange = currRange;
            } else {
                if (IsJsonData) {
                    joinedRange.Data = MergeJsonsData(currRange.Data, joinedRange.Data, traits.SortData, traits.CountMerges);
                } else {
                    joinedRange.Data = (joinedRange.Data == currRange.Data) ? joinedRange.Data : (joinedRange.Data + traits.ConcatSep + currRange.Data);
                }
                joinedRange.Last = currRange.Last;
            }
        }

        output << joinedRange;
    }

    void DoMerging3(TReader& input, IOutputStream& output, const TString& geodata, bool ByRegsOnly, bool silentMode) {
        MergerBy3 merger(geodata, output);
        merger.Process(input, ByRegsOnly, silentMode);
    }

    void DoPatching(TReader& base, TReader& patch, IOutputStream& output, bool sortData) {
        Patcher(base, patch, output, sortData).Process();
    }

    const TString STUB_DATA{"{\"is_placeholder\":1,\"region_id\":10000,\"reliability\":0}"};

    void AddStubRanges(TReader& input, IOutputStream& output) {
        TRange stub{
            TAddress::Lowest(),
            TAddress::Lowest(),
            STUB_DATA
        };

        while (input.Next()) {
            const auto& currRange = input.Get();

            if (stub.First > currRange.First) {
                const TString& errMsg = ">>> bad ranges ($stub.begin > $next.begin) // " + stub.First.AsShortIPv6() + " | " + currRange.First.AsShortIPv6();
                throw std::runtime_error(errMsg.data());
            }

            if (stub.First < currRange.First) {
                stub.Last = currRange.First.Prev();
                output << stub;
            }

            output << currRange;
            stub.First = currRange.Last.Next();
        }

        if (stub.First != TAddress::Highest()) {
            stub.Last  = TAddress::Highest();
            output << stub;
        }
    }

    void CheckAddressSpaceForCompleteness(IInputStream& input, IOutputStream& output) {
        TAddress lastIp = TAddress::Lowest();
        size_t row_number = 0;

        TString line;
        while (input.ReadLine(line)) {
            ++row_number;
            output << line << "\n";

            const auto& currRange = TRange::BuildRange(line);
            if (row_number == 1) {
                if (currRange.First != TAddress::Lowest()) {
                     const TString err_msg = "bad first addr (ip / wanted_ip) => " + currRange.First.AsIPv6() + " / " + TAddress::Lowest().AsIPv6();
                     throw std::runtime_error(err_msg);
                }
                lastIp = currRange.Last;
                continue;
            }

            if (lastIp == currRange.First || lastIp.Next() != currRange.First) {
                const TString err_msg = ">>> row#" + ToString(row_number) + " bad pair (last_ip / next_ip) => " + lastIp.AsIPv6() + " / " + currRange.First.AsIPv6();
                throw std::runtime_error(err_msg);
            }

            lastIp = currRange.Last;
        }

        if (lastIp != TAddress::Highest()) {
            const TString err_msg = "bad last addr (last_ip / wanted_ip) => " + lastIp.AsIPv6() + " / " + TAddress::Highest().AsIPv6();
            throw std::runtime_error(err_msg);
        }
    }

    void CheckRangesForMonotonicSequence(IInputStream& input, IOutputStream& output, bool IsStrict) {
        TAddress lastIp = TAddress::Lowest();

        size_t row = 0;
        TString line;
        while (input.ReadLine(line)) {
            ++row;
            output << line << "\n";

            const auto& currRange = TRange::BuildRange(line);
            if (row == 1) {
                lastIp = currRange.Last;
                continue;
            }

            if (IsStrict) {
                IpChecker::Less(row, lastIp, currRange.First);
            } else {
                IpChecker::LessOrEqual(row, lastIp, currRange.First);
            }
            lastIp = currRange.Last;
        }
    }

    NJson::TJsonValue ParseJsonString(const TString& data) {
        const auto throwIfError = true;

        NJson::TJsonValue json;
        NJson::ReadJsonFastTree(data, &json, throwIfError);
        return json;
    }

    TString SortJsonData(const NJson::TJsonValue& json) {
        const auto NoFormat = false;
        const auto SortKeys = true;

        return NJson::WriteJson(json, NoFormat, SortKeys);
    }

    TString SortJsonData(const TString& jsonStr) {
        return SortJsonData(ParseJsonString(jsonStr));
    }

    TString AddJsonAttrs(const TVector<TString>& addFieldsList, const TString& jsonStr, const TMaybe<TString>& attrValue) {
        if (addFieldsList.empty()) {
            return jsonStr;
        }

        auto json = ParseJsonString(jsonStr);
        for (const auto& newField : addFieldsList) {
            if (!newField.empty()) {
                if (attrValue) {
                    json.InsertValue(newField, *attrValue);
                } else {
                    json.InsertValue(newField, 1);
                }
            }
        }
        return json.GetStringRobust();
    }

    TString ExcludeJsonAttrs(const TVector<TString>& excludeFieldsList, const TString& jsonStr) {
        if (excludeFieldsList.empty()) {
            return jsonStr;
        }

        auto json = ParseJsonString(jsonStr);
        for (const auto& excludeField : excludeFieldsList) {
            if (!excludeField.empty()) {
                json.EraseValue(excludeField);
            }
        }
        return json.GetStringRobust();
    }

    TString ExtractJsonAttrs(const TVector<TString>& extractFieldsList, const TString& jsonStr) {
        if (extractFieldsList.empty()) {
            return jsonStr;
        }

        auto json = ParseJsonString(jsonStr);
        NJson::TJsonValue newJson;
        for (const auto& field : extractFieldsList) {
            if (json.Has(field)) {
                newJson.InsertValue(field, json[field]);
            }
        }
        if (!newJson.IsDefined()) {
            return {};
        }
        return newJson.GetStringRobust();
    }

    namespace CliParamsDesc {
        const TString InputFnameParam  = "input-data";
        const TString OutputFnameParam = "output-data";
        const TString OutputFullIpParam = "show-full-ip";
        const TString PrintStatsParam = "print-stats";
        const TString PrintYtStatsParam = "yt-stats";

        const TString InputFnameParamDesc  = "path to input IPREG-data; leave empty or use '-' for stdin";
        const TString OutputFnameParamDesc = "path to file for output results; leave empty for stdout";
        const TString OutputFullIpParamDesc = "print full ipv6 (by default - short)";
        const TString PrintStatsParamDesc = "print internal statistics; @stderr";
        const TString PrintYtStatsParamDesc = "print YT-stats (by default, file-descriptor 5)";
    }  // ns CliParamsDesc

    DefaultCliParams::DefaultCliParams() {
        using namespace CliParamsDesc;

        Opts.SetFreeArgsMax(0);
        Opts.AddHelpOption('h');

        Opts.AddLongOption('i', InputFnameParam)
            .RequiredArgument("filename")
            .DefaultValue(InputFname)
            .StoreResult(&InputFname).Help(InputFnameParamDesc);

        Opts.AddLongOption('o', OutputFnameParam)
            .RequiredArgument("filename")
            .DefaultValue(OutputFname)
            .StoreResult(&OutputFname).Help(OutputFnameParamDesc);

        Opts.AddLongOption('f', OutputFullIpParam)
            .Optional()
            .NoArgument()
            .DefaultValue("0")
            .OptionalValue("1")
            .StoreResult(&OutputFullIp).Help(OutputFullIpParamDesc);

        Opts.AddLongOption(PrintStatsParam)
            .Optional()
            .NoArgument()
            .DefaultValue("0")
            .OptionalValue("1")
            .StoreResult(&PrintStats).Help(PrintStatsParamDesc);

        Opts.AddLongOption(PrintYtStatsParam)
            .Optional()
            .NoArgument()
            .DefaultValue("0")
            .OptionalValue("1")
            .StoreResult(&PrintYtStats).Help(PrintYtStatsParamDesc);
    }

    void DefaultCliParams::ApplyFlags() const {
        if (OutputFullIp) {
            SetIpFullOutFormat();
        }
    }

    void DefaultCliParams::Parse(int argc, const char **argv) {
        NLastGetopt::TOptsParseResult optRes(&GetOpts(), argc, argv);
        ApplyFlags();
    }

} // NIPREG
