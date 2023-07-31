#pragma once

#include <library/cpp/getopt/opt.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>

class IInputStream;
class IOutputStream;

namespace NJson {
    class TJsonValue;
}

namespace NIPREG {
    class TReader;

    // @input   any form of range+payload
    // @output  $ip.begin-$ip.end \t {"region_id":$reg,"reliability":$rel}
    void DoCoarsening(IInputStream& input, IOutputStream& output);

    struct MergeTraits {
        const TVector<TString> ExcludeFieldsList;
        TString ConcatSep;
        bool SortData{};
        bool CountMerges{};
        bool JoinNestedRanges{};
    };

    void DoMerging(TReader& input, IOutputStream& output, const MergeTraits& traits);
    void DoMerging3(TReader& input, IOutputStream& output, const TString& geodata, bool ByRegsOnly = false, bool silentMode = false);
    void DoMergeEqualsRange(TReader& input, IOutputStream& output);

    void DoPatching(TReader& base, TReader& patch, IOutputStream& output, bool sortData = false);

    void AddStubRanges(TReader& input, IOutputStream& output);

    void CheckAddressSpaceForCompleteness(IInputStream& input, IOutputStream& output);
    void CheckRangesForMonotonicSequence(IInputStream& input, IOutputStream& output, bool IsStrict = false);

    NJson::TJsonValue ParseJsonString(const TString& data);
    TString SortJsonData(const NJson::TJsonValue& json);
    TString SortJsonData(const TString& json);

    TString AddJsonAttrs(const TVector<TString>& addFieldsList, const TString& jsonStr, const TMaybe<TString>& attrValue);
    TString ExcludeJsonAttrs(const TVector<TString>& excludeFieldsList, const TString& jsonStr);
    TString ExtractJsonAttrs(const TVector<TString>& excludeFieldsList, const TString& jsonStr);

    extern const TString STUB_DATA;

    struct DefaultCliParams {
        DefaultCliParams();

        NLastGetopt::TOpts& GetOpts() { return Opts; }
        void Parse(int argc, const char **argv);
        void ApplyFlags() const;

        TString InputFname  = "-";
        TString OutputFname = "";
        bool OutputFullIp = false;
        bool PrintStats = false;
        bool PrintYtStats = false;

        NLastGetopt::TOpts Opts;
    };
} // NIPREG
