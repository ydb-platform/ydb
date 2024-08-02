#include "tpc_base.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdbWorkload {

TTpcBaseWorkloadGenerator::TTpcBaseWorkloadGenerator(const TTpcBaseWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

void TTpcBaseWorkloadGenerator::PatchQuery(TString& query) const {
    TString header;
    switch (Params.GetFloatMode()) {
    case TTpcBaseWorkloadParams::EFloatMode::FLOAT:
        header = FilterHeader(NResource::Find("consts.yql"), query);
        break;
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL:
    case TTpcBaseWorkloadParams::EFloatMode::DECIMAL_YDB:
        header = FilterHeader(NResource::Find("consts_decimal.yql"), query);
        break;
    }
    PatchHeader(header);
    SubstGlobal(query, "{% include 'header.sql.jinja' %}", header);
}

TString TTpcBaseWorkloadGenerator::FilterHeader(TStringBuf header, const TString& query) const {
    TStringBuilder result;
    for(TStringBuf line; header.ReadLine(line);) {
        const auto pos = line.find('=');
        if (pos == line.npos) {
            continue;
        }
        const auto name = StripString(line.SubString(0, pos));
        for(auto posInQ = query.find(name); posInQ != query.npos; posInQ = query.find(name, posInQ)) {
            posInQ += name.length();
            if (posInQ >= query.length() || !IsAsciiAlnum(query[posInQ]) && query[posInQ] != '_') {
                result << line << Endl;
                break;
            }
        }
    }
    return result;
}

void TTpcBaseWorkloadGenerator::PatchHeader(TString& header) const {
    if (Params.GetFloatMode() != TTpcBaseWorkloadParams::EFloatMode::DECIMAL_YDB) {
        return;
    }
    header.to_lower();
    const TStringBuf dec("decimal(");
    auto p = header.find(dec);
    while (p != TString::npos) {
        p += dec.length();
        const auto q = header.find(')', p);
        TVector<ui32> decParams;
        StringSplitter(header.cbegin() + p, q - p).SplitBySet(", ").SkipEmpty().Limit(2).ParseInto(&decParams);
        TStringBuilder newDecParams;
        newDecParams
            << Max(decParams[0], NKikimr::NScheme::DECIMAL_PRECISION)
            << "," << Max(decParams[1], NKikimr::NScheme::DECIMAL_SCALE);
        header.replace(p, q - p, newDecParams);
        p = header.find(dec, q);
    }
}

void TTpcBaseWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    TWorkloadBaseParams::ConfigureOpts(opts, commandType, workloadType);
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("float-mode", "Float mode. Can be float, decimal or decimal_22_9. If set to 'float' - float will be used, 'decimal' means use decimal with cannonical size and 'decimal_22_9' means, that all floats will be converted to decimal(22,9)")
            .StoreResult(&FloatMode).DefaultValue(FloatMode);
        break;
    default:
        break;
    }
}

}
