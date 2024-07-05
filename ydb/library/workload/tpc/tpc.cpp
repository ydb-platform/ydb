#include "tpc.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/resource/resource.h>
#include <util/stream/file.h>
#include <util/string/split.h>

namespace NYdbWorkload {

TTpcWorkloadGenerator::TTpcWorkloadGenerator(const TTpcWorkloadParams& params)
    : TWorkloadGeneratorBase(params)
    , Params(params)
{}

void TTpcWorkloadGenerator::PatchQuery(TString& query) const {
    TString header;
    switch (Params.GetFloatMode()) {
    case TTpcWorkloadParams::EFloatMode::FLOAT:
        header = NResource::Find("consts.yql");
        break;
    case TTpcWorkloadParams::EFloatMode::DECIMAL:
        header = NResource::Find("consts_decimal.yql");
        break;
    case TTpcWorkloadParams::EFloatMode::DECIMAL_YDB: {
            header = NResource::Find("consts_decimal.yql");
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
        break;
    }
    SubstGlobal(query, "{% include 'header.sql.jinja' %}", header);
}

void TTpcWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
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
