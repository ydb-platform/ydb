#pragma once

#include <util/generic/string.h>

#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/tests/tools/kqprun/runlib/settings.h>

namespace NFqRun {

constexpr i64 MAX_RESULT_SET_ROWS = 1000;

struct TFqSetupSettings : public NKikimrRun::TServerSettings {
    NFq::NConfig::TConfig FqConfig;
    NKikimrConfig::TLogConfig LogConfig;
};

struct TRunnerOptions {
    IOutputStream* ResultOutput = nullptr;
    NKikimrRun::EResultOutputFormat ResultOutputFormat = NKikimrRun::EResultOutputFormat::RowsJson;

    TFqSetupSettings FqSettings;
};

struct TRequestOptions {
    TString Query;
};

}  // namespace NFqRun
