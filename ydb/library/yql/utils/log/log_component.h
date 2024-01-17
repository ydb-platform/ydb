#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>


namespace NYql {
namespace NLog {

// keep this enum in sync with simmilar enum from ydb/library/yql/utils/log/proto/logger_config.proto
enum class EComponent {
    Default = 0,
    Core,
    CoreExecution,
    Sql,
    ProviderCommon,
    ProviderConfig,
    ProviderResult,
    ProviderYt,
    ProviderKikimr,
    ProviderKqp,
    ProviderRtmr,
    Performance, Perf = Performance,
    Net,
    ProviderStat,
    ProviderSolomon,
    CoreEval,
    CorePeepHole,
    ProviderDq,
    ProviderClickHouse,
    ProviderYdb,
    ProviderPq,
    ProviderS3,
    CoreDq,
    HttpGateway,
    ProviderGeneric,
    ProviderPg,
    // <--- put other log components here
    MaxValue
};

struct EComponentHelpers {
    static constexpr int ToInt(EComponent component) {
        return static_cast<int>(component);
    }

    static constexpr EComponent FromInt(int component) {
        return (component >= ToInt(EComponent::Default) &&
                component < ToInt(EComponent::MaxValue))
                    ? static_cast<EComponent>(component)
                    : EComponent::Default;
    }

    static TStringBuf ToString(EComponent component) {
        switch (component) {
        case EComponent::Default: return TStringBuf("default");
        case EComponent::Core: return TStringBuf("core");
        case EComponent::CoreEval: return TStringBuf("core eval");
        case EComponent::CorePeepHole: return TStringBuf("core peephole");
        case EComponent::CoreExecution: return TStringBuf("core exec");
        case EComponent::Sql: return TStringBuf("sql");
        case EComponent::ProviderCommon: return TStringBuf("common provider");
        case EComponent::ProviderConfig: return TStringBuf("CONFIG");
        case EComponent::ProviderResult: return TStringBuf("RESULT");
        case EComponent::ProviderYt: return TStringBuf("YT");
        case EComponent::ProviderKikimr: return TStringBuf("KIKIMR");
        case EComponent::ProviderKqp: return TStringBuf("KQP");
        case EComponent::ProviderRtmr: return TStringBuf("RTMR");
        case EComponent::Performance: return TStringBuf("perf");
        case EComponent::Net: return TStringBuf("net");
        case EComponent::ProviderStat: return TStringBuf("STATFACE");
        case EComponent::ProviderSolomon: return TStringBuf("SOLOMON");
        case EComponent::ProviderDq: return TStringBuf("DQ");
        case EComponent::ProviderClickHouse: return TStringBuf("CLICKHOUSE");
        case EComponent::ProviderYdb: return TStringBuf("YDB");
        case EComponent::ProviderPq: return TStringBuf("PQ");
        case EComponent::ProviderS3: return TStringBuf("S3");
        case EComponent::CoreDq: return TStringBuf("core dq");
        case EComponent::HttpGateway: return TStringBuf("http gw");
        case EComponent::ProviderGeneric: return TStringBuf("generic");
        case EComponent::ProviderPg: return TStringBuf("PG");
        default:
            ythrow yexception() << "invalid log component value: "
                                << ToInt(component);
        }
    }

    static EComponent FromString(TStringBuf str) {
        if (str == TStringBuf("default")) return EComponent::Default;
        if (str == TStringBuf("core")) return EComponent::Core;
        if (str == TStringBuf("core eval")) return EComponent::CoreEval;
        if (str == TStringBuf("core peephole")) return EComponent::CorePeepHole;
        if (str == TStringBuf("core exec")) return EComponent::CoreExecution;
        if (str == TStringBuf("sql")) return EComponent::Sql;
        if (str == TStringBuf("common provider")) return EComponent::ProviderCommon;
        if (str == TStringBuf("CONFIG")) return EComponent::ProviderConfig;
        if (str == TStringBuf("RESULT")) return EComponent::ProviderResult;
        if (str == TStringBuf("YT")) return EComponent::ProviderYt;
        if (str == TStringBuf("KIKIMR")) return EComponent::ProviderKikimr;
        if (str == TStringBuf("KQP")) return EComponent::ProviderKqp;
        if (str == TStringBuf("RTMR")) return EComponent::ProviderRtmr;
        if (str == TStringBuf("perf")) return EComponent::Performance;
        if (str == TStringBuf("net")) return EComponent::Net;
        if (str == TStringBuf("STATFACE")) return EComponent::ProviderStat;
        if (str == TStringBuf("SOLOMON")) return EComponent::ProviderSolomon;
        if (str == TStringBuf("DQ")) return EComponent::ProviderDq;
        if (str == TStringBuf("CLICKHOUSE")) return EComponent::ProviderClickHouse;
        if (str == TStringBuf("YDB")) return EComponent::ProviderYdb;
        if (str == TStringBuf("PQ")) return EComponent::ProviderPq;
        if (str == TStringBuf("S3")) return EComponent::ProviderS3;
        if (str == TStringBuf("core dq")) return EComponent::CoreDq;
        if (str == TStringBuf("http gw")) return EComponent::HttpGateway;
        if (str == TStringBuf("generic")) return EComponent::ProviderGeneric;
        if (str == TStringBuf("PG")) return EComponent::ProviderPg;
        ythrow yexception() << "unknown log component: '" << str << '\'';
    }

    template <typename TFunctor>
    static void ForEach(TFunctor&& f) {
        static const int minValue = ToInt(EComponent::Default);
        static const int maxValue = ToInt(EComponent::MaxValue);

        for (int c = minValue; c < maxValue; c++) {
            f(FromInt(c));
        }
    }
};

} // namespace NLog
} // namespace NYql
