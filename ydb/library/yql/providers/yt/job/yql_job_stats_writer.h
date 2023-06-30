#pragma once

#include <ydb/library/yql/minikql/mkql_stats_registry.h>

#include <util/stream/output.h>

namespace NYql {

struct TJobCountersProvider;

/**
 * @brief Writes stats to out stream (if defined) or to special YT-job file
 *        descriptor (see https://wiki.yandex-team.ru/yt/userdoc/jobs/#sborstatistikivdzhobax).
 */
void WriteJobStats(
        const NKikimr::NMiniKQL::IStatsRegistry* stats,
        const TJobCountersProvider& countersProvider,
        IOutputStream* out = nullptr);

} // namspace NYql
