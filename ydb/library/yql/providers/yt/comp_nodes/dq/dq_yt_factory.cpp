#include "dq_yt_factory.h"
#include "dq_yt_reader.h"
#include "dq_yt_writer.h"

namespace NYql {

using namespace NKikimr::NMiniKQL;

TComputationNodeFactory GetDqYtFactory(NKikimr::NMiniKQL::IStatsRegistry* jobStats) {
    return [=] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        TStringBuf name = callable.GetType()->GetName();
        if (name == "DqYtRead") {
            return NDqs::WrapDqYtRead(callable, jobStats, ctx);
        }

        if (name == "YtDqRowsWideWrite") {
            return NDqs::WrapYtDqRowsWideWrite(callable, ctx);
        }

        return nullptr;
        };
}

} // NYql
