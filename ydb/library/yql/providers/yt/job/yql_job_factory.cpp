#include "yql_job_factory.h"

#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_input.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_output.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_table_content.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>

#include <util/generic/strbuf.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TComputationNodeFactory GetJobFactory(NYql::NCommon::TCodecContext& codecCtx, const TString& optLLVM,
    const TMkqlIOSpecs* specs, NYT::IReaderImplBase* reader, TJobMkqlWriterImpl* writer)
{
    TMaybe<ui32> exprContextObject;
    return [&codecCtx, optLLVM, specs, reader, writer, exprContextObject](NMiniKQL::TCallable& callable, const TComputationNodeFactoryContext& ctx) mutable -> IComputationNode* {
        TStringBuf name = callable.GetType()->GetName();
        if (name.SkipPrefix("Yt") && name.ChopSuffix("Job")) {
            if (name == "TableContent") {
                return WrapYtTableContent(codecCtx, ctx.Mutables, callable, optLLVM, {} /*empty pathPrefix inside job*/);
            }
            if (name == "Input") {
                YQL_ENSURE(reader);
                YQL_ENSURE(specs);
                return WrapYtInput(callable, ctx, *specs, reader);
            }
            if (name == "Output") {
                YQL_ENSURE(writer);
                return WrapYtOutput(callable, ctx, *writer);
            }
        }

        if (!exprContextObject) {
           exprContextObject = ctx.Mutables.CurValueIndex++;
        }

        auto yql = GetYqlFactory(*exprContextObject)(callable, ctx);
        if (yql) {
            return yql;
        }

        auto pg = GetPgFactory()(callable, ctx);
        if (pg) {
            return pg;
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}


} // NYql
