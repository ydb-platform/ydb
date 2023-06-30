#include "yql_job_calc.h"
#include "yql_job_factory.h"

#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/lib/lambda_builder/lambda_builder.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

#include <library/cpp/yson/node/node_builder.h>
#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>

#include <library/cpp/streams/brotli/brotli.h>

#include <util/generic/xrange.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/stream/input.h>
#include <util/ysaveload.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

void TYqlCalcJob::Save(IOutputStream& stream) const {
    TYqlJobBase::Save(stream);
    ::Save(&stream, Columns_);
    ::Save(&stream, UseResultYson_);
}

void TYqlCalcJob::Load(IInputStream& stream) {
    TYqlJobBase::Load(stream);
    ::Load(&stream, Columns_);
    ::Load(&stream, UseResultYson_);
}

void TYqlCalcJob::DoImpl(const TFile& inHandle, const TVector<TFile>& outHandles) {
    NYT::TTableReader<NYT::TNode> reader(MakeIntrusive<NYT::TNodeTableReader>(MakeIntrusive<NYT::TJobReader>(inHandle)));
    NYT::TTableWriter<NYT::TNode> writer(MakeIntrusive<NYT::TNodeTableWriter>(MakeHolder<NYT::TJobWriter>(outHandles)));

    Init();

    TLambdaBuilder builder(FunctionRegistry.Get(), *Alloc,Env.Get(),
        RandomProvider.Get(), TimeProvider.Get(), JobStats.Get(), nullptr, SecureParamsProvider.Get());

    std::function<void(const NUdf::TUnboxedValuePod&, TType*, TVector<ui32>*)> flush;
    if (UseResultYson_) {
        flush = [&writer] (const NUdf::TUnboxedValuePod& v, TType* type, TVector<ui32>* structPositions) {
            NYT::TNode row = NYT::TNode::CreateMap();
            NYT::TNodeBuilder nodeBuilder(&row["output"]);
            NCommon::WriteYsonValue(nodeBuilder, v, type, structPositions);
            writer.AddRow(row);
        };
    }
    else {
        flush = [&writer] (const NUdf::TUnboxedValuePod& v, TType* type, TVector<ui32>* /*structPositions*/) {
            writer.AddRow(NYT::TNode()("output", NCommon::ValueToNode(v, type)));
        };
    }

    auto factory = GetJobFactory(*CodecCtx, OptLLVM, nullptr, nullptr, nullptr);
    for (; reader.IsValid(); reader.Next()) {
        const auto& row = reader.GetRow();
        TStringStream lambda;
        {
            TStringInput in(row["input"].AsString());
            TBrotliDecompress decompress(&in);
            TransferData(&decompress, &lambda);
        }
        TRuntimeNode rootNode = DeserializeRuntimeNode(lambda.Str(), *Env);
        rootNode = builder.TransformAndOptimizeProgram(rootNode, MakeTransformProvider());
        TType* outType = rootNode.GetStaticType();
        TExploringNodeVisitor explorer;
        auto graph = builder.BuildGraph(
            factory,
            UdfValidateMode,
            NUdf::EValidatePolicy::Fail, OptLLVM,
            EGraphPerProcess::Single,
            explorer, rootNode);
        const TBindTerminator bind(graph->GetTerminator());
        graph->Prepare();
        auto value = graph->GetValue();
        if (outType->IsTuple()) {
            auto tupleType = AS_TYPE(NMiniKQL::TTupleType, outType);
            for (ui32 i: xrange(tupleType->GetElementsCount())) {
                flush(value.GetElement(i), tupleType->GetElementType(i), nullptr);
            }
        } else if (outType->IsList()) {
            auto itemType = AS_TYPE(NMiniKQL::TListType, outType)->GetItemType();
            TMaybe<TVector<ui32>> structPositions = NCommon::CreateStructPositions(itemType, Columns_.Get());
            const auto it = value.GetListIterator();
            for (NUdf::TUnboxedValue item; it.Next(item);) {
                flush(item, itemType, structPositions.Get());
            }
        } else {
            flush(value, outType, nullptr);
        }
    }
}

} // NYql
