#include "yql_yt_file_comp_nodes.h"
#include "yql_yt_file.h"
#include "yql_yt_file_text_yson.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_file_input_state.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_file_list.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_table.h>
#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_ungrouping_list.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/public/udf/udf_version.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node.h>


#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/writer.h>

#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/stream/buffer.h>
#include <util/system/fs.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/xrange.h>
#include <util/generic/ylimits.h>
#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>

#include <utility>
#include <algorithm>
#include <iterator>
#include <array>

namespace NYql::NFile {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TFileInputStateWithTableState: public TFileInputState {
public:
    TFileInputStateWithTableState(const TMkqlIOSpecs& spec, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<NYT::TRawTableReaderPtr>&& rawReaders,
        size_t blockCount, size_t blockSize, TTableState&& tableState)
        : TFileInputState(spec, holderFactory, std::move(rawReaders), blockCount, blockSize)
        , TableState_(std::move(tableState))
    {
        UpdateTableState();
    }

protected:
    void Next() override {
        TFileInputState::Next();
        UpdateTableState();
    }

    void UpdateTableState() {
        if (IsValid()) {
            TableState_.Update(GetTableIndex(), GetRecordIndex());
        } else {
            TableState_.Reset();
        }
    }

private:
    TTableState TableState_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TTextYsonFileListValue : public TFileListValueBase {
public:
    TTextYsonFileListValue(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo,
        const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const TVector<std::pair<TString, TColumnsInfo>>& tablePaths,
        TTableState&& tableState, std::optional<ui64> length)
        : TFileListValueBase(memInfo, spec, holderFactory, length)
        , TablePaths_(tablePaths)
        , TableState_(std::move(tableState))
    {
    }

protected:
    THolder<IInputState> MakeState() const override {
        return MakeHolder<TFileInputStateWithTableState>(Spec, HolderFactory, MakeTextYsonInputs(TablePaths_),
            0u, 1_MB, TTableState(TableState_));
    }

private:
    TVector<std::pair<TString, TColumnsInfo>> TablePaths_;
    TTableState TableState_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

template <bool TableContent>
class TYtTableFileWrapper : public TMutableComputationNode<TYtTableFileWrapper<TableContent>> {
    typedef TMutableComputationNode<TYtTableFileWrapper<TableContent>> TBaseComputation;
public:
    TYtTableFileWrapper(TComputationMutables& mutables, NCommon::TCodecContext& codecCtx,
        TVector<std::pair<TString, TColumnsInfo>>&& tablePaths,
        const NYT::TNode& inputSpecs, const TVector<ui32>& groups, TType* itemType,
        TVector<TString>&& tableNames, TVector<ui64>&& rowOffsets, THashSet<TString>&& auxColumns,
        std::array<IComputationExternalNode*, 5>&& argNodes, std::optional<ui64> length)
        : TBaseComputation(mutables)
        , TablePaths_(std::move(tablePaths))
        , ArgNodes_(std::move(argNodes))
        , Length_(std::move(length))
    {
        Spec_.Init(codecCtx, inputSpecs, groups, tableNames, itemType, auxColumns, NYT::TNode());
        if (!rowOffsets.empty()) {
            Spec_.SetTableOffsets(rowOffsets);
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (TableContent) {
            return ctx.HolderFactory.Create<TTextYsonFileListValue>(Spec_, ctx.HolderFactory, TablePaths_,
                TTableState(Spec_.TableNames, Spec_.TableOffsets, ctx, ArgNodes_), Length_);
        }
        else {
            THolder<TFileInputState> inputState(new TFileInputStateWithTableState(Spec_, ctx.HolderFactory, MakeTextYsonInputs(TablePaths_),
                0, 1_MB, TTableState(Spec_.TableNames, Spec_.TableOffsets, ctx, ArgNodes_)));
            NUdf::TUnboxedValue singlePassIter(ctx.HolderFactory.Create<THoldingInputStreamValue>(inputState.Release()));
            return ctx.HolderFactory.Create<TForwardListValue>(std::move(singlePassIter));
        }
    }

private:
    void RegisterDependencies() const final {
        for (auto node: ArgNodes_) {
            TBaseComputation::Own(node);
        }
    }

    TMkqlIOSpecs Spec_;
    const TVector<std::pair<TString, TColumnsInfo>> TablePaths_;
    const std::array<IComputationExternalNode*, 5> ArgNodes_;
    std::optional<ui64> Length_;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TFileWriteApplyContext : public NUdf::IApplyContext {
public:
    TFileWriteApplyContext(TMkqlWriterImpl& writer)
        : Writer(writer)
    {
    }

    void WriteStream(const NUdf::TUnboxedValue& res) {
        NUdf::TUnboxedValue value;
        for (auto status = res.Fetch(value); status != NUdf::EFetchStatus::Finish; status = res.Fetch(value)) {
            if (status != NUdf::EFetchStatus::Yield)
                Writer.AddRow(value);
        }
    }

private:
    TMkqlWriterImpl& Writer;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TFileWriteWrapper : public TMutableComputationNode<TFileWriteWrapper> {
    typedef TMutableComputationNode<TFileWriteWrapper> TBaseComputation;
public:
    class TResult : public TComputationValue<TResult> {
    public:
        TResult(TMemoryUsageInfo* memInfo, const NUdf::TUnboxedValue& result)
            : TComputationValue(memInfo)
            , Result(result)
        {
        }

    private:
        void Apply(NUdf::IApplyContext& applyContext) const override {
            CheckedCast<TFileWriteApplyContext*>(&applyContext)->WriteStream(Result);
        }

        const NUdf::TUnboxedValue& Result;
    };


    TFileWriteWrapper(TComputationMutables& mutables, IComputationNode* result)
        : TBaseComputation(mutables)
        , Result(result)
        , StreamValueIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& streamRef = ctx.MutableValues[StreamValueIndex];
        streamRef = Result->GetValue(ctx);
        return ctx.HolderFactory.Create<TResult>(streamRef);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Result);
    }

    IComputationNode* const Result;
    const ui32 StreamValueIndex;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TDqWriteWrapper : public TMutableComputationNode<TDqWriteWrapper> {
    typedef TMutableComputationNode<TDqWriteWrapper> TBaseComputation;
public:
    TDqWriteWrapper(TComputationMutables& mutables, IComputationNode* item, const TString& path, TType* itemType, const TYtFileServices::TPtr& services)
        : TBaseComputation(mutables)
        , Item(item)
        , Path(path)
        , Packer(true, itemType)
        , Services(services)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Item->GetValue(ctx);
        Services->PushTableContent(Path, TString{Packer.Pack(value)});
        return {};
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Item);
    }

    IComputationNode* const Item;
    const TString Path;
    TValuePacker Packer;
    TYtFileServices::TPtr Services;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

template <bool TableContent>
IComputationNode* WrapYtTableFile(NMiniKQL::TCallable& callable, const TComputationNodeFactoryContext& ctx,
    const TYtFileServices::TPtr& services, bool noLocks)
{
    if (TableContent) {
        YQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    } else {
        YQL_ENSURE(callable.GetInputsCount() == 8 || callable.GetInputsCount() == 4, "Expected 8 or 4 args");
    }
    const TString cluster(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());

    TType* itemType = AS_TYPE(NMiniKQL::TListType, callable.GetType()->GetReturnType())->GetItemType();
    TVector<std::pair<TString, TColumnsInfo>> tablePaths;
    TVector<TString> tableNames;
    TVector<ui64> rowOffsets;
    ui64 currentRowOffset = 0;
    TVector<ui32> groups;
    NYT::TNode inputAttrs = NYT::TNode::CreateList();
    THashSet<TString> auxColumns;

    NCommon::TCodecContext codecCtx(ctx.Env, ctx.FunctionRegistry, &ctx.HolderFactory);

    TListLiteral* groupList = AS_VALUE(TListLiteral, callable.GetInput(1));
    const bool multiGroup = groupList->GetItemsCount() > 1;

    TVector<THashSet<TStringBuf>> outColumnGroups;
    if (multiGroup) {
        TVariantType* itemVarType = AS_TYPE(TVariantType, itemType);
        for (ui32 g = 0; g < groupList->GetItemsCount(); ++g) {
            NMiniKQL::TStructType* itemStruct = AS_TYPE(TStructType, itemVarType->GetAlternativeType(g));
            auto& outColumns = outColumnGroups.emplace_back();
            for (ui32 index = 0; index < itemStruct->GetMembersCount(); ++index) {
                outColumns.insert(itemStruct->GetMemberName(index));
            }
        }
    }
    else {
        NMiniKQL::TStructType* itemStruct = AS_TYPE(NMiniKQL::TStructType, itemType);
        auto& outColumns = outColumnGroups.emplace_back();
        for (ui32 index = 0; index < itemStruct->GetMembersCount(); ++index) {
            outColumns.insert(itemStruct->GetMemberName(index));
        }
    }

    for (ui32 g = 0; g < groupList->GetItemsCount(); ++g) {
        TListLiteral* tableList = AS_VALUE(TListLiteral, groupList->GetItems()[g]);
        auto& outColumns = outColumnGroups[g];
        currentRowOffset = 0;
        for (ui32 t = 0; t < tableList->GetItemsCount(); ++t) {
            TTupleLiteral* tuple = AS_VALUE(TTupleLiteral, tableList->GetItems()[t]);
            YQL_ENSURE(tuple->GetValuesCount() == 7, "Expect 7 elements in the table tuple");

            NYT::TRichYPath richYPath;
            NYT::Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, tuple->GetValue(0))->AsValue().AsStringRef())));

            const bool isTemporary = AS_VALUE(TDataLiteral, tuple->GetValue(1))->AsValue().Get<bool>();
            auto tableMeta = NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, tuple->GetValue(2))->AsValue().AsStringRef()));
            TMkqlIOSpecs::TSpecInfo specInfo;
            TMkqlIOSpecs::LoadSpecInfo(true, tableMeta, codecCtx, specInfo);
            NMiniKQL::TStructType* itemStruct = AS_TYPE(NMiniKQL::TStructType, specInfo.Type);
            for (ui32 index = 0; index < itemStruct->GetMembersCount(); ++index) {
                // Ignore extra columns, which are not selected from the table
                if (!outColumns.contains(itemStruct->GetMemberName(index))) {
                    auxColumns.insert(TString{itemStruct->GetMemberName(index)});
                }
            }
            for (auto& aux: specInfo.AuxColumns) {
                if (!outColumns.contains(aux.first)) {
                    auxColumns.insert(aux.first);
                }
            }

            inputAttrs.Add(tableMeta);
            auto path = services->GetTablePath(cluster, richYPath.Path_, isTemporary, noLocks);
            tableNames.push_back(isTemporary ? TString() : richYPath.Path_);
            tablePaths.emplace_back(path, TColumnsInfo{richYPath.Columns_, richYPath.RenameColumns_});
            if (!richYPath.Columns_ && !isTemporary && specInfo.StrictSchema) {
                TVector<TString> columns;
                for (ui32 index = 0; index < itemStruct->GetMembersCount(); ++index) {
                    columns.emplace_back(itemStruct->GetMemberName(index));
                }
                for (auto& aux: specInfo.AuxColumns) {
                    columns.push_back(aux.first);
                }
                tablePaths.back().second.Columns.ConstructInPlace(std::move(columns));
            }
            if (multiGroup) {
                groups.push_back(g);
            }
            rowOffsets.push_back(currentRowOffset);
            currentRowOffset += AS_VALUE(TDataLiteral, tuple->GetValue(4))->AsValue().Get<ui64>();
        }
    }

    std::optional<ui64> length;
    TTupleLiteral* lengthTuple = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    if (lengthTuple->GetValuesCount() > 0) {
        YQL_ENSURE(lengthTuple->GetValuesCount() == 1, "Expect 1 element in the length tuple");
        length = AS_VALUE(TDataLiteral, lengthTuple->GetValue(0))->AsValue().Get<ui64>();
    }

    std::array<IComputationExternalNode*, 5> argNodes;
    argNodes.fill(nullptr);

    if (!TableContent && callable.GetInputsCount() == 8) {
        argNodes[0] = LocateExternalNode(ctx.NodeLocator, callable, 4, false); // TableIndex
        argNodes[1] = LocateExternalNode(ctx.NodeLocator, callable, 5, false); // TablePath
        argNodes[2] = LocateExternalNode(ctx.NodeLocator, callable, 6, false); // TableRecord
        argNodes[4] = LocateExternalNode(ctx.NodeLocator, callable, 7, false); // RowNumber
    }

    // sampling arg is ignored in the file provider
    return new TYtTableFileWrapper<TableContent>(ctx.Mutables, codecCtx, std::move(tablePaths),
        NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, std::move(inputAttrs)),
        groups, itemType, std::move(tableNames), std::move(rowOffsets), std::move(auxColumns), std::move(argNodes), length);
}

IComputationNode* WrapFileWrite(NMiniKQL::TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    YQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    return new TFileWriteWrapper(ctx.Mutables, list);
}

IComputationNode* WrapDqWrite(NMiniKQL::TCallable& callable, const TComputationNodeFactoryContext& ctx, const TYtFileServices::TPtr& services) {
    YQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    const auto item = LocateNode(ctx.NodeLocator, callable, 0);
    TType* itemType = callable.GetInput(0).GetStaticType();
    auto path = TString(AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().AsStringRef());
    return new TDqWriteWrapper(ctx.Mutables, item, path, itemType, services);
}

} // unnamed

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TYtFileFactory {
public:
    TYtFileFactory(const TYtFileServices::TPtr& services)
        : Services_(services)
    {
    }

    IComputationNode* operator() (NMiniKQL::TCallable& callable, const TComputationNodeFactoryContext& ctx) {
        auto name = callable.GetType()->GetName();
        if (name.ChopSuffix("File")) {
            if (name == TStringBuf("YtWrite")) {
                return WrapFileWrite(callable, ctx);
            }
            if (name == TStringBuf("DqWrite")) {
                return WrapDqWrite(callable, ctx, Services_);
            }
            if (name == NNodes::TYtTableContent::CallableName()) {
                return WrapYtTableFile<true>(callable, ctx, Services_, false);
            }
            if (name == "YtUngroupingList") {
                return WrapYtUngroupingList(callable, ctx);
            }
            if (name == "YtTableInput") {
                return WrapYtTableFile<false>(callable, ctx, Services_, false);
            }
            if (name == "YtTableInputNoCtx") {
                return WrapYtTableFile<false>(callable, ctx, Services_, true);
            }
        }

        return nullptr;
    }
private:
    TYtFileServices::TPtr Services_;
    TMaybe<ui32> ExprContextObject;
};

TComputationNodeFactory GetYtFileFactory(const TYtFileServices::TPtr& services) {
    return TYtFileFactory(services);
}

NKikimr::NMiniKQL::TComputationNodeFactory GetYtFileFullFactory(const TYtFileServices::TPtr& services) {
    return NMiniKQL::GetCompositeWithBuiltinFactory({
        GetYtFileFactory(services),
        NMiniKQL::GetYqlFactory(),
        GetPgFactory()
    });
}

TVector<TString> GetFileWriteResult(const TTypeEnvironment& env, const IFunctionRegistry& functionRegistry, TComputationContext& ctx, const NKikimr::NUdf::TUnboxedValue& value,
    const NYT::TNode& outSpecs)
{
    NCommon::TCodecContext codecCtx(env, functionRegistry, &ctx.HolderFactory);
    TMkqlIOSpecs spec;
    spec.Init(codecCtx, outSpecs);

    TVector<TStringStream> streams(spec.Outputs.size());
    {
        TVector<IOutputStream*> out(Reserve(spec.Outputs.size()));
        std::transform(streams.begin(), streams.end(), std::back_inserter(out), [] (TStringStream& s) { return &s; });
        TMkqlWriterImpl writer(out, 0, 4_MB);
        writer.SetSpecs(spec);

        TFileWriteApplyContext applyCtx(writer);
        ApplyChanges(value, applyCtx);
        writer.Finish();
    }
    TVector<TString> res;
    res.reserve(spec.Outputs.size());
    std::transform(streams.begin(), streams.end(), std::back_inserter(res), [] (TStringStream& s) { return s.Str(); });
    return res;
}


} // NYql::NFile
