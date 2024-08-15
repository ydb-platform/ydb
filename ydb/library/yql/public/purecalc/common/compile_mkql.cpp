#include "compile_mkql.h"

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/core/yql_user_data_storage.h>
#include <ydb/library/yql/public/purecalc/common/names.h>

#include <util/stream/file.h>

namespace NYql::NPureCalc {

namespace {

NCommon::IMkqlCallableCompiler::TCompiler MakeSelfCallableCompiler() {
    return [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
        MKQL_ENSURE(node.ChildrenSize() == 1, "Self takes exactly 1 argument");
        const auto* argument = node.Child(0);
        MKQL_ENSURE(argument->IsAtom(), "Self argument must be atom");
        ui32 inputIndex = 0;
        MKQL_ENSURE(TryFromString(argument->Content(), inputIndex), "Self argument must be UI32");
        auto type = NCommon::BuildType(node, *node.GetTypeAnn(), ctx.ProgramBuilder);
        NKikimr::NMiniKQL::TCallableBuilder call(ctx.ProgramBuilder.GetTypeEnvironment(), node.Content(), type);
        call.Add(ctx.ProgramBuilder.NewDataLiteral<ui32>(inputIndex));
        return NKikimr::NMiniKQL::TRuntimeNode(call.Build(), false);
    };
}

NCommon::IMkqlCallableCompiler::TCompiler MakeFilePathCallableCompiler(const TUserDataTable& userData) {
    return [&](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
        const TString name(node.Child(0)->Content());
        auto block = TUserDataStorage::FindUserDataBlock(userData, TUserDataKey::File(name));
        if (!block) {
            auto blockKey = TUserDataKey::File(GetDefaultFilePrefix() + name);
            block = TUserDataStorage::FindUserDataBlock(userData, blockKey);
        }
        MKQL_ENSURE(block, "file not found: " << name);
        MKQL_ENSURE(block->Type == EUserDataType::PATH,
                    "FilePath not supported for non-filesystem user data, name: "
                        << name << ", block type: " << block->Type);
        return ctx.ProgramBuilder.NewDataLiteral<NKikimr::NUdf::EDataSlot::String>(block->Data);
    };
}

NCommon::IMkqlCallableCompiler::TCompiler MakeFileContentCallableCompiler(const TUserDataTable& userData) {
    return [&](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
        const TString name(node.Child(0)->Content());
        auto block = TUserDataStorage::FindUserDataBlock(userData, TUserDataKey::File(name));
        if (!block) {
            auto blockKey = TUserDataKey::File(GetDefaultFilePrefix() + name);
            block = TUserDataStorage::FindUserDataBlock(userData, blockKey);
        }
        MKQL_ENSURE(block, "file not found: " << name);
        if (block->Type == EUserDataType::PATH) {
            auto content = TFileInput(block->Data).ReadAll();
            return ctx.ProgramBuilder.NewDataLiteral<NKikimr::NUdf::EDataSlot::String>(content);
        } else if (block->Type == EUserDataType::RAW_INLINE_DATA) {
            return ctx.ProgramBuilder.NewDataLiteral<NKikimr::NUdf::EDataSlot::String>(block->Data);
        } else {
            // TODO support EUserDataType::URL
            MKQL_ENSURE(false, "user data blocks of type URL are not supported by FileContent: " << name);
            Y_UNREACHABLE();
        }
    };
}

NCommon::IMkqlCallableCompiler::TCompiler MakeFolderPathCallableCompiler(const TUserDataTable& userData) {
    return [&](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
        const TString name(node.Child(0)->Content());
        auto folderName = TUserDataStorage::MakeFolderName(name);
        TMaybe<TString> folderPath;
        for (const auto& x : userData) {
            if (!x.first.Alias().StartsWith(folderName)) {
                continue;
            }

            MKQL_ENSURE(x.second.Type == EUserDataType::PATH,
                        "FilePath not supported for non-file data block, name: "
                            << x.first.Alias() << ", block type: " << x.second.Type);

            auto pathPrefixLength = x.second.Data.size() - (x.first.Alias().size() - folderName.size());
            auto newFolderPath = x.second.Data.substr(0, pathPrefixLength);
            if (!folderPath) {
                folderPath = newFolderPath;
            } else {
                MKQL_ENSURE(*folderPath == newFolderPath,
                            "file " << x.second.Data << " is out of directory " << *folderPath);
            }
        }
        return ctx.ProgramBuilder.NewDataLiteral<NKikimr::NUdf::EDataSlot::String>(*folderPath);
    };
}

}

NKikimr::NMiniKQL::TRuntimeNode CompileMkql(const TExprNode::TPtr& exprRoot, TExprContext& exprCtx,
    const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry, const NKikimr::NMiniKQL::TTypeEnvironment& env, const TUserDataTable& userData)
{
    NCommon::TMkqlCommonCallableCompiler compiler;

    compiler.AddCallable(PurecalcInputCallableName, MakeSelfCallableCompiler());
    compiler.AddCallable(PurecalcBlockInputCallableName, MakeSelfCallableCompiler());
    compiler.OverrideCallable("FileContent", MakeFileContentCallableCompiler(userData));
    compiler.OverrideCallable("FilePath", MakeFilePathCallableCompiler(userData));
    compiler.OverrideCallable("FolderPath", MakeFolderPathCallableCompiler(userData));

    // Prepare build context

    NKikimr::NMiniKQL::TProgramBuilder pgmBuilder(env, funcRegistry);
    NCommon::TMkqlBuildContext buildCtx(compiler, pgmBuilder, exprCtx);

    // Build the root MKQL node

    return NCommon::MkqlBuildExpr(*exprRoot, buildCtx);
}

} // NYql::NPureCalc
