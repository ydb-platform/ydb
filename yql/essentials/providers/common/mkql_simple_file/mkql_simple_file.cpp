#include "mkql_simple_file.h"

#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <util/stream/file.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TSimpleFileTransformProvider::TSimpleFileTransformProvider(const IFunctionRegistry* functionRegistry,
    const TUserDataTable& userDataBlocks)
    : FunctionRegistry_(functionRegistry)
    , UserDataBlocks_(userDataBlocks)
{}

TCallableVisitFunc TSimpleFileTransformProvider::operator()(TInternName name) {
    if (name == "FilePath") {
        return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
            MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
            const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
            auto block = TUserDataStorage::FindUserDataBlock(UserDataBlocks_, name);
            MKQL_ENSURE(block, "File not found: " << name);
            MKQL_ENSURE(block->Type == EUserDataType::PATH || block->FrozenFile, "File is not frozen, name: "
                << name << ", block type: " << block->Type);
            return TProgramBuilder(env, *FunctionRegistry_).NewDataLiteral<NUdf::EDataSlot::String>(
                block->Type == EUserDataType::PATH ? block->Data : block->FrozenFile->GetPath().GetPath()
            );
        };
    }

    if (name == "FolderPath") {
        return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
            MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
            const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
            auto folderName = TUserDataStorage::MakeFolderName(name);
            TMaybe<TString> folderPath;
            for (const auto& x : UserDataBlocks_) {
                if (!x.first.Alias().StartsWith(folderName)) {
                    continue;
                }

                MKQL_ENSURE(x.second.Type == EUserDataType::PATH, "FolderPath not supported for non-file data block, name: "
                    << x.first.Alias() << ", block type: " << x.second.Type);
                auto newFolderPath = x.second.Data.substr(0, x.second.Data.size() - (x.first.Alias().size() - folderName.size()));
                if (!folderPath) {
                    folderPath = newFolderPath;
                } else {
                    MKQL_ENSURE(*folderPath == newFolderPath, "File " << x.second.Data << " is out of directory " << *folderPath);
                }
            }

            return TProgramBuilder(env, *FunctionRegistry_).NewDataLiteral<NUdf::EDataSlot::String>(*folderPath);
        };
    }

    if (name == "FileContent") {
        return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
            MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
            const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
            auto block = TUserDataStorage::FindUserDataBlock(UserDataBlocks_, name);
            MKQL_ENSURE(block, "File not found: " << name);
            const TProgramBuilder pgmBuilder(env, *FunctionRegistry_);
            if (block->Type == EUserDataType::PATH) {
                auto content = TFileInput(block->Data).ReadAll();
                return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
            }
            else if (block->Type == EUserDataType::RAW_INLINE_DATA) {
                return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(block->Data);
            }
            else if (block->FrozenFile && block->Type == EUserDataType::URL) {
                auto content = TFileInput(block->FrozenFile->GetPath().GetPath()).ReadAll();
                return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
            } else {
                MKQL_ENSURE(false, "Unsupported block type");
            }
        };
    }

    return TCallableVisitFunc();
}

} // NYql
