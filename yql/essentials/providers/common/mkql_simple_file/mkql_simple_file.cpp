#include "mkql_simple_file.h"

#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/rand_guid.h>

#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

TSimpleFileTransformProvider::TSimpleFileTransformProvider(const IFunctionRegistry* functionRegistry,
                                                           const TUserDataTable& userDataBlocks, TFileStoragePtr fileStorage)
    : FunctionRegistry_(functionRegistry)
    , UserDataBlocks_(userDataBlocks)
    , FileStorage_(std::move(fileStorage))
{
}

TString TSimpleFileTransformProvider::MaterializeFolder(const TString& folderName) {
    MKQL_ENSURE(FileStorage_, "File storage is required for FolderPath: " << folderName);
    if (!FolderRoot_) {
        FolderRoot_ = FileStorage_->GetTemp() / TRandGuid().GenGuid();
    }
    for (const auto& [key, block] : UserDataBlocks_) {
        if (!key.IsFile() || !key.Alias().StartsWith(folderName)) {
            continue;
        }
        const auto path = *FolderRoot_ / key.Alias().substr(1);
        MKQL_ENSURE(path.IsSubpathOf(*FolderRoot_), "Invalid file alias: " << key.Alias());
        if (path.Exists()) {
            continue;
        }
        path.Parent().MkDirs();
        MKQL_ENSURE(block.Type == EUserDataType::PATH || block.FrozenFile, "File is not frozen: " << key.Alias());
        const auto source = block.Type == EUserDataType::PATH ? TFsPath(block.Data) : block.FrozenFile->GetPath();
        NFs::HardLinkOrCopy(source, path);
    }
    // The file storage owns this directory beyond the callable transformation.
    return (*FolderRoot_ / folderName.substr(1)).GetPath() + '/';
}

TString TSimpleFileTransformProvider::PrepareFolderPath(const TString& name) {
    const auto folderName = TUserDataStorage::MakeFolderName(name);
    TMaybe<TString> folderPath;
    for (const auto& [key, block] : UserDataBlocks_) {
        if (!key.IsFile() || !key.Alias().StartsWith(folderName)) {
            continue;
        }
        if (block.Type != EUserDataType::PATH) {
            return MaterializeFolder(folderName);
        }
        const auto newFolderPath = block.Data.substr(0, block.Data.size() - (key.Alias().size() - folderName.size()));
        if (!folderPath) {
            folderPath = newFolderPath;
        } else if (*folderPath != newFolderPath) {
            return MaterializeFolder(folderName);
        }
    }
    MKQL_ENSURE(folderPath, "Folder not found: " << name);
    return *folderPath;
}

TCallableVisitFunc TSimpleFileTransformProvider::operator()(TInternName name) {
    if (name == "FilePath") {
        return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
            MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
            const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
            auto block = TUserDataStorage::FindUserDataBlock(UserDataBlocks_, name);
            MKQL_ENSURE(block, "File not found: " << name);
            MKQL_ENSURE(block->Type == EUserDataType::PATH || block->FrozenFile, "File is not frozen, name: "
                                                                                     << name << ", block type: " << block->Type);
            return TProgramBuilder(env, *FunctionRegistry_).NewDataLiteral<NUdf::EDataSlot::String>(block->Type == EUserDataType::PATH ? block->Data : block->FrozenFile->GetPath().GetPath());
        };
    }

    if (name == "FolderPath") {
        return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env) {
            MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arguments");
            const TString name(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
            return TProgramBuilder(env, *FunctionRegistry_).NewDataLiteral<NUdf::EDataSlot::String>(PrepareFolderPath(name));
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
            } else if (block->Type == EUserDataType::RAW_INLINE_DATA) {
                return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(block->Data);
            } else if (block->FrozenFile && block->Type == EUserDataType::URL) {
                auto content = TFileInput(block->FrozenFile->GetPath().GetPath()).ReadAll();
                return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(content);
            } else {
                MKQL_ENSURE(false, "Unsupported block type");
            }
        };
    }

    return TCallableVisitFunc();
}

} // namespace NYql
