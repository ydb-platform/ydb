#include "yql_user_data_storage.h"

#include <ydb/library/yql/utils/log/profile.h>

#include <ydb/library/yql/utils/future_action.h>

#include <util/folder/iterator.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/system/file.h>

namespace {
const auto Sep = '/';
const TString Root(1, Sep);
const TString Home = "home";
const TString HomePath = Root + Home + Sep;

template <typename DataMapType, typename F>
void EnumFolderContent(DataMapType& data, const TString& folderPath, ui32 maxFileCount, F&& f) {
    ui32 count = 0;
    for (auto& p : data) {
        if (!p.first.IsFile() || !p.first.Alias().StartsWith(folderPath)) {
            continue;
        }

        f(p.first, p.second);

        if (count++ >= maxFileCount) {
            break;
        }
    }
}

}

namespace NYql {

TUserDataStorage::TUserDataStorage(TFileStoragePtr fileStorage, TUserDataTable data, IUdfResolver::TPtr udfResolver, TUdfIndex::TPtr udfIndex)
    : FileStorage_(std::move(fileStorage))
    , UserData_(std::move(data))
    , UdfResolver_(std::move(udfResolver))
    , UdfIndex_(std::move(udfIndex))
{
}

void TUserDataStorage::SetTokenResolver(TTokenResolver tokenResolver) {
    TokenResolver_ = std::move(tokenResolver);
}

void TUserDataStorage::SetUrlPreprocessor(IUrlPreprocessing::TPtr urlPreprocessing) {
    UrlPreprocessing_ = std::move(urlPreprocessing);
}

void TUserDataStorage::SetUserDataTable(TUserDataTable data) {
     UserData_ = std::move(data);
     FillUserDataUrls();
}

void TUserDataStorage::AddUserDataBlock(const TStringBuf& name, const TUserDataBlock& block) {
    const auto key = ComposeUserDataKey(name);
    AddUserDataBlock(key, block);
}

void TUserDataStorage::AddUserDataBlock(const TUserDataKey& key, const TUserDataBlock& block) {
    auto res = UserData_.emplace(key, block);
    if (!res.second) {
        throw yexception() << "Failed to add user data block, key " << key << " already registered";
    }
    TryFillUserDataUrl(res.first->second);
}

bool TUserDataStorage::ContainsUserDataBlock(const TStringBuf& name) const {
    const auto key = ComposeUserDataKey(name);
    return ContainsUserDataBlock(key);
}

bool TUserDataStorage::ContainsUserDataBlock(const TUserDataKey& key) const {
    return UserData_.contains(key);
}

TUserDataBlock& TUserDataStorage::GetUserDataBlock(const TUserDataKey& key) {
    auto block = FindUserDataBlock(key);
    if (!block) {
        ythrow yexception() << "Failed to find user data block by key " << key;
    }

    return *block;
}

TUserDataBlock* TUserDataStorage::FindUserDataBlock(const TStringBuf& name) {
    auto key = ComposeUserDataKey(name);
    return FindUserDataBlock(key);
}

const TUserDataBlock* TUserDataStorage::FindUserDataBlock(const TUserDataKey& key) const {
    return FindUserDataBlock(UserData_, key);
}

TUserDataBlock* TUserDataStorage::FindUserDataBlock(const TUserDataKey& key) {
    return FindUserDataBlock(UserData_, key);
}

const TUserDataBlock* TUserDataStorage::FindUserDataBlock(const TUserDataTable& userData, const TStringBuf& name) {
    const auto key = ComposeUserDataKey(name);
    return FindUserDataBlock(userData, key);
}

const TUserDataBlock* TUserDataStorage::FindUserDataBlock(const TUserDataTable& userData, const TUserDataKey& key) {
    return userData.FindPtr(key);
}

TUserDataBlock* TUserDataStorage::FindUserDataBlock(TUserDataTable& userData, const TStringBuf& name) {
    const auto key = ComposeUserDataKey(name);
    return FindUserDataBlock(userData, key);
}

TUserDataBlock* TUserDataStorage::FindUserDataBlock(TUserDataTable& userData, const TUserDataKey& key) {
    return userData.FindPtr(key);
}

TString TUserDataStorage::MakeFullName(const TStringBuf& name) {
    return name.StartsWith(Sep) ? TString(name) : HomePath + name;
}

TString TUserDataStorage::MakeFolderName(const TStringBuf& name) {
    auto fullName = MakeFullName(name);
    if (!fullName.EndsWith(Sep)) {
        fullName += Sep;
    }

    return fullName;
}

TString TUserDataStorage::MakeRelativeName(const TStringBuf& name) {
    if (name.StartsWith(HomePath)) {
        return TString(name.substr(HomePath.size()));
    }
    return name.StartsWith(Sep) ? TString(name.substr(1)) : TString(name);
}

TUserDataKey TUserDataStorage::ComposeUserDataKey(const TStringBuf& name) {
    auto fullName = MakeFullName(name);
    return TUserDataKey::File(fullName);
}

bool TUserDataStorage::ContainsUserDataFolder(const TStringBuf& name) const {
    return FindUserDataFolder(name, 1).Defined();
}

TMaybe<std::map<TUserDataKey, const TUserDataBlock*>> TUserDataStorage::FindUserDataFolder(const TStringBuf& name, ui32 maxFileCount) const {
    return FindUserDataFolder(UserData_,name,maxFileCount);
}

TMaybe<std::map<TUserDataKey, const TUserDataBlock*>> TUserDataStorage::FindUserDataFolder(const TUserDataTable& userData, const TStringBuf& name, ui32 maxFileCount) {
    auto fullName = MakeFolderName(name);
    TMaybe<std::map<TUserDataKey, const TUserDataBlock*>> res;
    EnumFolderContent(userData, fullName, maxFileCount, [&](auto& key, auto& block) {
        if (!res) {
            res.ConstructInPlace();
        }

        res->emplace(key, &block);
    });

    return res;
}

void TUserDataStorage::FillUserDataUrls() {
    for (auto& p : UserData_) {
        TryFillUserDataUrl(p.second);
    }
}

void TUserDataStorage::TryFillUserDataUrl(TUserDataBlock& block) const {
    if (block.Type != EUserDataType::URL) {
        return;
    }

    TString alias;
    if (UrlPreprocessing_) {
        std::tie(block.Data, alias) = UrlPreprocessing_->Preprocess(block.Data);
    }
    if (!block.UrlToken && TokenResolver_) {
        block.UrlToken = TokenResolver_(block.Data, alias);
    }
}

std::map<TString, const TUserDataBlock*> TUserDataStorage::GetDirectoryContent(const TStringBuf& path, ui32 maxFileCount) const {
    const auto fullPath = MakeFolderName(path);

    std::map<TString, const TUserDataBlock*> result;
    EnumFolderContent(UserData_, fullPath, maxFileCount, [&](auto& key, auto& block) {
        const auto name = key.Alias().substr(fullPath.size());
        const auto pos = name.find(Sep);
        if (TString::npos == pos)
            result.emplace(name, &block);
        else
            result.emplace(name.substr(0U, pos), nullptr);
    });

    return result;
}

TUserDataBlock& TUserDataStorage::Freeze(const TUserDataKey& key) {
    TUserDataBlock& block = GetUserDataBlock(key);
    if (block.FrozenFile) {
        return block;
    }

    // do it outside of the lock
    auto link = FileStorage_.FreezeFile(block);
    return RegisterLink(key, link);
}

TUserDataBlock* TUserDataStorage::FreezeNoThrow(const TUserDataKey& key, TString& errorMessage) {
    try {
        return &Freeze(key);
    } catch (const std::exception& e) {
        errorMessage = TStringBuilder() << "Failed to freeze file with key " << key << ", details: " << e.what();
        return nullptr;
    }
}

TUserDataBlock* TUserDataStorage::FreezeUdfNoThrow(const TUserDataKey& key,
                                                    TString& errorMessage,const TString& customUdfPrefix) {
    TUserDataBlock* block = FreezeNoThrow(key, errorMessage);
    if (!block) {
        return nullptr;
    }
    block->CustomUdfPrefix = customUdfPrefix;

    if (!ScannedUdfs_.insert(key).second) {
        // already scanned
        return block;
    }

    if (!UdfIndex_) {
        return block;
    }

    try {
        TString scope = "ScanUdfStrategy " + key.Alias();
        YQL_PROFILE_SCOPE(DEBUG, scope.c_str());
        Y_ENSURE(UdfResolver_);
        Y_ENSURE(UdfIndex_);
        LoadRichMetadataToUdfIndex(*UdfResolver_, *block, TUdfIndex::EOverrideMode::ReplaceWithNew, *UdfIndex_);
    } catch (const std::exception& e) {
        errorMessage = TStringBuilder() << "Failed to scan udf with key " << key << ", details: " << e.what();
        return nullptr;
    }

    return block;
}

NThreading::TFuture<std::function<TUserDataBlock()>> TUserDataStorage::FreezeAsync(const TUserDataKey& key) {
    auto block = GetUserDataBlock(key);
    if (block.FrozenFile) {
        return MakeFutureWithConstantAction(block);
    }

    return MapFutureAction(FileStorage_.FreezeFileAsync(block), [this, key](TFileLinkPtr link) {
        return this->RegisterLink(key, link);
    });
}

TUserDataBlock& TUserDataStorage::RegisterLink(const TUserDataKey& key, TFileLinkPtr link) {
    auto block = FindUserDataBlock(key);
    Y_ENSURE(block);

    if (!block->FrozenFile) {
        block->FrozenFile = link;
    }
    return *block;
}

const TString& GetDefaultFilePrefix() {
    return HomePath;
}

void FillUserDataTableFromFileSystem(const TString& aliasPrefix, const TString& path, bool isLibrary, TUserDataTable& userData) {
    if (!NFs::Exists(path) || !TFileStat(path).IsDir()) {
        return;
    }

    TDirIterator dir(path);

    for (auto it = dir.begin(); it != dir.end(); ++it) {
        if (FTS_F == it->fts_info) {
            TString filePath = it->fts_path;
            if (!filePath.empty()) {
                auto ptr = &*filePath.begin();
                for (size_t i = 0; i < filePath.size(); ++i) {
                    if (ptr[i] == '\\') {
                        ptr[i] = Sep;
                    }
                }
            }

            const TString alias = aliasPrefix + filePath.substr(path.size());

            if (filePath.EndsWith(".url")) {
                TFile file(filePath, EOpenModeFlag::RdOnly);
                if (file.GetLength() > 0) {
                    auto& entry = userData[TUserDataKey::File(alias)];
                    entry.Type = EUserDataType::URL;
                    entry.Usage.Set(EUserDataBlockUsage::Library, isLibrary);
                    std::vector<TString::value_type> buffer(file.GetLength());
                    file.Load(buffer.data(), buffer.size());
                    entry.Data = StripStringRight(TString(buffer.data(), buffer.size()));
                }
            } else {
                auto& entry = userData[TUserDataKey::File(alias)];
                entry.Usage.Set(EUserDataBlockUsage::Library, isLibrary);
                entry.Type = EUserDataType::PATH;
                entry.Data = filePath;
            }
        }
    }
}

void FillUserDataTableFromFileSystem(const NYqlMountConfig::TMountConfig& mount, TUserDataTable& userData) {
    for (const auto& mp : mount.GetMountPoints()) {
        FillUserDataTableFromFileSystem(mp.GetRootAlias(), mp.GetMountPoint(), mp.GetLibrary(), userData);
    }
}

NThreading::TFuture<std::function<TUserDataTable()>> FreezeUserDataTableIfNeeded(TUserDataStorage::TPtr userDataStorage, const TUserDataTable& files, const std::function<bool(const TString&)>& urlDownloadFilter) {
    if (files.empty()) {
        return MakeFutureWithConstantAction(TUserDataTable());
    }

    TVector<TUserDataKey> keysForDownloading;
    for (auto& p : files) {
        if (p.second.FrozenFile) {
            continue;
        }

        if (p.second.Type != EUserDataType::URL || urlDownloadFilter(p.second.Data)) {
            keysForDownloading.push_back(p.first);
        }
    }

    // fast exit
    if (keysForDownloading.empty()) {
        return MakeFutureWithConstantAction(files);
    }

    TVector<NThreading::TFuture<std::function<std::pair<TUserDataKey, TUserDataBlock>()>>> futures;
    TVector<NThreading::TFuture<void>> voidFutures;
    futures.reserve(keysForDownloading.size());
    voidFutures.reserve(keysForDownloading.size());
    for (auto& k : keysForDownloading) {
        auto f = MapFutureAction(userDataStorage->FreezeAsync(k), [k](const TUserDataBlock& block) {
            return std::make_pair(k, block);
        });

        futures.push_back(f);
        voidFutures.push_back(f.IgnoreResult());
    }

    return NThreading::WaitExceptionOrAll(voidFutures).Apply([files = files, futures = std::move(futures)](NThreading::TFuture<void> f) mutable {
        std::function<TUserDataTable()> result = [f, files, futures]() mutable {
            // rethrow exception if any
            f.GetValue();

            for (auto func : futures) {
                std::pair<TUserDataKey, TUserDataBlock> p = func.GetValue()();
                Y_ENSURE(files.contains(p.first));
                files[p.first].FrozenFile = p.second.FrozenFile;
            }

            return files;
        };

        return result;
    });
}

TVector<TString> TUserDataStorage::GetLibraries() const {
    TVector<TString> result;
    for (const auto& x : UserData_) {
        if (x.first.IsFile() && x.first.Alias().StartsWith(HomePath) &&
            x.second.Usage.Test(EUserDataBlockUsage::Library)) {
            result.push_back(x.first.Alias().substr(HomePath.size()));
        }
    }

    return result;
}

}
