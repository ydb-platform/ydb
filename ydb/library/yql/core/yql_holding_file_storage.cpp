#include "yql_holding_file_storage.h"
#include <ydb/library/yql/utils/future_action.h>

namespace NYql {

THoldingFileStorage::THoldingFileStorage(TFileStoragePtr fileStorage)
    : FileStorage_(std::move(fileStorage))
{
    // FileStorage_ can be nullptr
}

TFileStoragePtr THoldingFileStorage::GetRawStorage() const {
    return FileStorage_;
}

bool THoldingFileStorage::CanFreeze() const {
    return FileStorage_ != nullptr;
}

TFileLinkPtr THoldingFileStorage::FreezeFile(const TUserDataBlock& block) {
    if (block.FrozenFile) {
        return block.FrozenFile;
    }

    return FindOrPutData(block);
}

NThreading::TFuture<std::function<TFileLinkPtr()>> THoldingFileStorage::FreezeFileAsync(const TUserDataBlock& block) {
    if (block.FrozenFile) {
        return MakeFutureWithConstantAction(block.FrozenFile);
    }

    return FindOrPutDataAsync(block);
}

TFileLinkPtr THoldingFileStorage::FindOrPutData(const TUserDataBlock& block) {
    auto it = Links_.find(block);
    if (it != Links_.end()) {
        return it->second;
    }

    return PutDataAndRegister(block);
}

TFileLinkPtr THoldingFileStorage::PutDataAndRegister(const TUserDataBlock& block) {
    // download outside of the lock
    TFileLinkPtr link = PutData(block);
    return RegisterLink(block, link);
}

TFileLinkPtr THoldingFileStorage::RegisterLink(const TUserDataBlock& block, TFileLinkPtr link) {
    // important: do not overwrite existing value if any and return it
    return Links_.emplace(block, link).first->second;
}

TFileLinkPtr THoldingFileStorage::PutData(const TUserDataBlock& block) {
    if (!FileStorage_) {
        ythrow yexception() << "FileStorage is not available, unable to load " << block.Data;
    }

    switch (block.Type) {
    case EUserDataType::PATH:
        return FileStorage_->PutFile(block.Data);

    case EUserDataType::URL:
        return FileStorage_->PutUrl(block.Data, block.UrlToken);

    case EUserDataType::RAW_INLINE_DATA:
        return FileStorage_->PutInline(block.Data);

    default:
        ythrow yexception() << "Unknown user data type " << block.Type;
    }
}

NThreading::TFuture<std::function<TFileLinkPtr()>> THoldingFileStorage::FindOrPutDataAsync(const TUserDataBlock& block) {
    auto it = Links_.find(block);
    if (it != Links_.end()) {
        return MakeFutureWithConstantAction(it->second);
    }

    return PutDataAndRegisterAsync(block);
}

NThreading::TFuture<std::function<TFileLinkPtr()>> THoldingFileStorage::PutDataAndRegisterAsync(const TUserDataBlock& block) {
    NThreading::TFuture<TFileLinkPtr> future = PutDataAsync(block);

    return AddActionToFuture(future, [this, block](TFileLinkPtr link) {
        return this->RegisterLink(block, link);
    });
}

NThreading::TFuture<TFileLinkPtr> THoldingFileStorage::PutDataAsync(const TUserDataBlock& block) {
    if (!FileStorage_) {
        ythrow yexception() << "FileStorage is not available, unable to load " << block.Data;
    }

    switch (block.Type) {
    case EUserDataType::PATH:
        return FileStorage_->PutFileAsync(block.Data);

    case EUserDataType::URL:
        return FileStorage_->PutUrlAsync(block.Data, block.UrlToken);

    case EUserDataType::RAW_INLINE_DATA:
        return FileStorage_->PutInlineAsync(block.Data);

    default:
        ythrow yexception() << "Unknown user data type " << block.Type;
    }
}

}
