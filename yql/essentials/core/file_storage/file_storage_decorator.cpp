#include "file_storage_decorator.h"


namespace NYql {

TFileStorageDecorator::TFileStorageDecorator(TFileStoragePtr fs)
    : Inner_(std::move(fs))
{
}

TFileLinkPtr TFileStorageDecorator::PutFile(const TString& file, const TString& outFileName) {
    return Inner_->PutFile(file, outFileName);
}
TFileLinkPtr TFileStorageDecorator::PutFileStripped(const TString& file, const TString& originalMd5) {
    return Inner_->PutFileStripped(file, originalMd5);
}
TFileLinkPtr TFileStorageDecorator::PutInline(const TString& data) {
    return Inner_->PutInline(data);
}
TFileLinkPtr TFileStorageDecorator::PutUrl(const TString& url, const TString& token) {
    return Inner_->PutUrl(url, token);
}
NThreading::TFuture<TFileLinkPtr> TFileStorageDecorator::PutFileAsync(const TString& file, const TString& outFileName) {
    return Inner_->PutFileAsync(file, outFileName);
}
NThreading::TFuture<TFileLinkPtr> TFileStorageDecorator::PutInlineAsync(const TString& data) {
    return Inner_->PutInlineAsync(data);
}
NThreading::TFuture<TFileLinkPtr> TFileStorageDecorator::PutUrlAsync(const TString& url, const TString& token) {
    return Inner_->PutUrlAsync(url, token);
}
TFsPath TFileStorageDecorator::GetRoot() const {
    return Inner_->GetRoot();
}
TFsPath TFileStorageDecorator::GetTemp() const {
    return Inner_->GetTemp();
}
const TFileStorageConfig& TFileStorageDecorator::GetConfig() const {
    return Inner_->GetConfig();
}

} // NYql
