#include "external_table_validation.h"

namespace NYql {

NThreading::TFuture<IKikimrGateway::TGenericResult> ValidateExternalTableLocation(
    const TString& table, const TString& dataSource, const TString& location, bool existingOk,
    const NKikimr::NExternalSource::IExternalSourceFactory::TPtr& factory,
    const TLoadExternalTableMetadata& loadMetadata)
{
    using namespace NThreading;
    using TResult = IKikimrGateway::TGenericResult;
    auto shouldValidate = existingOk
        ? loadMetadata(table, false).Apply([](const TFuture<IKikimrGateway::TTableMetadataResult>& future) {
            const auto& result = future.GetValue();
            return !result.Success() || !result.Metadata || !result.Metadata->DoesExist;
        })
        : MakeFuture(true);
    return shouldValidate.Apply([factory, loadMetadata, dataSource, location](const TFuture<bool>& decision) {
        if (!decision.GetValue()) {
            TResult result;
            result.SetSuccess();
            return MakeFuture(result);
        }
        // Keep the metadata-loading context alive until the lookup completes.
        return loadMetadata(dataSource, true).Apply([factory, loadMetadata, dataSource, location]
            (const TFuture<IKikimrGateway::TTableMetadataResult>& future) {
            const auto& result = future.GetValue();
            if (!result.Success()) {
                TResult error;
                error.SetStatus(result.Status());
                error.AddIssues(result.Issues());
                return MakeFuture(error);
            }
            if (!result.Metadata || !result.Metadata->IsExternalDataSource()) {
                throw yexception() << "DATA_SOURCE '" << dataSource << "' must refer to an existing external data source";
            }
            const auto& sourceMetadata = result.Metadata->ExternalDataSource();
            const auto& type = sourceMetadata.GetDatabaseType();
            if (!factory || !type) {
                throw yexception() << "Location validation is not available for this external source";
            }
            auto source = factory->GetOrCreate(*type);
            auto metadata = sourceMetadata.MakeExternalSourceMetadata();
            metadata.TableLocation = location;
            return source->ValidateExternalTableLocation(metadata).Apply([source](const TFuture<void>& validation) {
                validation.GetValue();
                TResult result;
                result.SetSuccess();
                return result;
            });
        });
    }).Apply([](const TFuture<TResult>& future) {
        try {
            return future.GetValue();
        } catch (const std::exception& e) {
            return NCommon::ResultFromIssues<TResult>(TIssuesIds::KIKIMR_BAD_REQUEST, e.what(), {});
        }
    });
}

} // namespace NYql
