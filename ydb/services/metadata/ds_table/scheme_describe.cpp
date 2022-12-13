#include "scheme_describe.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NMetadata::NProvider {

void TSchemeDescriptionActor::Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
    auto g = PassAwayGuard();
    const auto& record = ev->Get()->GetRecord();
    const auto status = record.GetStatus();
    if (record.HasReason()) {
        auto issue = NYql::TIssue(record.GetReason());
        Controller->OnDescriptionFailed(issue.ToString(), RequestId);
        return;
    }
    Ydb::Table::DescribeTableResult describeTableResult;
    if (status != NKikimrScheme::StatusSuccess) {
        Controller->OnDescriptionFailed(::ToString(status), RequestId);
        return;
    }
    const auto& pathDescription = record.GetPathDescription();
    Ydb::Scheme::Entry* selfEntry = describeTableResult.mutable_self();
    selfEntry->set_name(pathDescription.GetSelf().GetName());
    selfEntry->set_type(static_cast<Ydb::Scheme::Entry::Type>(pathDescription.GetSelf().GetPathType()));
    ConvertDirectoryEntry(pathDescription.GetSelf(), selfEntry, true);

    if (pathDescription.HasColumnTableDescription()) {
        const auto& tableDescription = pathDescription.GetColumnTableDescription();
        FillColumnDescription(describeTableResult, tableDescription);
    } else {
        const auto& tableDescription = pathDescription.GetTable();
        NKikimrMiniKQL::TType splitKeyType;

        try {
            FillColumnDescription(describeTableResult, splitKeyType, tableDescription);
        } catch (const std::exception& ex) {
            Controller->OnDescriptionFailed("Unable to fill column description: " + CurrentExceptionMessage(), RequestId);
            return;
        }

        describeTableResult.mutable_primary_key()->CopyFrom(tableDescription.GetKeyColumnNames());
    }
    Controller->OnDescriptionSuccess(std::move(describeTableResult), RequestId);
}

void TSchemeDescriptionActor::Execute() {
    auto event = std::make_unique<NSchemeShard::TEvSchemeShard::TEvDescribeScheme>(Path);
    event->Record.MutableOptions()->SetReturnPartitioningInfo(false);
    event->Record.MutableOptions()->SetReturnPartitionConfig(false);
    event->Record.MutableOptions()->SetReturnChildren(false);
    Send(SchemeShardPipe, new TEvPipeCache::TEvForward(event.release(), SchemeShardId, false));
}

void TSchemeDescriptionActor::OnFail(const TString& errorMessage) {
    Controller->OnDescriptionFailed(errorMessage, RequestId);
}

}
