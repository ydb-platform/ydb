#include "export_create_table.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/sys_view/show_create/formatters/create_table_formatter.h>

namespace NKikimr::NDataShard {
namespace {

NSysView::TFormatResult FormatCreateTable(const NKikimrSchemeOp::TBackupTask& task) {
    const auto& pathDescription = task.GetTable();
    const auto& tableName = pathDescription.GetSelf().GetName();
    const auto& fullPath = task.HasS3Settings()
        ? task.GetS3Settings().GetSourceTablePath()
        : task.GetFSSettings().GetSourceTablePath();
    Y_ENSURE(tableName && fullPath, "Missing original table path for SQL backup");

    NSysView::TCreateTableFormatter formatter;
    if (pathDescription.HasColumnTableDescription()) {
        const auto& description = pathDescription.GetColumnTableDescription();
        Y_ENSURE(description.GetSchema().ColumnsSize(), "Column table description has no columns");
        return formatter.Format(tableName, fullPath, description, false,
            AppData()->FeatureFlags.GetEnableLocalIndexAsSchemeObject());
    }

    Y_ENSURE(pathDescription.HasTable(), "Missing table description for SQL backup");
    const auto& description = pathDescription.GetTable();
    const auto& topics = task.GetChangefeedUnderlyingTopics();
    Y_ENSURE(description.GetCdcStreams().size() == topics.size(),
        "Number of changefeeds does not match backup topic descriptions");

    THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>> persQueues;
    for (int i = 0; i < description.GetCdcStreams().size(); ++i) {
        const auto topicPath = JoinPath({tableName, description.GetCdcStreams(i).GetName(), "streamImpl"});
        persQueues.emplace(topicPath,
            MakeHolder<NKikimrSchemeOp::TPersQueueGroupDescription>(topics.Get(i).GetPersQueueGroup()));
    }

    THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>> sequences;
    for (const auto& sequence : description.GetSequences()) {
        const auto pathId = TPathId::FromProto(sequence.GetPathId());
        auto result = MakeHolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>(pathId);
        result->StartValue = sequence.GetStartValue();
        result->Increment = sequence.GetIncrement();
        result->NextValue = sequence.HasSetVal() ? sequence.GetSetVal().GetNextValue() : sequence.GetStartValue();
        result->NextUsed = sequence.GetSetVal().GetNextUsed();
        sequences.emplace(pathId, std::move(result));
    }

    return formatter.Format(tableName, fullPath, description, false, persQueues, sequences);
}

} // namespace

TConclusion<TString> GenCreateTableQuery(const NKikimrSchemeOp::TBackupTask& task) {
    try {
        auto result = FormatCreateTable(task);
        if (!result.IsSuccess()) {
            return TConclusionStatus::Fail(result.GetError());
        }
        return result.ExtractOut();
    } catch (const NSysView::TFormatFail& error) {
        return TConclusionStatus::Fail(error.Error);
    } catch (const std::exception& error) {
        return TConclusionStatus::Fail(error.what());
    }
}

} // namespace NKikimr::NDataShard
