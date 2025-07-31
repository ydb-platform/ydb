#include "validators.h"
#include "ydb_control_plane_storage_impl.h"
#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>


#include <ydb/public/api/protos/draft/fq.pb.h>

#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/db_schema/db_schema.h>

#include <ydb/library/protobuf_printer/security_printer.h>
#include "request_actor.h"

#include <cstdint>


#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/schema.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <ydb/core/fq/libs/shared_resources/db_exec.h>

#include <util/digest/multi.h>

void NFq::TYdbControlPlaneStorageActor::Handle(NFq::TEvControlPlaneStorage::TEvDeleteFolderResourcesRequest::TPtr& ev){
    TInstant startTime = TInstant::Now();
    const TEvControlPlaneStorage::TEvDeleteFolderResourcesRequest& event = *ev->Get();
    const TString cloudId = event.CloudId;
    const TString scope = event.Scope;
    TRequestCounters requestCounters = Counters.GetCounters(cloudId, scope, RTS_DELETE_FOLDER_RESOURCES, RTC_DELETE_FOLDER_RESOURCES);
    requestCounters.IncInFly();
    requestCounters.Common->RequestBytes->Add(event.GetByteSize());
    const TString user = event.User;
    const TString token = event.Token;
    TPermissions permissions = Config->Proto.GetEnablePermissions()
                        ? event.Permissions
                        : TPermissions{TPermissions::MANAGE_PUBLIC};
    if (IsSuperUser(user)) {
        permissions.SetAll();
    }
    const TString folderId = event.FolderId;

    TSqlQueryBuilder queryBuilder(YdbConnection->TablePathPrefix, "DeleteFolderResources");
    queryBuilder.AddString("scope", scope);
    queryBuilder.AddTimestamp("now", TInstant::Now());

    queryBuilder.AddText(
        "UPDATE `" JOBS_TABLE_NAME "` SET `" EXPIRE_AT_COLUMN_NAME "` = $now\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;\n"
        "UPDATE `" QUERIES_TABLE_NAME "` SET `" EXPIRE_AT_COLUMN_NAME "` = $now\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;\n"
        "DELETE FROM `" BINDINGS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;\n"
        "DELETE FROM `" PENDING_SMALL_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;\n"
        "DELETE FROM `" CONNECTIONS_TABLE_NAME "`\n"
        "WHERE `" SCOPE_COLUMN_NAME "` = $scope;\n"
    );

    std::shared_ptr<Ydb::Operations::Operation> response =
    std::make_shared<Ydb::Operations::Operation>();

    const auto query = queryBuilder.Build();
    auto debugInfo = Config->Proto.GetEnableDebugMode() ? std::make_shared<TDebugInfo>() : TDebugInfoPtr{};
    auto result = Write(query.Sql, query.Params, requestCounters, debugInfo);
    auto prepare = [response] { return *response; };

    auto success = result.Apply([=, requestCounters=requestCounters](const auto& future) mutable {
            NYql::TIssues internalIssues;
            NYql::TIssues issues;
            Ydb::Operations::Operation result;
            TString name = MakeLogPrefix(scope, user) + "DeleteFolderResourcesRequest";
            try {
                TStatus status = future.GetValue();
                if (status.IsSuccess()) {
                    result = prepare();
                } else {
                    issues.AddIssues(NYdb::NAdapters::ToYqlIssues(status.GetIssues()));
                    internalIssues.AddIssues(NYdb::NAdapters::ToYqlIssues(status.GetIssues()));
                }
            } catch (const NYql::TCodeLineException& exception) {
                NYql::TIssue issue = MakeErrorIssue(exception.Code, exception.GetRawMessage());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(exception.Code, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            } catch (const std::exception& exception) {
                NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, exception.what());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            } catch (...) {
                NYql::TIssue issue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                issues.AddIssue(issue);
                NYql::TIssue internalIssue = MakeErrorIssue(TIssuesIds::INTERNAL_ERROR, CurrentExceptionMessage());
                internalIssues.AddIssue(internalIssue);
            }

            size_t responseByteSize = 0;
            if (issues) {
                auto event = std::make_unique<TEvControlPlaneStorage::TEvDeleteFolderResourcesResponse>(issues);
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
                requestCounters.IncError();
                for (const auto& issue : issues) {
                    NYql::WalkThroughIssues(issue, true, [&requestCounters](const NYql::TIssue& err, ui16 level) {
                      Y_UNUSED(level);
                      requestCounters.Common->Issues->GetCounter(ToString(err.GetCode()), true)->Inc();
                    });
                }
            } else {
                CPS_LOG_AS_T(*NActors::TActivationContext::ActorSystem(), name << ": {" << TrimForLogs(SecureDebugString(result)) << "} SUCCESS");
                std::unique_ptr<TEvControlPlaneStorage::TEvDeleteFolderResourcesResponse> event;
                event = std::make_unique<TEvControlPlaneStorage::TEvDeleteFolderResourcesResponse>(result);
                event->DebugInfo = debugInfo;
                responseByteSize = event->GetByteSize();
                NActors::TActivationContext::ActorSystem()->Send(new IEventHandle(ev->Sender, SelfId(), event.release(), 0, ev->Cookie));
                requestCounters.IncOk();
            }
            requestCounters.DecInFly();
            requestCounters.Common->ResponseBytes->Add(responseByteSize);
            TDuration delta = TInstant::Now() - startTime;
            requestCounters.Common->LatencyMs->Collect(delta.MilliSeconds());
            return MakeFuture(!issues);
        });

}
