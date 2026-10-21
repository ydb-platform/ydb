#include "delete_publication_query.h"

#include "events.h"
#include "query_utils.h"
#include "tables_creator.h"

#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TDeletePublicationQuery : public TQueryBase {
public:
    TDeletePublicationQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        ui64 intPublicationId)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , IntPublicationId(intPublicationId)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TDeletePublicationQuery::OnRunQuery
            DECLARE $int_publication_id AS Uint64;

            DELETE FROM `)" << DestinationsTablePath() << R"(`
            WHERE `int_publication_id` = $int_publication_id;

            DELETE FROM `)" << PublicationsTablePath() << R"(`
            WHERE `int_publication_id` = $int_publication_id
            RETURNING `int_publication_id`;
        )";

        NYdb::TParamsBuilder params;
        params
            .AddParam("$int_publication_id")
                .Uint64(IntPublicationId)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.empty() || ResultSets[0].RowsCount() == 0) {
            Finish(Ydb::StatusIds::NOT_FOUND, "Publication not found");
            return;
        }
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (IsRegistryTableMissing(status)) {
            status = Ydb::StatusIds::NOT_FOUND;
            issues.Clear();
            issues.AddIssue("Publication not found");
        }

        Send(Owner, [&] {
            auto* event = new TEvDeletePublicationResponse;
            event->Status = status;
            event->Issues = std::move(issues);
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const ui64 IntPublicationId;
};

} // namespace

NActors::IActor* CreateDeletePublicationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    ui64 intPublicationId)
{
    return new TDeletePublicationQuery(replyTo, database, intPublicationId);
}

} // namespace NKikimr::NPQ::NDeferredPublish
