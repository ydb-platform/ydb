#include "insert_publication_query.h"

#include "tables_creator.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/query_actor/query_actor.h>

namespace NKikimr::NPQ::NDeferredPublish {

namespace {

class TInsertPublicationQuery : public TQueryBase {
public:
    TInsertPublicationQuery(
        const NActors::TActorId& replyTo,
        const TString& database,
        const TString& extPublicationId,
        const TMaybe<TString>& writerIdentity,
        const TString& createdBy)
        : TQueryBase(NKikimrServices::PERSQUEUE, {}, database, /* isSystemUser */ true)
        , ReplyTo(replyTo)
        , ExtPublicationId(extPublicationId)
        , WriterIdentity(writerIdentity)
        , CreatedBy(createdBy)
    {}

    void OnRunQuery() override {
        TString sql = TStringBuilder() << R"(
            -- TInsertPublicationQuery::OnRunQuery
            DECLARE $ext_publication_id AS Text;
            DECLARE $writer_identity AS Optional<Text>;
            DECLARE $created_by AS Optional<Text>;

            INSERT INTO `)" << PublicationsTablePath() << R"(` (
                `ext_publication_id`,
                `writer_identity`,
                `created_at`,
                `created_by`
            ) VALUES (
                $ext_publication_id,
                $writer_identity,
                CurrentUtcTimestamp(),
                $created_by
            )
            RETURNING `int_publication_id`;
        )";

        std::optional<std::string> writerIdentity;
        if (WriterIdentity) {
            writerIdentity = *WriterIdentity;
        }
        std::optional<std::string> createdBy;
        if (CreatedBy && CreatedBy != BUILTIN_ACL_NO_USER_SID) {
            createdBy = CreatedBy;
        }

        NYdb::TParamsBuilder params;
        params
            .AddParam("$ext_publication_id")
                .Utf8(ExtPublicationId)
                .Build()
            .AddParam("$writer_identity")
                .OptionalUtf8(writerIdentity)
                .Build()
            .AddParam("$created_by")
                .OptionalUtf8(createdBy)
                .Build();

        RunDataQuery(sql, &params);
    }

    void OnQueryResult() override {
        if (ResultSets.empty() || ResultSets[0].RowsCount() == 0) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Failed to allocate int_publication_id");
            return;
        }

        NYdb::TResultSetParser result(ResultSets[0]);
        if (!result.TryNextRow()) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Failed to allocate int_publication_id");
            return;
        }

        IntPublicationId = result.ColumnParser("int_publication_id").GetUint64();
        Finish();
    }

    void OnFinish(Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) override {
        if (status == Ydb::StatusIds::PRECONDITION_FAILED) {
            status = Ydb::StatusIds::ALREADY_EXISTS;
        }

        Send(Owner, [&] {
            auto* event = new TEvInsertPublicationFinished;
            event->ReplyTo = ReplyTo;
            event->Status = status;
            event->Issues = std::move(issues);
            event->IntPublicationId = IntPublicationId;
            return event;
        }());
    }

private:
    const NActors::TActorId ReplyTo;
    const TString ExtPublicationId;
    const TMaybe<TString> WriterIdentity;
    const TString CreatedBy;
    ui64 IntPublicationId = 0;
};

} // namespace

NActors::IActor* CreateInsertPublicationQueryActor(
    const NActors::TActorId& replyTo,
    const TString& database,
    const TString& extPublicationId,
    const TMaybe<TString>& writerIdentity,
    const TString& createdBy)
{
    Y_UNUSED(replyTo);
    return new TInsertPublicationQuery(replyTo, database, extPublicationId, writerIdentity, createdBy);
}

} // namespace NKikimr::NPQ::NDeferredPublish
