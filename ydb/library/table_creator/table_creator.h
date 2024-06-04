#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

namespace NKikimr {

struct TEvTableCreator {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_TABLE_CREATOR),
        EvCreateTableResponse,
    };

    struct TEvCreateTableResponse : public TEventLocal<TEvCreateTableResponse, EvCreateTableResponse> {
        explicit TEvCreateTableResponse(bool success, NYql::TIssues issues = {})
            : Success(success)
            , Issues(std::move(issues))
        {}

        const bool Success;
        const NYql::TIssues Issues;
    };
};

namespace NTableCreator {

class TMultiTableCreator : public NActors::TActorBootstrapped<TMultiTableCreator> {
    using TBase = NActors::TActorBootstrapped<TMultiTableCreator>;

public:
    explicit TMultiTableCreator(std::vector<NActors::IActor*> tableCreators);

    void Bootstrap();

protected:
    virtual void OnTablesCreated(bool success, NYql::TIssues issues) = 0;

    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, const char* columnType);

    static NKikimrSchemeOp::TColumnDescription Col(const TString& columnName, NScheme::TTypeId columnType);

    static NKikimrSchemeOp::TTTLSettings TtlCol(const TString& columnName, TDuration expireAfter, TDuration runInterval);

private:
    void Registered(NActors::TActorSystem* sys, const NActors::TActorId& owner) override;

    void Handle(TEvTableCreator::TEvCreateTableResponse::TPtr& ev);

    STFUNC(StateFunc);

protected:
    NActors::TActorId Owner;

private:
    std::vector<NActors::IActor*> TableCreators;
    size_t TablesCreating = 0;
    bool Success = true;
    NYql::TIssues Issues;
};

} // namespace NTableCreator

NActors::IActor* CreateTableCreator(
    TVector<TString> pathComponents,
    TVector<NKikimrSchemeOp::TColumnDescription> columns,
    TVector<TString> keyColumns,
    NKikimrServices::EServiceKikimr logService,
    TMaybe<NKikimrSchemeOp::TTTLSettings> ttlSettings = Nothing());

} // namespace NKikimr
