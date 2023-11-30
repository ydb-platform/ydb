#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/draft/fq.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <optional>

namespace NFq {

struct TEvAuditService {
    struct TExtraInfo {
        TString Token;
        TString CloudId;
        TString FolderId;
        TString User;

        TString PeerName;
        TString UserAgent;
        TString RequestId;
        TString SubjectType;
    };

    // Event ids.
    enum EEv : ui32 {
        EvCreateBindingReport = YqEventSubspaceBegin(NFq::TYqEventSubspace::AuditService),
        EvModifyBindingReport,
        EvDeleteBindingReport,
        EvCreateConnectionReport,
        EvModifyConnectionReport,
        EvDeleteConnectionReport,
        EvCreateQueryReport,
        EvControlQueryReport,
        EvModifyQueryReport,
        EvDeleteQueryReport,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(NFq::TYqEventSubspace::AuditService), "All events must be in their subspace");

private:
    template <class TRequest, class TAuditDetailsObj>
    struct TAuditReportBase {
        explicit TAuditReportBase(
            TExtraInfo&& extraInfo,
            const TRequest& request,
            const NYql::TIssues& issues,
            const TAuditDetails<TAuditDetailsObj>& details,
            std::optional<TString> eventId = std::nullopt)
            : ExtraInfo(std::move(extraInfo))
            , Request(request)
            , Issues(issues)
            , Details(details)
            , EventId(eventId)
        {
        }

        TExtraInfo ExtraInfo;
        TRequest Request;
        NYql::TIssues Issues;
        TAuditDetails<TAuditDetailsObj> Details;
        std::optional<TString> EventId;
    };

public:
    template <class TRequest, class TAuditDetailsObj>
    struct TAuditReport;

    // CreateBinding
    using CreateBindingAuditReport = TAuditReport<FederatedQuery::CreateBindingRequest, FederatedQuery::Binding>;

    template<>
    struct TAuditReport<FederatedQuery::CreateBindingRequest, FederatedQuery::Binding>
        : NActors::TEventLocal<CreateBindingAuditReport, EvCreateBindingReport>
        , TAuditReportBase<FederatedQuery::CreateBindingRequest, FederatedQuery::Binding> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ModifyBinding
    using ModifyBindingAuditReport = TAuditReport<FederatedQuery::ModifyBindingRequest, FederatedQuery::Binding>;

    template<>
    struct TAuditReport<FederatedQuery::ModifyBindingRequest, FederatedQuery::Binding>
        : NActors::TEventLocal<ModifyBindingAuditReport, EvModifyBindingReport>
        , TAuditReportBase<FederatedQuery::ModifyBindingRequest, FederatedQuery::Binding> {
            using TAuditReportBase::TAuditReportBase;
        };

    // DeleteBinding
    using DeleteBindingAuditReport = TAuditReport<FederatedQuery::DeleteBindingRequest, FederatedQuery::Binding>;

    template<>
    struct TAuditReport<FederatedQuery::DeleteBindingRequest, FederatedQuery::Binding>
        : NActors::TEventLocal<DeleteBindingAuditReport, EvDeleteBindingReport>
        , TAuditReportBase<FederatedQuery::DeleteBindingRequest, FederatedQuery::Binding> {
            using TAuditReportBase::TAuditReportBase;
        };

    // CreateConnection
    using CreateConnectionAuditReport = TAuditReport<FederatedQuery::CreateConnectionRequest, FederatedQuery::Connection>;

    template<>
    struct TAuditReport<FederatedQuery::CreateConnectionRequest, FederatedQuery::Connection>
        : NActors::TEventLocal<CreateConnectionAuditReport, EvCreateConnectionReport>
        , TAuditReportBase<FederatedQuery::CreateConnectionRequest, FederatedQuery::Connection> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ModifyConnection
    using ModifyConnectionAuditReport = TAuditReport<FederatedQuery::ModifyConnectionRequest, FederatedQuery::Connection>;

    template<>
    struct TAuditReport<FederatedQuery::ModifyConnectionRequest, FederatedQuery::Connection>
        : NActors::TEventLocal<ModifyConnectionAuditReport, EvModifyConnectionReport>
        , TAuditReportBase<FederatedQuery::ModifyConnectionRequest, FederatedQuery::Connection> {
            using TAuditReportBase::TAuditReportBase;
        };

    // DeleteConnection
    using DeleteConnectionAuditReport = TAuditReport<FederatedQuery::DeleteConnectionRequest, FederatedQuery::Connection>;

    template<>
    struct TAuditReport<FederatedQuery::DeleteConnectionRequest, FederatedQuery::Connection>
        : NActors::TEventLocal<DeleteConnectionAuditReport, EvDeleteConnectionReport>
        , TAuditReportBase<FederatedQuery::DeleteConnectionRequest, FederatedQuery::Connection> {
            using TAuditReportBase::TAuditReportBase;
        };

    // CreateQuery
    using CreateQueryAuditReport = TAuditReport<FederatedQuery::CreateQueryRequest, FederatedQuery::Query>;

    template<>
    struct TAuditReport<FederatedQuery::CreateQueryRequest, FederatedQuery::Query>
        : NActors::TEventLocal<CreateQueryAuditReport, EvCreateQueryReport>
        , TAuditReportBase<FederatedQuery::CreateQueryRequest, FederatedQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ControlQuery
    using ControlQueryAuditReport = TAuditReport<FederatedQuery::ControlQueryRequest, FederatedQuery::Query>;

    template<>
    struct TAuditReport<FederatedQuery::ControlQueryRequest, FederatedQuery::Query>
        : NActors::TEventLocal<ControlQueryAuditReport, EvControlQueryReport>
        , TAuditReportBase<FederatedQuery::ControlQueryRequest, FederatedQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ModifyQuery
    using ModifyQueryAuditReport = TAuditReport<FederatedQuery::ModifyQueryRequest, FederatedQuery::Query>;

    template<>
    struct TAuditReport<FederatedQuery::ModifyQueryRequest, FederatedQuery::Query>
        : NActors::TEventLocal<ModifyQueryAuditReport, EvModifyQueryReport>
        , TAuditReportBase<FederatedQuery::ModifyQueryRequest, FederatedQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };

    // DeleteQuery
    using DeleteQueryAuditReport = TAuditReport<FederatedQuery::DeleteQueryRequest, FederatedQuery::Query>;

    template<>
    struct TAuditReport<FederatedQuery::DeleteQueryRequest, FederatedQuery::Query>
        : NActors::TEventLocal<DeleteQueryAuditReport, EvDeleteQueryReport>
        , TAuditReportBase<FederatedQuery::DeleteQueryRequest, FederatedQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };


    template <class TRequest, class TAuditDetailsObj>
    static TAuditReport<TRequest, TAuditDetailsObj>* MakeAuditEvent(
            TExtraInfo&& extraInfo,
            const TRequest& request,
            const NYql::TIssues& issues,
            const TAuditDetails<TAuditDetailsObj>& details,
            std::optional<TString> eventId = std::nullopt) {
        return new TAuditReport<TRequest, TAuditDetailsObj>(std::move(extraInfo), request, issues, details, eventId);
    }
};

} // namespace NFq
