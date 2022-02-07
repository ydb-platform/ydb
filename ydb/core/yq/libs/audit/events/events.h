#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/checkpointing_common/defs.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/interconnect/events_local.h>

#include <optional>

namespace NYq {

struct TEvAuditService {
    struct TExtraInfo {
        TString Token;
        TString CloudId;
        TString FolderId;
        TString User;

        TString PeerName;
        TString UserAgent;
        TString RequestId;
    };

    // Event ids.
    enum EEv : ui32 {
        EvCreateBindingReport = YqEventSubspaceBegin(NYq::TYqEventSubspace::AuditService),
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

    static_assert(EvEnd <= YqEventSubspaceEnd(NYq::TYqEventSubspace::AuditService), "All events must be in their subspace");

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
    using CreateBindingAuditReport = TAuditReport<YandexQuery::CreateBindingRequest, YandexQuery::Binding>;

    template<>
    struct TAuditReport<YandexQuery::CreateBindingRequest, YandexQuery::Binding>
        : NActors::TEventLocal<CreateBindingAuditReport, EvCreateBindingReport>
        , TAuditReportBase<YandexQuery::CreateBindingRequest, YandexQuery::Binding> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ModifyBinding
    using ModifyBindingAuditReport = TAuditReport<YandexQuery::ModifyBindingRequest, YandexQuery::Binding>;

    template<>
    struct TAuditReport<YandexQuery::ModifyBindingRequest, YandexQuery::Binding>
        : NActors::TEventLocal<ModifyBindingAuditReport, EvModifyBindingReport>
        , TAuditReportBase<YandexQuery::ModifyBindingRequest, YandexQuery::Binding> {
            using TAuditReportBase::TAuditReportBase;
        };

    // DeleteBinding
    using DeleteBindingAuditReport = TAuditReport<YandexQuery::DeleteBindingRequest, YandexQuery::Binding>;

    template<>
    struct TAuditReport<YandexQuery::DeleteBindingRequest, YandexQuery::Binding>
        : NActors::TEventLocal<DeleteBindingAuditReport, EvDeleteBindingReport>
        , TAuditReportBase<YandexQuery::DeleteBindingRequest, YandexQuery::Binding> {
            using TAuditReportBase::TAuditReportBase;
        };

    // CreateConnection
    using CreateConnectionAuditReport = TAuditReport<YandexQuery::CreateConnectionRequest, YandexQuery::Connection>;

    template<>
    struct TAuditReport<YandexQuery::CreateConnectionRequest, YandexQuery::Connection>
        : NActors::TEventLocal<CreateConnectionAuditReport, EvCreateConnectionReport>
        , TAuditReportBase<YandexQuery::CreateConnectionRequest, YandexQuery::Connection> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ModifyConnection
    using ModifyConnectionAuditReport = TAuditReport<YandexQuery::ModifyConnectionRequest, YandexQuery::Connection>;

    template<>
    struct TAuditReport<YandexQuery::ModifyConnectionRequest, YandexQuery::Connection>
        : NActors::TEventLocal<ModifyConnectionAuditReport, EvModifyConnectionReport>
        , TAuditReportBase<YandexQuery::ModifyConnectionRequest, YandexQuery::Connection> {
            using TAuditReportBase::TAuditReportBase;
        };

    // DeleteConnection
    using DeleteConnectionAuditReport = TAuditReport<YandexQuery::DeleteConnectionRequest, YandexQuery::Connection>;

    template<>
    struct TAuditReport<YandexQuery::DeleteConnectionRequest, YandexQuery::Connection>
        : NActors::TEventLocal<DeleteConnectionAuditReport, EvDeleteConnectionReport>
        , TAuditReportBase<YandexQuery::DeleteConnectionRequest, YandexQuery::Connection> {
            using TAuditReportBase::TAuditReportBase;
        };

    // CreateQuery
    using CreateQueryAuditReport = TAuditReport<YandexQuery::CreateQueryRequest, YandexQuery::Query>;

    template<>
    struct TAuditReport<YandexQuery::CreateQueryRequest, YandexQuery::Query>
        : NActors::TEventLocal<CreateQueryAuditReport, EvCreateQueryReport>
        , TAuditReportBase<YandexQuery::CreateQueryRequest, YandexQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ControlQuery
    using ControlQueryAuditReport = TAuditReport<YandexQuery::ControlQueryRequest, YandexQuery::Query>;

    template<>
    struct TAuditReport<YandexQuery::ControlQueryRequest, YandexQuery::Query>
        : NActors::TEventLocal<ControlQueryAuditReport, EvControlQueryReport>
        , TAuditReportBase<YandexQuery::ControlQueryRequest, YandexQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };

    // ModifyQuery
    using ModifyQueryAuditReport = TAuditReport<YandexQuery::ModifyQueryRequest, YandexQuery::Query>;

    template<>
    struct TAuditReport<YandexQuery::ModifyQueryRequest, YandexQuery::Query>
        : NActors::TEventLocal<ModifyQueryAuditReport, EvModifyQueryReport>
        , TAuditReportBase<YandexQuery::ModifyQueryRequest, YandexQuery::Query> {
            using TAuditReportBase::TAuditReportBase;
        };

    // DeleteQuery
    using DeleteQueryAuditReport = TAuditReport<YandexQuery::DeleteQueryRequest, YandexQuery::Query>;

    template<>
    struct TAuditReport<YandexQuery::DeleteQueryRequest, YandexQuery::Query>
        : NActors::TEventLocal<DeleteQueryAuditReport, EvDeleteQueryReport>
        , TAuditReportBase<YandexQuery::DeleteQueryRequest, YandexQuery::Query> {
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

} // namespace NYq
