#include "scheme_cache.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/path.h>

#include <util/string/builder.h>

namespace NKikimr {
namespace NSchemeCache {

TSchemeCacheConfig::TSchemeCacheConfig(const TAppData* appData, ::NMonitoring::TDynamicCounterPtr counters)
    : Counters(counters)
{
    Y_ABORT_UNLESS(appData);
    Y_ABORT_UNLESS(appData->DomainsInfo);
    if (const auto& domain = appData->DomainsInfo->Domain; domain && domain->SchemeRoot) {
        Roots.emplace_back(domain->DomainRootTag(), domain->SchemeRoot, domain->Name);
    }
}

TString TDomainInfo::ToString() const {
    auto result = TStringBuilder() << "{"
        << " DomainKey: " << DomainKey
        << " ResourcesDomainKey: " << ResourcesDomainKey
        << " Params { " << Params.ShortDebugString() << " }"
        << " ServerlessComputeResourcesMode: " << ServerlessComputeResourcesMode;

    result << " Users: [";
    for (ui32 i = 0; i < Users.size(); ++i) {
        if (i) {
            result << ",";
        }

        result << Users.at(i).ToString();
    }
    result << "]";

    result << " Groups: [";
    for (ui32 i = 0; i < Groups.size(); ++i) {
        if (i) {
            result << ",";
        }

        result << Groups.at(i).ToString();
    }
    result << "]";

    result << " }";
    return result;
}

TString TDomainInfo::TUser::ToString() const {
    return TStringBuilder() << "{"
        << " Sid: " << Sid
    << " }";
}

TString TDomainInfo::TGroup::ToString() const {
    auto result = TStringBuilder() << "{"
        << " Sid: " << Sid;

    result << " Members: [";
    for (ui32 i = 0; i < Members.size(); ++i) {
        if (i) {
            result << ",";
        }

        result << Members.at(i);
    }
    result << "]";

    result << " }";
    return result;
}

TString TSchemeCacheNavigate::TEntry::ToString() const {
    auto out = TStringBuilder() << "{"
        << " Path: " << JoinPath(Path)
        << " TableId: " << TableId
        << " RequestType: " << RequestType
        << " Operation: " << Operation
        << " RedirectRequired: " << (RedirectRequired ? "true" : "false")
        << " ShowPrivatePath: " << (ShowPrivatePath ? "true" : "false")
        << " SyncVersion: " << (SyncVersion ? "true" : "false")
        << " Status: " << Status
        << " Kind: " << Kind
        << " DomainInfo " << (DomainInfo ? DomainInfo->ToString() : "<null>");
    
    if (ListNodeEntry) {
        out << " Children [";
        for (ui32 i = 0; i < ListNodeEntry->Children.size(); ++i) {
            if (i) {
                out << ",";
            }

            out << ListNodeEntry->Children.at(i).Name;
        }
        out << "]";
    }
    
    out << " }";
    return out;
}

TString TSchemeCacheNavigate::TEntry::ToString(const NScheme::TTypeRegistry& typeRegistry) const {
    Y_UNUSED(typeRegistry);
    return ToString();
}

template <typename TResultSet>
static TString ResultSetToString(const TResultSet& rs, const NScheme::TTypeRegistry& typeRegistry) {
    TStringBuilder out;

    for (ui32 i = 0; i < rs.size(); ++i) {
        if (i) {
            out << ",";
        }

        out << rs.at(i).ToString(typeRegistry);
    }

    return out;
}

TString TSchemeCacheNavigate::ToString(const NScheme::TTypeRegistry& typeRegistry) const {
    return TStringBuilder() << "{"
        << " ErrorCount: " << ErrorCount
        << " DatabaseName: " << DatabaseName
        << " DomainOwnerId: " << DomainOwnerId
        << " Instant: " << Instant
        << " ResultSet [" << ResultSetToString(ResultSet, typeRegistry) << "]"
    << " }";
}

TString TSchemeCacheRequest::TEntry::ToString() const {
    return TStringBuilder() << "{"
        << " TableId: " << (KeyDescription ? ::ToString(KeyDescription->TableId.PathId) : "<moved>")
        << " Access: " << Access
        << " SyncVersion: " << (SyncVersion ? "true" : "false")
        << " Status: " << Status
        << " Kind: " << Kind
        << " PartitionsCount: " << (KeyDescription ? ::ToString(KeyDescription->GetPartitions().size()) : "<moved>")
        << " DomainInfo " << (DomainInfo ? DomainInfo->ToString() : "<null>")
    << " }";
}

TString TSchemeCacheRequest::TEntry::ToString(const NScheme::TTypeRegistry& typeRegistry) const {
    TStringBuilder out;
    out << "{"
        << " TableId: " << (KeyDescription ? ::ToString(KeyDescription->TableId.PathId) : "<moved>")
        << " Access: " << Access
        << " SyncVersion: " << (SyncVersion ? "true" : "false")
        << " Status: " << Status
        << " Kind: " << Kind
        << " PartitionsCount: " << (KeyDescription ? ::ToString(KeyDescription->GetPartitions().size()) : "<moved>")
        << " DomainInfo " << (DomainInfo ? DomainInfo->ToString() : "<null>");

    if (KeyDescription) {
        TDbTupleRef from(KeyDescription->KeyColumnTypes.data(), KeyDescription->Range.From.data(), KeyDescription->Range.From.size());
        TDbTupleRef to(KeyDescription->KeyColumnTypes.data(), KeyDescription->Range.To.data(), KeyDescription->Range.To.size());

        if (KeyDescription->Range.Point) {
            out << " Point: " << DbgPrintTuple(from, typeRegistry);
        } else {
            out << " From: " << DbgPrintTuple(from, typeRegistry)
                << " IncFrom: " << KeyDescription->Range.InclusiveFrom
                << " To: " << DbgPrintTuple(to, typeRegistry)
                << " IncTo: " << KeyDescription->Range.InclusiveTo;
        }
    }

    out << " }";
    return out;
}

TString TSchemeCacheRequest::ToString(const NScheme::TTypeRegistry& typeRegistry) const {
    return TStringBuilder() << "{"
        << " ErrorCount: " << ErrorCount
        << " DatabaseName: " << DatabaseName
        << " DomainOwnerId: " << DomainOwnerId
        << " ResultSet [" << ResultSetToString(ResultSet, typeRegistry) << "]"
    << " }";
}

} // NSchemeCache
} // NKikimr
