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
    const auto usersView = std::views::keys(Users);
    for (auto it = usersView.begin(); it != usersView.end(); ++it) {
        if (it != usersView.begin()) {
            result << ", ";
        }

        result << "{ Sid: " << *it << " }";
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

namespace NKikimr::NSchemeCache {

TDomainInfo::TDomainInfo(const NKikimrSubDomains::TDomainDescription& descr)
    : DomainKey(TPathId::FromDomainKey(descr.GetDomainKey()))
    , Params(descr.GetProcessingParams())
    , Coordinators(descr.GetProcessingParams())
{
    if (descr.HasResourcesDomainKey()) {
        ResourcesDomainKey = TPathId::FromDomainKey(descr.GetResourcesDomainKey());
    } else {
        ResourcesDomainKey = DomainKey;
    }

    if (descr.HasServerlessComputeResourcesMode()) {
        ServerlessComputeResourcesMode = descr.GetServerlessComputeResourcesMode();
    }

    if (descr.HasSharedHive()) {
        SharedHiveId = descr.GetSharedHive();
    }

    if (descr.HasSecurityState()) {
        for (const auto& sid : descr.GetSecurityState().GetSids()) {
            switch (sid.GetType()) {
            case NLoginProto::ESidType_SidType_USER: {
                THashInitParams userHashesInitParams;
                userHashesInitParams.reserve(sid.HashesInitParamsSize());
                for (const auto& hashInitParams : sid.GetHashesInitParams()) {
                    userHashesInitParams.emplace(hashInitParams.GetHashType(), hashInitParams.GetInitParams());
                }
                Users.emplace(sid.GetName(), std::move(userHashesInitParams));
                break;
            }
            case NLoginProto::ESidType_SidType_GROUP: {
                TVector<TString> members(sid.GetMembers().begin(), sid.GetMembers().end());
                Groups.emplace_back(sid.GetName(), std::move(members));
                break;
            }
            default:
                break;
            }
        }
    }
}

ui64 TDomainInfo::ExtractSchemeShard() const {
    if (Params.HasSchemeShard()) {
        return Params.GetSchemeShard();
    } else {
        return DomainKey.OwnerId;
    }
}

ui64 TDomainInfo::ExtractHive() const {
    if (IsServerless()) {
        return SharedHiveId;
    } else {
        return Params.GetHive();
    }
}

} // namespace NKikimr::NSchemeCache
