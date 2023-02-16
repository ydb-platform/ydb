#include "proxy_ut_helpers.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NTxProxyUT {

void Print(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp) {
    TString out;
    ::google::protobuf::TextFormat::PrintToString(resp.Get()->Record, &out);
    Cerr << out << Endl;
}

namespace NTestLs {

TString IsUnavailable(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusNotAvailable);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString Finished(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;

    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    Y_UNUSED(type);
    UNIT_ASSERT_VALUES_EQUAL(record.GetPathDescription().GetSelf().GetPathType(), type);
    UNIT_ASSERT(record.GetPathDescription().GetSelf().GetCreateFinished());
    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString NotInSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, type);
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    UNIT_ASSERT(descr.HasDomainDescription());
    UNIT_ASSERT_VALUES_EQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), 1);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString HasUserAttributes(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, TVector<std::pair<TString, TString>> attrs) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    std::sort(attrs.begin(), attrs.end());

    TVector<std::pair<TString, TString>> present;
    for (const auto& item: descr.GetUserAttributes()) {
        present.emplace_back(item.GetKey(), item.GetValue());
    }
    std::sort(present.begin(), present.end());

    TVector<std::pair<TString, TString>> diff;
    std::set_difference(attrs.begin(), attrs.end(),
                        present.begin(), present.end(),
                        std::back_inserter(diff));
    UNIT_ASSERT_C(diff.size() == 0,
                  diff.size() << " attrs not present or has different value, for example name: " << diff.front().first << " value: " << diff.front().second);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}


TString IsSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, NKikimrSchemeOp::EPathTypeSubDomain);
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    UNIT_ASSERT(descr.HasDomainDescription());
    UNIT_ASSERT_VALUES_UNEQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), 1);
    UNIT_ASSERT_VALUES_EQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), descr.GetSelf().GetPathId());

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString IsExtSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), NMsgBusProxy::MSTATUS_OK,
                               "status was: " << NMsgBusProxy::EResponseStatus(record.GetStatus())
                               << " expected: " << NMsgBusProxy::MSTATUS_OK
                               << " explain: " << record.GetErrorReason()
                               << " message: " << record.ShortDebugString()
                               );
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, NKikimrSchemeOp::EPathTypeSubDomain);
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    UNIT_ASSERT(descr.HasDomainDescription());
    UNIT_ASSERT_VALUES_EQUAL(descr.GetSelf().GetPathId(), 1);
    UNIT_ASSERT_VALUES_UNEQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), 1);
    UNIT_ASSERT_VALUES_UNEQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), descr.GetSelf().GetPathId());

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString WithPools(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    UNIT_ASSERT(descr.GetDomainDescription().StoragePoolsSize() > 0);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}


TString InSubdomain(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, type);
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    UNIT_ASSERT(descr.HasDomainDescription());

    if (actualType == NKikimrSchemeOp::EPathTypeSubDomain) {
        UNIT_ASSERT_VALUES_UNEQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), descr.GetSelf().GetPathId());
    }

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString InSubdomainWithPools(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrSchemeOp::EPathType type) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, type);
    UNIT_ASSERT(descr.GetSelf().GetCreateFinished());

    UNIT_ASSERT(descr.HasDomainDescription());
    UNIT_ASSERT_VALUES_UNEQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), 1);
    if (actualType == NKikimrSchemeOp::EPathTypeSubDomain) {
        UNIT_ASSERT_VALUES_EQUAL(descr.GetDomainDescription().GetDomainKey().GetPathId(), descr.GetSelf().GetPathId());
    }

    UNIT_ASSERT(descr.GetDomainDescription().StoragePoolsSize() > 0);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

NKikimrSubDomains::TDomainDescription ExtractDomainDescription(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    UNIT_ASSERT(descr.HasDomainDescription());
    return descr.GetDomainDescription();
}

TVector<NKikimrSchemeOp::TTablePartition> ExtractTablePartitions(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, NKikimrSchemeOp::EPathTypeTable);

    auto& parts = descr.GetTablePartitions();
    UNIT_ASSERT_VALUES_UNEQUAL(0, parts.size());
    return TVector<NKikimrSchemeOp::TTablePartition>(parts.begin(), parts.end());
}

NKikimrSchemeOp::TTableDescription ExtractTableDescription(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, NKikimrSchemeOp::EPathTypeTable);
    UNIT_ASSERT(descr.HasTable());
    return descr.GetTable();
}

TString CheckStatus(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp, NKikimrScheme::EStatus schemeStatus) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), schemeStatus);
    if (NKikimrScheme::EStatus::StatusSuccess == schemeStatus) {
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    }

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TPathVersion ExtractPathVersion(const TAutoPtr<NMsgBusProxy::TBusResponse>& resp) {
    return Tests::TClient::ExtractPathVersion(resp);
}

TString NoChildren(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    return ChildrenCount(resp, 0);
}

TString ChildrenCount(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp, ui64 count) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, NKikimrSchemeOp::EPathTypeDir);

    auto& children = descr.GetChildren();
    UNIT_ASSERT_VALUES_EQUAL(count, children.size());

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString HasChild(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp, TString name, NKikimrSchemeOp::EPathType type) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, NKikimrSchemeOp::EPathTypeDir);

    auto& children = descr.GetChildren();
    bool found = false;
    bool correctType = false;
    for (auto& x: children) {
        if (x.GetName() == name) {
            found = true;
            correctType = x.GetPathType() == type;
            break;
        }
    }

    UNIT_ASSERT(found);
    UNIT_ASSERT(correctType);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString IsDir(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    return PathType(resp, NKikimrSchemeOp::EPathTypeDir);
}

TString IsTable(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    return PathType(resp, NKikimrSchemeOp::EPathTypeTable);
}

TString PathType(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp, NKikimrSchemeOp::EPathType type) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    auto& descr = record.GetPathDescription();
    auto actualType = descr.GetSelf().GetPathType();
    UNIT_ASSERT_VALUES_EQUAL(actualType, type);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

TString IsDoesNotExist(const TAutoPtr<NMsgBusProxy::TBusResponse> &resp) {
    UNIT_ASSERT(resp.Get());
    auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetSchemeStatus(), NKikimrScheme::EStatus::StatusPathDoesNotExist);

    TString out;
    ::google::protobuf::TextFormat::PrintToString(record, &out);
    return out;
}

}


namespace NHelpers {

TChannelBind GetChannelBind(const TString& storagePool) {
    TChannelBind bind;
    bind.SetStoragePoolName(storagePool);
    return bind;
}

ui64 CreateSubDomainAndTabletInside(TBaseTestEnv &env, const TString &name, ui64 shard_index, const TStoragePools &pools) {
    const ui64 owner = THash<TString>()(name);

    //usually (ex for DataShards) SchemeShard proccesses channels binding
    TChannelsBindings channelBindings;
    if (pools) {
        auto poolIt = pools.begin();
        channelBindings.emplace_back(GetChannelBind(poolIt->GetName()));

        if (pools.size() > 1) {
            ++poolIt;
        }
        channelBindings.emplace_back(GetChannelBind(poolIt->GetName()));

        if (pools.size() > 2) {
            ++poolIt;
            channelBindings.emplace_back(GetChannelBind(poolIt->GetName()));
        }
    }

    auto subdomain = GetSubDomainDeclareSetting(name, pools);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK, env.GetClient().CreateSubdomain("/dc-1", subdomain));
    const TString subdomain_path = JoinPath({"/dc-1", subdomain.GetName()});
    const auto desc = NTestLs::ExtractDomainDescription(
                env.GetClient().Ls(subdomain_path));
    const auto key = TSubDomainKey(desc.GetDomainKey());

    TAutoPtr<NMsgBusProxy::TBusResponse> resp = env.GetClient().HiveCreateTablet(
                env.GetSettings().Domain,
                owner, shard_index,
                TTabletTypes::Dummy,
                {},
                {key},
                channelBindings);

    const auto& record = resp->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.CreateTabletResultSize(), 1);

    const auto& result = record.GetCreateTabletResult(0);

    UNIT_ASSERT_EQUAL(
                NKikimrProto::OK,
                result.GetStatus());

    const ui64 tablet_id = result.GetTabletId();

    UNIT_ASSERT(
                env.GetClient().TabletExistsInHive(&env.GetRuntime(), tablet_id));

    return tablet_id;
}

void CheckTableIsOfline(TBaseTestEnv &env, ui64 tablet_id) {
    UNIT_ASSERT_EQUAL(
                Max<ui32>(),
                env.GetClient().GetLeaderNode(&env.GetRuntime(), tablet_id));
}

void CheckTableBecomeAlive(TBaseTestEnv &env, ui64 tablet_id) {
    auto prev = env.GetRuntime().SetDispatchTimeout(WaitTimeOut);
    UNIT_ASSERT(
                env.GetClient().WaitForTabletAlive(&env.GetRuntime(), tablet_id, true, WaitTimeOut));
    env.GetRuntime().SetDispatchTimeout(prev);
}

void CheckTableBecomeOfline(TBaseTestEnv &env, ui64 tablet_id) {
    {
        auto prev = env.GetRuntime().SetDispatchTimeout(WaitTimeOut);
        UNIT_ASSERT(
                    env.GetClient().WaitForTabletDown(&env.GetRuntime(), tablet_id, true, WaitTimeOut));
        env.GetRuntime().SetDispatchTimeout(prev);
    }
    // check that tablet did not wake up
    TDuration negativeTimeout = TDuration::Seconds(1);
    {
        auto prev = env.GetRuntime().SetDispatchTimeout(negativeTimeout);
        UNIT_ASSERT(
                    !env.GetClient().WaitForTabletAlive(&env.GetRuntime(), tablet_id, true, negativeTimeout));
        env.GetRuntime().SetDispatchTimeout(prev);
    }
}

void CheckTableRunOnProperTenantNode(TBaseTestEnv &env, const TString &tenant, ui64 tablet_id) {
    UNIT_ASSERT(
                env.GetTenants().IsActive(tenant,
                                          env.GetClient().GetLeaderNode(&env.GetRuntime(),
                                                                        tablet_id)));
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSetting(const TString &name, const TStoragePools &pools) {
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    for (auto& pool: pools) {
        *subdomain.AddStoragePools() = pool;
    }
    return subdomain;
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSetting(const TString &name, const TStoragePools &pools) {
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    subdomain.SetCoordinators(2);
    subdomain.SetMediators(2);
    subdomain.SetPlanResolution(50);
    subdomain.SetTimeCastBucketsPerMediator(2);
    for (auto& pool: pools) {
        *subdomain.AddStoragePools() = pool;
    }
    return subdomain;
}

NKikimrSchemeOp::TTableDescription GetTableSimpleDescription(const TString &name) {
    NKikimrSchemeOp::TTableDescription tableDescr;
    tableDescr.SetName(name);
    {
        auto *c1 = tableDescr.AddColumns();
        c1->SetName("key");
        c1->SetType("Uint64");
    }
    {
        auto *c2 = tableDescr.AddColumns();
        c2->SetName("value");
        c2->SetType("Uint64");
    }
    tableDescr.SetUniformPartitionsCount(2);
    tableDescr.MutablePartitionConfig()->SetFollowerCount(2);
    *tableDescr.AddKeyColumnNames() = "key";
    return tableDescr;
}

void SetRowInSimpletable(TBaseTestEnv &env, ui64 key, ui64 value, const TString &path) {
    NKikimrMiniKQL::TResult res;
    TString query = Sprintf("("
                            "(let row '('('key (Uint64 '%" PRIu64 "))))"
                                                                  "(let myUpd '("
                                                                  "    '('value (Uint64 '%" PRIu64 "))"
                                                                                                   "))"
                                                                                                   "(let pgmReturn (AsList"
                                                                                                   "    (UpdateRow '%s row myUpd)"
                                                                                                   "))"
                                                                                                   "(return pgmReturn)"
                                                                                                   ")", key, value, path.c_str());

    env.GetClient().FlatQuery(query, res);
}


} //NTestLs

} //NTxProxyUT

} //NKikimr

template<>
inline void Out<NKikimrSchemeOp::EPathType>(IOutputStream& o, NKikimrSchemeOp::EPathType x) {
    o << NKikimrSchemeOp::EPathType_Name(x).data();
    return;
}
