#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include "viewer.h"
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using NSchemeShard::TEvSchemeShard;

class TJsonACL : public TViewerPipeClient<TJsonACL> {
    using TBase = TViewerPipeClient<TJsonACL>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonACL(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void FillParams(NKikimrSchemeOp::TDescribePath* record, const TCgiParameters& params) {
        if (params.Has("path")) {
            record->SetPath(params.Get("path"));
        }
        if (params.Has("path_id")) {
            record->SetPathId(FromStringWithDefault<ui64>(params.Get("path_id")));
        }
        if (params.Has("schemeshard_id")) {
            record->SetSchemeshardId(FromStringWithDefault<ui64>(params.Get("schemeshard_id")));
        }
    }

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);

        if (params.Has("schemeshard_id")) {
            THolder<TEvSchemeShard::TEvDescribeScheme> request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>();
            FillParams(&request->Record, params);
            ui64 schemeShardId = FromStringWithDefault<ui64>(params.Get("schemeshard_id"));
            SendRequestToPipe(ConnectTabletPipe(schemeShardId), request.Release());
        } else {
            THolder<TEvTxUserProxy::TEvNavigate> request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            FillParams(request->Record.MutableDescribePath(), params);
            request->Record.SetUserToken(Event->Get()->UserToken);
            SendRequest(MakeTxProxyID(), request.Release());
        }
        Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        DescribeResult = ev->Release();
        RequestDone();
    }

    void ReplyAndPassAway() {
        TStringStream json;
        TString headers = Viewer->GetHTTPOKJSON(Event->Get());
        if (DescribeResult != nullptr) {
            //TProtoToJson::ProtoToJson(json, DescribeResult->GetRecord(), JsonSettings);
            const auto& pbRecord(DescribeResult->GetRecord());
            NKikimrViewer::TMetaInfo metaInfo;
            NKikimrViewer::TMetaCommonInfo& pbCommon = *metaInfo.MutableCommon();
            pbCommon.SetPath(pbRecord.GetPath());
            if (pbRecord.HasPathDescription()) {
                const auto& pbPathDescription(pbRecord.GetPathDescription());
                if (pbPathDescription.HasSelf()) {
                    const auto& pbSelf(pbPathDescription.GetSelf());
                    pbCommon.SetOwner(pbSelf.GetOwner());
                }
                if (pbPathDescription.GetSelf().HasACL()) {
                    NACLib::TACL acl(pbPathDescription.GetSelf().GetACL());
                    auto& aces(acl.GetACE());
                    for (auto it = aces.begin(); it != aces.end(); ++it) {
                        const NACLibProto::TACE& ace = *it;
                        auto& pbAce = *pbCommon.AddACL();
                        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Deny) {
                            pbAce.SetAccessType("Deny");
                        } else
                        if (static_cast<NACLib::EAccessType>(ace.GetAccessType()) == NACLib::EAccessType::Allow) {
                            pbAce.SetAccessType("Allow");
                        }
                        static std::unordered_map<ui32, TString> mapAccessRights = {
                            {NACLib::EAccessRights::SelectRow, "SelectRow"},
                            {NACLib::EAccessRights::UpdateRow, "UpdateRow"},
                            {NACLib::EAccessRights::EraseRow, "EraseRow"},
                            {NACLib::EAccessRights::ReadAttributes, "ReadAttributes"},
                            {NACLib::EAccessRights::WriteAttributes, "WriteAttributes"},
                            {NACLib::EAccessRights::CreateDirectory, "CreateDirectory"},
                            {NACLib::EAccessRights::CreateTable, "CreateTable"},
                            {NACLib::EAccessRights::CreateQueue, "CreateQueue"},
                            {NACLib::EAccessRights::RemoveSchema, "RemoveSchema"},
                            {NACLib::EAccessRights::DescribeSchema, "DescribeSchema"},
                            {NACLib::EAccessRights::AlterSchema, "AlterSchema"},
                            {NACLib::EAccessRights::CreateDatabase, "CreateDatabase"},
                            {NACLib::EAccessRights::DropDatabase, "DropDatabase"},
                            {NACLib::EAccessRights::GrantAccessRights, "GrantAccessRights"},
                            {NACLib::EAccessRights::WriteUserAttributes, "WriteUserAttributes"},
                            {NACLib::EAccessRights::ConnectDatabase, "ConnectDatabase"},
                            {NACLib::EAccessRights::ReadStream, "ReadStream"},
                            {NACLib::EAccessRights::WriteStream, "WriteStream"},
                            {NACLib::EAccessRights::ReadTopic, "ReadTopic"},
                            {NACLib::EAccessRights::WriteTopic, "WriteTopic"}
                        };
                        auto ar = ace.GetAccessRight();
                        int shift = 0;
                        while (ar > 0) {
                            if (ar & (1 << shift)) {
                                pbAce.AddAccessRights(mapAccessRights[1 << shift]);
                                ar ^= 1 << shift;
                            }
                            ++shift;
                        }
                        pbAce.SetSubject(ace.GetSID());
                        auto inht = ace.GetInheritanceType();
                        if ((inht & NACLib::EInheritanceType::InheritObject) != 0) {
                            pbAce.AddInheritanceType("InheritObject");
                        }
                        if ((inht & NACLib::EInheritanceType::InheritContainer) != 0) {
                            pbAce.AddInheritanceType("InheritContainer");
                        }
                        if ((inht & NACLib::EInheritanceType::InheritOnly) != 0) {
                            pbAce.AddInheritanceType("InheritOnly");
                        }
                    }
                }
            }

            TProtoToJson::ProtoToJson(json, metaInfo, JsonSettings);

            switch (DescribeResult->GetRecord().GetStatus()) {
            case NKikimrScheme::StatusAccessDenied:
                headers = Viewer->GetHTTPFORBIDDEN(Event->Get());
                break;
            default:
                break;
            }
        } else {
            json << "null";
        }

        Send(Event->Sender, new NMon::TEvHttpInfoRes(headers + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonACL> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TMetaInfo>();
    }
};

template <>
struct TJsonRequestParameters<TJsonACL> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: path
              in: query
              description: schema path
              required: false
              type: string
            - name: schemeshard_id
              in: query
              description: schemeshard identifier (tablet id)
              required: false
              type: integer
            - name: path_id
              in: query
              description: path id
              required: false
              type: integer
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonACL> {
    static TString GetSummary() {
        return "ACL information";
    }
};

template <>
struct TJsonRequestDescription<TJsonACL> {
    static TString GetDescription() {
        return "Returns information about acl of an object";
    }
};

}
}
