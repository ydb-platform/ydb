#pragma once

#include "pdisk_state.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/mon/mon.h>

#include <google/protobuf/util/json_util.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/serialized_enum.h>

#include <iostream>

namespace NKikimr::NCms {

class TJsonProxyProto : public TActorBootstrapped<TJsonProxyProto> {
private:
    using TBase = TActorBootstrapped<TJsonProxyProto>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SERVICE_PROXY;
    }

    TJsonProxyProto(NMon::TEvHttpInfo::TPtr &event)
        : RequestEvent(event)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                    "TJsonProxyProto::Bootstrap url=" << RequestEvent->Get()->Request.GetPathInfo());
        ProcessRequest(ctx);
        Die(ctx);
    }

protected:
    void ProcessRequest(const TActorContext &ctx) {
        const TCgiParameters &cgi = RequestEvent->Get()->Request.GetParams();

        if (cgi.Has("enum")) {
            TString name = cgi.Get("enum");
            if (name == "NKikimrConsole::TConfigItem::EKind")
                return ReplyWithEnumDescription(*NKikimrConsole::TConfigItem::EKind_descriptor(), ctx);
            else if (name == "NKikimrConsole::TConfigItem::EMergeStrategy")
                return ReplyWithEnumDescription(*NKikimrConsole::TConfigItem::EMergeStrategy_descriptor(), ctx);
            else if (name == "NKikimrServices::EServiceKikimr")
                return ReplyWithLogComponents(ctx.LoggerSettings(), ctx);
            else if (name == "NKikimrCms::TCmsConfig::TLogConfig::ELevel")
                return ReplyWithEnumDescription(*NKikimrCms::TCmsConfig::TLogConfig::ELevel_descriptor(), ctx);
            else if (name == "NKikimrCms::TLogRecordData::EType")
                return ReplyWithEnumDescription(*NKikimrCms::TLogRecordData::EType_descriptor(), ctx);
            else if (name == "NCms::EPDiskState")
                return ReplyWithEnumDescription(*NKikimrBlobStorage::TPDiskState::E_descriptor(), ctx);
        } else if (cgi.Has("type")) {
            TString name = cgi.Get("type");
            if (name == ".NKikimrConfig.TImmediateControlsConfig")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TDataShardControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TDataShardControls::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TDataShardControls.TExecutionProfileOptions")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TDataShardControls::TExecutionProfileOptions::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TTxLimitControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TTxLimitControls::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TCoordinatorControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TCoordinatorControls::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TSchemeShardControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TSchemeShardControls::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TTCMallocControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TTCMallocControls::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TVDiskControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TVDiskControls::descriptor(), ctx);
            else if (name == ".NKikimrConfig.TImmediateControlsConfig.TTabletControls")
                return ReplyWithTypeDescription(*NKikimrConfig::TImmediateControlsConfig::TTabletControls::descriptor(), ctx);
        }

        ctx.Send(RequestEvent->Sender,
                 new NMon::TEvHttpInfoRes(TString(NMonitoring::HTTPNOTFOUND) + "unsupported args",
                                          0,
                                          NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    void ReplyWithLogComponents(NLog::TSettings *settings, const TActorContext &ctx) {
        NJson::TJsonValue json;

        if (settings) {
            for (NLog::EComponent i = settings->MinVal; i < settings->MaxVal; i++) {
                auto name = settings->ComponentName(i);
                if (!*name) {
                    continue;
                }

                NJson::TJsonValue value;
                value["name"] = name;
                value["number"] = static_cast<ui32>(i);
                json["value"].AppendValue(value);
            }
        }

        Reply(WriteJson(json), ctx);
    }

    void ReplyWithEnumDescription(const ::google::protobuf::EnumDescriptor &descriptor, const TActorContext &ctx) {
        ::google::protobuf::EnumDescriptorProto result;
        descriptor.CopyTo(&result);
        Reply(result, ctx);
    }

    void ReplyWithTypeDescription(const ::google::protobuf::Descriptor &descriptor, const TActorContext &ctx) {
        ::google::protobuf::DescriptorProto result;
        descriptor.CopyTo(&result);

        auto json = ProtoToJson(result);

        // Add custom field options to json.
        NJson::TJsonValue val;
        auto ok = ReadJsonTree(json, &val);
        Y_ABORT_UNLESS(ok);

        AddCustomFieldOptions(descriptor, val);
        json = WriteJson(val);

        Reply(json, ctx);
    }

    void AddCustomFieldOptions(const ::google::protobuf::Descriptor &descriptor, NJson::TJsonValue &val) {
        if (!val.IsMap() || !val.Has("field"))
            return;

        auto &fields = val["field"];
        if (!fields.IsArray())
            return;

        for (size_t i = 0; i < fields.GetArray().size(); ++i) {
            auto &field = fields[i];
            if (!field.IsMap() || !field.Has("options"))
                continue;

            auto num = field["number"].GetUInteger();
            auto &opts = field["options"];

            auto *fieldDesc = descriptor.FindFieldByNumber(num);
            Y_ABORT_UNLESS(fieldDesc);

            auto &optsMsg = fieldDesc->options();
            auto *reflection = optsMsg.GetReflection();

            std::vector<const ::google::protobuf::FieldDescriptor *> optsFields;
            reflection->ListFields(optsMsg, &optsFields);

            for (auto *optsFieldDesc : optsFields) {
                if (optsFieldDesc->is_extension()
                    && optsFieldDesc->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
                    auto &ext = reflection->GetMessage(optsMsg, optsFieldDesc);
                    auto json = ProtoToJson(ext);
                    NJson::TJsonValue extVal;
                    ReadJsonTree(json, &extVal);
                    opts[optsFieldDesc->name()] = extVal;
                }
            }
        }
    }


    TString ProtoToJson(const google::protobuf::Message &resp) {
        auto config = NProtobufJson::TProto2JsonConfig()
            .SetFormatOutput(false)
            .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
        return NProtobufJson::Proto2Json(resp, config);
    }

    void Reply(const google::protobuf::Message &resp, const TActorContext &ctx) {
        Reply(ProtoToJson(resp), ctx);
    }

    void Reply(const TString &json, const TActorContext &ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::CMS,
                    "TJsonProxyProto reply with json '" << json << "'");

        ctx.Send(RequestEvent->Sender, new NMon::TEvHttpInfoRes(TString(NMonitoring::HTTPOKJSON) + json, 0,
                                                                NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    NMon::TEvHttpInfo::TPtr RequestEvent;
};

} // namespace NKikimr::NCms
