#include "distconf.h"
#include "node_warden_impl.h"

#include <google/protobuf/util/json_util.h>

namespace NKikimr::NStorage {

    namespace {

        class TInvokeRequestHandler : public TActorBootstrapped<TInvokeRequestHandler> {
            std::unique_ptr<TEventHandle<NMon::TEvHttpInfo>> Event;

        public:
            TInvokeRequestHandler(std::unique_ptr<TEventHandle<NMon::TEvHttpInfo>>&& ev)
                : Event(std::move(ev))
            {}

            void Bootstrap(TActorId parentId) {
                Become(&TThis::StateFunc);

                const auto& request = Event->Get()->Request;

                // validate content type, we accept only JSON
                TString contentType;
                {
                    const auto& headers = request.GetHeaders();
                    if (const auto *header = headers.FindHeader("Content-Type")) {
                        TStringBuf value = header->Value();
                        contentType = value.NextTok(';');
                    }
                }
                if (contentType != "application/json") {
                    return FinishWithError("invalid or unset Content-Type");
                }

                // parse the record
                auto ev = std::make_unique<TEvNodeConfigInvokeOnRoot>();
                const auto status = google::protobuf::util::JsonStringToMessage(request.GetPostContent(), &ev->Record);
                if (!status.ok()) {
                    return FinishWithError("failed to parse JSON");
                }
                STLOG(PRI_DEBUG, BS_NODE, NWDC04, "sending TEvNodeConfigInvokeOnRoot", (Record, ev->Record));

                // send it to the actor
                const TActorId nondeliveryId = SelfId();
                auto handle = std::make_unique<IEventHandle>(parentId, SelfId(), ev.release(),
                    IEventHandle::FlagForwardOnNondelivery, 0, &nondeliveryId);
                TActivationContext::Send(handle.release());
            }

            void Handle(TEvNodeConfigInvokeOnRootResult::TPtr ev) {
                STLOG(PRI_DEBUG, BS_NODE, NWDC39, "receive TEvNodeConfigInvokeOnRootResult", (Record, ev->Get()->Record));

                TString data;
                google::protobuf::util::MessageToJsonString(ev->Get()->Record, &data);

                TStringStream s;
                s << NMonitoring::HTTPOKJSON << data;

                Send(Event->Sender, new NMon::TEvHttpInfoRes(s.Str(), Event->Get()->SubRequestId,
                    NMon::TEvHttpInfoRes::Custom), 0, Event->Cookie);
                PassAway();
            }

            void FinishWithError(TString error) {
                Send(Event->Sender, new NMon::TEvHttpInfoRes(std::move(error), Event->Get()->SubRequestId,
                    NMon::TEvHttpInfoRes::Html), 0, Event->Cookie);
                PassAway();
            }

            void Handle(TEvents::TEvUndelivered::TPtr /*ev*/) {
                FinishWithError("event delivery failed");
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvNodeConfigInvokeOnRootResult, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
            )
        };

    } // anonymous

    void TDistributedConfigKeeper::Handle(NMon::TEvHttpInfo::TPtr ev) {
        const auto& request = ev->Get()->Request;
        if (request.GetMethod() == HTTP_METHOD_POST) {
            std::unique_ptr<TEventHandle<NMon::TEvHttpInfo>> evPtr(ev.Release());
            Register(new TInvokeRequestHandler(std::move(evPtr)));
            return;
        }

        const TCgiParameters& cgi = request.GetParams();
        NMon::TEvHttpInfoRes::EContentType contentType = NMon::TEvHttpInfoRes::Custom;
        TStringStream out;

        if (cgi.Has("json")) {
            out << NMonitoring::HTTPOKJSON;

            auto getBinding = [&]() -> NJson::TJsonValue {
                if (Binding) {
                    return NJson::TJsonMap{
                        {"node_id", Binding->NodeId},
                        {"root_node_id", Binding->RootNodeId},
                        {"cookie", Binding->Cookie},
                        {"session_id", Binding->SessionId.ToString()},
                    };
                } else {
                    return NJson::JSON_NULL;
                }
            };

            auto getDirectBoundNodes = [&]() -> NJson::TJsonValue {
                NJson::TJsonValue res(NJson::JSON_ARRAY);
                for (const auto& [nodeId, info] : DirectBoundNodes) {
                    NJson::TJsonValue boundNodeIds(NJson::JSON_ARRAY);
                    for (const auto& boundNodeId : info.BoundNodeIds) {
                        boundNodeIds.AppendValue(NJson::TJsonMap{
                            {"host", std::get<0>(boundNodeId)},
                            {"port", std::get<1>(boundNodeId)},
                            {"node_id", std::get<2>(boundNodeId)},
                        });
                    }
                    NJson::TJsonValue scatterTasks(NJson::JSON_ARRAY);
                    for (const ui64 cookie : info.ScatterTasks) {
                        scatterTasks.AppendValue(cookie);
                    }
                    res.AppendValue(NJson::TJsonMap{
                        {"node_id", nodeId},
                        {"cookie", info.Cookie},
                        {"session_id", info.SessionId.ToString()},
                        {"bound_node_ids", std::move(boundNodeIds)},
                        {"scatter_tasks", std::move(scatterTasks)},
                    });
                }
                return res;
            };

            auto getAllBoundNodes = [&] {
                NJson::TJsonValue res(NJson::JSON_ARRAY);

                for (const auto& [nodeId, node] : AllBoundNodes) {
                    NJson::TJsonValue item(NJson::JSON_MAP);
                    item["node_id"] = NJson::TJsonMap{
                        {"host", std::get<0>(nodeId)},
                        {"port", std::get<1>(nodeId)},
                        {"node_id", std::get<2>(nodeId)},
                    };

                    NJson::TJsonValue refs(NJson::JSON_MAP);
                    for (const auto& [refererNodeId, it] : node.Refs) {
                        refs[ToString(refererNodeId)] = NJson::TJsonMap{
                            {"generation", it->GetGeneration()},
                            {"fingerprint", HexEncode(it->GetFingerprint())},
                        };
                    }
                    item["refs"] = std::move(refs);

                    res.AppendValue(std::move(item));
                }

                return res;
            };

            NJson::TJsonValue root = NJson::TJsonMap{
                {"binding", getBinding()},
                {"direct_bound_nodes", getDirectBoundNodes()},
                {"all_bound_nodes", getAllBoundNodes()},
                {"root_state", TString(TStringBuilder() << RootState)},
                {"error_reason", ErrorReason},
                {"has_quorum", GlobalQuorum},
                {"scepter", Scepter ? NJson::TJsonMap{
                    {"id", Scepter->Id},
                } : NJson::TJsonValue{NJson::JSON_NULL}},
            };

            NJson::WriteJson(&out, &root);
        } else {
            HTML(out) {
                DIV() {
                    TAG(TH2) {
                        out << "Distributed config keeper";
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Main operational parameters";
                    }
                    DIV_CLASS("panel-body") {
                        out << "Self-management enabled: " << (SelfManagementEnabled ? "yes" : "no") << "<br/>";
                        out << "Self pile id: ";
                        if (BridgeInfo) {
                            out << BridgeInfo->SelfNodePile->BridgePileId;
                        } else {
                            out << "<none>";
                        }
                        out << "<br/>";
                    }
                }

                auto outputConfig = [&](const char *name, auto *config) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            out << name;
                        }
                        DIV_CLASS("panel-body") {
                            if (config) {
                                out << "<pre>";
                                OutputPrettyMessage(out, *config);
                                out << "</pre>";
                                if (config->HasConfigComposite()) {
                                    out << "<strong>config.yaml</strong><br/>";
                                    TString yaml;
                                    ui64 version;
                                    TString fetchYaml;
                                    auto error = DecomposeConfig(config->GetConfigComposite(), &yaml, &version, &fetchYaml);
                                    if (error) {
                                        out << "<strong>error: " << error << "</strong>";
                                    } else {
                                        out << "<pre>" << yaml << "</pre>";
                                    }
                                }
                                if (config->HasCompressedStorageYaml()) {
                                    TStringInput ss(config->GetCompressedStorageYaml());
                                    TZstdDecompress zstd(&ss);
                                    TString yaml = zstd.ReadAll();
                                    out << "<strong>storage.yaml (size " << yaml.size() << ")</strong><br/><pre>"
                                        << yaml << "</pre>";
                                }
                            } else {
                                out << "not defined";
                            }
                        }
                    }
                };
                outputConfig("StorageConfig", StorageConfig.get());
                outputConfig("BaseConfig", BaseConfig.get());
                outputConfig("InitialConfig", InitialConfig.get());
                outputConfig("ProposedStorageConfig", ProposedStorageConfig ? &ProposedStorageConfig.value() : nullptr);

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Outgoing binding";
                    }
                    DIV_CLASS("panel-body") {
                        out << "Binding: ";
                        if (Binding) {
                            if (Binding->RootNodeId) {
                                out << "<a href='/node/" << Binding->RootNodeId << "/actors/nodewarden?page=distconf" << "'>"
                                    << Binding->ToString() << "</a>";
                            } else {
                                out << "trying " << Binding->ToString();
                            }
                        } else {
                            out << "not bound";
                        }
                        out << "<br/>";
                        out << "RootState: " << RootState << "<br/>";
                        if (ErrorReason) {
                           out << "ErrorReason: " << ErrorReason << "<br/>";
                        }
                        out << "Quorum: " << (GlobalQuorum ? "yes" : "no") << "<br/>";
                        out << "Scepter: " << (Scepter ? ToString(Scepter->Id) : "null") << "<br/>";
                        out << "NodeIdsForOutgoingBinding: " << FormatList(NodeIdsForOutgoingBinding) << "<br/>";
                        out << "NodeIdsForPrimaryPileOutgoingBinding: " << FormatList(NodeIdsForPrimaryPileOutgoingBinding) << "<br/>";
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Static <-> dynamic node interaction";
                    }
                    DIV_CLASS("panel-body") {
                        out << "IsSelfStatic: " << (IsSelfStatic ? "yes" : "no") << "<br/>";
                        out << "ConnectedToStaticNode: " << ConnectedToStaticNode << "<br/>";
                        out << "StaticNodeSessionId: " << StaticNodeSessionId << "<br/>";
                        out << "ConnectedDynamicNodes: " << FormatList(ConnectedDynamicNodes) << "<br/>";
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Incoming bindings";
                    }
                    DIV_CLASS("panel-body") {
                        DIV() {
                            out << "AllBoundNodes count: " << AllBoundNodes.size() << "<br/>";
                            out << "LocalPileQuorum: " << (LocalPileQuorum ? "yes" : "no") << "<br/>";
                            out << "GlobalQuorum: " << (GlobalQuorum ? "yes" : "no") << "<br/>";
                            out << "NodeIdsForIncomingBinding: " << FormatList(NodeIdsForIncomingBinding) << "<br/>";
                            out << "ConnectedUnsyncedPiles: " << FormatList(ConnectedUnsyncedPiles) << "<br/>";
                        }
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { out << "NodeId"; }
                                    TABLEH() { out << "Cookie"; }
                                    TABLEH() { out << "SessionId"; }
                                    TABLEH() { out << "BoundNodeIds"; }
                                    TABLEH() { out << "ScatterTasks"; }
                                }
                            }
                            TABLEBODY() {
                                std::vector<ui32> nodeIds;
                                for (const auto& [nodeId, info] : DirectBoundNodes) {
                                    nodeIds.push_back(nodeId);
                                }
                                std::sort(nodeIds.begin(), nodeIds.end());
                                for (const ui32 nodeId : nodeIds) {
                                    const auto& info = DirectBoundNodes.at(nodeId);

                                    auto makeBoundNodeIds = [&] {
                                        TStringStream s;
                                        std::vector<ui32> ids;
                                        for (const auto& boundNodeId : info.BoundNodeIds) {
                                            ids.push_back(std::get<2>(boundNodeId));
                                        }
                                        std::sort(ids.begin(), ids.end());
                                        for (size_t begin = 0; begin < ids.size(); ) {
                                            size_t end;
                                            for (end = begin + 1; end < ids.size() && ids[end - 1] + 1 == ids[end]; ++end) {}
                                            if (begin) {
                                                s << "<br/>";
                                            }
                                            if (end == begin + 1) {
                                                s << ids[begin];
                                            } else {
                                                s << ids[begin] << '-' << ids[end - 1];
                                            }
                                            begin = end;
                                        }
                                        return s.Str();
                                    };

                                    TABLER() {
                                        TABLED() { out << "<a href=\"/node/" << nodeId << "/actors/nodewarden?page=distconf\">" << nodeId << "</a>"; }
                                        TABLED() { out << info.Cookie; }
                                        TABLED() { out << info.SessionId; }
                                        TABLED() { out << makeBoundNodeIds(); }
                                        TABLED() { out << FormatList(info.ScatterTasks); }
                                    }
                                }
                            }
                        }
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Bound nodes";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { out << "NodeId"; }
                                    TABLEH() { out << "FQDN:IcPort"; }
                                    TABLEH() { out << "RefererNodeId"; }
                                    TABLEH() { out << "Generation"; }
                                    TABLEH() { out << "Fingerprint"; }
                                }
                            }
                            TABLEBODY() {
                                auto r = AllBoundNodes | std::views::keys;
                                std::vector<TNodeIdentifier> nodeIds(r.begin(), r.end());
                                std::ranges::sort(nodeIds);
                                for (const auto& nodeId : nodeIds) {
                                    auto& node = AllBoundNodes.at(nodeId);
                                    auto r = node.Refs | std::views::keys;
                                    std::vector<ui32> refererNodeIds(r.begin(), r.end());
                                    std::ranges::sort(refererNodeIds);
                                    for (ui32 refererNodeId : refererNodeIds) {
                                        const auto& it = node.Refs.at(refererNodeId);
                                        TABLER() {
                                            TABLED() { out << nodeId.NodeId(); }
                                            TABLED() { out << std::get<0>(nodeId) << ':' << std::get<1>(nodeId); }
                                            TABLED() { out << refererNodeId; }
                                            TABLED() { out << it->GetGeneration(); }
                                            TABLED() { out << HexEncode(it->GetFingerprint()); }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Cache";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { out << "Key"; }
                                    TABLEH() { out << "Generation"; }
                                    TABLEH() { out << "Value size"; }
                                }
                            }
                            TABLEBODY() {
                                std::vector<TString> keys;
                                for (const auto& [key, value] : Cache) {
                                    keys.push_back(key);
                                }
                                std::ranges::sort(keys);
                                for (const TString& key : keys) {
                                    const TCacheItem& value = Cache.at(key);
                                    TABLER() {
                                        TABLED() { out << key; }
                                        TABLED() { out << value.Generation; }
                                        TABLED() { out << (value.Value ? value.Value->size() : 0); }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            contentType = NMon::TEvHttpInfoRes::Html;
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), ev->Get()->SubRequestId, contentType), 0, ev->Cookie);
    }

} // NKikimr::NStorage
