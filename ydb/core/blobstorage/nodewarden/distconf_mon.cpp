#include "distconf.h"

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::Handle(NMon::TEvHttpInfo::TPtr ev) {
        const TCgiParameters& cgi = ev->Get()->Request.GetParams();
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

            NJson::TJsonValue root = NJson::TJsonMap{
                {"binding", getBinding()},
                {"direct_bound_nodes", getDirectBoundNodes()},
                {"root_state", TString(TStringBuilder() << RootState)},
                {"has_quorum", HasQuorum()},
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
                        out << "Quorum: " << (HasQuorum() ? "yes" : "no") << "<br/>";
                    }
                }

                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        out << "Incoming bindings";
                    }
                    DIV_CLASS("panel-body") {
                        DIV() {
                            out << "AllBoundNodes count: " << AllBoundNodes.size();
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
            }

            contentType = NMon::TEvHttpInfoRes::Html;
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), ev->Get()->SubRequestId, contentType), 0, ev->Cookie);
    }

} // NKikimr::NStorage
