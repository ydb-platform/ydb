#include "volatile_tx.h"
#include "datashard_impl.h"
#include <ydb/core/change_exchange/change_sender_monitoring.h>
#include <library/cpp/resource/resource.h>
#include <util/string/cast.h>

namespace NKikimr::NDataShard {

    void TDataShard::HandleMonVolatileTxs(NMon::TEvRemoteHttpInfo::TPtr& ev) {
        using namespace NChangeExchange;

        const auto& cgi = ev->Get()->Cgi();
        if (const auto& str = cgi.Get("TxId")) {
            ui64 txId;
            if (!TryFromString(str, txId)) {
                Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
                return;
            }
            HandleMonVolatileTxs(ev, txId);
            return;
        }

        size_t limit = 1000;
        if (const auto& str = cgi.Get("limit")) {
            TryFromString(str, limit);
        }

        TStringStream html;

        HTML(html) {
            DIV_CLASS("page-header") {
                TAG(TH3) {
                    html << "Volatile Transactions";
                    SMALL() {
                        html << "&nbsp;";
                        html << "<a href=\"app?TabletID=" << TabletID() << "\">";
                        html << "Back to tablet " << TabletID();
                        html << "</a>";
                    }
                }
            }

            TABLE_CLASS("table table-sortable") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { html << "TxId"; }
                        TABLEH() { html << "State"; }
                        TABLEH() { html << "Version"; }
                        TABLEH() { html << "CommitOrder"; }
                        TABLEH() { html << "Participants"; }
                        TABLEH() { html << "Dependencies"; }
                        TABLEH() { html << "Flags"; }
                    }
                }
                TABLEBODY() {
                    std::vector<ui64> txIds;
                    for (const auto& pr : VolatileTxManager.VolatileTxs) {
                        txIds.push_back(pr.first);
                    }
                    std::sort(txIds.begin(), txIds.end());
                    if (txIds.size() > limit) {
                        txIds.resize(limit);
                    }
                    for (ui64 txId : txIds) {
                        const auto& info = VolatileTxManager.VolatileTxs.at(txId);
                        TABLER() {
                            TABLED() {
                                html << "<a href=\"app?TabletID=" << TabletID()
                                    << "&page=volatile-txs&TxId=" << txId << "\">"
                                    << txId << "</a>";
                            }
                            TABLED() { html << info->State; }
                            TABLED() { html << info->Version; }
                            TABLED() { html << info->CommitOrder; }
                            TABLED() { html << info->Participants.size(); }
                            TABLED() { html << info->Dependencies.size(); }
                            TABLED() {
                                bool empty = true;
                                if (info->CommitOrdered) {
                                    html << "O";
                                    empty = false;
                                }
                                if (info->IsArbiter) {
                                    html << "A";
                                    empty = false;
                                }
                                if (empty) {
                                    html << "&nbsp;";
                                }
                            }
                        }
                    }
                }
            }
        }

        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
    }

    void TDataShard::HandleMonVolatileTxs(NMon::TEvRemoteHttpInfo::TPtr& ev, ui64 txId) {
        auto it = VolatileTxManager.VolatileTxs.find(txId);
        if (it == VolatileTxManager.VolatileTxs.end()) {
            Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPNOTFOUND));
            return;
        }

        const auto& info = it->second;

        TStringStream html;

        auto containerString = [](const auto& v, const auto& elemConv) -> TString {
            TStringBuilder b;
            b << "[";
            size_t i = 0;
            for (const auto& e : v) {
                if (i != 0) {
                    b << ", ";
                }
                b << elemConv(e);
                ++i;
            }
            b << "]";
            return std::move(b);
        };

        auto linkToTxId = [&](ui64 txId) -> TString {
            return TStringBuilder() << "<a href=\"app?TabletID=" << TabletID()
                << "&page=volatile-txs&TxId=" << txId << "\">" << txId << "</a>";
        };

        auto linkToTabletId = [&](ui64 tabletId) -> TString {
            return TStringBuilder() << "<a href=\"../tablets?TabletID=" << tabletId << "\">" << tabletId << "</a>";
        };

        HTML(html) {
            DIV_CLASS("page-header") {
                TAG(TH3) {
                    html << "Volatile Transaction " << txId;
                    SMALL() {
                        html << "&nbsp;";
                        html << "<a href=\"app?TabletID=" << TabletID() << "&page=volatile-txs\">";
                        html << "Back to volatile transactions";
                        html << "</a>";
                        html << "&nbsp;";
                        html << "<a href=\"app?TabletID=" << TabletID() << "\">";
                        html << "Back to tablet " << TabletID();
                        html << "</a>";
                    }
                }
            }

            PRE() {
                html << "TxId:                     " << info->TxId
                    << (info->AddCommitted ? "" : " (not persisted yet)") << "\n";
                html << "State:                    " << info->State << "\n";
                html << "Version:                  " << info->Version << "\n";
                html << "CommitOrder:              " << info->CommitOrder
                    << (info->CommitOrdered ? " (ordered)" : "") << "\n";
                html << "CommitTxIds:              " << containerString(info->CommitTxIds, std::identity{}) << "\n";
                html << "Dependencies:             " << containerString(info->Dependencies, linkToTxId) << "\n";
                html << "Dependents:               " << containerString(info->Dependents, linkToTxId) << "\n";
                html << "Participants:             " << containerString(info->Participants, linkToTabletId) << "\n";
                if (info->ChangeGroup) {
                    html << "ChangeGroup:              " << *info->ChangeGroup << "\n";
                }
                html << "BlockedOperations:        " << containerString(info->BlockedOperations, std::identity{}) << "\n";
                html << "WaitingRemovalOperations: " << containerString(info->WaitingRemovalOperations, std::identity{}) << "\n";
                html << "Callbacks:                " << info->Callbacks.size() << "\n";
                html << "\n";
                html << "DelayedAcks:              " << info->DelayedAcks.size() << "\n";
                html << "DelayedConfirmations:     " << containerString(info->DelayedConfirmations, linkToTabletId) << "\n";
                html << "\n";
                html << "IsArbiter:                " << (info->IsArbiter ? "true" : "false") << "\n";
                html << "IsArbiterOnHold:          " << (info->IsArbiterOnHold ? "true" : "false") << "\n";
                if (info->IsArbiterOnHold) {
                    html << "ArbiterReadSets:          " << containerString(info->ArbiterReadSets, std::identity{}) << "\n";
                }
                html << "Latency:                  " << TDuration::Seconds(1) * info->LatencyTimer.Passed() << "\n";
            }
        }

        Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(html.Str()));
    }

} // namespace NKikimr::NDataShard
