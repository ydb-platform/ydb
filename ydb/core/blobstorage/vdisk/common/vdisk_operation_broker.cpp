#include "vdisk_operation_broker.h"
#include "vdisk_log.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/builder.h>

#include <algorithm>
#include <list>
#include <optional>
#include <unordered_map>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_NODE

namespace NKikimr {

    namespace {

        class TVDiskOperationBroker
            : public TActorBootstrapped<TVDiskOperationBroker>
        {
            using TThis = TVDiskOperationBroker;

            struct TEntry {
                TActorId VDiskServiceId;
                ui32 PDiskId = 0;
                TActorId OwnerActorId;
            };

            TControlWrapper MaxInProgressCount;
            TControlWrapper MaxInProgressPerPDiskCount;
            std::unordered_map<TActorId, TEntry> Active;
            std::unordered_map<ui32, ui32> ActivePerPDisk;
            std::list<TEntry> WaitQueue;
            std::unordered_map<TActorId, std::list<TEntry>::iterator> WaitingIndex;
            bool ProcessQueueWakeupScheduled = false;

            static TString MakePDiskActorPageUrl(ui32 nodeId, ui32 pdiskId) {
                return Sprintf("/node/%" PRIu32 "/actors/pdisks/pdisk%09" PRIu32, nodeId, pdiskId);
            }

            static TString MakeVDiskActorPageUrl(const TActorId& vdiskServiceId) {
                const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(vdiskServiceId);
                return Sprintf("/node/%" PRIu32 "/actors/vdisks/vdisk%09" PRIu32 "_%09" PRIu32,
                    nodeId, pdiskId, vslotId);
            }

            static void RenderActorPageLink(IOutputStream& str, const TString& url, TStringBuf title) {
                str << "<a href=\"" << url << "\">" << title << "</a>";
            }

            static void RenderActorPageLink(IOutputStream& str, const TString& url, ui32 id) {
                str << "<a href=\"" << url << "\">" << id << "</a>";
            }

            ui64 GetNodeLimit() const {
                return static_cast<ui64>(MaxInProgressCount);
            }

            ui64 GetPerPDiskLimit() const {
                return static_cast<ui64>(MaxInProgressPerPDiskCount);
            }

            ui32 GetActiveOnPDisk(ui32 pdiskId) const {
                if (const auto it = ActivePerPDisk.find(pdiskId); it != ActivePerPDisk.end()) {
                    return it->second;
                }
                return 0;
            }

            ui32 GetLocalNodeIdForPages() const {
                if (!Active.empty()) {
                    return Active.begin()->first.NodeId();
                }
                if (!WaitQueue.empty()) {
                    return WaitQueue.front().VDiskServiceId.NodeId();
                }
                return 0;
            }

            TString BuildLogString(
                    const TActorId& vdiskServiceId = TActorId(),
                    std::optional<ui32> pdiskId = std::nullopt,
                    const TActorId& senderActorId = TActorId()) const {
                TStringBuilder str;
                str << "broker service id: " << this->SelfId()
                    << ", active: " << Active.size()
                    << ", waiting: " << WaitQueue.size();
                if (vdiskServiceId != TActorId()) {
                    str << ", VDisk service id: " << vdiskServiceId;
                }
                if (pdiskId) {
                    str << ", PDiskId: " << *pdiskId
                        << ", active on PDisk: " << GetActiveOnPDisk(*pdiskId);
                }
                if (senderActorId != TActorId()) {
                    str << ", sender actor id: " << senderActorId;
                }
                return str;
            }

            bool HasNodeCapacity() const {
                const ui64 nodeLimit = GetNodeLimit();
                return !nodeLimit || Active.size() < nodeLimit;
            }

            bool HasPDiskCapacity(ui32 pdiskId) const {
                const ui64 perPDiskLimit = GetPerPDiskLimit();
                return !perPDiskLimit || GetActiveOnPDisk(pdiskId) < perPDiskLimit;
            }

            bool CanActivate(ui32 pdiskId) const {
                return HasNodeCapacity() && HasPDiskCapacity(pdiskId);
            }

            auto FindWaiting(const TActorId& vdiskServiceId) {
                if (const auto it = WaitingIndex.find(vdiskServiceId); it != WaitingIndex.end()) {
                    return it->second;
                }
                return WaitQueue.end();
            }

            auto EnqueueWaiting(TEntry&& entry) {
                WaitQueue.emplace_back(std::move(entry));
                const auto it = std::prev(WaitQueue.end());
                WaitingIndex.emplace(it->VDiskServiceId, it);
                return it;
            }

            TEntry DequeueWaiting(std::list<TEntry>::iterator it) {
                TEntry entry = std::move(*it);
                WaitingIndex.erase(entry.VDiskServiceId);
                WaitQueue.erase(it);
                return entry;
            }

            void EraseWaiting(std::list<TEntry>::iterator it) {
                WaitingIndex.erase(it->VDiskServiceId);
                WaitQueue.erase(it);
            }

            void SendToken(const TEntry& entry) {
                this->Send(entry.OwnerActorId, new TEvVDiskOperationToken, IEventHandle::FlagTrackDelivery);
            }

            void Activate(TEntry&& entry) {
                SendToken(entry);
                ++ActivePerPDisk[entry.PDiskId];
                Active.emplace(entry.VDiskServiceId, std::move(entry));
            }

            void Deactivate(typename std::unordered_map<TActorId, TEntry>::iterator it) {
                const ui32 pdiskId = it->second.PDiskId;
                Active.erase(it);

                auto activeByPDisk = ActivePerPDisk.find(pdiskId);
                Y_ABORT_UNLESS(activeByPDisk != ActivePerPDisk.end() && activeByPDisk->second);
                if (--activeByPDisk->second == 0) {
                    ActivePerPDisk.erase(activeByPDisk);
                }
            }

            void ProcessQueue() {
                auto it = WaitQueue.begin();
                while (it != WaitQueue.end() && HasNodeCapacity()) {
                    if (HasPDiskCapacity(it->PDiskId)) {
                        auto current = it++;
                        auto entry = DequeueWaiting(current);
                        Activate(std::move(entry));
                    } else {
                        ++it;
                    }
                }
            }

            size_t GrantAllWaitingOnce() {
                size_t numGranted = 0;
                while (!WaitQueue.empty()) {
                    auto entry = DequeueWaiting(WaitQueue.begin());
                    Activate(std::move(entry));
                    ++numGranted;
                }

                YDB_LOG_WARN("GrantAllWaitingOnce, granted waiting",
                    {"logString", BuildLogString()},
                    {"VDisks", numGranted});
                return numGranted;
            }

            void ScheduleProcessQueueWakeup() {
                if (!ProcessQueueWakeupScheduled && !WaitQueue.empty()) {
                    this->Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
                    ProcessQueueWakeupScheduled = true;
                }
            }

        public:
            static constexpr auto ActorActivityType() {
                return NKikimrServices::TActivity::NODE_WARDEN;
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvAcquireVDiskOperationToken, Handle)
                hFunc(TEvReleaseVDiskOperationToken, Handle)
                hFunc(TEvents::TEvUndelivered, Handle)
                hFunc(TEvents::TEvWakeup, Handle)
                hFunc(NMon::TEvHttpInfo, Handle)
            )

            explicit TVDiskOperationBroker(const TControlWrapper& maxInProgressCount,
                    const TControlWrapper& maxInProgressPerPDiskCount)
                : MaxInProgressCount(maxInProgressCount)
                , MaxInProgressPerPDiskCount(maxInProgressPerPDiskCount)
            {}

            void Bootstrap() {
                this->Become(&TThis::StateFunc);
            }

            void Handle(TEvAcquireVDiskOperationToken::TPtr& ev) {
                const auto vdiskServiceId = ev->Get()->VDiskServiceId;
                const ui32 pdiskId = ev->Get()->PDiskId;
                const auto actorId = ev->Sender;

                if (const auto it = Active.find(vdiskServiceId); it != Active.end()) {
                    Y_ABORT_UNLESS(it->second.PDiskId == pdiskId);
                    if (it->second.OwnerActorId != actorId) {
                        YDB_LOG_WARN("TEvAcquireVDiskOperationToken, stale active owner actor, new owner actor, action: update active owner",
                            {"logString", BuildLogString(vdiskServiceId, pdiskId)},
                            {"id", it->second.OwnerActorId},
                            {"newId", actorId});
                        it->second.OwnerActorId = actorId;
                    }
                    SendToken(it->second);

                    YDB_LOG_DEBUG("TEvAcquireVDiskOperationToken, token sent",
                        {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                    return;
                }

                if (const auto it = FindWaiting(vdiskServiceId); it != WaitQueue.end()) {
                    Y_ABORT_UNLESS(it->PDiskId == pdiskId);
                    if (it->OwnerActorId != actorId) {
                        YDB_LOG_WARN("TEvAcquireVDiskOperationToken, stale waiting owner actor, new owner actor, action: update waiting owner",
                            {"logString", BuildLogString(vdiskServiceId, pdiskId)},
                            {"id", it->OwnerActorId},
                            {"newId", actorId});
                        it->OwnerActorId = actorId;
                    }

                    if (CanActivate(pdiskId)) {
                        auto entry = DequeueWaiting(it);
                        Activate(std::move(entry));

                        YDB_LOG_DEBUG("TEvAcquireVDiskOperationToken, queued request activated",
                            {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                        return;
                    }

                    YDB_LOG_DEBUG("TEvAcquireVDiskOperationToken, queued request still waiting",
                        {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                    ScheduleProcessQueueWakeup();
                    return;
                }

                if (CanActivate(pdiskId)) {
                    TEntry entry{vdiskServiceId, pdiskId, actorId};
                    Activate(std::move(entry));

                    YDB_LOG_DEBUG("TEvAcquireVDiskOperationToken, token sent",
                        {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                    return;
                }

                EnqueueWaiting(TEntry{vdiskServiceId, pdiskId, actorId});

                YDB_LOG_DEBUG("TEvAcquireVDiskOperationToken, enqueued",
                    {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                ScheduleProcessQueueWakeup();
            }

            void Handle(TEvReleaseVDiskOperationToken::TPtr& ev) {
                const auto vdiskServiceId = ev->Get()->VDiskServiceId;
                const ui32 pdiskId = ev->Get()->PDiskId;
                const auto actorId = ev->Sender;

                if (const auto it = Active.find(vdiskServiceId); it != Active.end()) {
                    Y_ABORT_UNLESS(it->second.PDiskId == pdiskId);
                    if (it->second.OwnerActorId != actorId) {
                        YDB_LOG_WARN("TEvReleaseVDiskOperationToken, stale active owner actor, current owner actor, action: ignore stale release",
                            {"logString", BuildLogString(vdiskServiceId, pdiskId)},
                            {"id", actorId},
                            {"ownerId", it->second.OwnerActorId});
                        return;
                    }
                    Deactivate(it);
                    ProcessQueue();
                    ScheduleProcessQueueWakeup();

                    YDB_LOG_DEBUG("TEvReleaseVDiskOperationToken, token released",
                        {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                    return;
                }

                if (const auto it = FindWaiting(vdiskServiceId); it != WaitQueue.end()) {
                    Y_ABORT_UNLESS(it->PDiskId == pdiskId);
                    if (it->OwnerActorId != actorId) {
                        YDB_LOG_WARN("TEvReleaseVDiskOperationToken, stale waiting owner actor, current queued owner actor, action: ignore stale release",
                            {"logString", BuildLogString(vdiskServiceId, pdiskId)},
                            {"id", actorId},
                            {"ownerId", it->OwnerActorId});
                        return;
                    }
                    EraseWaiting(it);

                    YDB_LOG_DEBUG("TEvReleaseVDiskOperationToken, removed from queue",
                        {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
                    return;
                }

                YDB_LOG_WARN("TEvReleaseVDiskOperationToken, action: ignore release for unknown VDisk",
                    {"logString", BuildLogString(vdiskServiceId, pdiskId, actorId)});
            }

            void Handle(TEvents::TEvUndelivered::TPtr& ev) {
                if (ev->Get()->SourceType != TEvVDiskOperationToken::EventType) {
                    return;
                }

                const auto actorId = ev->Sender;
                for (auto it = Active.begin(); it != Active.end(); ++it) {
                    if (it->second.OwnerActorId == actorId) {
                        YDB_LOG_WARN("TEvUndelivered, owner actor, source, action: drop active token owner and process queue",
                            {"logString", BuildLogString(it->second.VDiskServiceId, it->second.PDiskId)},
                            {"id", actorId},
                            {"type", ev->Get()->SourceType},
                            {"reason", ev->Get()->Reason});
                        Deactivate(it);
                        ProcessQueue();
                        ScheduleProcessQueueWakeup();
                        return;
                    }
                }
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                ProcessQueueWakeupScheduled = false;
                ProcessQueue();
                ScheduleProcessQueueWakeup();
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev) {
                const auto& request = ev->Get()->Request;
                const auto& cgi = request.GetParams();
                TString actionMessage;
                if (request.GetMethod() == HTTP_METHOD_POST) {
                    const auto& post = request.GetPostParams();
                    if (post.Get("broker_action") == "grant_all_waiting_once") {
                        const size_t numGranted = GrantAllWaitingOnce();
                        actionMessage = TStringBuilder()
                            << "Emergency action executed: granted tokens to " << numGranted << " waiting VDisk(s).";
                    }
                } else if (cgi.Get("broker_action") == "grant_all_waiting_once") {
                    actionMessage = "Emergency action was not executed: grant_all_waiting_once requires POST.";
                }

                TStringStream str;
                TString nodeLimit = GetNodeLimit() ? ToString(GetNodeLimit()) : "unlimited";
                TString perPDiskLimit = GetPerPDiskLimit() ? ToString(GetPerPDiskLimit()) : "unlimited";
                HTML(str) {
                    if (!actionMessage.empty()) {
                        DIV_CLASS("alert alert-warning") {
                            str << actionMessage;
                        }
                        str << "<script>"
                               "if (window.history && window.history.replaceState) {"
                               "const url = new URL(window.location.href);"
                               "url.searchParams.delete('broker_action');"
                               "window.history.replaceState({}, '', url.toString());"
                               "}"
                               "</script>";
                    }

                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-body") {
                            str << "Broker Service Id: " << this->SelfId() << "<br>";
                            str << "Node Limit: " << nodeLimit << "<br>";
                            str << "Per-PDisk Limit: " << perPDiskLimit << "<br>";
                            str << "Active VDisks: " << Active.size() << "<br>";
                            str << "Waiting VDisks: " << WaitQueue.size();
                        }

                        DIV_CLASS("panel-body") {
                            STRONG() { str << "Emergency actions"; }
                            str << "<br>";
                            str << "Use this only as a manual recovery tool when you suspect a broker bug or a stuck waiter.";
                            str << "<br><br>";
                            str << "<form method=\"POST\">";
                            str << "<input type=\"hidden\" name=\"broker_action\" value=\"grant_all_waiting_once\">";
                            str << "<button type=\"submit\" class=\"btn btn-warning\">Grant tokens to all waiting VDisks once</button>";
                            str << "</form>";
                        }

                        DIV_CLASS("panel-body") {
                            STRONG() {str << "Active per PDisk";}
                            str << "<br>";
                            if (ActivePerPDisk.empty()) {
                                str << "empty";
                            } else {
                                TABLE_CLASS("table table-condensed") {
                                    TABLEHEAD() {
                                        TABLER() {
                                            TABLEH() {str << "PDiskId";}
                                            TABLEH() {str << "Active VDisks";}
                                        }
                                    }
                                    TABLEBODY() {
                                        const ui32 nodeId = GetLocalNodeIdForPages();
                                        for (const auto& [pdiskId, count] : ActivePerPDisk) {
                                            TABLER() {
                                                TABLED() { RenderActorPageLink(str, MakePDiskActorPageUrl(nodeId, pdiskId), pdiskId); }
                                                TABLED() {str << count;}
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        DIV_CLASS("panel-body") {
                            STRONG() {str << "Active";}
                            str << "<br>";
                            if (Active.empty()) {
                                str << "empty";
                            } else {
                                TABLE_CLASS("table table-condensed") {
                                    TABLEHEAD() {
                                        TABLER() {
                                            TABLEH() {str << "VDiskServiceId";}
                                            TABLEH() {str << "VDisk page";}
                                            TABLEH() {str << "PDisk page";}
                                            TABLEH() {str << "PDiskId";}
                                            TABLEH() {str << "OwnerActorId";}
                                        }
                                    }
                                    TABLEBODY() {
                                        for (const auto& [vdiskServiceId, entry] : Active) {
                                            TABLER() {
                                                TABLED() {str << vdiskServiceId;}
                                                TABLED() { RenderActorPageLink(str, MakeVDiskActorPageUrl(vdiskServiceId), "page"); }
                                                TABLED() { RenderActorPageLink(str, MakePDiskActorPageUrl(vdiskServiceId.NodeId(), entry.PDiskId), "page"); }
                                                TABLED() {str << entry.PDiskId;}
                                                TABLED() {str << entry.OwnerActorId;}
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        DIV_CLASS("panel-body") {
                            STRONG() {str << "Waiting";}
                            str << "<br>";
                            if (WaitQueue.empty()) {
                                str << "empty";
                            } else {
                                TABLE_CLASS("table table-condensed") {
                                    TABLEHEAD() {
                                        TABLER() {
                                            TABLEH() {str << "VDiskServiceId";}
                                            TABLEH() {str << "VDisk page";}
                                            TABLEH() {str << "PDisk page";}
                                            TABLEH() {str << "PDiskId";}
                                            TABLEH() {str << "OwnerActorId";}
                                        }
                                    }
                                    TABLEBODY() {
                                        for (const auto& item : WaitQueue) {
                                            TABLER() {
                                                TABLED() {str << item.VDiskServiceId;}
                                                TABLED() { RenderActorPageLink(str, MakeVDiskActorPageUrl(item.VDiskServiceId), "page"); }
                                                TABLED() { RenderActorPageLink(str, MakePDiskActorPageUrl(item.VDiskServiceId.NodeId(), item.PDiskId), "page"); }
                                                TABLED() {str << item.PDiskId;}
                                                TABLED() {str << item.OwnerActorId;}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                this->Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
            }
        };

    } // anonymous namespace

    IActor* CreateVDiskOperationBrokerActor(const TControlWrapper& maxInProgressCount,
            const TControlWrapper& maxInProgressPerPDiskCount) {
        return new TVDiskOperationBroker(maxInProgressCount, maxInProgressPerPDiskCount);
    }

} // NKikimr
