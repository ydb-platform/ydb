#include "vdisk_operation_broker.h"
#include "vdisk_log.h"

#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <util/string/builder.h>

#include <algorithm>
#include <list>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr {

    namespace {

        class TVDiskOperationBroker
            : public TActorBootstrapped<TVDiskOperationBroker>
        {
            using TThis = TVDiskOperationBroker;

            struct TEntry {
                TActorId VDiskServiceId;
                ui32 PDiskId = 0;
                std::unordered_set<TActorId> ActorIds;
            };

            TControlWrapper MaxInProgressCount;
            TControlWrapper MaxInProgressPerPDiskCount;
            std::unordered_map<TActorId, TEntry> Active;
            std::unordered_map<ui32, ui32> ActivePerPDisk;
            std::list<TEntry> WaitQueue;

            static TString JoinActorIds(const std::unordered_set<TActorId>& actorIds) {
                TStringBuilder sb;
                bool first = true;
                for (const auto& actorId : actorIds) {
                    if (!first) {
                        sb << ", ";
                    }
                    first = false;
                    sb << actorId;
                }
                return sb;
            }

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

            bool CanActivate(ui32 pdiskId) const {
                const ui64 nodeLimit = GetNodeLimit();
                if (nodeLimit && Active.size() >= nodeLimit) {
                    return false;
                }

                const ui64 perPDiskLimit = GetPerPDiskLimit();
                return !perPDiskLimit || GetActiveOnPDisk(pdiskId) < perPDiskLimit;
            }

            auto FindWaiting(const TActorId& vdiskServiceId) {
                return std::find_if(WaitQueue.begin(), WaitQueue.end(), [&vdiskServiceId](const auto& item) {
                    return item.VDiskServiceId == vdiskServiceId;
                });
            }

            void Activate(TEntry&& entry) {
                for (const auto& actorId : entry.ActorIds) {
                    this->Send(actorId, new TEvVDiskOperationToken);
                }
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
                const ui64 nodeLimit = GetNodeLimit();

                bool progress = true;
                while (progress && (!nodeLimit || Active.size() < nodeLimit)) {
                    progress = false;
                    for (auto it = WaitQueue.begin(); it != WaitQueue.end() && (!nodeLimit || Active.size() < nodeLimit); ) {
                        if (CanActivate(it->PDiskId)) {
                            auto entry = std::move(*it);
                            it = WaitQueue.erase(it);
                            Activate(std::move(entry));
                            progress = true;
                        } else {
                            ++it;
                        }
                    }
                }
            }

        public:
            static constexpr auto ActorActivityType() {
                return NKikimrServices::TActivity::NODE_WARDEN;
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvAcquireVDiskOperationToken, Handle)
                hFunc(TEvReleaseVDiskOperationToken, Handle)
                hFunc(TEvents::TEvWakeup, Handle)
                hFunc(NMon::TEvHttpInfo, Handle)
            )

            explicit TVDiskOperationBroker(const TControlWrapper& maxInProgressCount,
                    const TControlWrapper& maxInProgressPerPDiskCount)
                : MaxInProgressCount(maxInProgressCount)
                , MaxInProgressPerPDiskCount(maxInProgressPerPDiskCount)
            {}

            void Bootstrap() {
                this->Become(&TThis::StateFunc, TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
            }

            void Handle(TEvAcquireVDiskOperationToken::TPtr& ev) {
                const auto vdiskServiceId = ev->Get()->VDiskServiceId;
                const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(vdiskServiceId);
                Y_UNUSED(nodeId);
                Y_UNUSED(vslotId);
                const auto actorId = ev->Sender;

                if (const auto it = Active.find(vdiskServiceId); it != Active.end()) {
                    Y_ABORT_UNLESS(it->second.PDiskId == pdiskId);
                    it->second.ActorIds.insert(actorId);
                    this->Send(actorId, new TEvVDiskOperationToken);

                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_NODE,
                        "TEvAcquireVDiskOperationToken"
                        << ", broker service id: " << this->SelfId()
                        << ", VDisk service id: " << vdiskServiceId
                        << ", PDiskId: " << pdiskId
                        << ", actor id: " << actorId
                        << ", token sent, active: " << Active.size()
                        << ", active on PDisk: " << GetActiveOnPDisk(pdiskId)
                        << ", waiting: " << WaitQueue.size());
                    return;
                }

                if (CanActivate(pdiskId)) {
                    TEntry entry{vdiskServiceId, pdiskId, {actorId}};
                    Activate(std::move(entry));

                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_NODE,
                        "TEvAcquireVDiskOperationToken"
                        << ", broker service id: " << this->SelfId()
                        << ", VDisk service id: " << vdiskServiceId
                        << ", PDiskId: " << pdiskId
                        << ", actor id: " << actorId
                        << ", token sent, active: " << Active.size()
                        << ", active on PDisk: " << GetActiveOnPDisk(pdiskId)
                        << ", waiting: " << WaitQueue.size());
                    return;
                }

                if (const auto it = FindWaiting(vdiskServiceId); it != WaitQueue.end()) {
                    Y_ABORT_UNLESS(it->PDiskId == pdiskId);
                    it->ActorIds.insert(actorId);

                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_NODE,
                        "TEvAcquireVDiskOperationToken"
                        << ", broker service id: " << this->SelfId()
                        << ", VDisk service id: " << vdiskServiceId
                        << ", PDiskId: " << pdiskId
                        << ", actor id: " << actorId
                        << ", enqueued, active: " << Active.size()
                        << ", active on PDisk: " << GetActiveOnPDisk(pdiskId)
                        << ", waiting: " << WaitQueue.size());
                    return;
                }

                TEntry item{vdiskServiceId, pdiskId, {actorId}};
                WaitQueue.emplace_back(std::move(item));

                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_NODE,
                    "TEvAcquireVDiskOperationToken"
                    << ", broker service id: " << this->SelfId()
                    << ", VDisk service id: " << vdiskServiceId
                    << ", PDiskId: " << pdiskId
                    << ", actor id: " << actorId
                    << ", enqueued, active: " << Active.size()
                    << ", active on PDisk: " << GetActiveOnPDisk(pdiskId)
                    << ", waiting: " << WaitQueue.size());
            }

            void Handle(TEvReleaseVDiskOperationToken::TPtr& ev) {
                const auto vdiskServiceId = ev->Get()->VDiskServiceId;
                const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(vdiskServiceId);
                Y_UNUSED(nodeId);
                Y_UNUSED(vslotId);
                const auto actorId = ev->Sender;

                if (const auto it = Active.find(vdiskServiceId); it != Active.end()) {
                    Y_ABORT_UNLESS(it->second.PDiskId == pdiskId);
                    it->second.ActorIds.erase(actorId);
                    if (it->second.ActorIds.empty()) {
                        Deactivate(it);
                        ProcessQueue();
                    }

                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_NODE,
                        "TEvReleaseVDiskOperationToken"
                        << ", broker service id: " << this->SelfId()
                        << ", VDisk service id: " << vdiskServiceId
                        << ", PDiskId: " << pdiskId
                        << ", actor id: " << actorId
                        << ", token released, active: " << Active.size()
                        << ", active on PDisk: " << GetActiveOnPDisk(pdiskId)
                        << ", waiting: " << WaitQueue.size());
                    return;
                }

                if (const auto it = FindWaiting(vdiskServiceId); it != WaitQueue.end()) {
                    Y_ABORT_UNLESS(it->PDiskId == pdiskId);
                    it->ActorIds.erase(actorId);
                    if (it->ActorIds.empty()) {
                        WaitQueue.erase(it);
                    }

                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_NODE,
                        "TEvReleaseVDiskOperationToken"
                        << ", broker service id: " << this->SelfId()
                        << ", VDisk service id: " << vdiskServiceId
                        << ", PDiskId: " << pdiskId
                        << ", actor id: " << actorId
                        << ", removed from queue, active: " << Active.size()
                        << ", active on PDisk: " << GetActiveOnPDisk(pdiskId)
                        << ", waiting: " << WaitQueue.size());
                }
            }

            void Handle(TEvents::TEvWakeup::TPtr&) {
                ProcessQueue();
                this->Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev) {
                TStringStream str;
                TString nodeLimit = GetNodeLimit() ? ToString(GetNodeLimit()) : "unlimited";
                TString perPDiskLimit = GetPerPDiskLimit() ? ToString(GetPerPDiskLimit()) : "unlimited";
                HTML(str) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-body") {
                            str << "Broker Service Id: " << this->SelfId() << "<br>";
                            str << "Node Limit: " << nodeLimit << "<br>";
                            str << "Per-PDisk Limit: " << perPDiskLimit << "<br>";
                            str << "Active VDisks: " << Active.size() << "<br>";
                            str << "Waiting VDisks: " << WaitQueue.size();
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
                                            TABLEH() {str << "ActorIds";}
                                        }
                                    }
                                    TABLEBODY() {
                                        for (const auto& [vdiskServiceId, entry] : Active) {
                                            TABLER() {
                                                TABLED() {str << vdiskServiceId;}
                                                TABLED() { RenderActorPageLink(str, MakeVDiskActorPageUrl(vdiskServiceId), "page"); }
                                                TABLED() { RenderActorPageLink(str, MakePDiskActorPageUrl(vdiskServiceId.NodeId(), entry.PDiskId), "page"); }
                                                TABLED() {str << entry.PDiskId;}
                                                TABLED() {str << JoinActorIds(entry.ActorIds);}
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
                                            TABLEH() {str << "ActorIds";}
                                        }
                                    }
                                    TABLEBODY() {
                                        for (const auto& item : WaitQueue) {
                                            TABLER() {
                                                TABLED() {str << item.VDiskServiceId;}
                                                TABLED() { RenderActorPageLink(str, MakeVDiskActorPageUrl(item.VDiskServiceId), "page"); }
                                                TABLED() { RenderActorPageLink(str, MakePDiskActorPageUrl(item.VDiskServiceId.NodeId(), item.PDiskId), "page"); }
                                                TABLED() {str << item.PDiskId;}
                                                TABLED() {str << JoinActorIds(item.ActorIds);}
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
