#pragma once

#include "defs.h"
#include "scheme.h"
#include "types.h"

namespace NKikimr::NBsController {

    template<typename Traits>
    struct TErrorParam {
        const typename Traits::Type Value;

        template<typename T>
        TErrorParam(T&& value)
            : Value(std::forward<T>(value))
        {}
    };

    struct TErrorParams {
#define P(NAME, TYPE) \
            struct T##NAME##Traits { \
                using Type = TYPE; \
                static constexpr const char *GetName() { return #NAME; } \
                static void SetProto(NKikimrBlobStorage::TConfigResponse::TStatus::TFailParam& p, const Type& value) { p.Set##NAME(value); } \
            }; \
            using NAME = TErrorParam<T##NAME##Traits>;
        P(Fqdn, TString)
        P(IcPort, i32)
        P(NodeId, ui32)
        P(PDiskId, ui32)
        P(Path, TString)
        P(HostConfigId, ui64)
        P(BoxId, ui64)
        P(StoragePoolId, ui64)
        P(ItemConfigGenerationProvided, ui64)
        P(ItemConfigGenerationExpected, ui64)
        P(GroupId, ui32)
        P(StoragePoolName, TString)
        P(DiskSerialNumber, TString)

        struct TVDiskIdTraits {
            using Type = TVDiskID;
            static constexpr const char *GetName() { return "VDiskId"; }
            static void SetProto(NKikimrBlobStorage::TConfigResponse::TStatus::TFailParam& p, const Type& value) {
                VDiskIDFromVDiskID(value, p.MutableVDiskId());
            }
        };
        using VDiskId = TErrorParam<TVDiskIdTraits>;

        struct TVSlotIdTraits {
            using Type = TVSlotId;
            static constexpr const char *GetName() { return "VSlotId"; }
            static void SetProto(NKikimrBlobStorage::TConfigResponse::TStatus::TFailParam& p, const Type& value) {
                value.Serialize(p.MutableVSlotId());
            }
        };
        using VSlotId = TErrorParam<TVSlotIdTraits>;
#undef P
    };

    struct TExError : yexception {
        mutable std::deque<NKikimrBlobStorage::TConfigResponse::TStatus::TFailParam> FailParams;

        void FillInStatus(NKikimrBlobStorage::TConfigResponse::TStatus& status) const {
            status.SetErrorDescription(what());
            status.SetFailReason(GetFailReason());
            for (const auto& p : FailParams) {
                auto *xp = status.AddFailParam();
                xp->CopyFrom(p);
            }
        }

        virtual NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kGeneric;
        }

        template<typename Traits>
        void Append(const TErrorParam<Traits>& p) {
            Traits::SetProto(*FailParams.emplace(FailParams.end()), p.Value);
            TStringStream s;
            s << " " << Traits::GetName() << "# " << p.Value;
            *this << s.Str();
        }

        template<typename T>
        void Append(const T& x) {
            yexception::Append(x);
        }
    };

    struct TExHostNotFound : TExError {
        TExHostNotFound(const NKikimrBlobStorage::THostKey& hostKey) {
            *this << "Host not found";
            if (hostKey.GetFqdn() || hostKey.GetIcPort()) {
                *this << TErrorParams::Fqdn(hostKey.GetFqdn()) << TErrorParams::IcPort(hostKey.GetIcPort());
            }
            if (hostKey.GetNodeId()) {
                *this << TErrorParams::NodeId(hostKey.GetNodeId());
            }
        }

        TExHostNotFound(const std::tuple<TString, i32>& hostKey) {
            *this << "Host not found"
                << TErrorParams::Fqdn(std::get<0>(hostKey))
                << TErrorParams::IcPort(std::get<1>(hostKey));
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kHostNotFound;
        }
    };

    struct TExPDiskNotFound : TExError {
        TExPDiskNotFound(const NKikimrBlobStorage::THostKey& hostKey, ui32 pdiskId, TString path) {
            *this << "PDisk not found"
                << TErrorParams::Fqdn(hostKey.GetFqdn())
                << TErrorParams::IcPort(hostKey.GetIcPort())
                << TErrorParams::NodeId(hostKey.GetNodeId())
                << TErrorParams::PDiskId(pdiskId)
                << TErrorParams::Path(path);
        }

        TExPDiskNotFound(ui32 nodeId, ui32 pdiskId) {
            *this << "PDisk not found"
                << TErrorParams::NodeId(nodeId)
                << TErrorParams::PDiskId(pdiskId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kPDiskNotFound;
        }
    };

    struct TExHostConfigNotFound : TExError {
        TExHostConfigNotFound(Schema::HostConfig::HostConfigId::Type hostConfigId) {
            *this << "HostConfig not found"
                << TErrorParams::HostConfigId(hostConfigId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kHostConfigNotFound;
        }
    };

    struct TExItemConfigGenerationMismatch : TExError {
        TExItemConfigGenerationMismatch(ui64 provided, ui64 expected) {
            *this << "ItemConfigGeneration mismatch"
                << TErrorParams::ItemConfigGenerationProvided(provided)
                << TErrorParams::ItemConfigGenerationExpected(expected);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kItemConfigGenerationMismatch;
        }
    };

    struct TExGroupNotFound : TExError {
        TExGroupNotFound(ui32 groupId) {
            *this << "Group not found"
                << TErrorParams::GroupId(groupId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kGroupNotFound;
        }
    };

    struct TExMayLoseData : TExError {
        TExMayLoseData(ui32 groupId) {
            *this << "Group may lose data"
                << TErrorParams::GroupId(groupId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kMayLoseData;
        }
    };

    struct TExMayGetDegraded : TExError {
        TExMayGetDegraded(ui32 groupId) {
            *this << "Group may become degraded"
                << TErrorParams::GroupId(groupId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kMayGetDegraded;
        }
    };

    struct TExVDiskIdIncorrect : TExError {
        TExVDiskIdIncorrect(const TVDiskID& vdiskId, const TVSlotId& vslotId) {
            *this << "VDiskId is incorrect for specified VSlotId" << TErrorParams::VDiskId(vdiskId)
                << TErrorParams::VSlotId(vslotId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kVDiskIdIncorrect;
        }
    };

    struct TExVSlotNotFound : TExError {
        TExVSlotNotFound(const TVSlotId& vslotId) {
            *this << "VSlot not found" << TErrorParams::VSlotId(vslotId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kVSlotNotFound;
        }
    };

    struct TExDiskIsNotDonor : TExError {
        TExDiskIsNotDonor(const TVSlotId& vslotId, const TVDiskID& vdiskId) {
            *this << "Disk is not in donor more" << TErrorParams::VSlotId(vslotId) << TErrorParams::VDiskId(vdiskId);
        }

        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kDiskIsNotDonor;
        }
    };

    struct TExAlready : TExError {
        NKikimrBlobStorage::TConfigResponse::TStatus::EFailReason GetFailReason() const override {
            return NKikimrBlobStorage::TConfigResponse::TStatus::kAlready;
        }
    };

} // NKikimr::NBsController
