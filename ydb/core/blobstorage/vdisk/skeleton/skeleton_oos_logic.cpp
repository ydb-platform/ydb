#include "skeleton_oos_logic.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_SKELETON

namespace NKikimr {

    class TOutOfSpaceLogic::TStat {
    public:
        struct TCell {
            ui64 AllowedMsgs = 0;
            ui64 NotAllowedMsgs = 0;
            ui64 AllowedBytes = 0;
            ui64 NotAllowedBytes = 0;
            ui64 TmpByteSize = 0;

            bool Allow() {
                ++AllowedMsgs;
                AllowedBytes += TmpByteSize;
                return true;
            }

            bool NotAllow() {
                ++NotAllowedMsgs;
                NotAllowedBytes += TmpByteSize;
                return false;
            }

            bool Pass(bool allow) {
                return allow ? Allow() : NotAllow();
            }

            TCell &HandleMsg(ui64 byteSize) {
                TmpByteSize = byteSize;
                return *this;
            }
        };

        enum EMsgType {
            UserPut = 0,
            SystemPut = 1,
            Block = 2,
            CollectGarbage = 3,
            LocalSyncData = 4,
            AnubisOsirisPut = 5,
            RecoveredHugeBlob = 6,
            DetectedPhantomBlob = 7,
            Count
        };

        static const char *MsgTypeToStr(EMsgType msgType) {
            switch (msgType) {
                case UserPut:               return "UserPut";
                case SystemPut:             return "SystemPut";
                case Block:                 return "Block";
                case CollectGarbage:        return "CollectGarbage";
                case LocalSyncData:         return "LocalSyncData";
                case AnubisOsirisPut:       return "AnubisOsirisPut";
                case RecoveredHugeBlob:     return "RecoveredHugeBlob";
                case DetectedPhantomBlob:   return "DetectedPhantomBlob";
                case Count:                 Y_ABORT();
            }
        }

        mutable THashMap<ui64, TCell> Stat[EMsgType::Count];

        TCell &Lookup(EMsgType msgType, ESpaceColor color) {
            return Stat[msgType][static_cast<ui64>(color)];
        }

        void RenderHtml(IOutputStream &str, const char *tableName, std::function<ui64(const TCell&)> &&func) const {
            HTML(str) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << tableName;
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS ("table table-condensed") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {str << "Message";}
                                    for (int j = 0; j < TSpaceColor::E_descriptor()->value_count(); ++j) {
                                        auto color = TSpaceColor::E_descriptor()->value(j)->name();
                                        TABLEH() {str << color;}
                                    }
                                }
                            }
                            TABLEBODY() {
                                for (int i = 0; i < EMsgType::Count; ++i) {
                                    TABLER() {
                                        auto msgType = (EMsgType)i;
                                        TABLED() {str << MsgTypeToStr(msgType);}
                                        for (int j = 0; j < TSpaceColor::E_descriptor()->value_count(); ++j) {
                                            auto color = TSpaceColor::E_descriptor()->value(j)->number();
                                            TABLED() {
                                                str << func(Stat[i][color]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        void RenderHtml(IOutputStream &str) const {
            RenderHtml(str, "Not Allowed Messages", [] (const TCell& c) { return c.NotAllowedMsgs; });
            RenderHtml(str, "Not Allowed Bytes", [] (const TCell& c) { return c.NotAllowedBytes; });
        }
    };

    /*

     Zone     |  Description
    ===============================================================================================
     Green    |  No restrictions.
    -----------------------------------------------------------------------------------------------
     Yellow   |  No restrictions, translate Yellow color to tablet and other VDisks in the group.
    -----------------------------------------------------------------------------------------------
     Orange   |  Disk space for user data is over. Tablet can boot (i.e. make TEvVPut for discovery
              |  with IgnoreBlock, block generation, delete data via garbage collection commands).
              |  Other VDisks in the group don't accept ordinary TEvVPuts also. System data (see
              |  TDataKind) is still accepted here, so that the tenant keeps running and the admin
              |  is able to drop tables.
    -----------------------------------------------------------------------------------------------
     Red      |  Disk space for system data is over as well. Tablet or someone else can only delete
              |  tablet's data; system tablets can still boot and block generations.
    -----------------------------------------------------------------------------------------------
     Black    |  Manual intervention required.

    */

    TOutOfSpaceLogic::TOutOfSpaceLogic(TIntrusivePtr<TVDiskContext> vctx, std::shared_ptr<THull> hull)
        : VCtx(std::move(vctx))
        , Hull(std::move(hull))
        , Stat(new TStat)
    {}

    TOutOfSpaceLogic::~TOutOfSpaceLogic() {}

    // Local gate for a blob put: USER must not walk this disk into ORANGE,
    // SYSTEM must not walk it into RED. `color` is the projected local color.
    bool TOutOfSpaceLogic::AllowByLocalColor(ESpaceColor color, bool system, bool unavoidable) {
        switch (color) {
            case TSpaceColor::GREEN:
            case TSpaceColor::CYAN:
            case TSpaceColor::LIGHT_YELLOW:
            case TSpaceColor::YELLOW:
            case TSpaceColor::LIGHT_ORANGE:
                return true;

            case TSpaceColor::PRE_ORANGE:
            case TSpaceColor::ORANGE:
                return system || unavoidable;

            case TSpaceColor::RED:
                return system && unavoidable;

            case TSpaceColor::BLACK:
                return false;

            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    // Neighbors: current color only, never projected. USER stops when any disk is
    // already in the orange zone; SYSTEM is not held back by a peer.
    bool TOutOfSpaceLogic::AllowByGlobalColor(ESpaceColor color, bool system, bool unavoidable) {
        if (system) {
            return true;
        }

        switch (color) {
            case TSpaceColor::GREEN:
            case TSpaceColor::CYAN:
            case TSpaceColor::LIGHT_YELLOW:
            case TSpaceColor::YELLOW:
            case TSpaceColor::LIGHT_ORANGE:
                return true;

            case TSpaceColor::PRE_ORANGE:
            case TSpaceColor::ORANGE:
                return unavoidable;

            case TSpaceColor::RED:
            case TSpaceColor::BLACK:
                return false;

            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    bool TOutOfSpaceLogic::AllowVPutLikeWrite(const TActorContext& /*ctx*/, bool ignoreBlock, bool isZeroEntry, ui32 size,
            NKikimrBlobStorage::TDataKind::E dataKind, ui64 freshChunks) const {
        // Restore-first reads and garbage collection zero entries: tiny writes a tablet cannot avoid
        // and the only way out of an out-of-space state, so they outlive the ordinary writes of the
        // same kind by one color, and are judged by the color the disk is in now.
        const bool unavoidable = ignoreBlock || isZeroEntry;
        const bool system = dataKind == NKikimrBlobStorage::TDataKind::SYSTEM;

        auto& oos = VCtx->GetOutOfSpaceState();
        const ESpaceColor local = unavoidable
            ? oos.GetLocalColor()
            : oos.GetSpaceHeadroom().Project(freshChunks, oos.GetLocalColor());
        const ESpaceColor global = oos.GetGlobalColor();

        auto& stat = Stat->Lookup(system ? TStat::SystemPut : TStat::UserPut, Max(local, global)).HandleMsg(size);
        return stat.Pass(AllowByLocalColor(local, system, unavoidable)
                && AllowByGlobalColor(global, system, unavoidable));
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& ctx, TEvBlobStorage::TEvVPut::TPtr &ev, ui64 freshChunks) const {
        auto& record = ev->Get()->Record;
        return AllowVPutLikeWrite(ctx, record.GetIgnoreBlock(), record.GetIsZeroEntry(), ev->Get()->GetBufferBytes(),
            record.GetDataKind(), freshChunks);
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvBlobStorage::TEvVBlock::TPtr &ev, bool hasExistingEntry) const {
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::Block, color).HandleMsg(ev->Get()->GetCachedByteSize());
        switch (color) {
            case TSpaceColor::GREEN:
            case TSpaceColor::CYAN:
            case TSpaceColor::LIGHT_YELLOW:
            case TSpaceColor::YELLOW:
            case TSpaceColor::LIGHT_ORANGE:
            case TSpaceColor::PRE_ORANGE:
            case TSpaceColor::ORANGE:
                return stat.Allow();
            case TSpaceColor::RED: {
                // FIXME: handle complete removal only
                return stat.Pass(hasExistingEntry);
            }
            case TSpaceColor::BLACK:
                return stat.NotAllow();
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvBlobStorage::TEvVCollectGarbage::TPtr &ev) const {
        // Garbage collection is the only thing that gives chunks back, so it outlives
        // the ordinary writes by one color and keeps running in RED, where a tablet
        // may still delete its data. Only BLACK, which asks for manual intervention,
        // refuses it. The barrier record it writes is tiny next to what it frees.
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::CollectGarbage, color).HandleMsg(ev->Get()->GetCachedByteSize());
        return stat.Pass(color <= TSpaceColor::RED);
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvLocalSyncData::TPtr &ev) const {
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::LocalSyncData, color).HandleMsg(ev->Get()->ByteSize());
        return stat.Pass(DefaultAllow(color));
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext &ctx, TEvAnubisOsirisPut::TPtr &ev) const {
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::AnubisOsirisPut, color).HandleMsg(ev->Get()->ByteSize());
        switch (color) {
            case TSpaceColor::GREEN:
            case TSpaceColor::CYAN:
            case TSpaceColor::LIGHT_YELLOW:
            case TSpaceColor::YELLOW:
            case TSpaceColor::LIGHT_ORANGE:
                return stat.Allow();
            case TSpaceColor::PRE_ORANGE:
            case TSpaceColor::ORANGE:
            {
                TEvAnubisOsirisPut *msg = ev->Get();
                if (msg->IsAnubis()) {
                    YDB_LOG_ERROR_CTX(ctx, "OUT OF SPACE while removing LogoBlob we got from Anubis;",
                        {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
                        {"logoBlobId", msg->LogoBlobId},
                        {"marker", "BSVSOOSL01"});
                    return stat.NotAllow();
                } else {
                    // We MUST allow Osiris writes. W/o Osiris we can't work.
                    // There should not be too much of them.
                    YDB_LOG_ERROR_CTX(ctx, "OUT OF SPACE while adding resurrected by Osiris LogoBlob; FORCING addition",
                        {"VDiskLogPrefix", VCtx->VDiskLogPrefix},
                        {"logoBlobId", msg->LogoBlobId},
                        {"marker", "BSVSOOSL02"});
                    return stat.Allow();
                }
            }
            case TSpaceColor::RED:
            case TSpaceColor::BLACK:
                return stat.NotAllow();
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvRecoveredHugeBlob::TPtr &ev) const {
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::RecoveredHugeBlob, color).HandleMsg(ev->Get()->ByteSize());
        return stat.Pass(DefaultAllow(color));
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvDetectedPhantomBlob::TPtr &ev) const {
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::DetectedPhantomBlob, color).HandleMsg(ev->Get()->ByteSize());
        return stat.Pass(DefaultAllow(color));
    }

    void TOutOfSpaceLogic::RenderHtml(IOutputStream &str) const {
        Stat->RenderHtml(str);
    }

    bool TOutOfSpaceLogic::DefaultAllow(ESpaceColor color) const {
        return color <= TSpaceColor::ORANGE;
    }

    ESpaceColor TOutOfSpaceLogic::GetSpaceColor() const {
        auto& oos = VCtx->GetOutOfSpaceState();
        const ESpaceColor global = oos.GetGlobalColor();
        return global >= TSpaceColor::ORANGE ? global : oos.GetLocalColor();
    }

} // NKikimr

