#include "skeleton_oos_logic.h"
#include <ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.h>

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
            Put = 0,
            Block = 1,
            CollectGarbage = 2,
            LocalSyncData = 3,
            AnubisOsirisPut = 4,
            RecoveredHugeBlob = 5,
            DetectedPhantomBlob = 6,
            Count
        };

        static const char *MsgTypeToStr(EMsgType msgType) {
            switch (msgType) {
                case Put:                   return "Put";
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
     Orange   |  Disk space for tablet is over. Tablet can boot (i.e. make TEvVPut for discovery
              |  with IgnoreBlock, block generation, delete data via garbage collection commands).
              |  Other VDisks in the group don't accept ordinary TEvVPuts also.
    -----------------------------------------------------------------------------------------------
     Red      |  Tablet or someone else can only delete tablet's data.
    -----------------------------------------------------------------------------------------------
     Black    |  Manual intervention required.

    */

    TOutOfSpaceLogic::TOutOfSpaceLogic(TIntrusivePtr<TVDiskContext> vctx, std::shared_ptr<THull> hull)
        : VCtx(std::move(vctx))
        , Hull(std::move(hull))
        , Stat(new TStat)
    {}

    TOutOfSpaceLogic::~TOutOfSpaceLogic() {}

    template <typename TPutEventPtr>
    bool AllowPut(const TOutOfSpaceLogic &logic, ESpaceColor color, TPutEventPtr &ev) {
        auto &stat = logic.Stat->Lookup(TOutOfSpaceLogic::TStat::Put, color).HandleMsg(ev->Get()->GetCachedByteSize());
        switch (color) {
            case TSpaceColor::GREEN:
            case TSpaceColor::CYAN:
            case TSpaceColor::LIGHT_YELLOW:
            case TSpaceColor::YELLOW:
            case TSpaceColor::LIGHT_ORANGE:
                return stat.Allow();
            case TSpaceColor::PRE_ORANGE:
            case TSpaceColor::ORANGE:
                return stat.Pass(ev->Get()->Record.GetIgnoreBlock()); // allow restore-first reads to pass through
            case TSpaceColor::RED:
            case TSpaceColor::BLACK:
                return stat.NotAllow();
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvBlobStorage::TEvVPut::TPtr &ev) const {
        return AllowPut(*this, GetSpaceColor(), ev);
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvBlobStorage::TEvVMultiPut::TPtr &ev) const {
        return AllowPut(*this, GetSpaceColor(), ev);
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvBlobStorage::TEvVBlock::TPtr &ev) const {
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::Block, color).HandleMsg(ev->Get()->GetCachedByteSize());
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
                NKikimrBlobStorage::TEvVBlock &record = ev->Get()->Record;
                const ui64 tabletId = record.GetTabletId();
                const bool allow = Hull->HasBlockRecordFor(tabletId);
                return stat.Pass(allow);
            }
            case TSpaceColor::RED: {
                // FIXME: handle complete removal only
                return stat.NotAllow();
            }
            case TSpaceColor::BLACK:
                return stat.NotAllow();
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MIN_SENTINEL_DO_NOT_USE_:
            case NKikimrBlobStorage::TPDiskSpaceColor_E_TPDiskSpaceColor_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                Y_ABORT();
        }
    }

    bool TOutOfSpaceLogic::Allow(const TActorContext& /*ctx*/, TEvBlobStorage::TEvVCollectGarbage::TPtr &ev) const {
        // FIXME: accept hard barriers in red color
        const ESpaceColor color = GetSpaceColor();
        auto &stat = Stat->Lookup(TStat::CollectGarbage, color).HandleMsg(ev->Get()->GetCachedByteSize());
        return stat.Pass(DefaultAllow(color));
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
                    LOG_ERROR_S(ctx, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                            << "OUT OF SPACE while removing LogoBlob we got from Anubis;"
                            << " LogoBlobId# " << msg->LogoBlobId
                            << " Marker# BSVSOOSL01");
                    return stat.NotAllow();
                } else {
                    // We MUST allow Osiris writes. W/o Osiris we can't work.
                    // There should not be too much of them.
                    LOG_ERROR_S(ctx, NKikimrServices::BS_SKELETON, VCtx->VDiskLogPrefix
                            << "OUT OF SPACE while adding resurrected by Osiris LogoBlob;"
                            << " FORCING addition: LogoBlobId# " << msg->LogoBlobId
                            << " Marker# BSVSOOSL02");
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

