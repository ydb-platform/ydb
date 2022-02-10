#pragma once

#include "flat_row_eggs.h"

namespace NKikimr {
namespace NTable {
namespace NRedo {

    enum class ERedo : ui8 {
        Noop    = 0,
        Update  = 1,
        Erase   = 2,    /* Legacy ERowOp::Erase operation */
        Begin   = 5,    /* ABI and API evolution label  */
        Annex   = 6,    /* Array of used external blobs */
        Flush   = 8,
        UpdateV = 9,
        UpdateTx = 10,
        CommitTx = 11,
        RemoveTx = 12,
    };

    #pragma pack(push, 1)

    /*_ Legacy, ugly, log protocol */

    struct TChunk_Legacy {
        ui32 RootId;
        ui32 Size;
        ERedo Op;
    } Y_PACKED;

    struct TEvUpdate_Legacy {
        TChunk_Legacy OpHeader;
        ui32 Keys;
        ui32 Ops;
    } Y_PACKED;

    struct TEvErase_Legacy {
        TChunk_Legacy OpHeader;
        ui32 Keys;
    } Y_PACKED;

    struct TEvFlush_Legacy {
        TChunk_Legacy Hdr;

        ui64 TxStamp;
        i64 Epoch;
        ui64 Unused1_;
    } Y_PACKED;

    /*_ New generic, TLV based, log */

    struct TChunk {
        ERedo Event;
        ui8 Pad0_;
        ui16 Pad1_;
        ui32 Size;
    } Y_PACKED;

    struct TEvBegin_v0 {
        TChunk Label;

        ui32 Tail;
        ui32 Head;
    } Y_PACKED;

    struct TEvBegin_v1 {
        TChunk Label;

        ui32 Tail;
        ui32 Head;
        ui64 Serial;
        ui64 Stamp;
    } Y_PACKED;

    struct TEvAnnex {
        TChunk Label;
        ui32 Items; /* Followed by array of NPageCollection::TGlobId */
    } Y_PACKED;

    struct TEvUpdate {
        TChunk Label;

        ui32 Table;
        ERowOp Rop;
        ui8 Pad0_;
        ui16 Keys;
        ui16 Ops;
    } Y_PACKED;

    struct TEvUpdateV {
        ui64 RowVersionStep;
        ui64 RowVersionTxId;
    } Y_PACKED;

    struct TEvUpdateTx {
        ui64 TxId;
    } Y_PACKED;

    struct TEvFlush {
        TChunk Label;

        ui32 Table;
        ui32 Pad0_;
        ui64 Stamp;
        i64 Epoch;
    } Y_PACKED;

    struct TEvRemoveTx {
        TChunk Label;

        ui32 Table;
        ui32 Pad0_;
        ui64 TxId;
    } Y_PACKED;

    struct TEvCommitTx {
        TChunk Label;

        ui32 Table;
        ui32 Pad0_;
        ui64 TxId;
        ui64 RowVersionStep;
        ui64 RowVersionTxId;
    } Y_PACKED;

    struct TValue {
        bool IsNull() const noexcept
        {
            return Size == 0 && TypeId == 0;
        }

        ui16 TypeId;
        ui32 Size;
    } Y_PACKED;

    struct TUpdate {
        NTable::TCellOp CellOp;
        NTable::TTag Tag;
        TValue Val;
    } Y_PACKED;

    #pragma pack(pop)

    static_assert(sizeof(TChunk) == 8, "");
    static_assert(sizeof(TEvBegin_v0) == 16, "");
    static_assert(sizeof(TEvBegin_v1) == 32, "");
    static_assert(sizeof(TEvUpdate) == 18, "");
    static_assert(sizeof(TEvUpdateV) == 16, "");
    static_assert(sizeof(TEvAnnex) == 12, "");
    static_assert(sizeof(TEvFlush) == 32, "");

}
}
}
