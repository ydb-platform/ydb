#include "blobstorage_hullhugedefs.h"
#include <util/system/unaligned_mem.h>

namespace NKikimr {

    namespace NHuge {

        const ui32 THugeSlot::SerializedSize = 3u * sizeof(ui32);

        ////////////////////////////////////////////////////////////////////////////
        // TFreeRes
        ////////////////////////////////////////////////////////////////////////////
        void TFreeRes::Output(IOutputStream &str) const {
            str << "{ChunkIdx: " << ChunkId << " Mask# ";
            for (size_t i = 0; i < MaskSize; ++i) {
                str << (Mask[i] ? "1" : "0");
            }
            str << "}";
        }

        ////////////////////////////////////////////////////////////////////////////
        // TAllocChunkRecoveryLogRec
        ////////////////////////////////////////////////////////////////////////////
        TString TAllocChunkRecoveryLogRec::Serialize() const {
            TStringStream str;
            str.Write(&ChunkId, sizeof(ui32));
            // refPointLsn (for backward compatibility, can be removed)
            ui64 refPointLsn = 0;
            str.Write(&refPointLsn, sizeof(ui64));
            return str.Str();
        }

        bool TAllocChunkRecoveryLogRec::ParseFromString(const TString &data) {
            return ParseFromArray(data.data(), data.size());
        }

        bool TAllocChunkRecoveryLogRec::ParseFromArray(const char* data, size_t size) {
            if (size != sizeof(ui32) + sizeof(ui64)) // refPointLsn(ui64) (for backward compatibility, can be removed)
                return false;
            const char *cur = data;
            ChunkId = ReadUnaligned<ui32>(cur);

            return true;
        }

        TString TAllocChunkRecoveryLogRec::ToString() const {
            TStringStream str;
            str << "{ChunkId# " << ChunkId << "}";
            return str.Str();
        }

        ////////////////////////////////////////////////////////////////////////////
        // TFreeChunkRecoveryLogRec
        ////////////////////////////////////////////////////////////////////////////
        TString TFreeChunkRecoveryLogRec::Serialize() const {
            TStringStream str;
            // refPointLsn (for backward compatibility, can be removed)
            ui64 refPointLsn = 0;
            str.Write(&refPointLsn, sizeof(ui64));
            ui32 size = ChunkIds.size();
            str.Write(&size, sizeof(ui32));
            for (const ui32 x : ChunkIds)
                str.Write(&x, sizeof(ui32));
            return str.Str();
        }

        bool TFreeChunkRecoveryLogRec::ParseFromString(const TString &data) {
            return ParseFromArray(data.data(), data.size());
        }

        bool TFreeChunkRecoveryLogRec::ParseFromArray(const char* data, size_t s) {
            ChunkIds.clear();
            const char *cur = data;
            const char *end = cur + s;
            if (size_t(end - cur) < sizeof(ui64) + sizeof(ui32))
                return false;

            cur += sizeof(ui64); // skip refPointLsn (for backward compatibility, can be removed)
            ui32 size = ReadUnaligned<ui32>(cur);
            cur += sizeof(ui32);

            if (size_t(end - cur) != sizeof(ui32) * size)
                return false;

            ChunkIds.reserve(size);
            for (ui32 i = 0; i < size; i++) {
                ChunkIds.push_back(ReadUnaligned<ui32>(cur));
                cur += sizeof(ui32);
            }
            return true;
        }

        TString TFreeChunkRecoveryLogRec::ToString() const {
            TStringStream str;
            str << "{ChunkIds# ";
            if (ChunkIds.empty())
                str << "empty";
            else {
                str << "[";
                bool first = true;
                for (const auto &x : ChunkIds) {
                    if (first)
                        first = false;
                    else
                        str << " ";
                    str << x;
                }
                str << "]";
            }
            str << "}";
            return str.Str();
        }

        ////////////////////////////////////////////////////////////////////////////
        // TPutRecoveryLogRec
        ////////////////////////////////////////////////////////////////////////////
        TString TPutRecoveryLogRec::Serialize() const {
            TStringStream str;

            // LogoBlobID
            NKikimrProto::TLogoBlobID proto;
            LogoBlobIDFromLogoBlobID(LogoBlobID, &proto);
            TString lbSerialized;
            bool res = proto.SerializeToString(&lbSerialized);
            Y_ABORT_UNLESS(res);
            ui16 lbSerializedSize = lbSerialized.size();
            str.Write(&lbSerializedSize, sizeof(ui16));
            str.Write(lbSerialized.data(), lbSerializedSize);

            // Ingress
            ui64 ingressData = Ingress.Raw();
            str.Write(&ingressData, sizeof(ui64));

            // DiskAddr
            DiskAddr.Serialize(str);

            // RefPointLsn (for backward compatibility, can be removed)
            ui64 refPointLsn = 0;
            str.Write(&refPointLsn, sizeof(ui64));

            return str.Str();
        }

        bool TPutRecoveryLogRec::ParseFromString(const TString &data) {
            return ParseFromArray(data.data(), data.size());
        }

        bool TPutRecoveryLogRec::ParseFromArray(const char* data, size_t size) {
            const char *cur = data;
            const char *end = cur + size;

            // LogoBlobID
            if (size_t(end - cur) < sizeof(ui16))
                return false;
            ui16 lbSerializedSize = ReadUnaligned<ui16>(cur);
            cur += sizeof(ui16);
            if (size_t(end - cur) < lbSerializedSize)
                return false;
            NKikimrProto::TLogoBlobID proto;
            bool res = proto.ParseFromArray(cur, lbSerializedSize);
            if (!res)
                return false;
            LogoBlobID = LogoBlobIDFromLogoBlobID(proto);
            cur += lbSerializedSize;

            // Ingress
            if (size_t(end - cur) < sizeof(ui64))
                return false;
            ui64 ingressData = ReadUnaligned<ui64>(cur);
            cur += sizeof(ui64);
            Ingress = TIngress(ingressData);

            // DiskAddr
            if (size_t(end - cur) < TDiskPart::SerializedSize)
                return false;
            if (!DiskAddr.Parse(cur, cur + TDiskPart::SerializedSize))
                return false;
            cur += TDiskPart::SerializedSize;

            // RefPointLsn (for backward compatibility, can be removed)
            if (size_t(end - cur) != sizeof(ui64))
                return false;

            return true;
        }

        TString TPutRecoveryLogRec::ToString() const {
            TStringStream str;
            str << "{LogoBlobID# " << LogoBlobID.ToString() << " DiskAddr# " << DiskAddr.ToString() << "}";
            return str.Str();
        }

    } // NHuge

} // NKikimr
