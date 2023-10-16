#pragma once

#include "defs.h"
#include <util/generic/vector.h>
#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/system/unaligned_mem.h>
#include <util/ysaveload.h>

// FIXME: only for TIngressCache (put it to vdisk/common)
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////////////
    // TBlobType
    ///////////////////////////////////////////////////////////////////////////////////////
    struct TBlobType {
        enum EType {
            DiskBlob = 0,
            HugeBlob = 1,
            ManyHugeBlobs = 2,
            MemBlob = 3
        };

        static const char *TypeToStr(EType type) {
            switch (type) {
                case DiskBlob:      return "Disk";
                case HugeBlob:      return "Huge";
                case ManyHugeBlobs: return "ManyHuge";
                case MemBlob:       return "Mem";
                default:            return "UNKNOWN";
            }
        }
    };

    ///////////////////////////////////////////////////////////////////////////////////////
    // TDiskPart
    ///////////////////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    struct TDiskPart {
        ui32 ChunkIdx;
        ui32 Offset;
        ui32 Size;

        static const ui32 SerializedSize = sizeof(ui32) * 3u;

        TDiskPart() {
            Clear();
        }

        TDiskPart(ui32 chunkIdx, ui32 offset, ui32 size)
            : ChunkIdx(chunkIdx)
            , Offset(offset)
            , Size(size)
        {
        }

        TDiskPart(const NKikimrVDiskData::TDiskPart& pb)
            : TDiskPart(pb.GetChunkIdx(), pb.GetOffset(), pb.GetSize())
        {}

        explicit operator NKikimrVDiskData::TDiskPart() const {
            NKikimrVDiskData::TDiskPart proto;
            proto.SetChunkIdx(ChunkIdx);
            proto.SetOffset(Offset);
            proto.SetSize(Size);
            return proto;
        }

        void SerializeToProto(NKikimrVDiskData::TDiskPart &pb) const {
            pb.SetChunkIdx(ChunkIdx);
            pb.SetOffset(Offset);
            pb.SetSize(Size);
        }

        void Clear() {
            ChunkIdx = Offset = Size = 0;
        }

        bool Empty() const {
            bool empty = ChunkIdx == 0;
            Y_DEBUG_ABORT_UNLESS((empty && Offset == 0 && Size == 0) || !empty);
            return empty;
        }

        void Save(IOutputStream *s) const {
            ::Save(s, ChunkIdx);
            ::Save(s, Offset);
            ::Save(s, Size);
        }

        void Load(IInputStream *s) {
            ::Load(s, ChunkIdx);
            ::Load(s, Offset);
            ::Load(s, Size);
        }

        void Serialize(IOutputStream &s) const {
            s.Write(&ChunkIdx, sizeof(ui32));
            s.Write(&Offset, sizeof(ui32));
            s.Write(&Size, sizeof(ui32));
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        void Output(IOutputStream &str) const {
            str << "{ChunkIdx: " << ChunkIdx << " Offset: " << Offset << " Size: " << Size << "}";
        }

        bool Parse(const char *b, const char *e) {
            if (e < b || size_t(e - b) != sizeof(ui32) + sizeof(ui32) + sizeof(ui32))
                return false;

            ChunkIdx = ReadUnaligned<ui32>(b);
            b += sizeof(ui32);
            Offset = ReadUnaligned<ui32>(b);
            b += sizeof(ui32);
            Size = ReadUnaligned<ui32>(b);
            return true;
        }

        bool Includes(const TDiskPart &part) const {
            return ChunkIdx == part.ChunkIdx && Offset <= part.Offset && (part.Offset + part.Size) <= (Offset + Size);
        }

        ui64 Hash() const {
            ui64 x = 0;
            x |= (ui64)ChunkIdx;
            x <<= 32u;
            x |= (ui64)Offset;
            return x;
        }

        inline bool operator <(const TDiskPart &x) const {
            return ChunkIdx < x.ChunkIdx
            || (ChunkIdx == x.ChunkIdx && Offset < x.Offset)
            || (ChunkIdx == x.ChunkIdx && Offset == x.Offset && Size < x.Size);
        }

        inline bool operator ==(const TDiskPart &x) const {
            return ChunkIdx == x.ChunkIdx && Offset == x.Offset && Size == x.Size;
        }

        inline bool operator !=(const TDiskPart &x) const {
            return ChunkIdx != x.ChunkIdx || Offset != x.Offset || Size != x.Size;
        }
    };
#pragma pack(pop)

    ///////////////////////////////////////////////////////////////////////////////////////
    // TDiskPartVec
    ///////////////////////////////////////////////////////////////////////////////////////
    struct TDiskPartVec {
        using TVec = TVector<TDiskPart>;
        TVec Vec;

        TDiskPartVec() = default;
        TDiskPartVec(const TDiskPartVec&) = default;
        TDiskPartVec(TDiskPartVec&&) = default;
        TDiskPartVec &operator=(const TDiskPartVec &) = default;
        TDiskPartVec &operator=(TDiskPartVec &&) = default;
        ~TDiskPartVec() = default;

        explicit TDiskPartVec(const NKikimrVDiskData::TDiskPartVec &pb)
            : Vec()
        {
            Vec.reserve(pb.DiskPartsSize());
            for (const auto &x : pb.GetDiskParts()) {
                Vec.emplace_back(x);
            }
        }

        void Reserve(ui64 size) {
            Vec.reserve(size);
        }

        void PushBack(const TDiskPart &p) {
            Vec.push_back(p);
        }

        template<typename T>
        void Append(const T& other) {
            Vec.insert(Vec.end(), other.begin(), other.end());
        }

        void SerializeToProto(NKikimrVDiskData::TDiskPartVec &pb) const {
            for (const auto &x : Vec) {
                auto p = pb.AddDiskParts();
                x.SerializeToProto(*p);
            }
        }

        static void ConvertToProto(NKikimrVDiskData::TDiskPartVec &pb, const TString &source) {
            TDiskPartVec v;
            TStringInput str(source);
            Load(&str, v.Vec);
            v.SerializeToProto(pb);
        }

        static bool Check(const TString &data) {
            Y_UNUSED(data);
            return true;
        }

        void Swap(TDiskPartVec &v) {
            Vec.swap(v.Vec);
        }

        bool Empty() const {
            return Vec.empty();
        }

        void Clear() {
            Vec.clear();
        }

        size_t Size() const {
            return Vec.size();
        }

        TString ToString() const {
            TStringStream str;
            if (Vec.empty()) {
                str << "empty";
            } else {
                str << "[";
                bool first = true;
                for (const auto &x : Vec) {
                    if (first)
                        first = false;
                    else
                        str << " ";
                    str << x.ToString();
                }
                str << "]";
            }
            return str.Str();
        }

        TVec::const_iterator begin() const {
            return Vec.begin();
        }

        TVec::const_iterator end() const {
            return Vec.end();
        }
    };

} // NKikimr

template<>
struct THash<NKikimr::TDiskPart> {
    inline ui64 operator()(const NKikimr::TDiskPart& x) const noexcept {
        return x.Hash();
    }
};

