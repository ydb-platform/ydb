#pragma once

#include <util/generic/vector.h>
#include <util/memory/blob.h>
#include <util/ysaveload.h>

#include <library/cpp/compproto/huff.h>

namespace NCompTable {
    struct TCompressorTable {
        ui32 Table[65536];
        ui32 HashMul;
        NCompProto::TCoderEntry HuffCodes[10];
        ui8 HuffIndices[256];

        void GetHuffCode(const NCompProto::TCoderEntry& entry, ui32 value, ui64& bitCode, ui8& bitLength) const;
        void GetLastHuffCode(ui32 value, ui64& bitCode, ui8& bitLength) const;
        void GetHuffCode(ui32 value, ui64& bitCode, ui8& bitLength) const;
        ui8 GetHuffIndex(ui8 prefix);
        void BuildHuffCodes(i64 totalFreq, i64 freqs[65536]);
        bool BuildHuffCodes(i64 totalFreq, i64 freqs[65536], i64 add);
    };

    struct TDataSampler {
        enum {
            Size = 1 << 18,
        };
        ui32 EntryVal[Size];
        i64 EntryHit[Size];
        i64 Counter;

    public:
        TDataSampler();
        void BuildTable(TCompressorTable& table) const;
        void AddStat(ui32 val);
        void AddStat(const TStringBuf& stringBuf);
    };

    class TDataCompressor;
    class TDataDecompressor;

    class TChunkCompressor {
    public:
        TChunkCompressor(bool highQuality, const TCompressorTable& table);
        void Compress(TStringBuf data, TVector<char>* result) const;
        ~TChunkCompressor();

    private:
        bool HighQuality;
        THolder<TDataCompressor> Compressor;
    };

    class TChunkDecompressor {
    public:
        TChunkDecompressor(bool highQuality, const TCompressorTable& table);
        void Decompress(TStringBuf data, TVector<char>* result) const;
        ~TChunkDecompressor();

    private:
        bool HighQuality;
        THolder<TDataDecompressor> Decompressor;
    };

}

template <>
class TSerializer<NCompTable::TCompressorTable> {
public:
    static inline void Save(IOutputStream* out, const NCompTable::TCompressorTable& entry) {
        SavePodType(out, entry);
    }
    static inline void Load(IInputStream* in, NCompTable::TCompressorTable& entry) {
        LoadPodType(in, entry);
    }
};
