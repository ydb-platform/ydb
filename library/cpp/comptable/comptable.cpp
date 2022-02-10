#include <util/system/defaults.h>
#include <util/system/filemap.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/stream/file.h>
#include <library/cpp/compproto/huff.h>

#include "comptable.h"

#include <cstdlib>

namespace NCompTable {
    static const ui32 magicHashMul = 0xd077cd1f;
    static const size_t hashSizeLog = 18;
    static const size_t hashSize = 1 << hashSizeLog;

    size_t HashIndex(ui32 value, ui32 hashMul) {
        return (value * hashMul) >> (32 - hashSizeLog);
    }

    void TCompressorTable::GetHuffCode(const NCompProto::TCoderEntry& entry, ui32 value, ui64& bitCode, ui8& bitLength) const {
        ui64 code = value - entry.MinValue;
        bitCode = (code << entry.PrefixBits) | entry.Prefix;
        bitLength = entry.AllBits;
    }
    void TCompressorTable::GetLastHuffCode(ui32 value, ui64& bitCode, ui8& bitLength) const {
        GetHuffCode(HuffCodes[9], value, bitCode, bitLength);
    }
    void TCompressorTable::GetHuffCode(ui32 value, ui64& bitCode, ui8& bitLength) const {
        for (auto huffCode : HuffCodes) {
            ui32 minValue = huffCode.MinValue;
            if (minValue <= value && value < huffCode.MaxValue()) {
                GetHuffCode(huffCode, value, bitCode, bitLength);
                return;
            }
        }
        abort();
    }
    ui8 TCompressorTable::GetHuffIndex(ui8 prefix) {
        for (size_t i = 0; i < 10; ++i) {
            if ((prefix & ((1 << HuffCodes[i].PrefixBits) - 1)) == HuffCodes[i].Prefix) {
                return i;
            }
        }
        abort();
        return 0;
    }

    void TCompressorTable::BuildHuffCodes(i64 totalFreq, i64 freqs[65536]) {
        i64 add = 1;
        while (1) {
            if (BuildHuffCodes(totalFreq, freqs, add)) {
                return;
            }
            add = add * 2;
        }
    }

    bool TCompressorTable::BuildHuffCodes(i64 totalFreq, i64 freqs[65536], i64 add) {
        TVector<NCompProto::TCode> codes;
        size_t bits[] = {0, 1, 2, 4, 8, 10, 12, 14, 16, 32};
        size_t off = 0;
        ui32 total = totalFreq;
        for (size_t i = 0; i < 9; ++i) {
            size_t size = 1 << bits[i];
            ui32 weight = 0;
            for (size_t j = off; j < off + size && j < 65536; ++j) {
                weight += freqs[j];
            }
            codes.push_back(NCompProto::TCode(weight + add, ui32(off), bits[i]));
            total = total > weight ? total - weight : 0;
            off += size;
        }
        codes.push_back(NCompProto::TCode(total + add, 0, 32));
        i64 ret = NCompProto::BuildHuff(codes);
        Y_UNUSED(ret);
        for (size_t i = 0; i < codes.size(); ++i) {
            NCompProto::TCoderEntry& code = HuffCodes[i];
            code.MinValue = codes[i].Start;
            code.Prefix = codes[i].Prefix;
            code.PrefixBits = codes[i].PrefLength;
            code.AllBits = code.PrefixBits + codes[i].Bits;
            if (code.PrefixBits > 8) {
                return false;
            }
        }
        for (size_t i = 0; i < 256; ++i) {
            HuffIndices[i] = GetHuffIndex(ui8(i));
        }
        return true;
    }

    template <class TIterator>
    void Iterate(const TStringBuf& data, TIterator& iterator) {
        size_t i = 0;
        iterator.Visit(ui32(data.size()));
        for (; i + 3 < data.size(); i += 4) {
            iterator.Visit(reinterpret_cast<const ui32*>(data.data() + i)[0]);
        }
        if (i != data.size()) {
            ui32 buffer[1] = {0};
            memcpy(buffer, data.data() + i, data.size() - i);
            iterator.Visit(buffer[0]);
        }
    }

    class TDataCompressor {
        ui32 Keys[hashSize];
        ui32 Vals[hashSize];
        ui64 BitCodes[hashSize];
        ui8 BitLengths[hashSize];
        ui32 HashMul;
        const TCompressorTable Table;

    public:
        TDataCompressor(const TCompressorTable& table)
            : Table(table)
        {
            HashMul = table.HashMul;
            for (size_t i = 0; i < hashSize; ++i) {
                Keys[i] = 0;
                Vals[i] = ui32(-1);
            }
            for (size_t i = 0; i < 65536; ++i) {
                size_t index = HashIndex(table.Table[i], table.HashMul);
                Keys[index] = table.Table[i];
                Vals[index] = i;
                table.GetHuffCode(ui32(i), BitCodes[index], BitLengths[index]);
            }
        }
        void GetStat(ui32 val, ui32& stat) const {
            size_t index = HashIndex(val, HashMul);
            if (Keys[index] == val) {
                stat = Vals[index];
            } else {
                stat = ui32(-1);
            }
        }
        void GetStat(ui32 val, ui64& bitCode, ui8& bitLength) const {
            size_t index = HashIndex(val, HashMul);
            if (Keys[index] == val) {
                bitCode = BitCodes[index];
                bitLength = BitLengths[index];
            } else {
                Table.GetLastHuffCode(val, bitCode, bitLength);
            }
        }
        size_t Compress4(const ui32* data4, ui8* dataBuff) const {
            ui8* oldBuff = dataBuff;
            ++dataBuff;
            ui32 status = 0;
            for (size_t i = 0; i < 4; ++i) {
                status = (status << 2);
                ui32 data = data4[i];
                if (data == 0) {
                    continue;
                }
                ui32 stat;
                GetStat(data, stat);
                if (stat < 256) {
                    memcpy(dataBuff, &stat, 1);
                    dataBuff += 1;
                    status += 1;
                } else if (stat < 65536) {
                    memcpy(dataBuff, &stat, 2);
                    dataBuff += 2;
                    status += 2;
                } else {
                    memcpy(dataBuff, &data, 4);
                    dataBuff += 4;
                    status += 3;
                }
            }
            oldBuff[0] = ui8(status);
            return dataBuff - oldBuff;
        }
        struct TCompressorIterator {
            TVector<char>& Result;
            const TDataCompressor& Compressor;
            ui32 Cached[4];
            size_t Index;
            size_t RealSize;
            TCompressorIterator(TVector<char>& result, const TDataCompressor& compressor)
                : Result(result)
                , Compressor(compressor)
                , Index(0)
                , RealSize(0)
            {
            }
            void Flush() {
                Result.yresize(RealSize + 32);
                RealSize += Compressor.Compress4(Cached, reinterpret_cast<ui8*>(Result.data()) + RealSize);
                Index = 0;
            }
            void Visit(const ui32 data) {
                Cached[Index] = data;
                ++Index;
                if (Index == 4) {
                    Flush();
                }
            }
            ~TCompressorIterator() {
                if (Index != 0) {
                    for (size_t i = Index; i < 4; ++i) {
                        Cached[i] = 0;
                    }
                    Flush();
                }
                Result.yresize(RealSize);
            }
        };
        struct THQCompressorIterator {
            TVector<char>& Result;
            const TDataCompressor& Compressor;
            size_t RealSize;
            ui64 Offset;
            THQCompressorIterator(TVector<char>& result, const TDataCompressor& compressor)
                : Result(result)
                , Compressor(compressor)
                , RealSize(0)
                , Offset(0)
            {
            }
            void Visit(const ui32 data) {
                size_t byteOff = Offset >> 3;
                Result.yresize(byteOff + 32);
                ui64 bitCode;
                ui8 bitLength;
                Compressor.GetStat(data, bitCode, bitLength);
                ui64 dst;
                memcpy(&dst, &Result[byteOff], sizeof(dst));
                ui64 mask = ((1ULL << (Offset & 7)) - 1ULL);
                dst = (dst & mask) | (bitCode << (Offset & 7));
                memcpy(&Result[byteOff], &dst, sizeof(dst));
                Offset += bitLength;
            }
            ~THQCompressorIterator() {
                Result.yresize((Offset + 7) >> 3);
                if (Offset & 7)
                    Result.back() &= (1 << (Offset & 7)) - 1;
            }
        };
        template <bool HQ>
        void Compress(const TStringBuf& stringBuf, TVector<char>& rslt) const {
            if (!HQ) {
                TCompressorIterator iterator(rslt, *this);
                Iterate(stringBuf, iterator);
            } else {
                THQCompressorIterator iterator(rslt, *this);
                Iterate(stringBuf, iterator);
            }
        }
    };

    class TDataDecompressor {
        const TCompressorTable Table;

    public:
        TDataDecompressor(const TCompressorTable& table)
            : Table(table)
        {
        }
        size_t Decompress(ui32* data4, const ui8* dataBuff, ui32 type) const {
            if (type == 0) {
                data4[0] = 0;
                return 0;
            }
            if (type == 1) {
                ui8 masked;
                memcpy(&masked, dataBuff, sizeof(masked));
                data4[0] = Table.Table[masked];
                return 1;
            } else if (type == 2) {
                ui16 masked;
                memcpy(&masked, dataBuff, sizeof(masked));
                data4[0] = Table.Table[masked];
                return 2;
            } else {
                memcpy(data4, dataBuff, sizeof(*data4));
                return 4;
            }
        }
        size_t Decompress(ui32* data4, const ui8* dataBuff) const {
            ui32 status = *dataBuff;
            const ui8* oldData = dataBuff;
            ++dataBuff;
            dataBuff += Decompress(data4 + 0, dataBuff, (status >> 6));
            dataBuff += Decompress(data4 + 1, dataBuff, (status >> 4) & 0x3);
            dataBuff += Decompress(data4 + 2, dataBuff, (status >> 2) & 0x3);
            dataBuff += Decompress(data4 + 3, dataBuff, (status)&0x3);
            return dataBuff - oldData;
        }
        void Decompress(ui32* data, const ui8* dataBuff, ui64& offset, ui64 border) const {
            size_t off = offset >> 3;
            ui64 val = 0;
            if (border - off >= sizeof(val)) {
                memcpy(&val, dataBuff + off, sizeof(val));
            } else {
                memcpy(&val, dataBuff + off, border - off);
            }
            val >>= (offset & 7);
            ui8 index = Table.HuffIndices[ui8(val)];
            const NCompProto::TCoderEntry& entry = Table.HuffCodes[index];
            ui8 allBits = entry.AllBits;
            val = (val & ((1ULL << allBits) - 1ULL)) >> entry.PrefixBits;
            val = val + entry.MinValue;
            data[0] = (index == 9) ? val : Table.Table[val];
            offset += allBits;
        }
        size_t GetJunkSize() const {
            return 8;
        }
        template <bool HQ>
        void Decompress(const TStringBuf& dataBuf, TVector<char>& rslt) const {
            rslt.clear();
            if (dataBuf.empty()) {
                return;
            }
            const ui8* src = reinterpret_cast<const ui8*>(dataBuf.data());
            ui64 border = dataBuf.size();
            ui32 len = 0;
            ui32 nullTerm = 1;
            if (HQ) {
                ui64 offset = 0;
                ui32 length = 0;
                Decompress(&length, src, offset, border);
                size_t length32 = (length + 3) / 4;
                rslt.yresize(length32 * 4 + nullTerm);
                ui32* result = reinterpret_cast<ui32*>(rslt.data());
                len = length;
                for (size_t i = 0; i < length32; ++i) {
                    Decompress(&result[i], src, offset, border);
                }
            } else {
                ui32 data[4];
                src += Decompress(data, src);
                len = data[0];
                size_t length32x4 = (4 + len + 15) / 16;
                rslt.yresize(length32x4 * 16 + nullTerm);
                ui32* result = reinterpret_cast<ui32*>(rslt.data());
                for (size_t i = 0; i < 3; ++i) {
                    result[i] = data[i + 1];
                }
                for (size_t j = 1; j < length32x4; ++j) {
                    src += Decompress(&result[j * 4 - 1], src);
                }
            }
            rslt.resize(len);
        }
    };

    struct TSamplerIterator {
        TDataSampler& Sampler;
        TSamplerIterator(TDataSampler& sampler)
            : Sampler(sampler)
        {
        }
        void Visit(const ui32 data) {
            Sampler.AddStat(data);
        }
    };

    struct TGreaterComparer {
        //std::greater in arcadia???
        bool operator()(ui64 a, ui64 b) const {
            return a > b;
        }
    };

    TDataSampler::TDataSampler() {
        memset(this, 0, sizeof(*this));
    }

    void TDataSampler::BuildTable(TCompressorTable& table) const {
        std::vector<ui64> sorted;
        for (size_t i = 0; i < Size; ++i) {
            ui64 res = (ui64(EntryHit[i]) << 32) + ui32(i);
            sorted.push_back(res);
        }
        std::vector<ui32> entryTbl(Size);
        std::sort(sorted.begin(), sorted.end(), TGreaterComparer());
        table.HashMul = magicHashMul;
        i64 freqs[65536];
        for (size_t i = 0; i < 65536; ++i) {
            ui32 ind = ui32(sorted[i]);
            table.Table[i] = EntryVal[ind];
            freqs[i] = EntryHit[ind];
        }
        table.BuildHuffCodes(Counter, freqs);
    }

    void TDataSampler::AddStat(ui32 val) {
        ++Counter;
        size_t hashInd = HashIndex(val, magicHashMul);
        if (EntryVal[hashInd] == val) {
            ++EntryHit[hashInd];
        } else if (EntryHit[hashInd] > 1) {
            --EntryHit[hashInd];
        } else {
            EntryHit[hashInd] = 1;
            EntryVal[hashInd] = val;
        }
    }

    void TDataSampler::AddStat(const TStringBuf& stringBuf) {
        TSamplerIterator iterator(*this);
        Iterate(stringBuf, iterator);
    }

    TChunkCompressor::TChunkCompressor(bool highQuality, const TCompressorTable& table)
        : HighQuality(highQuality)
    {
        Compressor.Reset(new TDataCompressor(table));
    }

    void TChunkCompressor::Compress(TStringBuf data, TVector<char>* rslt) const {
        if (HighQuality) {
            Compressor->Compress<1>(data, *rslt);
        } else {
            Compressor->Compress<0>(data, *rslt);
        }
    }

    TChunkCompressor::~TChunkCompressor() = default;

    TChunkDecompressor::TChunkDecompressor(bool highQuality, const TCompressorTable& table)
        : HighQuality(highQuality)
    {
        Decompressor.Reset(new TDataDecompressor(table));
    }

    void TChunkDecompressor::Decompress(TStringBuf data, TVector<char>* rslt) const {
        if (HighQuality) {
            Decompressor->Decompress<1>(data, *rslt);
        } else {
            Decompressor->Decompress<0>(data, *rslt);
        }
    }

    TChunkDecompressor::~TChunkDecompressor() = default;

}
