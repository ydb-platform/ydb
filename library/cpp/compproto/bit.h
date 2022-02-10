#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/stream/input.h>

#include "huff.h"
#include "compressor.h"
#include "metainfo.h"

namespace NCompProto {
    struct TBitBuffer {
        TVector<ui8> Out;
        ui64 Position;
        ui64 Size;
        ui8 Counter;
        TBitBuffer() {
            Clear();
        }

        static ui64 Read(const ui8* out, ui64 position, size_t size) {
            const ui8* dst = out + (size_t(position >> 3));
            ui64 outCode = *reinterpret_cast<const ui64*>(dst);
            ui8 shift = position & 7;
            return ((1ULL << size) - 1) & (outCode >> shift);
        }

        ui64 Read(ui64 position, size_t size) {
            return Read(&Out[0], position, size);
        }

        void Code(ui64 value, size_t size) {
            if (++Counter == 0) {
                Junk(257 * 64);
            }
            Position = Code(value, size, Position);
        }
        ui64 Code(ui64 value, size_t size, ui64 position) {
            ui8* dst = &Out[size_t(position >> 3)];
            ui64& outCode = *(ui64*)dst;
            ui8 shift = position & 7;
            ui64 mask = ((1ULL << size) - 1) << shift;
            outCode = ((value << shift) & mask) | (outCode & ~mask);
            return position + size;
        }
        void Junk(size_t junk = 1024) {
            size_t need = size_t(Position >> 3);
            if (Out.size() * 8 < Position + junk) {
                Out.resize(((need + junk) * 3) / 2);
                Size = Out.size();
            }
        }
        void Insert(ui64 value, ui64 position, size_t size) {
            ui64 newVal = Read(position, 56);
            position = Code(value, size, position);
            value = newVal;

            while (position < Position + 64) {
                newVal = Read(position + 56 - size, 56);
                position = Code(value, 56, position);
                value = newVal;
            }

            Position += size;
        }

        size_t ByteLength() const {
            return (Position + 7) / 8;
        }

        void ResetPosition() {
            Position = 0;
            Size = 0;
        }

        void Clear() {
            Counter = 0;
            Position = 0;
            Size = 0;
            Out.clear();
            Junk();
        }

        TArrayRef<const char> AsDataRegion() const {
            return TArrayRef<const char>(reinterpret_cast<const char*>(Out.data()), ByteLength());
        }
    };

    struct THuff {
        TCoder Coder;
        ui64 Position;
        THuff() {
            Position = (ui64)(-1);
        }
        void Load(IInputStream& stream) {
            TString name;
            Coder.InitDefault();
            Coder.Entries.clear();
            while (1) {
                stream >> name;
                if (name == "end")
                    return;
                else if (name == "entry") {
                    TCoderEntry entry;
                    ui32 value;
                    stream >> value;
                    entry.MinValue = value;
                    stream >> value;
                    entry.Prefix = value;
                    stream >> value;
                    entry.PrefixBits = value;
                    stream >> value;
                    entry.AllBits = value;
                    Coder.Entries.push_back(entry);
                }
            }
        }

        void Save(IOutputStream& stream, TString offset) {
            TString step = "    ";
            for (size_t i = 0; i < Coder.Entries.size(); ++i) {
                stream << offset << step << "entry ";
                stream << (ui32)Coder.Entries[i].MinValue << " ";
                stream << (ui32)Coder.Entries[i].Prefix << " ";
                stream << (ui32)Coder.Entries[i].PrefixBits << " ";
                stream << (ui32)Coder.Entries[i].AllBits << " ";
                stream << Endl;
            }
            stream << offset << "end" << Endl;
        }

        void BeginElement(TBitBuffer& out) {
            Position = out.Position;
        }
        void Add(ui32 value, TBitBuffer& out) {
            size_t val = 0;
            ui64 code = Coder.Code(value, val);
            out.Code(code, val);
        }
        void AddDelayed(ui32 value, TBitBuffer& out) {
            size_t val = 0;
            ui64 code = Coder.Code(value, val);
            if (Position == (ui64)(-1)) {
                ythrow yexception() << "Position == (ui64)(-1)";
            }
            out.Insert(code, Position, val);
            out.Junk();
        }
    };

    struct THist {
        TAccum Accum;
        THist() {
        }

        void Load(IInputStream& stream) {
            TString name;
            while (1) {
                stream >> name;
                if (name == "end")
                    return;
            }
        }
        // TODO: why not const TString& ???
        void Save(IOutputStream& /*stream*/, TString /*offset*/) {
        }

        void Add(ui32 value, TEmpty& /*empty*/) {
            Accum.Add(value);
        }
        void AddDelayed(ui32 value, TEmpty& /*empty*/) {
            Accum.Add(value);
        }
        void BeginElement(TEmpty& /*empty*/) {
        }
    };

    struct THistToHuff {
        static THistToHuff Instance() {
            return THistToHuff();
        }
        TEmpty Build() const {
            return TEmpty();
        }
        void Build(THuff& info, const THist& hist) const {
            Analyze(hist.Accum, info.Coder.Entries);
            info.Coder.Normalize();
        }
    };

    struct IDecompressor {
        // sequentially decompresses whole structure according to metainfo, starts at position offset
        virtual void Decompress(const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset) = 0;
        // decompresses one record of outer repeated structure starting at position offset
        virtual void DecompressOne(const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset, ui32 prevIndex = -1) = 0;
        virtual ~IDecompressor() = default;
    };

    template <class X>
    struct TMetaIterator: public IDecompressor {
        X Self;
        TMetaIterator() {
            Self.Parent = this;
        }

    private:
        inline void DecompressSingle(ui32 repeatedIndex, const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset) {
            Self.BeginElement(repeatedIndex);
            ui32 mask = table->Mask.Decompress(codes, offset);
            size_t index = 0;
            ui32 scalarMask = table->ScalarMask;
            while (mask || scalarMask) {
                if (mask & 1) {
                    if (scalarMask & 1) {
                        ui32 val = table->Scalar[index].Decompress(codes, offset);
                        Self.SetScalar(index, val);
                    } else {
                        Self.GetDecompressor(index).Decompress(table->Repeated[index].Get(), codes, offset);
                    }
                } else if ((scalarMask & 1) && table->Default[index].Type == TScalarDefaultValue::Fixed) {
                    Self.SetScalar(index, table->Default[index].Value);
                }
                scalarMask = scalarMask >> 1;
                mask = mask >> 1;
                ++index;
            }
            Self.EndElement();
        }

        inline void DecompressSingleScalarsOnly(ui32 repeatedIndex, const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset) {
            Self.BeginElement(repeatedIndex);
            ui32 mask = table->Mask.Decompress(codes, offset);
            ui32 scalarMask = table->ScalarMask;
            size_t index = 0;
            while (scalarMask) {
                if (mask & 1) {
                    ui32 val = table->Scalar[index].Decompress(codes, offset);
                    Self.SetScalar(index, val);
                } else if (table->Default[index].Type == TScalarDefaultValue::Fixed) {
                    Self.SetScalar(index, table->Default[index].Value);
                }
                mask = mask >> 1;
                scalarMask = scalarMask >> 1;
                ++index;
            }
            Self.EndElement();
        }

    public:
        void Decompress(const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset) override {
            ui64 locOffset = offset;
            ui32 count = table->Count.Decompress(codes, locOffset);
            ui32 repeatedIndex = (ui32)(-1);
            Self.BeginSelf(count, table->Id);
            if (table->RepeatedMask) {
                for (ui32 i = 0; i < count; ++i) {
                    repeatedIndex += table->Index.Decompress(codes, locOffset);
                    DecompressSingle(repeatedIndex, table, codes, locOffset);
                }
            } else {
                for (ui32 i = 0; i < count; ++i) {
                    repeatedIndex += table->Index.Decompress(codes, locOffset);
                    DecompressSingleScalarsOnly(repeatedIndex, table, codes, locOffset);
                }
            }
            offset = locOffset;
            Self.EndSelf();
        }

        // XXX: iterator needed?
        //
        // Following two functions serves the purpose of decompressing outer repeated-structure(such structure is mandatory now).
        // They can work for inner repeated structures too, if you supply correct table, codes and offset parameters.
        void DecompressOne(const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset, ui32 prevIndex = -1) override {
            table->Index.Decompress(codes, offset);
            DecompressSingle(prevIndex, table, codes, offset);
        }

        ui32 DecompressCount(const TMetaInfo<TTable>* table, const ui8* codes, ui64& offset) {
            return table->Count.Decompress(codes, offset);
        }
    };

    template <class X>
    struct TParentHold {
        TMetaIterator<X>* Parent;
        TParentHold()
            : Parent(nullptr)
        {
        }
    };

    struct TEmptyDecompressor: public TParentHold<TEmptyDecompressor> {
        void BeginSelf(ui32 /*count*/, ui32 /*id*/) {
        }
        void EndSelf() {
        }
        void BeginElement(ui32 /*element*/) {
        }
        void EndElement() {
        }
        void SetScalar(size_t /*index*/, ui32 /*val*/) {
        }
        TMetaIterator<TEmptyDecompressor>& GetDecompressor(size_t index) {
            Y_UNUSED(index);
            return *Parent;
        }
    };

    inline TMetaIterator<TEmptyDecompressor>& GetEmptyDecompressor() {
        static TMetaIterator<TEmptyDecompressor> empty;
        return empty;
    }

    struct THuffToTable {
        static THuffToTable Instance() {
            return THuffToTable();
        }
        void Build(TTable& table, const THuff& huff) const {
            // can happen if a field was never serialized across whole structure
            if (huff.Coder.Entries.empty())
                return;
            for (ui32 i = 0; i < 64; ++i) {
                const TCoderEntry& entry = huff.Coder.GetEntry(i, table.Id[i]);
                table.CodeBase[i] = entry.MinValue;
                table.PrefLength[i] = entry.PrefixBits;
                table.CodeMask[i] = (1ULL << (entry.AllBits - entry.PrefixBits)) - 1ULL;
                table.Length[i] = entry.AllBits;
            }
        }
    };

    struct THuffToTableWithDecompressor: private THuffToTable {
        TSimpleSharedPtr<IDecompressor> Decompressor;
        THuffToTableWithDecompressor(TSimpleSharedPtr<IDecompressor> decompressor)
            : Decompressor(decompressor)
        {
        }
        TSimpleSharedPtr<IDecompressor> Build() const {
            return Decompressor;
        }
        void Build(TTable& table, const THuff& huff) const {
            THuffToTable::Build(table, huff);
        }
    };

}
