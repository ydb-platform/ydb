#include "huffman_codec.h"
#include <library/cpp/bit_io/bitinput.h>
#include <library/cpp/bit_io/bitoutput.h>

#include <util/generic/algorithm.h>
#include <util/generic/bitops.h>
#include <util/stream/length.h>
#include <util/string/printf.h>

namespace NCodecs {
    template <typename T>
    struct TCanonicalCmp {
        bool operator()(const T& a, const T& b) const {
            if (a.CodeLength == b.CodeLength) {
                return a.Char < b.Char;
            } else {
                return a.CodeLength < b.CodeLength;
            }
        }
    };

    template <typename T>
    struct TByCharCmp {
        bool operator()(const T& a, const T& b) const {
            return a.Char < b.Char;
        }
    };

    struct TTreeEntry {
        static const ui32 InvalidBranch = (ui32)-1;

        ui64 Freq = 0;
        ui32 Branches[2]{InvalidBranch, InvalidBranch};

        ui32 CodeLength = 0;
        ui8 Char = 0;
        bool Invalid = false;

        TTreeEntry() = default;

        static bool ByFreq(const TTreeEntry& a, const TTreeEntry& b) {
            return a.Freq < b.Freq;
        }

        static bool ByFreqRev(const TTreeEntry& a, const TTreeEntry& b) {
            return a.Freq > b.Freq;
        }
    };

    using TCodeTree = TVector<TTreeEntry>;

    void InitTreeByFreqs(TCodeTree& tree, const ui64 freqs[256]) {
        tree.reserve(255 * 256 / 2); // worst case - balanced tree

        for (ui32 i = 0; i < 256; ++i) {
            tree.emplace_back();
            tree.back().Char = i;
            tree.back().Freq = freqs[i];
        }

        StableSort(tree.begin(), tree.end(), TTreeEntry::ByFreq);
    }

    void InitTree(TCodeTree& tree, ISequenceReader* in) {
        using namespace NPrivate;
        ui64 freqs[256];
        Zero(freqs);

        TStringBuf r;
        while (in->NextRegion(r)) {
            for (ui64 i = 0; i < r.size(); ++i)
                ++freqs[(ui8)r[i]];
        }

        InitTreeByFreqs(tree, freqs);
    }

    void CalculateCodeLengths(TCodeTree& tree) {
        Y_ENSURE(tree.size() == 256, " ");
        const ui32 firstbranch = tree.size();

        ui32 curleaf = 0;
        ui32 curbranch = firstbranch;

        // building code tree. two priority queues are combined in one.
        while (firstbranch - curleaf + tree.size() - curbranch >= 2) {
            TTreeEntry e;

            for (auto& branche : e.Branches) {
                ui32 br;

                if (curleaf >= firstbranch)
                    br = curbranch++;
                else if (curbranch >= tree.size())
                    br = curleaf++;
                else if (tree[curleaf].Freq < tree[curbranch].Freq)
                    br = curleaf++;
                else
                    br = curbranch++;

                Y_ENSURE(br < tree.size(), " ");
                branche = br;
                e.Freq += tree[br].Freq;
            }

            tree.push_back(e);
            PushHeap(tree.begin() + curbranch, tree.end(), TTreeEntry::ByFreqRev);
        }

        // computing code lengths
        for (ui64 i = tree.size() - 1; i >= firstbranch; --i) {
            TTreeEntry e = tree[i];

            for (auto branche : e.Branches)
                tree[branche].CodeLength = e.CodeLength + 1;
        }

        // chopping off the branches
        tree.resize(firstbranch);

        Sort(tree.begin(), tree.end(), TCanonicalCmp<TTreeEntry>());

        // simplification: we are stripping codes longer than 64 bits
        while (!tree.empty() && tree.back().CodeLength > 64)
            tree.pop_back();

        // will not compress
        if (tree.empty())
            return;

        // special invalid code word
        tree.back().Invalid = true;
    }

    struct TEncoderEntry {
        ui64 Code = 0;

        ui8 CodeLength = 0;
        ui8 Char = 0;
        ui8 Invalid = true;

        explicit TEncoderEntry(TTreeEntry e)
            : CodeLength(e.CodeLength)
            , Char(e.Char)
            , Invalid(e.Invalid)
        {
        }

        TEncoderEntry() = default;
    };

    struct TEncoderTable {
        TEncoderEntry Entries[256];

        void Save(IOutputStream* out) const {
            ui16 nval = 0;

            for (auto entrie : Entries)
                nval += !entrie.Invalid;

            ::Save(out, nval);

            for (auto entrie : Entries) {
                if (!entrie.Invalid) {
                    ::Save(out, entrie.Char);
                    ::Save(out, entrie.CodeLength);
                }
            }
        }

        void Load(IInputStream* in) {
            ui16 nval = 0;
            ::Load(in, nval);

            for (ui32 i = 0; i < 256; ++i)
                Entries[i].Char = i;

            for (ui32 i = 0; i < nval; ++i) {
                ui8 ch = 0;
                ui8 len = 0;
                ::Load(in, ch);
                ::Load(in, len);
                Entries[ch].CodeLength = len;
                Entries[ch].Invalid = false;
            }
        }
    };

    struct TDecoderEntry {
        ui32 NextTable : 10;
        ui32 Char : 8;
        ui32 Invalid : 1;
        ui32 Bad : 1;

        TDecoderEntry()
            : NextTable()
            , Char()
            , Invalid()
            , Bad()
        {
        }
    };

    struct TDecoderTable: public TIntrusiveListItem<TDecoderTable> {
        ui64 Length = 0;
        ui64 BaseCode = 0;

        TDecoderEntry Entries[256];

        TDecoderTable() {
            Zero(Entries);
        }
    };

    const int CACHE_BITS_COUNT = 16;
    class THuffmanCodec::TImpl: public TAtomicRefCount<TImpl> {
        TEncoderTable Encoder;
        TDecoderTable Decoder[256];

        TEncoderEntry Invalid;

        ui32 SubTablesNum;

        class THuffmanCache {
            struct TCacheEntry {
                int EndOffset : 24;
                int BitsLeft : 8;
            };
            TVector<char> DecodeCache;
            TVector<TCacheEntry> CacheEntries;
            const TImpl& Original;

        public:
            THuffmanCache(const THuffmanCodec::TImpl& encoder);

            void Decode(NBitIO::TBitInput& in, TBuffer& out) const;
        };

        THolder<THuffmanCache> Cache;

    public:
        TImpl()
            : SubTablesNum(1)
        {
            Invalid.CodeLength = 255;
        }

        ui8 Encode(TStringBuf in, TBuffer& out) const {
            out.Clear();

            if (in.empty()) {
                return 0;
            }

            out.Reserve(in.size() * 2);

            {
                NBitIO::TBitOutputVector<TBuffer> bout(&out);
                TStringBuf tin = in;

                // data is under compression
                bout.Write(1, 1);

                for (auto t : tin) {
                    const TEncoderEntry& ce = Encoder.Entries[(ui8)t];

                    bout.Write(ce.Code, ce.CodeLength);

                    if (ce.Invalid) {
                        bout.Write(t, 8);
                    }
                }

                // in canonical huffman coding there cannot be a code having no 0 in the suffix
                // and shorter than 8 bits.
                bout.Write((ui64)-1, bout.GetByteReminder());
                return bout.GetByteReminder();
            }
        }

        void Decode(TStringBuf in, TBuffer& out) const {
            out.Clear();

            if (in.empty()) {
                return;
            }

            NBitIO::TBitInput bin(in);
            ui64 f = 0;
            bin.ReadK<1>(f);

            // if data is uncompressed
            if (!f) {
                in.Skip(1);
                out.Append(in.data(), in.size());
            } else {
                out.Reserve(in.size() * 8);

                if (Cache.Get()) {
                    Cache->Decode(bin, out);
                } else {
                    while (ReadNextChar(bin, out)) {
                    }
                }
            }
        }

        Y_FORCE_INLINE int ReadNextChar(NBitIO::TBitInput& bin, TBuffer& out) const {
            const TDecoderTable* table = Decoder;
            TDecoderEntry e;

            int bitsRead = 0;
            while (true) {
                ui64 code = 0;

                if (Y_UNLIKELY(!bin.Read(code, table->Length)))
                    return 0;
                bitsRead += table->Length;

                if (Y_UNLIKELY(code < table->BaseCode))
                    return 0;

                code -= table->BaseCode;

                if (Y_UNLIKELY(code > 255))
                    return 0;

                e = table->Entries[code];

                if (Y_UNLIKELY(e.Bad))
                    return 0;

                if (e.NextTable) {
                    table = Decoder + e.NextTable;
                } else {
                    if (e.Invalid) {
                        code = 0;
                        bin.ReadK<8>(code);
                        bitsRead += 8;
                        out.Append((ui8)code);
                    } else {
                        out.Append((ui8)e.Char);
                    }

                    return bitsRead;
                }
            }

            Y_ENSURE(false, " could not decode input");
            return 0;
        }

        void GenerateEncoder(TCodeTree& tree) {
            const ui64 sz = tree.size();

            TEncoderEntry lastcode = Encoder.Entries[tree[0].Char] = TEncoderEntry(tree[0]);

            for (ui32 i = 1; i < sz; ++i) {
                const TTreeEntry& te = tree[i];
                TEncoderEntry& e = Encoder.Entries[te.Char];
                e = TEncoderEntry(te);

                e.Code = (lastcode.Code + 1) << (e.CodeLength - lastcode.CodeLength);
                lastcode = e;

                e.Code = ReverseBits(e.Code, e.CodeLength);

                if (e.Invalid)
                    Invalid = e;
            }

            for (auto& e : Encoder.Entries) {
                if (e.Invalid)
                    e = Invalid;

                Y_ENSURE(e.CodeLength, " ");
            }
        }

        void RegenerateEncoder() {
            for (auto& entrie : Encoder.Entries) {
                if (entrie.Invalid)
                    entrie.CodeLength = Invalid.CodeLength;
            }

            Sort(Encoder.Entries, Encoder.Entries + 256, TCanonicalCmp<TEncoderEntry>());

            TEncoderEntry lastcode = Encoder.Entries[0];

            for (ui32 i = 1; i < 256; ++i) {
                TEncoderEntry& e = Encoder.Entries[i];
                e.Code = (lastcode.Code + 1) << (e.CodeLength - lastcode.CodeLength);
                lastcode = e;

                e.Code = ReverseBits(e.Code, e.CodeLength);
            }

            for (auto& entrie : Encoder.Entries) {
                if (entrie.Invalid) {
                    Invalid = entrie;
                    break;
                }
            }

            Sort(Encoder.Entries, Encoder.Entries + 256, TByCharCmp<TEncoderEntry>());

            for (auto& entrie : Encoder.Entries) {
                if (entrie.Invalid)
                    entrie = Invalid;
            }
        }

        void BuildDecoder() {
            TEncoderTable enc = Encoder;
            Sort(enc.Entries, enc.Entries + 256, TCanonicalCmp<TEncoderEntry>());

            TEncoderEntry& e1 = enc.Entries[0];
            Decoder[0].BaseCode = e1.Code;
            Decoder[0].Length = e1.CodeLength;

            for (auto e2 : enc.Entries) {
                SetEntry(Decoder, e2.Code, e2.CodeLength, e2);
            }
            Cache.Reset(new THuffmanCache(*this));
        }

        void SetEntry(TDecoderTable* t, ui64 code, ui64 len, TEncoderEntry e) {
            Y_ENSURE(len >= t->Length, len << " < " << t->Length);

            ui64 idx = (code & MaskLowerBits(t->Length)) - t->BaseCode;
            TDecoderEntry& d = t->Entries[idx];

            if (len == t->Length) {
                Y_ENSURE(!d.NextTable, " ");

                d.Char = e.Char;
                d.Invalid = e.Invalid;
                return;
            }

            if (!d.NextTable) {
                Y_ENSURE(SubTablesNum < Y_ARRAY_SIZE(Decoder), " ");
                d.NextTable = SubTablesNum++;
                TDecoderTable* nt = Decoder + d.NextTable;
                nt->Length = Min<ui64>(8, len - t->Length);
                nt->BaseCode = (code >> t->Length) & MaskLowerBits(nt->Length);
            }

            SetEntry(Decoder + d.NextTable, code >> t->Length, len - t->Length, e);
        }

        void Learn(ISequenceReader* in) {
            {
                TCodeTree tree;
                InitTree(tree, in);
                CalculateCodeLengths(tree);
                Y_ENSURE(!tree.empty(), " ");
                GenerateEncoder(tree);
            }
            BuildDecoder();
        }

        void LearnByFreqs(const TArrayRef<std::pair<char, ui64>>& freqs) {
            TCodeTree tree;

            ui64 freqsArray[256];
            Zero(freqsArray);

            for (const auto& freq : freqs)
                freqsArray[static_cast<ui8>(freq.first)] += freq.second;

            InitTreeByFreqs(tree, freqsArray);
            CalculateCodeLengths(tree);

            Y_ENSURE(!tree.empty(), " ");

            GenerateEncoder(tree);
            BuildDecoder();
        }

        void Save(IOutputStream* out) {
            ::Save(out, Invalid.CodeLength);
            Encoder.Save(out);
        }

        void Load(IInputStream* in) {
            ::Load(in, Invalid.CodeLength);
            Encoder.Load(in);
            RegenerateEncoder();
            BuildDecoder();
        }
    };

    THuffmanCodec::TImpl::THuffmanCache::THuffmanCache(const THuffmanCodec::TImpl& codec)
        : Original(codec)
    {
        CacheEntries.resize(1 << CACHE_BITS_COUNT);
        DecodeCache.reserve(CacheEntries.size() * 2);
        char buffer[2];
        TBuffer decoded;
        for (size_t i = 0; i < CacheEntries.size(); i++) {
            buffer[1] = i >> 8;
            buffer[0] = i;
            NBitIO::TBitInput bin(buffer, buffer + sizeof(buffer));
            int totalBits = 0;
            while (true) {
                decoded.Resize(0);
                int bits = codec.ReadNextChar(bin, decoded);
                if (totalBits + bits > 16 || !bits) {
                    TCacheEntry e = {static_cast<int>(DecodeCache.size()), 16 - totalBits};
                    CacheEntries[i] = e;
                    break;
                }

                for (TBuffer::TConstIterator it = decoded.Begin(); it != decoded.End(); ++it) {
                    DecodeCache.push_back(*it);
                }
                totalBits += bits;
            }
        }
        DecodeCache.push_back(0);
        CacheEntries.shrink_to_fit();
        DecodeCache.shrink_to_fit();
    }

    void THuffmanCodec::TImpl::THuffmanCache::Decode(NBitIO::TBitInput& bin, TBuffer& out) const {
        int bits = 0;
        ui64 code = 0;
        while (!bin.Eof()) {
            ui64 f = 0;
            const int toRead = 16 - bits;
            if (toRead > 0 && bin.Read(f, toRead)) {
                code = (code >> (16 - bits)) | (f << bits);
                code &= 0xFFFF;
                TCacheEntry entry = CacheEntries[code];
                int start = code > 0 ? CacheEntries[code - 1].EndOffset : 0;
                out.Append((const char*)&DecodeCache[start], (const char*)&DecodeCache[entry.EndOffset]);
                bits = entry.BitsLeft;
            } else { // should never happen until there are exceptions or unaligned input
                bin.Back(bits);
                if (!Original.ReadNextChar(bin, out))
                    break;

                code = 0;
                bits = 0;
            }
        }
    }

    THuffmanCodec::THuffmanCodec()
        : Impl(new TImpl)
    {
        MyTraits.NeedsTraining = true;
        MyTraits.PreservesPrefixGrouping = true;
        MyTraits.PaddingBit = 1;
        MyTraits.SizeOnEncodeMultiplier = 2;
        MyTraits.SizeOnDecodeMultiplier = 8;
        MyTraits.RecommendedSampleSize = 1 << 21;
    }

    THuffmanCodec::~THuffmanCodec() = default;

    ui8 THuffmanCodec::Encode(TStringBuf in, TBuffer& bbb) const {
        if (Y_UNLIKELY(!Trained))
            ythrow TCodecException() << " not trained";

        return Impl->Encode(in, bbb);
    }

    void THuffmanCodec::Decode(TStringBuf in, TBuffer& bbb) const {
        Impl->Decode(in, bbb);
    }

    void THuffmanCodec::Save(IOutputStream* out) const {
        Impl->Save(out);
    }

    void THuffmanCodec::Load(IInputStream* in) {
        Impl->Load(in);
    }

    void THuffmanCodec::DoLearn(ISequenceReader& in) {
        Impl->Learn(&in);
    }

    void THuffmanCodec::LearnByFreqs(const TArrayRef<std::pair<char, ui64>>& freqs) {
        Impl->LearnByFreqs(freqs);
        Trained = true;
    }

}
