#pragma once

#include <util/system/defaults.h>
#include <util/generic/yexception.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <utility>

#include <queue>

#include "compressor.h"

namespace NCompProto {
    template <size_t CacheSize, typename TEntry>
    struct TCache {
        ui32 CacheKey[CacheSize];
        TEntry CacheVal[CacheSize];
        size_t Hits;
        size_t Misses;
        ui32 Hash(ui32 key) {
            return key % CacheSize;
        }
        TCache() {
            Hits = 0;
            Misses = 0;
            Clear();
        }
        void Clear() {
            for (size_t i = 0; i < CacheSize; ++i) {
                ui32 j = 0;
                for (; Hash(j) == i; ++j)
                    ;
                CacheKey[i] = j;
            }
        }
    };

    struct TCode {
        i64 Probability;
        ui32 Start;
        ui32 Bits;
        ui32 Prefix;
        ui32 PrefLength;
        TCode(i64 probability = 0, ui32 start = 0, ui32 bits = 0)
            : Probability(probability)
            , Start(start)
            , Bits(bits)
        {
        }

        bool operator<(const TCode& code) const {
            return Probability < code.Probability;
        }

        bool operator>(const TCode& code) const {
            return Probability > code.Probability;
        }
    };

    struct TAccum {
        struct TTable {
            TAutoPtr<TTable> Tables[16];
            i64 Counts[16];
            TTable(const TTable& other) {
                for (size_t i = 0; i < 16; ++i) {
                    Counts[i] = other.Counts[i];
                    if (other.Tables[i].Get()) {
                        Tables[i].Reset(new TTable(*other.Tables[i].Get()));
                    }
                }
            }
            TTable() {
                for (auto& count : Counts)
                    count = 0;
            }

            i64 GetCellCount(size_t i) {
                i64 count = Counts[i];
                if (Tables[i].Get()) {
                    for (size_t j = 0; j < 16; ++j) {
                        count += Tables[i]->GetCellCount(j);
                    }
                }
                return count;
            }

            i64 GetCount() {
                i64 count = 0;
                for (size_t j = 0; j < 16; ++j) {
                    count += GetCellCount(j);
                }
                return count;
            }

            void GenerateFreqs(TVector<std::pair<i64, TCode>>& codes, int depth, int termDepth, ui32 code, i64 cnt) {
                if (depth == termDepth) {
                    for (size_t i = 0; i < 16; ++i) {
                        i64 iCount = GetCellCount(i);
                        if (Tables[i].Get()) {
                            Counts[i] = iCount;
                            Tables[i].Reset(nullptr);
                        }

                        if (iCount > cnt || (termDepth == 0 && iCount > 0)) {
                            std::pair<i64, TCode> codep;
                            codep.first = iCount;
                            codep.second.Probability = iCount;
                            codep.second.Start = code + (i << (28 - depth));
                            codep.second.Bits = 28 - depth;
                            codes.push_back(codep);
                            Counts[i] = 0;
                        }
                    }
                }
                for (size_t i = 0; i < 16; ++i) {
                    if (Tables[i].Get()) {
                        Tables[i]->GenerateFreqs(codes, depth + 4, termDepth, code + (i << (28 - depth)), cnt);
                    }
                }
            }
        };

        TTable Root;
        int TableCount;
        i64 Total;
        ui64 Max;

        TAccum() {
            TableCount = 0;
            Total = 0;
            Max = 0;
        }

        void GenerateFreqs(TVector<std::pair<i64, TCode>>& codes, int mul) const {
            TTable root(Root);

            for (int i = 28; i > 0; i -= 4) {
                root.GenerateFreqs(codes, 0, i, 0, Total / mul);
            }

            i64 iCount = root.GetCount();
            if (iCount == 0)
                return;
            std::pair<i64, TCode> codep;
            codep.first = iCount;
            codep.second.Probability = iCount;
            codep.second.Start = 0;
            ui32 bits = 0;
            while (1) {
                if ((1ULL << bits) > Max)
                    break;
                ++bits;
            }
            codep.second.Bits = bits;
            codes.push_back(codep);
        }

        TCache<256, i64*> Cache;

        void AddMap(ui32 value, i64 weight = 1) {
            ui32 index = Cache.Hash(value);
            if (Cache.CacheKey[index] == value) {
                Cache.CacheVal[index][0] += weight;
                return;
            }
            TTable* root = &Root;
            for (size_t i = 0; i < 15; ++i) {
                ui32 index2 = (value >> (28 - i * 4)) & 0xf;
                if (!root->Tables[index2].Get()) {
                    if (TableCount < 1024) {
                        ++TableCount;
                        root->Tables[index2].Reset(new TTable);
                    } else {
                        Cache.CacheKey[index2] = value;
                        Cache.CacheVal[index2] = &root->Counts[index2];
                        root->Counts[index2] += weight;
                        return;
                    }
                }
                root = root->Tables[index2].Get();
            }

            Cache.CacheKey[index] = value;
            Cache.CacheVal[index] = &root->Counts[value & 0xf];
            root->Counts[value & 0xf] += weight;
        }

        void Add(ui32 value, i64 weight = 1) {
            Max = ::Max(Max, (ui64)value);
            Total += weight;
            AddMap(value, weight);
        }
    };

    struct THuffNode {
        i64 Weight;
        i64 Priority;
        THuffNode* Nodes[2];
        TCode* Code;
        THuffNode(i64 weight, i64 priority, TCode* code)
            : Weight(weight)
            , Priority(priority)
            , Code(code)
        {
            Nodes[0] = nullptr;
            Nodes[1] = nullptr;
        }

        void BuildPrefixes(ui32 depth, ui32 prefix) {
            if (Code) {
                Code->Prefix = prefix;
                Code->PrefLength = depth;
                return;
            }
            Nodes[0]->BuildPrefixes(depth + 1, prefix + (0UL << depth));
            Nodes[1]->BuildPrefixes(depth + 1, prefix + (1UL << depth));
        }

        i64 Iterate(size_t depth) const {
            if (Code) {
                return (depth + Code->Bits) * Code->Probability;
            }
            return Nodes[0]->Iterate(depth + 1) + Nodes[1]->Iterate(depth + 1);
        }

        size_t Depth() const {
            if (Code) {
                return 0;
            }
            return Max(Nodes[0]->Depth(), Nodes[1]->Depth()) + 1;
        }
    };

    struct THLess {
        bool operator()(const THuffNode* a, const THuffNode* b) {
            if (a->Weight > b->Weight)
                return 1;
            if (a->Weight == b->Weight && a->Priority > b->Priority)
                return 1;
            return 0;
        }
    };

    inline i64 BuildHuff(TVector<TCode>& codes) {
        TVector<TSimpleSharedPtr<THuffNode>> hold;
        std::priority_queue<THuffNode*, TVector<THuffNode*>, THLess> nodes;
        i64 ret = 0;

        int priority = 0;
        for (size_t i = 0; i < codes.size(); ++i) {
            TSimpleSharedPtr<THuffNode> node(new THuffNode(codes[i].Probability, priority++, &codes[i]));
            hold.push_back(node);
            nodes.push(node.Get());
        }

        while (nodes.size() > 1) {
            THuffNode* nodea = nodes.top();
            nodes.pop();
            THuffNode* nodeb = nodes.top();
            nodes.pop();
            TSimpleSharedPtr<THuffNode> node(new THuffNode(nodea->Weight + nodeb->Weight, priority++, nullptr));
            node->Nodes[0] = nodea;
            node->Nodes[1] = nodeb;
            hold.push_back(node);
            nodes.push(node.Get());
        }

        if (nodes.size()) {
            THuffNode* node = nodes.top();
            node->BuildPrefixes(0, 0);
            ret = node->Iterate(0);
        }

        return ret;
    }

    struct TCoderEntry {
        ui32 MinValue;
        ui16 Prefix;
        ui8 PrefixBits;
        ui8 AllBits;

        ui64 MaxValue() const {
            return MinValue + (1ULL << (AllBits - PrefixBits));
        }
    };

    inline i64 Analyze(const TAccum& acc, TVector<TCoderEntry>& retCodes) {
        i64 ret;
        for (int k = 256; k > 0; --k) {
            retCodes.clear();
            TVector<std::pair<i64, TCode>> pairs;
            acc.GenerateFreqs(pairs, k);
            TVector<TCode> codes;
            for (size_t i = 0; i < pairs.size(); ++i) {
                codes.push_back(pairs[i].second);
            }

            StableSort(codes.begin(), codes.end(), std::greater<TCode>());

            ret = BuildHuff(codes);
            bool valid = true;
            for (size_t i = 0; i < codes.size(); ++i) {
                TCoderEntry code;
                code.MinValue = codes[i].Start;
                code.Prefix = codes[i].Prefix;
                code.PrefixBits = codes[i].PrefLength;
                if (code.PrefixBits > 6)
                    valid = false;
                code.AllBits = code.PrefixBits + codes[i].Bits;
                retCodes.push_back(code);
            }
            if (valid)
                return ret;
        }

        return ret;
    }

    struct TComparer {
        bool operator()(const TCoderEntry& e0, const TCoderEntry& e1) const {
            return e0.AllBits < e1.AllBits;
        }
    };

    struct TCoder {
        TVector<TCoderEntry> Entries;
        void Normalize() {
            TComparer comp;
            StableSort(Entries.begin(), Entries.end(), comp);
        }
        TCoder() {
            InitDefault();
        }
        void InitDefault() {
            ui64 cum = 0;
            Cache.Clear();
            Entries.clear();
            ui16 b = 1;
            for (ui16 i = 0; i < 40; ++i) {
                ui16 bits = Min(b, (ui16)(32));
                b = (b * 16) / 10 + 1;
                if (b > 32)
                    b = 32;
                TCoderEntry entry;
                entry.PrefixBits = i + 1;
                entry.AllBits = entry.PrefixBits + bits;
                entry.MinValue = (ui32)Min(cum, (ui64)(ui32)(-1));
                cum += (1ULL << bits);
                entry.Prefix = ((1UL << i) - 1);
                Entries.push_back(entry);
                if (cum > (ui32)(-1)) {
                    return;
                }
            }
        }

        TCache<1024, TCoderEntry> Cache;

        ui64 RealCode(ui32 value, const TCoderEntry& entry, size_t& length) {
            length = entry.AllBits;
            return (ui64(value - entry.MinValue) << entry.PrefixBits) + entry.Prefix;
        }

        bool Empty() const {
            return Entries.empty();
        }
        const TCoderEntry& GetEntry(ui32 code, ui8& id) const {
            for (size_t i = 0; i < Entries.size(); ++i) {
                const TCoderEntry& entry = Entries[i];
                ui32 prefMask = (1UL << entry.PrefixBits) - 1UL;
                if (entry.Prefix == (code & prefMask)) {
                    id = ui8(i);
                    return entry;
                }
            }
            ythrow yexception() << "bad entry";
            return Entries[0];
        }

        ui64 Code(ui32 entry, size_t& length) {
            ui32 index = Cache.Hash(entry);
            if (Cache.CacheKey[index] == entry) {
                ++Cache.Hits;
                return RealCode(entry, Cache.CacheVal[index], length);
            }
            ++Cache.Misses;
            for (size_t i = 0; i < Entries.size(); ++i) {
                if (entry >= Entries[i].MinValue && entry < Entries[i].MaxValue()) {
                    Cache.CacheKey[index] = entry;
                    Cache.CacheVal[index] = Entries[i];
                    return RealCode(entry, Cache.CacheVal[index], length);
                }
            }

            ythrow yexception() << "bad huff tree";
            return 0;
        }
    };

}
