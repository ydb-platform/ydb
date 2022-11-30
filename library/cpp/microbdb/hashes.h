#pragma once

#include <library/cpp/on_disk/st_hash/static_hash.h>
#include <util/system/sysstat.h>
#include <util/stream/mem.h>
#include <util/string/printf.h>
#include <library/cpp/deprecated/fgood/fgood.h>

#include "safeopen.h"

/** This file currently implements creation of mappable read-only hash file.
    Basic usage of these "static hashes" is defined in util/static_hash.h (see docs there).
    Additional useful wrappers are available in util/static_hash_map.h

    There are two ways to create mappable hash file:

    A) Fill an THashMap/set structure in RAM, then dump it to disk.
       This is usually done by save_hash_to_file* functions defined in static_hash.h
       (see description in static_hash.h).

    B) Prepare all data using external sorter, then create hash file straight on disk.
       This approach is necessary when there isn't enough RAM to hold entire original THashMap.
       Implemented in this file as TStaticHashBuilder class.

       Current implementation's major drawback is that the size of the hash must be estimated
       before the hash is built (bucketCount), which is not always possible.
       Separate implementation with two sort passes is yet to be done.

       Another problem is that maximum stored size of the element (maxRecSize) must also be
       known in advance, because we use TDatSorterMemo, etc.
 */

template <class SizeType>
struct TSthashTmpRec {
    SizeType HashVal;
    SizeType RecSize;
    char Buf[1];
    size_t SizeOf() const {
        return &Buf[RecSize] - (char*)this;
    }
    bool operator<(const TSthashTmpRec& than) const {
        return HashVal < than.HashVal;
    }
    static const ui32 RecordSig = 20100124 + sizeof(SizeType) - 4;
};

template <typename T>
struct TReplaceMerger {
    T operator()(const T& oldRecord, const T& newRecord) const {
        Y_UNUSED(oldRecord);
        return newRecord;
    }
};

/** TStaticHashBuilder template parameters:
    HashType - THashMap map/set type for which we construct corresponding mappable hash;
    SizeType - type used to store offsets and length in resulting hash;
    MergerType - type of object to process records with equal key (see TReplaceMerger for example);
 */

template <class HashType, class SizeType, class MergerType = TReplaceMerger<typename HashType::mapped_type>>
struct TStaticHashBuilder {
    const size_t SrtIOPageSz;
    const size_t WrBufSz;
    typedef TSthashTmpRec<SizeType> TIoRec;
    typedef TSthashWriter<typename HashType::key_type, typename HashType::mapped_type, SizeType> TKeySaver;
    typedef typename HashType::value_type TValueType;
    typedef typename HashType::mapped_type TMappedType;
    typedef typename HashType::key_type TKeyType;

    TDatSorterMemo<TIoRec, TCompareByLess> Srt;
    TBuffer IoRec, CurrentBlockRecs;
    TKeySaver KeySaver;
    typename HashType::hasher Hasher;
    typename HashType::key_equal Equals;
    MergerType merger;
    TString HashFileName;
    TString OurTmpDir;
    size_t BucketCount;
    int FreeBits;

    // memSz is the Sorter buffer size;
    // maxRecSize is the maximum size (as reported by size_for_st) of our record(s)
    TStaticHashBuilder(size_t memSz, size_t maxRecSize)
        : SrtIOPageSz((maxRecSize * 16 + 65535) & ~size_t(65535))
        , WrBufSz(memSz / 16 >= SrtIOPageSz ? memSz / 16 : SrtIOPageSz)
        , Srt("unused", memSz, SrtIOPageSz, WrBufSz, 0)
        , IoRec(sizeof(TIoRec) + maxRecSize)
        , CurrentBlockRecs(sizeof(TIoRec) + maxRecSize)
        , BucketCount(0)
        , FreeBits(0)
    {
    }

    ~TStaticHashBuilder() {
        Close();
    }

    // if tmpDir is supplied, it must exist;
    // bucketCount should be HashBucketCount() of the (estimated) element count
    void Open(const char* fname, size_t bucketCount, const char* tmpDir = nullptr) {
        if (!tmpDir)
            tmpDir = ~(OurTmpDir = Sprintf("%s.temp", fname));
        Mkdir(tmpDir, MODE0775);
        Srt.Open(tmpDir);
        HashFileName = fname;
        BucketCount = bucketCount;
        int bitCount = 0;
        while (((size_t)1 << bitCount) <= BucketCount && bitCount < int(8 * sizeof(size_t)))
            ++bitCount;
        FreeBits = 8 * sizeof(size_t) - bitCount;
    }

    void Push(const TValueType& rec) {
        TIoRec* ioRec = MakeIoRec(rec);
        Srt.Push(ioRec);
    }
    TIoRec* MakeIoRec(const TValueType& rec) {
        TIoRec* ioRec = (TIoRec*)IoRec.Data();
        size_t mask = (1 << FreeBits) - 1;
        size_t hash = Hasher(rec.first);
        ioRec->HashVal = ((hash % BucketCount) << FreeBits) + ((hash / BucketCount) & mask);

        TMemoryOutput output(ioRec->Buf, IoRec.Capacity() - offsetof(TIoRec, Buf));
        KeySaver.SaveRecord(&output, rec);
        ioRec->RecSize = output.Buf() - ioRec->Buf;
        return ioRec;
    }

    bool Merge(TVector<std::pair<TKeyType, TMappedType>>& records, size_t newRecordSize) {
        TSthashIterator<const TKeyType, const TMappedType, typename HashType::hasher,
                        typename HashType::key_equal>
            newPtr(CurrentBlockRecs.End() - newRecordSize);
        for (size_t i = 0; i < records.size(); ++i) {
            if (newPtr.KeyEquals(Equals, records[i].first)) {
                TMappedType oldValue = records[i].second;
                TMappedType newValue = newPtr.Value();
                newValue = merger(oldValue, newValue);
                records[i].second = newValue;
                return true;
            }
        }
        records.push_back(std::make_pair(newPtr.Key(), newPtr.Value()));
        return false;
    }

    void PutRecord(const char* buf, size_t rec_size, TFILEPtr& f, SizeType& cur_off) {
        f.fsput(buf, rec_size);
        cur_off += rec_size;
    }

    void Finish() {
        Srt.Sort();
        // We use variant 1.
        // Variant 1: read sorter once, write records, fseeks to write buckets
        //            (this doesn't allow fname to be stdout)
        // Variant 2: read sorter (probably temp. file) twice: write buckets, then write records
        //            (this allows fname to be stdout but seems to be longer)
        TFILEPtr f(HashFileName, "wb");
        setvbuf(f, nullptr, _IOFBF, WrBufSz);
        TVector<SizeType> bucketsBuf(WrBufSz, 0);
        // prepare header (note: this code must be unified with save_stl.h)
        typedef sthashtable_nvm_sv<typename HashType::hasher, typename HashType::key_equal, SizeType> sv_type;
        sv_type sv = {Hasher, Equals, BucketCount, 0, 0};
        // to do: m.b. use just the size of corresponding object?
        SizeType cur_off = sizeof(sv_type) +
                           (sv.num_buckets + 1) * sizeof(SizeType);
        SizeType bkt_wroff = sizeof(sv_type), bkt_bufpos = 0, prev_bkt = 0, prev_hash = (SizeType)-1;
        bucketsBuf[bkt_bufpos++] = cur_off;
        // if might me better to write many zeroes here
        f.seek(cur_off, SEEK_SET);
        TVector<std::pair<TKeyType, TMappedType>> currentBlock;
        bool emptyFile = true;
        size_t prevRecSize = 0;
        // seek forward
        while (true) {
            const TIoRec* rec = Srt.Next();
            if (currentBlock.empty() && !emptyFile) {
                if (rec && prev_hash == rec->HashVal) {
                    Merge(currentBlock, prevRecSize);
                } else {
                    // if there is only one record with this hash, don't recode it, just write
                    PutRecord(CurrentBlockRecs.Data(), prevRecSize, f, cur_off);
                    sv.num_elements++;
                }
            }
            if (!rec || prev_hash != rec->HashVal) {
                // write buckets table
                for (size_t i = 0; i < currentBlock.size(); ++i) {
                    TIoRec* ioRec = MakeIoRec(TValueType(currentBlock[i]));
                    PutRecord(ioRec->Buf, ioRec->RecSize, f, cur_off);
                }
                sv.num_elements += currentBlock.size();
                currentBlock.clear();
                CurrentBlockRecs.Clear();
                if (rec) {
                    prev_hash = rec->HashVal;
                }
            }
            // note: prev_bkt's semantics here is 'cur_bkt - 1', thus we are actually cycling
            //       until cur_bkt == rec->HashVal *inclusively*
            while (!rec || prev_bkt != (rec->HashVal >> FreeBits)) {
                bucketsBuf[bkt_bufpos++] = cur_off;
                if (bkt_bufpos == bucketsBuf.size()) {
                    f.seek(bkt_wroff, SEEK_SET);
                    size_t sz = bkt_bufpos * sizeof(bucketsBuf[0]);
                    if (f.write(bucketsBuf.begin(), 1, sz) != sz)
                        throw yexception() << "could not write " << sz << " bytes to " << ~HashFileName;
                    bkt_wroff += sz;
                    bkt_bufpos = 0;
                    f.seek(cur_off, SEEK_SET);
                }
                prev_bkt++;
                if (!rec) {
                    break;
                }
                assert(prev_bkt < BucketCount);
            }
            if (!rec) {
                break;
            }
            emptyFile = false;
            CurrentBlockRecs.Append(rec->Buf, rec->RecSize);
            if (!currentBlock.empty()) {
                Merge(currentBlock, rec->RecSize);
            } else {
                prevRecSize = rec->RecSize;
            }
        }
        // finish buckets table
        f.seek(bkt_wroff, SEEK_SET);
        size_t sz = bkt_bufpos * sizeof(bucketsBuf[0]);
        if (sz && f.write(bucketsBuf.begin(), 1, sz) != sz)
            throw yexception() << "could not write " << sz << " bytes to " << ~HashFileName;
        bkt_wroff += sz;
        for (; prev_bkt < BucketCount; prev_bkt++)
            f.fput(cur_off);
        // finally write header
        sv.data_end_off = cur_off;
        f.seek(0, SEEK_SET);
        f.fput(sv);
        f.close();
    }

    void Close() {
        Srt.Close();
        if (+OurTmpDir)
            rmdir(~OurTmpDir);
    }
};
