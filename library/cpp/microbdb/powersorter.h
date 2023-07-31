#pragma once

#include "safeopen.h"

#include <util/generic/vector.h>
#include <util/generic/deque.h>
#include <util/system/mutex.h>
#include <util/system/condvar.h>
#include <util/thread/pool.h>

template <
    class TRecord,
    template <typename T> class TCompare,
    class TSieve,
    class TMemoFile = TOutDatFile<TRecord>>
class TDatSorterBuf {
public:
    typedef TRecord TRec;
    typedef TVector<TRec*> TVectorType;
    typedef TMemoFile TMemo;
    typedef TCompare<TRecord> TComp;

public:
    TDatSorterBuf(size_t memory, size_t pageSize)
        : Memo("memo", pageSize, memory, 0)
        , Cur()
    {
        Memo.Open(nullptr);
        Memo.Freeze();
    }

    ~TDatSorterBuf() {
        Vector.clear();
        Memo.Close();
    }

    const TRec* Push(const TRec* v) {
        const TRec* u = Memo.Push(v);
        if (u)
            Vector.push_back((TRec*)u);
        return u;
    }

    const TRec* Next() {
        if (Ptr == Vector.end()) {
            if (Cur)
                TSieve::Sieve(Cur, Cur);
            Cur = nullptr;
        } else {
            Cur = *Ptr++;
            if (!TIsSieveFake<TSieve>::Result)
                while (Ptr != Vector.end() && TSieve::Sieve(Cur, *Ptr))
                    ++Ptr;
        }
        return Cur;
    }

    const TRec* Current() {
        return Cur;
    }

    size_t Size() {
        return Vector.size();
    }

    void Sort() {
        Ptr = Vector.begin();
        Cur = nullptr;

        MBDB_SORT_FUN(Vector.begin(), Vector.end(), TComp());
    }

    void Clear() {
        Vector.clear();
        Memo.Freeze();
        Ptr = Vector.begin();
        Cur = nullptr;
    }

private:
    TVectorType Vector;
    TMemo Memo;

    typename TVectorType::iterator
        Ptr;
    TRec* Cur;
};

template <
    class TRecord,
    class TInput,
    template <typename T> class TCompare,
    class TSieve>
class TDatMerger {
public:
    typedef TRecord TRec;
    typedef TCompare<TRecord> TComp;
    typedef TSimpleSharedPtr<TInput> TInputPtr;
    typedef TVector<TInputPtr> TInputVector;

public:
    ~TDatMerger() {
        Close();
    }

    void Init(const TInputVector& inputs) {
        Inputs = inputs;
        TVector<TInput*> v;
        for (int i = 0; i < Inputs.ysize(); ++i)
            v.push_back(Inputs[i].Get());
        HeapIter.Init(&v[0], v.size());
        if (!TIsSieveFake<TSieve>::Result)
            PNext = HeapIter.Next();
    }

    const TRec* Next() {
        if (TIsSieveFake<TSieve>::Result) {
            return HeapIter.Next();
        }

        if (!PNext) {
            if (PCur) {
                TSieve::Sieve(PCur, PCur);
                PCur = nullptr;
            }
            return nullptr;
        }

        PCur = &Cur;
        memcpy(PCur, PNext, SizeOf((const TRec*)PNext));

        do {
            PNext = HeapIter.Next();
        } while (PNext && TSieve::Sieve(PCur, PNext));

        return PCur;
    }

    const TRec* Current() {
        return (TIsSieveFake<TSieve>::Result ? HeapIter.Current() : PCur);
    }

    void Close() {
        Inputs.clear();
        HeapIter.Term();
    }

private:
    TInputVector Inputs;
    THeapIter<TRec, TInput, TComp> HeapIter;
    TRec Cur;
    TRec* PCur = nullptr;
    const TRec* PNext = nullptr;
};

class TPortionManager {
public:
    void Open(const char* tempDir) {
        TGuard<TMutex> guard(Mutex);
        TempDir = tempDir;
    }

    TString Next() {
        TGuard<TMutex> guard(Mutex);
        if (Portions == 0)
            DoOpen();
        TString fname = GeneratePortionFilename(Portions++);
        return fname;
    }

    void Close() {
        TGuard<TMutex> guard(Mutex);
        Portions = 0;
    }

private:
    void DoOpen() {
        if (MakeSorterTempl(PortionFilenameTempl, TempDir.data())) {
            PortionFilenameTempl[0] = 0;
            ythrow yexception() << "portion-manager: bad tempdir \"" << TempDir.data() << "\": " << LastSystemErrorText();
        }
    }

    TString GeneratePortionFilename(int i) {
        char str[FILENAME_MAX];
        snprintf(str, sizeof(str), PortionFilenameTempl, i);
        return TString(str);
    }

private:
    TMutex Mutex;

    TString TempDir;
    char PortionFilenameTempl[FILENAME_MAX] = {};
    int Portions = 0;
};

// A merger powered by threads
template <
    class TRecord,
    template <typename T> class TCompare,
    class TSieve,
    class TInput = TInDatFile<TRecord>,
    class TOutput = TOutDatFile<TRecord>>
class TPowerMerger {
public:
    typedef TRecord TRec;
    typedef TDatMerger<TRecord, TInput, TCompare, TSieve> TMerger;
    typedef TSimpleSharedPtr<TMerger> TMergerPtr;
    typedef TPowerMerger<TRecord, TCompare, TSieve, TInput, TOutput> TFileMerger;

    struct TMergePortionTask: public IObjectInQueue {
        TFileMerger* FileMerger;
        int Begin;
        int End;
        TString OutFname;

        TMergePortionTask(TFileMerger* fileMerger, int begin, int end, const TString& outFname)
            : FileMerger(fileMerger)
            , Begin(begin)
            , End(end)
            , OutFname(outFname)
        {
        }

        void Process(void*) override {
            THolder<TMergePortionTask> This(this);
            //fprintf(stderr, "MergePortion: (%i, %i, %s)\n", Begin, End, ~OutFname);
            FileMerger->MergePortion(Begin, End, OutFname);
        }
    };

public:
    TPowerMerger(const TSimpleSharedPtr<TThreadPool>& mtpQueue, const TSimpleSharedPtr<TPortionManager>& portMan,
                 int memory, int pageSize, bool autoUnlink)
        : MtpQueue(mtpQueue)
        , PortionManager(portMan)
        , Memory(memory)
        , PageSize(pageSize)
        , AutoUnlink(autoUnlink)
    {
    }

    TPowerMerger(const TSimpleSharedPtr<TThreadPool>& mtpQueue, const char* tempDir,
                 int memory, int pageSize, bool autoUnlink)
        : MtpQueue(mtpQueue)
        , PortionManager(new TPortionManager)
        , Memory(memory)
        , PageSize(pageSize)
        , AutoUnlink(autoUnlink)
    {
        PortionManager->Open(tempDir);
    }

    ~TPowerMerger() {
        Close();
    }

    void SetMtpQueue(const TSimpleSharedPtr<TThreadPool>& mtpQueue) {
        MtpQueue = mtpQueue;
    }

    void MergePortion(int begin, int end, const TString& outFname) {
        TMerger merger;
        InitMerger(merger, begin, end);

        TOutput out("mergeportion-tmpout", PageSize, BufSize, 0);
        out.Open(outFname.data());
        const TRec* rec;
        while ((rec = merger.Next()))
            out.Push(rec);
        out.Close();

        merger.Close();

        {
            TGuard<TMutex> guard(Mutex);
            UnlinkFiles(begin, end);
            Files.push_back(outFname);
            --Tasks;
            TaskFinishedCond.Signal();
        }
    }

    void Add(const TString& fname) {
        TGuard<TMutex> guard(Mutex);
        //        fprintf(stderr, "TPowerMerger::Add: %s\n", ~fname);
        Files.push_back(fname);
        if (InitialFilesEnd > 0)
            ythrow yexception() << "TPowerMerger::Add: no more files allowed";
    }

    void Merge(int maxPortions) {
        TGuard<TMutex> guard(Mutex);
        InitialFilesEnd = Files.ysize();
        if (!InitialFilesEnd)
            ythrow yexception() << "TPowerMerger::Merge: no files added";
        Optimize(maxPortions);
        MergeMT();
        InitMerger(Merger, CPortions, Files.ysize());
    }

    void Close() {
        TGuard<TMutex> guard(Mutex);
        Merger.Close();
        UnlinkFiles(CPortions, Files.ysize());
        InitialFilesEnd = CPortions = 0;
        Files.clear();
    }

    const TRec* Next() {
        return Merger.Next();
    }

    const TRec* Current() {
        return Merger.Current();
    }

    int FileCount() const {
        TGuard<TMutex> guard(Mutex);
        return Files.ysize();
    }

private:
    void InitMerger(TMerger& merger, int begin, int end) {
        TGuard<TMutex> guard(Mutex);
        TVector<TSimpleSharedPtr<TInput>> inputs;
        for (int i = begin; i < end; ++i) {
            inputs.push_back(new TInput("mergeportion-tmpin", BufSize, 0));
            inputs.back()->Open(Files[i]);
            //            fprintf(stderr, "InitMerger: %i, %s\n", i, ~Files[i]);
        }
        merger.Init(inputs);
    }

    void UnlinkFiles(int begin, int end) {
        TGuard<TMutex> guard(Mutex);
        for (int i = begin; i < end; ++i) {
            if (i >= InitialFilesEnd || AutoUnlink)
                unlink(Files[i].c_str());
        }
    }

    void Optimize(int maxPortions, size_t maxBufSize = 4u << 20) {
        TGuard<TMutex> guard(Mutex);
        maxPortions = std::min(maxPortions, Memory / PageSize - 1);
        maxBufSize = std::max((size_t)PageSize, maxBufSize);

        if (maxPortions <= 2) {
            FPortions = MPortions = 2;
            BufSize = PageSize;
            return;
        }

        int Portions = Files.ysize();
        if (maxPortions >= Portions) {
            FPortions = MPortions = Portions;
        } else if (((Portions + maxPortions - 1) / maxPortions) <= maxPortions) {
            while (((Portions + maxPortions - 1) / maxPortions) <= maxPortions)
                --maxPortions;
            MPortions = ++maxPortions;
            int total = ((Portions + MPortions - 1) / MPortions) + Portions;
            FPortions = (total % MPortions) ? (total % MPortions) : (int)MPortions;
        } else
            FPortions = MPortions = maxPortions;

        BufSize = std::min((size_t)(Memory / (MPortions + 1)), maxBufSize);
        //        fprintf(stderr, "Optimize: Portions=%i; MPortions=%i; FPortions=%i; Memory=%i; BufSize=%i\n",
        //                (int)Portions, (int)MPortions, (int)FPortions, (int)Memory, (int)BufSize);
    }

    void MergeMT() {
        TGuard<TMutex> guard(Mutex);
        do {
            int n;
            while ((n = Files.ysize() - CPortions) > MPortions) {
                int m = std::min((CPortions == 0 ? (int)FPortions : (int)MPortions), n);
                TString fname = PortionManager->Next();
                if (!MtpQueue->Add(new TMergePortionTask(this, CPortions, CPortions + m, fname)))
                    ythrow yexception() << "TPowerMerger::MergeMT: failed to add task";
                CPortions += m;
                ++Tasks;
            }
            if (Tasks > 0)
                TaskFinishedCond.Wait(Mutex);
        } while (Tasks > 0);
    }

private:
    TMutex Mutex;
    TCondVar TaskFinishedCond;

    TMerger Merger;
    TSimpleSharedPtr<TThreadPool> MtpQueue;
    TSimpleSharedPtr<TPortionManager> PortionManager;
    TVector<TString> Files;
    int Tasks = 0;
    int InitialFilesEnd = 0;
    int CPortions = 0;
    int MPortions = 0;
    int FPortions = 0;
    int Memory = 0;
    int PageSize = 0;
    int BufSize = 0;
    bool AutoUnlink = false;
};

// A sorter powered by threads
template <
    class TRecord,
    template <typename T> class TCompare,
    class TSieve = TFakeSieve<TRecord>,
    class TTmpInput = TInDatFile<TRecord>,
    class TTmpOutput = TOutDatFile<TRecord>>
class TPowerSorter {
public:
    typedef TPowerSorter<TRecord, TCompare, TSieve, TTmpInput, TTmpOutput> TSorter;
    typedef TRecord TRec;
    typedef TTmpOutput TTmpOut;
    typedef TTmpInput TTmpIn;
    typedef TDatSorterBuf<TRecord, TCompare, TSieve> TSorterBuf;
    typedef TCompare<TRecord> TComp;
    typedef TPowerMerger<TRecord, TCompare, TSieve, TTmpInput, TTmpOutput> TFileMerger;

    struct TSortPortionTask: public IObjectInQueue {
        TSorter* Sorter;
        TSorterBuf* SorterBuf;
        int Portion;

        TSortPortionTask(TSorter* sorter, TSorterBuf* sorterBuf, int portion)
            : Sorter(sorter)
            , SorterBuf(sorterBuf)
            , Portion(portion)
        {
        }

        void Process(void*) override {
            TAutoPtr<TSortPortionTask> This(this);
            //            fprintf(stderr, "SortPortion: %i\n", Portion);
            Sorter->SortPortion(SorterBuf);
        }
    };

    class TSorterBufQueue {
    private:
        TMutex Mutex;
        TCondVar Cond;
        TVector<TSimpleSharedPtr<TSorterBuf>> V;
        TDeque<TSorterBuf*> Q;

        int Memory, PageSize, MaxSorterBufs;

    public:
        TSorterBufQueue(int memory, int pageSize, int maxSorterBufs)
            : Memory(memory)
            , PageSize(pageSize)
            , MaxSorterBufs(maxSorterBufs)
        {
        }

        void Push(TSorterBuf* sb) {
            TGuard<TMutex> guard(Mutex);
            sb->Clear();
            Q.push_back(sb);
            Cond.Signal();
        }

        TSorterBuf* Pop() {
            TGuard<TMutex> guard(Mutex);
            if (!Q.size() && V.ysize() < MaxSorterBufs) {
                V.push_back(new TSorterBuf(Memory / MaxSorterBufs, PageSize));
                return V.back().Get();
            } else {
                while (!Q.size())
                    Cond.Wait(Mutex);
                TSorterBuf* t = Q.front();
                Q.pop_front();
                return t;
            }
        }

        void Clear() {
            TGuard<TMutex> guard(Mutex);
            Q.clear();
            V.clear();
        }

        void WaitAll() {
            TGuard<TMutex> guard(Mutex);
            while (Q.size() < V.size()) {
                Cond.Wait(Mutex);
            }
        }

        int GetMaxSorterBufs() const {
            return MaxSorterBufs;
        }
    };

public:
    TPowerSorter(const TSimpleSharedPtr<TThreadPool>& mtpQueue, size_t maxSorterBufs,
                 const char* name, size_t memory, size_t pageSize, size_t bufSize)
        : MaxSorterBufs(maxSorterBufs)
        , Name(name)
        , Memory(memory)
        , PageSize(pageSize)
        , BufSize(bufSize)
        , MtpQueue(mtpQueue)
        , PortionManager(new TPortionManager)
        , SBQueue(Memory, PageSize, MaxSorterBufs)
        , FileMerger(MtpQueue, PortionManager, Memory, PageSize, true)
    {
    }

    TPowerSorter(size_t maxSorterBufs,
                 const char* name, size_t memory, size_t pageSize, size_t bufSize)
        : MaxSorterBufs(maxSorterBufs)
        , Name(name)
        , Memory(memory)
        , PageSize(pageSize)
        , BufSize(bufSize)
        , PortionManager(new TPortionManager)
        , SBQueue(Memory, PageSize, maxSorterBufs)
        , FileMerger(MtpQueue, PortionManager, Memory, PageSize, true)
    {
    }

    TPowerSorter(const char* name, size_t memory, size_t pageSize, size_t bufSize)
        : MaxSorterBufs(5)
        , Name(name)
        , Memory(memory)
        , PageSize(pageSize)
        , BufSize(bufSize)
        , PortionManager(new TPortionManager)
        , SBQueue(Memory, PageSize, MaxSorterBufs)
        , FileMerger(MtpQueue, PortionManager, Memory, PageSize, true)
    {
    }

    ~TPowerSorter() {
        Close();
    }

    void Open(const char* tempDir) {
        Close();
        CurSB = SBQueue.Pop();
        PortionManager->Open(tempDir);
    }

    void Reopen(const char* fname) {
        Open(fname);
    }

    void Close() {
        CurSB = nullptr;
        SBQueue.Clear();
        PortionCount = 0;
        FileMerger.Close();
        PortionManager->Close();
    }

    const TRec* Push(const TRec* v) {
        CheckOpen("Push");
        const TRec* u = CurSB->Push(v);
        if (!u) {
            NextPortion();
            u = CurSB->Push(v);
        }
        return u;
    }

    void Sort(int maxPortions = 1000) {
        CheckOpen("Sort");
        if (!PortionCount) {
            CurSB->Sort();
        } else {
            NextPortion();
            SBQueue.Push(CurSB);
            CurSB = nullptr;
            SBQueue.WaitAll();
            SBQueue.Clear();
            FileMerger.Merge(maxPortions);
        }
    }

    const TRec* Next() {
        return PortionCount ? FileMerger.Next() : CurSB->Next();
    }

    const TRec* Current() {
        return PortionCount ? FileMerger.Current() : CurSB->Current();
    }

    int GetBufSize() const {
        return BufSize;
    }

    int GetPageSize() const {
        return PageSize;
    }

    const char* GetName() const {
        return Name.data();
    }

private:
    void CheckOpen(const char* m) {
        if (!CurSB)
            ythrow yexception() << "TPowerSorter::" << m << ": the sorter is not open";
    }

    void NextPortion() {
        if (!CurSB->Size())
            return;
        ++PortionCount;
        if (MaxSorterBufs <= 1) {
            SortPortion(CurSB);
        } else {
            if (!MtpQueue.Get()) {
                MtpQueue.Reset(new TThreadPool);
                MtpQueue->Start(MaxSorterBufs - 1);
                FileMerger.SetMtpQueue(MtpQueue);
            }
            if (!MtpQueue->Add(new TSortPortionTask(this, CurSB, PortionCount)))
                ythrow yexception() << "TPowerSorter::NextPortion: failed to add task";
        }
        CurSB = SBQueue.Pop();
    }

    void SortPortion(TSorterBuf* sorterBuf) {
        TString portionFilename = PortionManager->Next();
        try {
            sorterBuf->Sort();

            //            fprintf(stderr, "TPowerSorter::SortPortion: -> %s\n", ~portionFilename);
            TTmpOut out("powersorter-portion", PageSize, BufSize, 0);
            out.Open(portionFilename.data());

            while (sorterBuf->Next())
                out.Push(sorterBuf->Current());

            out.Close();
            FileMerger.Add(portionFilename);
            SBQueue.Push(sorterBuf);
        } catch (const yexception& e) {
            unlink(portionFilename.data());
            ythrow yexception() << "SortPortion: " << e.what();
        }
    }

private:
    int MaxSorterBufs = 0;
    TString Name;
    int Memory = 0;
    int PageSize = 0;
    int BufSize = 0;

    TMutex Mutex;
    TSimpleSharedPtr<TThreadPool> MtpQueue;
    TSimpleSharedPtr<TPortionManager> PortionManager;

    TSorterBufQueue SBQueue;
    TSorterBuf* CurSB = nullptr;
    int PortionCount = 0;

    TFileMerger FileMerger;
};
