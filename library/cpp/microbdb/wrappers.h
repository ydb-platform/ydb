#pragma once

#include "microbdb.h"

#define MAKEFILTERTMPL(TRecord, MemberFunc, NS) \
    template <typename T>                       \
    struct MemberFunc;                          \
    template <>                                 \
    struct MemberFunc<TRecord> {                \
        bool operator()(const TRecord* r) {     \
            return NS::MemberFunc(r);           \
        }                                       \
    }

#define MAKEJOINTMPL(TRecordA, TRecordB, MemberFunc, NS, TMergeType) \
    template <typename A, typename B>                                \
    struct MemberFunc;                                               \
    template <>                                                      \
    struct MemberFunc<TRecordA, TRecordB> {                          \
        int operator()(const TRecordA* l, const TRecordB* r) {       \
            return NS::MemberFunc(l, r);                             \
        }                                                            \
    };                                                               \
    typedef TMergeRec<TRecordA, TRecordB> TMergeType

#define MAKEJOINTMPL2(TRecordA, TRecordB, MemberFunc, StructName, TMergeType) \
    template <typename A, typename B>                                         \
    struct StructName;                                                        \
    template <>                                                               \
    struct StructName<TRecordA, TRecordB> {                                   \
        int operator()(const TRecordA* l, const TRecordB* r) {                \
            return MemberFunc(l, r);                                          \
        }                                                                     \
    };                                                                        \
    typedef TMergeRec<TRecordA, TRecordB> TMergeType

#define MAKEJOINTMPLLEFT(TRecordA, TRecordB, MemberFunc, NS, TMergeType) \
    template <typename A, typename B>                                    \
    struct MemberFunc;                                                   \
    template <>                                                          \
    struct MemberFunc<TRecordA, TRecordB> {                              \
        int operator()(const TRecordA* l, const TRecordB* r) {           \
            return NS::MemberFunc(l->RecA, r);                           \
        }                                                                \
    };                                                                   \
    typedef TMergeRec<TRecordA, TRecordB> TMergeType

template <class TRec>
class IDatNextSource {
public:
    virtual const TRec* Next() = 0;
    virtual void Work() {
    }
};

template <class TRec>
class IDatNextReceiver {
public:
    IDatNextReceiver(IDatNextSource<TRec>& source)
        : Source(source)
    {
    }

    virtual void Work() {
        Source.Work();
    }

protected:
    IDatNextSource<TRec>& Source;
};

template <class TInRec, class TOutRec>
class IDatNextChannel: public IDatNextReceiver<TInRec>, public IDatNextSource<TOutRec> {
public:
    IDatNextChannel(IDatNextSource<TInRec>& source)
        : IDatNextReceiver<TInRec>(source)
    {
    }

    virtual void Work() {
        IDatNextReceiver<TInRec>::Work();
    }
};

class IDatWorker {
public:
    virtual void Work() = 0;
};

template <class TRec>
class IDatPushReceiver {
public:
    virtual void Push(const TRec* rec) = 0;
    virtual void Work() = 0;
};

template <class TRec>
class IDatPushSource {
public:
    IDatPushSource(IDatPushReceiver<TRec>& receiver)
        : Receiver(receiver)
    {
    }

    virtual void Work() {
        Receiver.Work();
    }

protected:
    IDatPushReceiver<TRec>& Receiver;
};

template <class TInRec, class TOutRec>
class IDatPushChannel: public IDatPushReceiver<TInRec>, public IDatPushSource<TOutRec> {
public:
    IDatPushChannel(IDatPushReceiver<TOutRec>& receiver)
        : IDatPushSource<TOutRec>(receiver)
    {
    }

    virtual void Work() {
        IDatPushSource<TOutRec>::Work();
    }
};

template <class TRec>
class IDatNextToPush: public IDatNextReceiver<TRec>, public IDatPushSource<TRec> {
    typedef IDatNextReceiver<TRec> TNextReceiver;
    typedef IDatPushSource<TRec> TPushSource;

public:
    IDatNextToPush(IDatNextSource<TRec>& source, IDatPushReceiver<TRec>& receiver)
        : TNextReceiver(source)
        , TPushSource(receiver)
    {
    }

    virtual void Work() {
        const TRec* rec;
        while (rec = TNextReceiver::Source.Next())
            TPushSource::Receiver.Push(rec);
        TPushSource::Work();
        TNextReceiver::Work();
    }
};

template <class TRec>
class TDatNextPNSplitter: public IDatNextReceiver<TRec>, public IDatNextSource<TRec>, public IDatPushSource<TRec> {
public:
    TDatNextPNSplitter(IDatNextSource<TRec>& source, IDatPushReceiver<TRec>& receiver)
        : IDatNextReceiver<TRec>(source)
        , IDatNextSource<TRec>()
        , IDatPushSource<TRec>(receiver)
    {
    }

    const TRec* Next() {
        const TRec* rec = IDatNextReceiver<TRec>::Source.Next();
        if (rec) {
            IDatPushSource<TRec>::Receiver.Push(rec);
            return rec;
        } else {
            return 0;
        }
    }

    virtual void Work() {
        IDatNextReceiver<TRec>::Work();
        IDatPushSource<TRec>::Work();
    }
};

template <class TRec, class TOutRecA = TRec, class TOutRecB = TRec>
class TDatPushPPSplitter: public IDatPushReceiver<TRec>, public IDatPushSource<TOutRecA>, public IDatPushSource<TOutRecB> {
public:
    TDatPushPPSplitter(IDatPushReceiver<TOutRecA>& receiverA, IDatPushReceiver<TOutRecB>& receiverB)
        : IDatPushSource<TOutRecA>(receiverA)
        , IDatPushSource<TOutRecB>(receiverB)
    {
    }

    void Push(const TRec* rec) {
        IDatPushSource<TOutRecA>::Receiver.Push(rec);
        IDatPushSource<TOutRecB>::Receiver.Push(rec);
    }

    void Work() {
        IDatPushSource<TOutRecA>::Work();
        IDatPushSource<TOutRecB>::Work();
    }
};

template <class TRec>
class TFastInDatFile: public TInDatFile<TRec>, public IDatNextSource<TRec> {
public:
    typedef TInDatFile<TRec> Base;

    TFastInDatFile(const char* name, bool open = true, size_t pages = dbcfg::fbufsize, int pagesOrBytes = 0)
        : TInDatFile<TRec>(name, pages, pagesOrBytes)
        , FileName(name)
    {
        if (open)
            Base::Open(name);
    }

    void Open() {
        Base::Open(FileName);
    }

    template <class TPassRec>
    bool PassToUid(const TRec* inrec, const TPassRec* torec) {
        inrec = Base::Current();
        while (inrec && CompareUids(inrec, torec) < 0)
            inrec = Base::Next();
        return (inrec && CompareUids(inrec, torec) == 0);
    }

    void Work() {
        Base::Close();
    }

    const TRec* Next() {
        return Base::Next();
    }

private:
    TString FileName;
};

template <class TRec>
class TPushOutDatFile: public TOutDatFile<TRec>, public IDatPushReceiver<TRec> {
public:
    typedef TOutDatFile<TRec> Base;

    TPushOutDatFile(const char* name, bool open = true)
        : Base(name, dbcfg::pg_docuid, dbcfg::fbufsize, 0)
        , FileName(name)
    {
        if (open)
            Base::Open(name);
    }

    void Open() {
        Base::Open(~FileName);
    }

    void Push(const TRec* rec) {
        Base::Push(rec);
    }

    void Work() {
        Base::Close();
    }

private:
    TString FileName;
};

template <class TRec>
class TNextOutDatFile: public IDatNextToPush<TRec> {
public:
    typedef IDatNextToPush<TRec> TBase;

    TNextOutDatFile(const char* name, IDatNextSource<TRec>& source, bool open = true)
        : TBase(source, File)
        , File(name, open)
    {
    }

    void Open() {
        File.Open();
    }

private:
    TPushOutDatFile<TRec> File;
};

template <class TVal, template <typename T> class TCompare>
class TNextDatSorterMemo: public TDatSorterMemo<TVal, TCompare>, public IDatNextChannel<TVal, TVal> {
    typedef TDatSorterMemo<TVal, TCompare> TImpl;

public:
    TNextDatSorterMemo(IDatNextSource<TVal>& source, const char* dir = dbcfg::fname_temp, const char* name = "yet another sorter", size_t memory = dbcfg::small_sorter_size, size_t pagesize = dbcfg::pg_docuid, size_t pages = dbcfg::fbufsize, int pagesOrBytes = 0)
        : TImpl(name, memory, pagesize, pages, pagesOrBytes)
        , IDatNextChannel<TVal, TVal>(source)
        , Sorted(false)
    {
        TImpl::Open(dir);
    }

    void Sort() {
        const TVal* rec;
        while (rec = IDatNextChannel<TVal, TVal>::Source.Next()) {
            TImpl::Push(rec);
        }
        TImpl::Sort();
        Sorted = true;
    }

    const TVal* Next() {
        if (!Sorted)
            Sort();
        return TImpl::Next();
    }

private:
    bool Sorted;
    TString Dir;
};

template <class TInRec, class TOutRec>
class TDatConverter: public IDatNextChannel<TInRec, TOutRec> {
public:
    TDatConverter(IDatNextSource<TInRec>& source)
        : IDatNextChannel<TInRec, TOutRec>(source)
    {
    }

    virtual void Convert(const TInRec& inrec, TOutRec& outrec) {
        outrec(inrec);
    }

    const TOutRec* Next() {
        const TInRec* rec = IDatNextChannel<TInRec, TOutRec>::Source.Next();
        if (!rec)
            return 0;
        Convert(*rec, CurrentRec);
        return &CurrentRec;
    }

private:
    TOutRec CurrentRec;
};

template <class TRecA, class TRecB>
class TMergeRec {
public:
    const TRecA* RecA;
    const TRecB* RecB;
};

enum NMergeTypes {
    MT_JOIN = 0,
    MT_ADD = 1,
    MT_OVERWRITE = 2,
    MT_TYPENUM
};

template <class TRecA, class TRecB, template <typename TA, typename TB> class TCompare>
class TNextDatMerger: public IDatNextReceiver<TRecA>, public IDatNextReceiver<TRecB>, public IDatNextSource<TMergeRec<TRecA, TRecB>> {
public:
    TNextDatMerger(IDatNextSource<TRecA>& sourceA, IDatNextSource<TRecB>& sourceB, ui8 mergeType)
        : IDatNextReceiver<TRecA>(sourceA)
        , IDatNextReceiver<TRecB>(sourceB)
        , MergeType(mergeType)
        , MoveA(false)
        , MoveB(false)
        , NotInit(true)
    {
    }

    const TMergeRec<TRecA, TRecB>* Next() {
        if (MoveA || NotInit)
            SourceARec = IDatNextReceiver<TRecA>::Source.Next();
        if (MoveB || NotInit)
            SourceBRec = IDatNextReceiver<TRecB>::Source.Next();
        NotInit = false;

        //        Cout << "Next " << SourceARec->HostId << "\t" << SourceBRec->HostId << "\t" << TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) << "\t" << ::compare(SourceARec->HostId, SourceBRec->HostId) << "\t" << ::compare(1, 2) << "\t" << ::compare(2,1) << Endl;
        if (MergeType == MT_ADD && SourceARec && (!SourceBRec || TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) < 0)) {
            MergeRec.RecA = SourceARec;
            MergeRec.RecB = 0;
            MoveA = true;
            MoveB = false;
            return &MergeRec;
        }

        if (MergeType == MT_ADD && SourceBRec && (!SourceARec || TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) < 0)) {
            MergeRec.RecA = 0;
            MergeRec.RecB = SourceBRec;
            MoveA = false;
            MoveB = true;
            return &MergeRec;
        }

        if (MergeType == MT_ADD && SourceARec && SourceBRec && TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) == 0) {
            MergeRec.RecA = SourceARec;
            MergeRec.RecB = SourceBRec;
            MoveA = true;
            MoveB = true;
            return &MergeRec;
        }

        while (MergeType == MT_JOIN && SourceARec && SourceBRec && TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) != 0) {
            while (SourceARec && TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) < 0) {
                SourceARec = IDatNextReceiver<TRecA>::Source.Next();
            }
            while (SourceARec && SourceBRec && TCompare<TRecA, TRecB>()(SourceARec, SourceBRec) > 0) {
                SourceBRec = IDatNextReceiver<TRecB>::Source.Next();
            }
        }

        if (MergeType == MT_JOIN && SourceARec && SourceBRec) {
            MergeRec.RecA = SourceARec;
            MergeRec.RecB = SourceBRec;
            MoveA = true;
            MoveB = true;
            return &MergeRec;
        }

        MergeRec.RecA = 0;
        MergeRec.RecB = 0;
        return 0;
    }

    void Work() {
        IDatNextReceiver<TRecA>::Source.Work();
        IDatNextReceiver<TRecB>::Source.Work();
    }

private:
    TMergeRec<TRecA, TRecB> MergeRec;
    const TRecA* SourceARec;
    const TRecB* SourceBRec;
    ui8 MergeType;
    bool MoveA;
    bool MoveB;
    bool NotInit;
};

/*template<class TRec, class TSource, template <typename T> class TCompare, class TReceiver = TPushOutDatFile<TRec> >
class TPushDatMerger {
public:
    TPushDatMerger(TSource& source, TReceiver& receiver, ui8 mergeType)
        : Source(source)
        , Receiver(receiver)
        , MergeType(mergeType)
    {
    }

    virtual void Init() {
        SourceRec = Source.Next();
    }

    virtual void Push(const TRec* rec) {
        while (SourceRec && TCompare<TRec>()(SourceRec, rec, 0) < 0) {
            if (MergeType == MT_OVERWRITE || MergeType == MT_ADD)
                Receiver.Push(SourceRec);
            SourceRec = Source.Next();
        }

        bool intersected = false;
        while (SourceRec && TCompare<TRec>()(SourceRec, rec, 0) == 0) {
            intersected = true;
            if (MergeType == MT_ADD)
                Receiver.Push(SourceRec);
            SourceRec = Source.Next();
        }

        if (intersected && MergeType == MT_JOIN)
            Receiver.Push(rec);

        if (MergeType == MT_OVERWRITE || MergeType == MT_ADD)
            Receiver.Push(rec);
    }

    virtual void Term() {
        if (MergeType == MT_OVERWRITE || MergeType == MT_ADD) {
            while (SourceRec) {
                Receiver.Push(SourceRec);
                SourceRec = Source.Next();
            }
        }
    }

private:
    TSource&                Source;
    const TRec*             SourceRec;
    TReceiver&              Receiver;
    ui8                     MergeType;
};*/

/*template <class TRec, class TSourceA, class TSourceB, template <typename T> class TCompare, class TReceiver = TPushOutDatFile<TRec> >
class TNextDatMerger: public TPushDatMerger<TRec, TSourceA, TCompare, TReceiver> {
    typedef TPushDatMerger<TRec, TSourceA, TCompare, TReceiver> TImpl;
public:
    TNextDatMerger(TSourceA& sourceA, TSourceB& sourceB, TReceiver& receiver, ui8 mergeType)
        : TImpl(sourceA, receiver, mergeType)
        , SourceB(sourceB)
    {
    }

    virtual void Work() {
        TImpl::Init();
        while (SourceBRec = SourceB.Next()) {
            TImpl::Push(SourceBRec);
        }
        TImpl::Term();
    }
private:
    TSourceB&               SourceB;
    const TRec*             SourceBRec;
};*/

/*template <class TRec, template <typename T> class TCompare, class TReceiver = TPushOutDatFile<TRec> >
class TFilePushDatMerger: public TPushDatMerger<TRec, TFastInDatFile<TRec>, TCompare, TReceiver> {
    typedef TPushDatMerger<TRec, TFastInDatFile<TRec>, TCompare, TReceiver> TImpl;
public:
    TFilePushDatMerger(const char* name, TReceiver& receiver, ui8 mergeType)
        : TImpl(SourceFile, receiver, mergeType)
        , SourceFile(name)
    {
    }

    virtual void Push(const TRec* rec) {
        TImpl::Push(rec);
    }

    virtual void Term() {
        TImpl::Term();
    }
private:
    TFastInDatFile<TRec>        SourceFile;
};*/

/*template <class TRec, template <typename T> class TCompare, class TReceiver = TPushOutDatFile<TRec> >
class TFileNextDatMerger: public TNextDatMerger<TRec, TFastInDatFile<TRec>, TFastInDatFile<TRec>, TCompare, TReceiver> {
    typedef TNextDatMerger<TRec, TFastInDatFile<TRec>, TFastInDatFile<TRec>, TCompare, TReceiver> TImpl;
public:
    TFileNextDatMerger(const char* sourceAname, const char* sourceBname, TReceiver& receiver, ui8 mergeType)
        : TImpl(FileA, FileB, receiver, mergeType)
        , FileA(sourceAname)
        , FileB(sourceBname)
    {
    }

    virtual void Work() {
        TImpl::Work();
    }
private:
    TFastInDatFile<TRec>      FileA;
    TFastInDatFile<TRec>      FileB;
};*/

template <class TRec, template <typename T> class TPredicate>
class TDatNextFilter: public IDatNextChannel<TRec, TRec> {
public:
    TDatNextFilter(IDatNextSource<TRec>& source)
        : IDatNextChannel<TRec, TRec>(source)
    {
    }

    virtual const TRec* Next() {
        const TRec* rec;
        while ((rec = IDatNextChannel<TRec, TRec>::Source.Next()) != 0 && !Check(rec)) {
        }
        if (!rec)
            return 0;
        return rec;
    }

protected:
    virtual bool Check(const TRec* rec) {
        return TPredicate<TRec>()(rec);
    }
};

template <class TRec, template <typename T> class TPredicate>
class TDatPushFilter: public IDatPushChannel<TRec, TRec> {
public:
    TDatPushFilter(IDatPushReceiver<TRec>& receiver)
        : IDatPushChannel<TRec, TRec>(receiver)
    {
    }

    virtual void Push(const TRec* rec) {
        if (Check(rec))
            IDatPushChannel<TRec, TRec>::Receiver.Push(rec);
    }

private:
    virtual bool Check(const TRec* rec) {
        return TPredicate<TRec>()(rec);
    }
};

template <class TInRec, class TOutRec, template <typename T> class TCompare>
class TDatGrouper: public IDatNextChannel<TInRec, TOutRec> {
public:
    TDatGrouper(IDatNextSource<TInRec>& source)
        : IDatNextChannel<TInRec, TOutRec>(source)
        , Begin(true)
        , Finish(false)
        , HasOutput(false)
    {
    }

    const TOutRec* Next() {
        while (CurrentRec = IDatNextChannel<TInRec, TOutRec>::Source.Next()) {
            int cmp = 0;
            if (Begin) {
                Begin = false;
                OnStart();
            } else if ((cmp = TCompare<TInRec>()(CurrentRec, LastRec, 0)) != 0) {
                OnFinish();
                OnStart();
            }
            OnRecord();
            LastRec = CurrentRec;
            if (HasOutput) {
                HasOutput = false;
                return &OutRec;
            }
        }
        if (!Finish)
            OnFinish();
        Finish = true;
        if (HasOutput) {
            HasOutput = false;
            return &OutRec;
        }
        return 0;
    }

protected:
    virtual void OnStart() = 0;
    virtual void OnRecord() = 0;
    virtual void OnFinish() = 0;

    const TInRec* CurrentRec;
    const TInRec* LastRec;
    TOutRec OutRec;

    bool Begin;
    bool Finish;
    bool HasOutput;
};
