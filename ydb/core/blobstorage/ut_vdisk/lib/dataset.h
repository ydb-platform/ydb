#pragma once

#include "defs.h"

#include <util/generic/ptr.h>

extern const ui64 DefaultTestTabletId;

TString CreateData(const TString &orig, ui32 minHugeBlobSize, bool huge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ABOUT DATA GENERATION FOR TESTS
// TDataItem
// ~~~~~~~~~
// This struct describes one item we are going to put to the database
//
// IDataSet
// ~~~~~~~~
// This is a base abstract class for all datasets we provide for tests. All data sets must be
// inherited from IDataSet.
//
// TVectorDataSet
// ~~~~~~~~~~~~~~
// This class is good for small functional tests, we have a TVector under.
//
// TGeneratedDataSet
// ~~~~~~~~~~~~~~~~~
// This class incapsulates generated dataset (via IDataGenerator)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDataItem
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct TDataItem {
    NKikimr::TLogoBlobID Id;
    TString Data;
    NKikimrBlobStorage::EPutHandleClass HandleClass;

    TDataItem(const NKikimr::TLogoBlobID &id, const TString &data, NKikimrBlobStorage::EPutHandleClass cls)
        : Id(id)
        , Data(std::move(data))
        , HandleClass(cls)
    {}

    TDataItem() = default;
    TDataItem(const TDataItem& other) = default;
    TDataItem(TDataItem&& other) = default;
    TDataItem& operator =(const TDataItem& other) = default;

};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class IDataSet : public TAtomicRefCount<IDataSet> {
public:
    struct TIterator {
        virtual bool IsValid() const = 0;
        virtual const TDataItem *Get() const = 0;
        virtual void Next() = 0;
        virtual ~TIterator() {}
    };

    // normally we go through the whole dataset from begin to the end
    virtual TAutoPtr<TIterator> First() const = 0;

    // sometimes it's usefull to convert a dataset to a vector to have random access to elements
    virtual const TVector<TDataItem> &ToVector() const = 0;

    IDataSet() = default;
    virtual ~IDataSet() {}
};

using IDataSetPtr = TIntrusivePtr<IDataSet>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TVectorDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TVectorDataSet : public IDataSet {
public:
    virtual TAutoPtr<IDataSet::TIterator> First() const override {
        return new TMyIterator(Items);
    }

    virtual const TVector<TDataItem> &ToVector() const override {
        return Items;
    }

protected:
    TVector<TDataItem> Items;

    struct TMyIterator : public IDataSet::TIterator {
        TMyIterator(const TVector<TDataItem> &items)
            : Cur(items.begin())
            , End(items.end())
        {}

        virtual bool IsValid() const override {
            return Cur != End;
        }

        virtual const TDataItem *Get() const override {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            return &*Cur;
        }

        virtual void Next() override {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            ++Cur;
        }

        TVector<TDataItem>::const_iterator Cur;
        TVector<TDataItem>::const_iterator End;
    };
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// IDataGenerator
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct IDataGenerator {
    virtual ~IDataGenerator() = default;

    // generate next item; returns false on eof (and no generated item)
    virtual bool Next(TDataItem& item) = 0;

    // factory member; clones iterator at its current state
    virtual IDataGenerator* Clone() = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TGeneratedDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TGeneratedDataSet : public IDataSet {
public:
    explicit TGeneratedDataSet(TAutoPtr<IDataGenerator> generator)
        : Generator(generator)
        , Items()
    {}

    virtual TAutoPtr<IDataSet::TIterator> First() const override {
        return new TMyIterator(Generator.Get());
    }

    virtual const TVector<TDataItem> &ToVector() const override {
        if (Items.empty()) {
            TMyIterator it(Generator.Get());
            while (it.IsValid()) {
                Items.push_back(*it.Get());
                it.Next();
            }
        }
        return Items;
    }

private:
    TAutoPtr<IDataGenerator> Generator;
    mutable TVector<TDataItem> Items;

    struct TMyIterator : public IDataSet::TIterator {
        TMyIterator(IDataGenerator *generator)
            : Generator(generator)
            , Cur()
            , Eof(true)
        {
            Eof = !Generator->Next(Cur);
        }

        virtual bool IsValid() const override {
            return !Eof;
        }
        virtual const TDataItem *Get() const override {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            return &Cur;
        }

        virtual void Next() override {
            Y_DEBUG_ABORT_UNLESS(IsValid());
            Eof = !Generator->Next(Cur);
        }

        IDataGenerator *Generator;
        TDataItem Cur;
        bool Eof;
    };
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSmallCommonDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSmallCommonDataSet : public TVectorDataSet {
public:
    TSmallCommonDataSet();
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCustomDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TCustomDataSet : public TVectorDataSet {
public:
    TCustomDataSet(ui64 tabletId, ui32 gen, ui32 channel, ui32 step, ui32 num, ui32 blobSize,
                   NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// T3PutDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class T3PutDataSet : public TVectorDataSet {
public:
    T3PutDataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// T1PutHandoff2DataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class T1PutHandoff2DataSet : public TVectorDataSet {
public:
    T1PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// T3PutHandoff2DataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class T3PutHandoff2DataSet : public TVectorDataSet {
public:
    T3PutHandoff2DataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateBlobGenerator
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
IDataGenerator* CreateBlobGenerator(ui64 maxCumSize, ui32 maxNumBlobs, ui32 minBlobSize, ui32 maxBlobSize,
        ui32 differentTablets, ui32 startingStep, TIntrusivePtr<NKikimr::TBlobStorageGroupInfo> info,
        TVector<NKikimr::TVDiskID> matchingVDisks, bool reuseData = false);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBadIdsDataSet
////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBadIdsDataSet : public TVectorDataSet {
public:
    TBadIdsDataSet(NKikimrBlobStorage::EPutHandleClass cls, ui32 minHugeBlobSize, bool huge);
};




