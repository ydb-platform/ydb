#pragma once

#include <ydb/core/tablet_flat/test/libs/table/test_pretty.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tablet_flat/flat_update_op.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

// Unified interface for iterator
class ITestIterator {
public:
    virtual ~ITestIterator() {}

    virtual EReady Next(ENext mode) = 0;
    virtual bool IsValid() = 0;
    virtual bool IsRowDeleted() = 0;
    virtual TDbTupleRef GetKey() = 0;
    virtual TDbTupleRef GetValues() = 0;
    virtual TCell GetValue(ui32 idx) = 0;
};

// Unified interface for real DB implementation and simple reference implementation
class ITestDb {
public:
    using ECodec = NPage::ECodec;
    using ECache = NPage::ECache;

    virtual ~ITestDb() {}

    virtual void Init(const TScheme& scheme) = 0;
    virtual const TScheme& GetScheme() const = 0;
    virtual TString FinishTransaction(bool commit) = 0;
    virtual void Update(ui32 root, ERowOp, TRawVals key, TArrayRef<const TUpdateOp> ops) = 0;

    virtual void Precharge(ui32 root,
                           TRawVals keyFrom, TRawVals keyTo,
                           TTagsRef tags, ui32 flags) = 0;
    virtual ITestIterator* Iterate(ui32 root, TRawVals key, TTagsRef tags, ELookup) = 0;
    virtual void Apply(const TSchemeChanges&) = 0;
};

// Means that iterator needs pages to be loaded
class TIteratorNotReady : public yexception {
public:
    TIteratorNotReady() : yexception() {}
};

class TFlatDbIterator : public ITestIterator {
    TAutoPtr<TTableIter> Iter;

public:
    explicit TFlatDbIterator(TAutoPtr<TTableIter> iter)
        : Iter(iter)
    {}

    EReady Next(ENext mode) override
    {
        const auto ready = Iter->Next(mode);

        if (ready == EReady::Page)
            throw TIteratorNotReady();

        return ready;
    }

    bool IsValid() override {
        return Iter->Last() == EReady::Data;
    }

    bool IsRowDeleted() override {
        return Iter->Row().GetRowState() == ERowOp::Erase;
    }

    TDbTupleRef GetKey() override {
        return Iter->GetKey();
    }

    TDbTupleRef GetValues() override {
        return Iter->GetValues();
    }

    TCell GetValue(ui32 idx) override {
        return Iter->Row().Get(idx);
    }
};

class TFlatDbWrapper : public ITestDb {
private:
    TDatabase* Db;

public:

    TFlatDbWrapper()
        : Db(nullptr)
    {}

    void SetDb(TDatabase* db) {
        Db = db;
    }

    const TDatabase* GetDb() const {
        return Db;
    }

    void Init(const TScheme& scheme) override {
        Y_UNUSED(scheme);
        Y_ABORT_UNLESS("Not supported by flat db wrapper");
    }

    const TScheme& GetScheme() const override {
        return Db->GetScheme();
    }

    TString FinishTransaction(bool commit) override {
        Y_UNUSED(commit);
        Y_ABORT_UNLESS("Not supported by flat db wrapper");
        return "42";
    }

    void Update(ui32 root, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops) override
    {
        Db->Update(root, rop, key, ops);
    }

    void Precharge(ui32 root,
                           TRawVals keyFrom, TRawVals keyTo,
                           TTagsRef tags, ui32 flags) override {
        bool res = Db->Precharge(root, keyFrom, keyTo, tags, flags, -1, -1);
        if (!res)
            throw TIteratorNotReady();
    }

    ITestIterator* Iterate(ui32 root, TRawVals key, TTagsRef tags, ELookup mode) override {
        if (auto res = Db->Iterate(root, key, tags, mode))
            return new TFlatDbIterator(res);
        throw TIteratorNotReady();
    }

    void Apply(const TSchemeChanges &delta) override
    {
        Db->Alter().Merge(delta);
    }
};


// Create a simple refenrce DB implementation wrapped in ITestDB interface
TAutoPtr<ITestDb> CreateFakeDb();


// Iterator for TDbPair
class TIteratorPair : public ITestIterator {
    TAutoPtr<ITestIterator> It1;
    TAutoPtr<ITestIterator> It2;

public:
    TIteratorPair(TAutoPtr<ITestIterator> it1, TAutoPtr<ITestIterator> it2)
        : It1(it1)
        , It2(it2)
    {}

    EReady Next(ENext mode) override
    {
        const auto one = It1->Next(mode);
        const auto two = It2->Next(mode);
        UNIT_ASSERT(one == two);
        return one;
    }

    bool IsValid() override {
        return It1->IsValid();
    }

    bool IsRowDeleted() override {
        return It2->IsRowDeleted();
    }

    TDbTupleRef GetKey() override {
        TDbTupleRef keyTuple1 = It1->GetKey();
        TDbTupleRef keyTuple2 = It2->GetKey();
        VerifyEqualTuples(keyTuple1, keyTuple2);
        return keyTuple1;
    }

    TDbTupleRef GetValues() override {
        TDbTupleRef tuple1 = It1->GetValues();
        TDbTupleRef tuple2 = It2->GetValues();
        VerifyEqualTuples(tuple1, tuple2);
        return tuple1;
    }

    TCell GetValue(ui32 idx) override {
        TCell c1 = It1->GetValue(idx);
        TCell c2 = It2->GetValue(idx);
        UNIT_ASSERT(c1.IsNull() == c2.IsNull());
        if (!c1.IsNull()) {
            UNIT_ASSERT(c1.Size() == c2.Size());
            UNIT_ASSERT(0 == memcmp(c1.Data(), c2.Data(), c1.Size()));
        }
        return c1;
    }

private:
    static void VerifyEqualTuples(const TDbTupleRef& a, const TDbTupleRef& b) {
        TString aStr = PrintRow(a);
        TString bStr = PrintRow(b);
        UNIT_ASSERT_NO_DIFF(aStr, bStr);
    }
};

// Applies operations to 2 DBs and compares results
class TDbPair : public ITestDb {
private:
    ITestDb* Db1;
    ITestDb* Db2;

public:

    TDbPair(ITestDb& db1, ITestDb& db2)
        : Db1(&db1)
        , Db2(&db2)
    {}

    void Init(const TScheme& scheme) override {
        Db1->Init(scheme);
        Db2->Init(scheme);
    }

    const TScheme& GetScheme() const override {
        return Db1->GetScheme();
    }

    const ITestDb* GetDb1() const {
        return Db1;
    }

    const ITestDb* GetDb2() const {
        return Db2;
    }

    TString FinishTransaction(bool commit) override {
        auto res = Db1->FinishTransaction(commit);
        Db2->FinishTransaction(commit);
        return res;
    }

    void Update(ui32 root, ERowOp rop, TRawVals key, TArrayRef<const TUpdateOp> ops) override {
        Db1->Update(root, rop, key, ops);
        Db2->Update(root, rop, key, ops);
    }

    void Precharge(ui32 root,
                           TRawVals keyFrom, TRawVals keyTo,
                           TTagsRef tags, ui32 flags) override {
        Db1->Precharge(root, keyFrom, keyTo, tags, flags);
        Db2->Precharge(root, keyFrom, keyTo, tags, flags);
    }

    ITestIterator* Iterate(ui32 root, TRawVals key, TTagsRef tags, ELookup mode) override {
        return new TIteratorPair(
                    Db1->Iterate(root, key, tags, mode),
                    Db2->Iterate(root, key, tags, mode));
    }

    void Apply(const TSchemeChanges &delta) override
    {
        Db1->Apply(delta);
        Db2->Apply(delta);
    }

    bool CompareDBs() {
        bool res = true;
        for (auto& ti : Db1->GetScheme().Tables) {
            bool tabRes = CompareTables(ti.first);
            if (!tabRes) {
                Cerr << "Tables " << ti.first << " are different!" << Endl;
                res = false;
            }
        }
        return res;
    }

private:
    bool CompareTables(ui32 root) {
        TVector<ui32> valTags;
        for (const auto& ci : Db1->GetScheme().GetTableInfo(root)->Columns) {
            valTags.push_back(ci.first);
        }

        TAutoPtr<ITestIterator> it1 = Db1->Iterate(root, {}, valTags, ELookup::GreaterOrEqualThan);
        TAutoPtr<ITestIterator> it2 = Db2->Iterate(root, {}, valTags, ELookup::GreaterOrEqualThan);

        ui64 errors = 0;
        int cmp = 0;
        EReady rdy1 = EReady::Gone;
        EReady rdy2 = EReady::Gone;

        while (true) {
            TDbTupleRef key1, key2, val1, val2;

            if ((rdy1 = cmp <= 0 ? it1->Next(ENext::Data) : rdy1) == EReady::Data) {
                key1 = it1->GetKey();
                val1 = it1->GetValues();
            }

            if ((rdy2 = cmp >= 0 ? it2->Next(ENext::Data) : rdy2) == EReady::Data) {
                key2 = it2->GetKey();
                val2 = it2->GetValues();
            }

            if (rdy1 == rdy2 && rdy2 == EReady::Gone) {
                break;
            } else if (rdy1 == EReady::Data && rdy2 != EReady::Data) {
                cmp = -1;
            } else if (rdy1 != EReady::Data && rdy2 == EReady::Data) {
                cmp = 1;
            } else {
                cmp = CmpTupleRefs(key1, key2);
            }

            errors += (cmp != 0) ? 1 : 0;

            if (cmp > 0) {
                Cerr << "Missing row in DB #1:" << Endl
                     << PrintRow(key2) << " : "<< PrintRow(val2) << Endl << Endl;
            } else if (cmp < 0) {
                Cerr << "Missing row in DB #2:" << Endl
                     << PrintRow(key1) << " : "<< PrintRow(val1) << Endl << Endl;
            } else if (CmpTupleRefs(val1, val2) != 0) {
                errors++;
                Cerr << "Different values for key " << PrintRow(key1) << Endl;
                Cerr << PrintRow(val1) << Endl
                        << PrintRow(val2) << Endl << Endl;
            }
        }

        return errors == 0;
    }

    static int CmpTupleRefs(const TDbTupleRef &one, const TDbTupleRef &two) {
        const auto num = one.ColumnCount;

        if (num != two.ColumnCount) {
            Y_ABORT("Got different key columns count");
        } else if (!std::equal(one.Types, one.Types + num, two.Types)) {
            Y_ABORT("TDbTupleRef rows types vec are not the same");
        } else {
            return CompareTypedCellVectors(one.Columns, two.Columns, one.Types, num);
        }
    }
};

} // namspace NTabletFlatExecutor
} // namespace NKikimr
