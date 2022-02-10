#include "snap_vec.h"
#include <ydb/core/blobstorage/base/utility.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

// change to Cerr if you want logging
#define STR Cnull

namespace NKikimr {

    /////////////////////////////////////////////////////////////////////////////////////////
    // TSVec Tests
    /////////////////////////////////////////////////////////////////////////////////////////
    Y_UNIT_TEST_SUITE(TSVecTest) {

        struct TBundle {
            TBundle() = default;
            TBundle(int v)
                : Element(v)
            {}

            void Output(IOutputStream &str) const {
                str << Element;
            }

            int GetValue() const {
                return Element;
            }

            ui64 GetSize() const {
                return 1;
            }

            ui64 SizeApproximation() const {
                return sizeof(Element);
            }

            int Element = 0;
        };

        void CheckSnap(const TSVecSnap<TBundle> &snap, const TVector<int> &target) {
            TSVecSnap<TBundle>::TIterator it(&snap);
            TVector<int> res;
            it.SeekToFirst();
            while (it.Valid()) {
                STR << it.Get()->GetValue() << " ";
                res.push_back(it.Get()->GetValue());
                it.Next();
            }
            STR << "\n";
            UNIT_ASSERT_EQUAL(res, target);
        }


        Y_UNIT_TEST(Basic) {
            TSVec<TBundle> svec(4);

            svec.Add(std::make_shared<TBundle>(1));
            svec.Add(std::make_shared<TBundle>(2));
            svec.Add(std::make_shared<TBundle>(3));

            auto snap1 = svec.GetSnapshot();
            UNIT_ASSERT_EQUAL(snap1.GetSize(), 3);
            CheckSnap(snap1, TVector<int>({1, 2, 3}));
            UNIT_ASSERT_EQUAL(svec.SizeApproximation(), 3 * sizeof(int));

            // rebuild happens
            svec.Add(std::make_shared<TBundle>(4));
            svec.Add(std::make_shared<TBundle>(5));
            svec.Add(std::make_shared<TBundle>(6));
            svec.Add(std::make_shared<TBundle>(7));

            CheckSnap(snap1, TVector<int>({1, 2, 3}));

            auto snap2 = svec.GetSnapshot();
            UNIT_ASSERT_EQUAL(snap2.GetSize(), 7);
            CheckSnap(snap2, TVector<int>({1, 2, 3, 4, 5, 6, 7}));
            UNIT_ASSERT_EQUAL(svec.SizeApproximation(), 7 * sizeof(int));

            svec.RemoveFirstElements(5);
            UNIT_ASSERT_EQUAL(svec.SizeApproximation(), 2 * sizeof(int));
            auto snap3 = svec.GetSnapshot();
            CheckSnap(snap1, TVector<int>({1, 2, 3}));
            CheckSnap(snap2, TVector<int>({1, 2, 3, 4, 5, 6, 7}));
            CheckSnap(snap3, TVector<int>({6, 7}));
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    // TSTree Tests
    /////////////////////////////////////////////////////////////////////////////////////////
    Y_UNIT_TEST_SUITE(TSTreeTest) {

        struct TCtx {
        };

        struct TBundle {
            TBundle() = default;
            TBundle(int v) {
                Vec.push_back(v);
            }

            void Output(IOutputStream &str) const {
                str << FormatList(Vec);
            }

            void Append(const TBundle &v) {
                AppendToVector(Vec, v.Vec);
            }

            ui64 GetSize() const {
                return Vec.size();
            }

            ui64 SizeApproximation() const {
                return sizeof(int) * Vec.size();
            }

            static std::shared_ptr<TBundle> Merge(const TCtx &ctx, const TVector<std::shared_ptr<TBundle>> &v) {
                Y_UNUSED(ctx);
                std::shared_ptr<TBundle> result = std::make_shared<TBundle>();
                for (const auto &item : v) {
                    result->Append(*item);
                }
                return result;
            }

            TVector<int> Vec;
        };

        static void SnapshotPrinter(IOutputStream &str, const std::shared_ptr<TBundle> &val) {
            val->Output(str);
        }

        void DebugPrint(const TSTreeSnap<TBundle, TCtx> &snap, const TString &message) {
            TStringStream str;
            snap.Output<>(str, SnapshotPrinter);
            STR << "====================================\n";
            STR << message << "\n";
            STR << str.Str();
            STR << "\n";
        }

        void CheckSnap(const TSTreeSnap<TBundle, TCtx> &snap, const TString &canonized) {
            TStringStream str;
            snap.Output<>(str, SnapshotPrinter);
            TString res = str.Str();
            if (res != canonized) {
                STR << "#1##" << res << "#1##\n";
                STR << "#2##" << canonized << "#2##\n";
                if (res.length() != canonized.length()) {
                    STR << "Size is different: res.length()# " << res.length()
                        << " canonized.length()# " << canonized.length() << "\n";
                }
                size_t len = Min(res.length(), canonized.length());
                for (size_t i = 0; i < len; ++i) {
                    if (res[i] != canonized[i]) {
                        STR << "found diff in " << i << "character: canonized[i]# "
                            << int(canonized[i]) << " res[i]# " << int(res[i]) << "\n";
                    }
                }
            }
            UNIT_ASSERT(res == canonized);
        }

        Y_UNIT_TEST(Basic) {
            TCtx ctx;
            TSTree<TBundle, TCtx> stree(ctx, 4, 4, 4);
            std::unique_ptr<TSTreeSnap<TBundle, TCtx>> intermedSnap;

            TString intermedCanonized = "Staging: \n"
                "Level# 0 Value# [50 51 52 53 54]\n"
                "Level# 0 Value# [55 56 57 58 59]\n"
                "Level# 0 Value# [60 61 62 63 64]\n"
                "Level# 0 Value# [65 66 67 68 69]\n"
                "Level# 1 Value# [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24]\n"
                "Level# 1 Value# [25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49]\n";

            TString lastCanonized = "Staging: [100][101]\n"
                "Level# 1 Value# [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24]\n"
                "Level# 1 Value# [25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49]\n"
                "Level# 1 Value# [50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74]\n"
                "Level# 1 Value# [75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99]\n";

            for (int i = 0; i < 102; ++i) {
                stree.Add(std::make_shared<TBundle>(i));
                std::shared_ptr<ISTreeCompaction> cjob = stree.Compact();
                if (cjob) {
                    STR << "compaction\n";
                    cjob->Work();
                    stree.ApplyCompactionResult(cjob);
                }
                UNIT_ASSERT_EQUAL(stree.SizeApproximation(), 4u * (i + 1));
                if (i == 69) {
                    intermedSnap = std::make_unique<TSTreeSnap<TBundle, TCtx>>(stree.GetSnapshot());
                    UNIT_ASSERT_EQUAL(stree.SizeApproximation(), 4 * 5 * 4 + 25 * 4 + 25 * 4);
                    DebugPrint(*intermedSnap, "Intermed (at time of creation)");
                    CheckSnap(*intermedSnap, intermedCanonized);
                }
            }

            STR << "CompactionStat: ";
            stree.GetCompactionStat().Output(STR);
            STR << "\n";

            {
                // Check intermed snapshot
                DebugPrint(*intermedSnap, "Intermed");
                CheckSnap(*intermedSnap, intermedCanonized);
            }

            {
                // check last snapshot
                auto lastSnap = stree.GetSnapshot();
                DebugPrint(lastSnap, "Last");
                CheckSnap(lastSnap, lastCanonized);
            }
        }
    }
} // NKikimr
