#include <ydb/core/scheme/scheme_borders.h>

#include <ydb/core/scheme_types/scheme_types.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(SchemeBorders) {

    class THelper {
    public:
        explicit THelper(size_t keyCount) {
            Types.reserve(keyCount);
            for (size_t i = 0; i < keyCount; ++i) {
                Types.push_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint32));
            }
        }

        template<class... TArgs>
        THelper& Left(TArgs&&... args) {
            LeftKey.clear();
            Add(LeftKey, std::forward<TArgs>(args)...);
            return *this;
        }

        template<class... TArgs>
        THelper& Right(TArgs&&... args) {
            RightKey.clear();
            Add(RightKey, std::forward<TArgs>(args)...);
            return *this;
        }

        void AssertLt(EPrefixMode leftMode, EPrefixMode rightMode) const {
            int cmp = Compare(leftMode, rightMode);
            UNIT_ASSERT_C(cmp < 0,
                "expected "
                << DumpBorder(LeftKey, leftMode)
                << " LT "
                << DumpBorder(RightKey, rightMode)
                << ", got " << cmp);
        }

        void AssertGt(EPrefixMode leftMode, EPrefixMode rightMode) const {
            int cmp = Compare(leftMode, rightMode);
            UNIT_ASSERT_C(cmp > 0,
                "expected "
                << DumpBorder(LeftKey, leftMode)
                << " GT "
                << DumpBorder(RightKey, rightMode)
                << ", got " << cmp);
        }

        void AssertEq(EPrefixMode leftMode, EPrefixMode rightMode) const {
            int cmp = Compare(leftMode, rightMode);
            UNIT_ASSERT_C(cmp == 0,
                "expected "
                << DumpBorder(LeftKey, leftMode)
                << " EQ "
                << DumpBorder(RightKey, rightMode)
                << ", got " << cmp);
        }

    private:
        int Compare(EPrefixMode leftMode, EPrefixMode rightMode) const {
            return ComparePrefixBorders(Types, LeftKey, leftMode, RightKey, rightMode);
        }

        TString DumpBorder(const TVector<TCell>& key, EPrefixMode mode) const {
            TStringBuilder out;
            switch (mode) {
                case PrefixModeLeftBorderInclusive:
                    out << '[';
                    break;
                case PrefixModeLeftBorderNonInclusive:
                    out << '(';
                    break;
                default:
                    break;
            }
            out << '{';
            for (size_t i = 0; i < key.size() && i < Types.size(); ++i) {
                if (i != 0) {
                    out << ',' << ' ';
                }
                TString value;
                DbgPrintValue(value, key[i], Types[i]);
                out << value;
            }
            out << '}';
            switch (mode) {
                case PrefixModeRightBorderInclusive:
                    out << ']';
                    break;
                case PrefixModeRightBorderNonInclusive:
                    out << ')';
                    break;
                default:
                    break;
            }
            return out;
        }

    private:
        void Add(TVector<TCell>& key) {
            Y_UNUSED(key);
        }

        void Add(TVector<TCell>& key, std::nullptr_t) {
            key.emplace_back();
        }

        void Add(TVector<TCell>& key, ui32 value) {
            key.emplace_back(TCell::Make(value));
        }

        void Add(TVector<TCell>& key, int value) {
            key.emplace_back(TCell::Make(ui32(value)));
        }

        template<class TArg, class... TArgs>
        void Add(TVector<TCell>& key, TArg&& arg, TArgs&&... args) {
            Add(key, std::forward<TArg>(arg));
            Add(key, std::forward<TArgs>(args)...);
        }

    private:
        TVector<NScheme::TTypeInfo> Types;
        TVector<TCell> LeftKey;
        TVector<TCell> RightKey;
    };

    Y_UNIT_TEST(Full) {
        THelper test(1);

        // Shorter aliases
        auto li = PrefixModeLeftBorderInclusive;
        auto ln = PrefixModeLeftBorderNonInclusive;
        auto ri = PrefixModeRightBorderInclusive;
        auto rn = PrefixModeRightBorderNonInclusive;

        // Init the same left and right key
        test.Left(42).Right(42);

        // Compare left borders: first non-inclusive key is to the right
        test.AssertEq(li, li);
        test.AssertLt(li, ln);
        test.AssertGt(ln, li);
        test.AssertEq(ln, ln);

        // Compare right borders: first non-inclusive key is to the left
        test.AssertEq(ri, ri);
        test.AssertGt(ri, rn);
        test.AssertLt(rn, ri);
        test.AssertEq(rn, rn);

        // Compare intersections and gaps
        test.AssertEq(li, ri);
        test.AssertEq(ri, li);
        test.AssertLt(rn, li);
        test.AssertLt(ri, ln);
        test.AssertLt(rn, ln);
        test.AssertGt(li, rn);
        test.AssertGt(ln, ri);
        test.AssertGt(ln, rn);

        // Init with different keys
        test.Left(42).Right(52);

        // Don't bother with exhaustive checks, it boils down to key relations
        test.AssertLt(li, li);
    }

    Y_UNIT_TEST(Partial) {
        THelper test(3);

        // Shorter aliases
        auto li = PrefixModeLeftBorderInclusive;
        auto ln = PrefixModeLeftBorderNonInclusive;
        auto ri = PrefixModeRightBorderInclusive;
        auto rn = PrefixModeRightBorderNonInclusive;
        EPrefixMode allModes[4] = { li, ln, ri, rn };

        // Check different key cells, mode doesn't even matter
        test.Left(42).Right(51);
        for (EPrefixMode leftMode : allModes) {
            for (EPrefixMode rightMode : allModes) {
                test.AssertLt(leftMode, rightMode);
            }
        }

        // Check same prefix (boils down to left/right mode comparisons)
        test.Left(42).Right(42);
        test.AssertEq(li, li);
        test.AssertLt(li, ln);
        test.AssertGt(ln, li);
        test.AssertEq(ln, ln);
        test.AssertEq(ri, ri);
        test.AssertGt(ri, rn);
        test.AssertLt(rn, ri);
        test.AssertEq(rn, rn);
        test.AssertLt(li, ri);
        test.AssertGt(li, rn);
        test.AssertGt(ln, ri);
        test.AssertGt(ln, rn);
        test.AssertGt(ri, li);
        test.AssertLt(ri, ln);
        test.AssertLt(rn, li);
        test.AssertLt(rn, ln);

        // Check where one key has more cells than the other (expanded with null/+inf)
        test.Left(42).Right(42, 51);
        test.AssertLt(li, li);
        test.AssertLt(li, ln);
        test.AssertGt(ln, li);
        test.AssertGt(ri, ri);
        test.AssertGt(ri, rn);
        test.AssertLt(rn, ri);
        test.Left(42, 51).Right(42);
        test.AssertGt(li, li);
        test.AssertLt(li, ln);
        test.AssertGt(ln, li);
        test.AssertLt(ri, ri);
        test.AssertGt(ri, rn);
        test.AssertLt(rn, ri);

        // Check where one key has more cells that are null
        test.Left(42).Right(42, nullptr);
        test.AssertEq(li, li);
        test.AssertLt(li, ln);
        test.AssertGt(ln, li);
        test.AssertGt(ri, ri);
        test.AssertGt(ri, rn);
        test.AssertLt(rn, ri);
        test.AssertLt(li, ri);
        test.Left(42, nullptr).Right(42);
        test.AssertEq(li, li);
        test.AssertLt(li, ln);
        test.AssertGt(ln, li);
        test.AssertLt(ri, ri);
        test.AssertGt(ri, rn);
        test.AssertLt(rn, ri);
        test.AssertLt(li, ri);

        // Check where one key has full null suffix
        test.Left(42).Right(42, nullptr, nullptr);
        test.AssertEq(li, li);
        test.AssertGt(ri, ri);
        test.AssertEq(li, ri);
        test.AssertGt(ri, li);
        test.Left(42, nullptr, nullptr).Right(42);
        test.AssertEq(li, li);
        test.AssertLt(ri, ri);
        test.AssertLt(li, ri);
        test.AssertEq(ri, li);
    }
}

}
