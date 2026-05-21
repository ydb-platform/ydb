#include "kqp_json_index_predicate.h"

#include <fmt/format.h>
#include <util/random/mersenne.h>

namespace NKikimr::NKqp {

namespace {

struct TAtom {
    std::string Sql;
    std::function<void(NYdb::TParamsBuilder&)> AddParams;
    bool IsJsonIndexable = true;
};

class TPredicateBatchGenerator {
public:
    TPredicateBatchGenerator(const TJsonCorpus& corpus, bool isStrict, ui64 seed, const TPredicateBuilderOptions& opts)
        : Opts(opts), Rng(seed), Mode(isStrict ? "strict" : "lax"), Rows(corpus.Rows())
    {
        FillKeyPools();
    }

    std::vector<TBuiltPredicate> Build(size_t maxCount) {
        if (Opts.EnableJsonExists) {
            GenerateJsonExists();
        }

        if (Opts.EnableJsonValue) {
            GenerateJsonValue();
        }

        if (Opts.EnableJsonPathMethods) {
            GenerateJsonPathMethods();
        }

        if (Opts.EnablePassingVariables) {
            GeneratePassingVariables();
        }

        if (Opts.EnableSqlParameters) {
            GenerateSqlParameters();
        }

        if (Opts.EnableRangeComparisons) {
            GenerateRangeComparisons();
        }

        if (Opts.EnableBetween) {
            GenerateBetween();
        }

        if (Opts.EnableInList) {
            GenerateInList();
        }

        if (Opts.EnableJsonPathPredicates) {
            GenerateJsonPathPredicates();
        }

        if (Opts.EnableComplexJsonPathFilters) {
            GenerateComplexJsonPathFilters();
        }

        if (Opts.EnableJsonIsLiteral) {
            GenerateJsonIsLiteral();
        }

        if (Opts.EnableArithmeticOperators) {
            GenerateArithmeticOperators();
        }

        if (Opts.EnableSqlNullChecks) {
            GenerateSqlNullChecks();
        }

        if (Opts.EnableDistinctFrom) {
            GenerateDistinctFrom();
        }

        if (Opts.EnableNonJsonFilters) {
            GenerateNonJsonFilters();
        }

        return BuildResult(maxCount);
    }

private:
    void FillKeyPools() {
        for (const auto& row : Rows) {
            switch (row.Shape) {
                case EJsonShape::Scalar:
                    switch (row.Key % 6) {
                        case 0:
                            KeysWithScalarNull.push_back(row.Key);
                            break;
                        case 1:
                        case 2:
                            KeysWithScalarBoolean.push_back(row.Key);
                            break;
                        case 3:
                            KeysWithScalarNumber.push_back(row.Key);
                            KeysWithScalarInt.push_back(row.Key);
                            break;
                        case 4:
                            KeysWithScalarNumber.push_back(row.Key);
                            KeysWithScalarDouble.push_back(row.Key);
                            break;
                        case 5:
                            KeysWithScalarString.push_back(row.Key);
                            break;
                        default:
                            break;
                    }
                    break;

                case EJsonShape::FlatObj:
                    KeysWithUKey.push_back(row.Key);
                    KeysWithIntUVal.push_back(row.Key);
                    KeysWithShared.push_back(row.Key);
                    KeysWithFlatObj.push_back(row.Key);
                    KeysWithBothSharedAndU.push_back(row.Key);
                    FlatObjByRank[row.Key % 50].push_back(row.Key);
                    break;

                case EJsonShape::EmptyContainers:
                    KeysWithUKey.push_back(row.Key);
                    KeysWithShared.push_back(row.Key);
                    KeysWithEmptyContainers.push_back(row.Key);
                    break;

                case EJsonShape::EmptyKey:
                    KeysWithUKey.push_back(row.Key);
                    KeysWithStrUVal.push_back(row.Key);
                    break;

                case EJsonShape::ArrayLiterals:
                    KeysWithArrayLiterals.push_back(row.Key);
                    break;

                case EJsonShape::ObjWithArray:
                    KeysWithUKey.push_back(row.Key);
                    KeysWithUArr.push_back(row.Key);
                    KeysWithShared.push_back(row.Key);
                    KeysWithBothSharedAndU.push_back(row.Key);
                    break;

                case EJsonShape::ArrayOfArrays:
                    KeysWithArrayOfArrays.push_back(row.Key);
                    break;

                case EJsonShape::NestedObj:
                    KeysWithNestedObj.push_back(row.Key);
                    KeysWithShared.push_back(row.Key);
                    break;

                case EJsonShape::DeepNested:
                    KeysWithDeepNested.push_back(row.Key);
                    break;

                case EJsonShape::HomogeneousArrayObjs:
                    KeysWithHomoArr.push_back(row.Key);
                    break;

                case EJsonShape::HeterogeneousArrayObjs:
                    KeysWithHeteroArr.push_back(row.Key);
                    break;

                case EJsonShape::Mixed:
                    KeysWithMixed.push_back(row.Key);
                    KeysWithShared.push_back(row.Key);
                    break;

                case EJsonShape::ObjWithArrayObjs:
                    KeysWithItems.push_back(row.Key);
                    break;

                case EJsonShape::FullLiteralMix:
                    KeysWithFullMix.push_back(row.Key);
                    break;

                default:
                    break;
            }
        }
    }

    ui64 PickFrom(const std::vector<ui64>& pool) {
        if (!pool.empty()) {
            return pool[Rng.Uniform(pool.size())];
        }
        return Rows[Rng.Uniform(Rows.size())].Key;
    }

    ui64 RandKey() {
        return Rows[Rng.Uniform(Rows.size())].Key;
    }

    std::string NewPname() {
        return "$p" + std::to_string(ParamCount++);
    }

    static std::function<void(NYdb::TParamsBuilder&)> MergeAdd(
        std::function<void(NYdb::TParamsBuilder&)> a,
        std::function<void(NYdb::TParamsBuilder&)> b)
    {
        if (!a && !b) {
            return nullptr;
        }
        return [a = std::move(a), b = std::move(b)](NYdb::TParamsBuilder& bld) {
            if (a) {
                a(bld);
            }
            if (b) {
                b(bld);
            }
        };
    }

    TAtom AndAtom(const TAtom& a, const TAtom& b) {
        return TAtom{
            .Sql = "(" + a.Sql + ") AND (" + b.Sql + ")",
            .AddParams = MergeAdd(a.AddParams, b.AddParams),
            .IsJsonIndexable = a.IsJsonIndexable || b.IsJsonIndexable};
    }

    TAtom OrAtom(const TAtom& a, const TAtom& b) {
        return TAtom{
            .Sql = "(" + a.Sql + ") OR (" + b.Sql + ")",
            .AddParams = MergeAdd(a.AddParams, b.AddParams),
            .IsJsonIndexable = a.IsJsonIndexable && b.IsJsonIndexable};
    }

    void AddJ(std::string sql, std::function<void(NYdb::TParamsBuilder&)> addP = nullptr) {
        JsonAtoms.push_back(TAtom{
            .Sql = std::move(sql),
            .AddParams = std::move(addP),
            .IsJsonIndexable = true});
    }

    void AddJErr(std::string sql, std::function<void(NYdb::TParamsBuilder&)> addP = nullptr) {
        JsonAtoms.push_back(TAtom{
            .Sql = std::move(sql),
            .AddParams = std::move(addP),
            .IsJsonIndexable = false});
    }

    void GenerateJsonExists() {
        // Root key
        AddJ(fmt::format("JSON_EXISTS(Text, '{} $')", Mode));

        // Member access
        {
            for (size_t i = 0; i < 6; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}')", Mode, PickFrom(KeysWithUKey)));
            }

            for (size_t i = 0; i < 4; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}')", Mode, RandKey()));
            }

            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared') = true", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.nope_xyz')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.*')", Mode));

            for (int j = 0; j < 5; ++j) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.g5_{}')", Mode, j));
            }
        }

        // Array access
        {
            for (size_t i = 0; i < 3; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[*]')", Mode, PickFrom(KeysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[0]')", Mode, PickFrom(KeysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[last]')", Mode, PickFrom(KeysWithUArr)));
            }

            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[last - 1]')", Mode, PickFrom(KeysWithUArr)));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[0 to 1]')", Mode, PickFrom(KeysWithUArr)));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[0, 2]')", Mode, PickFrom(KeysWithUArr)));

            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared[*]')", Mode));
        }

        // Equality and inequality with literals
        {
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == null)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ != null)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == true)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == false)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ != null)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ != true)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ != false)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == {})')", Mode, PickFrom(KeysWithScalarNumber)));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ != {})')", Mode, PickFrom(KeysWithScalarNumber)));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == {})')", Mode, PickFrom(KeysWithScalarString)));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ != {})')", Mode, PickFrom(KeysWithScalarString)));
        }

        // Equality with member access
        {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == {1})')", Mode, k));
            }

            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1})')", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == "shared_v")'))", Mode));
            }

            for (size_t i = 0; i < 3; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank == {})')", Mode, (int)Rng.Uniform(50)));
            }

            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.* ? (@ == {1})')", Mode, k));
            }
        }

        // Equality with array access
        {
            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[1]')", Mode, PickFrom(KeysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[*] ? (@ == {})')", Mode, k, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == {2})')", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.u_{1}[last] ? (@ == "u_v_{1}")'))", Mode, k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[last - 1] ? (@ == {2})')", Mode, k, k + 1));
            }

            {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[0 to 1] ? (@ == {1})')", Mode, k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[0, 2] ? (@ == {1})')", Mode, k));
            }
        }

        // Equality with jsonpath AND/OR
        {
            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} && @.shared == "shared_v")'))", Mode, k));
            }

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.* == {1} && @.shared == "shared_v")'))", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} || @.shared == "shared_v")'))", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} || @.* == "shared_v")'))", Mode, k));
            }
        }

        // Equality and inequality member access
        {
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ == "shared_v")'))", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared ? (@ != null)')", Mode));
        }

        // All
        {
            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared.u_{}')", Mode, PickFrom(KeysWithNestedObj)));
                }

                for (int j = 0; j < 5; ++j) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared.g5_{}')", Mode, j));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == {1})')", Mode, k));
                }
            }

            if (!KeysWithDeepNested.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}')", Mode, PickFrom(KeysWithDeepNested)));
                }

                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.a.b.c')", Mode));
            }

            if (!KeysWithHomoArr.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*].u_{}')", Mode, PickFrom(KeysWithHomoArr)));
                }

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $[*].shared ? (@ == "v")'))", Mode));
            }

            if (!KeysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*].k_a ? (@ == {})')", Mode, k));
                }

                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*].k_b')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*].shared')", Mode));
            }

            if (!KeysWithMixed.empty()) {
                for (int j = 0; j < 5; ++j) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.g5_{}.deep')", Mode, j));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.g5_{}.deep.v')", Mode, j));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithMixed);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == {2})')", Mode, k % 5, k));
                }
            }

            if (!KeysWithItems.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items')", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithItems);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*].id ? (@ == {})')", Mode, k));
                }

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ == "shared_v")'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.items[*].* ? (@ == "shared_v")'))", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithItems);
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.items[*].name ? (@ == "u_v_{1}")'))", Mode, k));
                }
            }

            if (!KeysWithFullMix.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_s')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_b')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_null')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_arr')", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n ? (@ == {})')", Mode, k));
                }

                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_b ? (@ == true)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_b ? (@ == false)')", Mode));
            }
        }

        // Negation
        {
            AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{} $')", Mode));
            AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{} $.shared')", Mode));
            AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{} $.nope_xyz')", Mode));

            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') = false", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') = false", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.nope_xyz') = false", Mode));

            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') <> true", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') <> true", Mode));

            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') = Just(false)", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') = Just(false)", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') <> Just(true)", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') <> Just(true)", Mode));

            if (!KeysWithUKey.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithUKey);
                    AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{} $.u_{}')", Mode, k));
                }
            }

            if (!KeysWithUArr.empty()) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[*]') = false", Mode, k));
            }

            if (!KeysWithNestedObj.empty()) {
                const ui64 k = PickFrom(KeysWithNestedObj);
                AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared.u_{}') <> true", Mode, k));
            }

            if (!KeysWithDeepNested.empty()) {
                const ui64 k = PickFrom(KeysWithDeepNested);
                AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}') = Just(false)", Mode, k));
                AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}') <> Just(true)", Mode, k));
            }

            if (!KeysWithItems.empty()) {
                AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{} $.items')", Mode));
            }

            if (!KeysWithIntUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1})')", Mode, k));
                    AddJErr(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1})') <> true", Mode, k));
                }
            }
        }
    }

    void GenerateJsonValue() {
        // Member access
        {
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) = \"shared_v\"u", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.*' RETURNING Utf8) = \"shared_v\"u", Mode));

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = PickFrom(KeysWithStrUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) = \"u_v_{1}\"u", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = RandKey();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) = \"u_v_{1}\"u", Mode, k));
            }

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = RandKey();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", Mode, k));
            }

            for (int j = 0; j < 5; ++j) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) = true", Mode, j));
            }

            for (int j = 0; j < 5; ++j) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) != false", Mode, j));
            }

            for (int j = 0; j < 5; ++j) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool)", Mode, j));
            }

            for (size_t i = 0; i < 5; ++i) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) = {}", Mode, (int)Rng.Uniform(50)));
            }
        }

        // Array access
        {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64) = {2}", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}[2]' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}[last]' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[last - 1]' RETURNING Int64) = {2}", Mode, k, k + 1));
            }

            {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0 to 1] ? (@ == {1})' RETURNING Int64) = {1}", Mode, k));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0, 2] ? (@ == {1})' RETURNING Int64) = {1}", Mode, k));
            }
        }

        // Nested array/object access
        {
            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) = {1}", Mode, k));
                }

                for (int j = 0; j < 5; ++j) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}' RETURNING Utf8) = "v"u)", Mode, j));
                }
            }

            if (!KeysWithDeepNested.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = PickFrom(KeysWithDeepNested);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) = {1}", Mode, k));
                }
            }

            if (!KeysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHomoArr);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) = {1}", Mode, k));
                }

                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $[*].shared' RETURNING Utf8) = "v"u)", Mode));
            }

            if (!KeysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) = {1}", Mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
                }
            }

            if (!KeysWithMixed.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = PickFrom(KeysWithMixed);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) = {2}", Mode, k % 5, k));
                }
            }

            if (!KeysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithItems);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) = {1}", Mode, k));
                }

                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[1].name' RETURNING Utf8) = "shared_v"u)", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithItems);
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.items[0].name' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
                }
            }

            if (!KeysWithFullMix.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) = {1}", Mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
                }

                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) = true", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) != false", Mode));
            }
        }

        // Equality and inequality with literals
        {
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ == null)' RETURNING Bool)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ != null)' RETURNING Bool)", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ == true)' RETURNING Bool)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ == false)' RETURNING Bool)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ != true)' RETURNING Bool)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ != false)' RETURNING Bool)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool) == true", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool) != false", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ == {})' RETURNING Bool)", Mode, PickFrom(KeysWithScalarNumber)));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ != {})' RETURNING Bool)", Mode, PickFrom(KeysWithScalarNumber)));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int32) == {}", Mode, PickFrom(KeysWithScalarNumber)));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int32) != {}", Mode, PickFrom(KeysWithScalarNumber)));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ == {})' RETURNING Bool)", Mode, PickFrom(KeysWithScalarString)));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (@ != {})' RETURNING Bool)", Mode, PickFrom(KeysWithScalarString)));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Utf8) == \"{}\"u", Mode, PickFrom(KeysWithScalarString)));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Utf8) != \"{}\"u", Mode, PickFrom(KeysWithScalarString)));
        }

        // JV op JV
        {
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) = JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8))", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Double) = JSON_VALUE(Text, '{0} $.rank' RETURNING Double)", Mode));

            if (!KeysWithStrUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithStrUVal);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) = JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8)", Mode, k));
                }
            }

            if (!KeysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode, k));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) != JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode, k));
                }

                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) != JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode));
            }

            if (!KeysWithItems.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) = JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) != JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.items[0].name' RETURNING Utf8) != JSON_VALUE(Text, '{0} $.items[1].name' RETURNING Utf8))", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.items[1].name' RETURNING Utf8) = JSON_VALUE(Text, '{0} $.items[1].name' RETURNING Utf8))", Mode));
            }

            if (!KeysWithUArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithUArr);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64)", Mode, k));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) != JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)", Mode, k));
                }
            }

            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) = JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64)", Mode, k));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double) = JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double)", Mode, k));
                }
            }

            if (!KeysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithDeepNested);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) = JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64)", Mode, k));
                }
            }

            if (!KeysWithFullMix.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) = JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) = JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8))", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) != JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8))", Mode));
            }

            if (!KeysWithHeteroArr.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) = JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) = JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8))", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) != JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Utf8))", Mode));
            }

            for (int j = 0; j < 3; ++j) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) = JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool)", Mode, j));
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) != JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool)", Mode, j));
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) = JSON_VALUE(Text, '{0} $.g5_{2}' RETURNING Bool)", Mode, j, (j + 1) % 5));
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) != JSON_VALUE(Text, '{0} $.g5_{2}' RETURNING Bool)", Mode, j, (j + 2) % 5));
            }

            if (!KeysWithFullMix.empty()) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) = JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool)", Mode));
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) != JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool)", Mode));
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) = JSON_VALUE(Text, '{0} $.g5_0' RETURNING Bool)", Mode));
            }
        }

        // Bool negation
        {
            for (int j = 0; j < 2; ++j) {
                AddJErr(fmt::format("NOT JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool)", Mode, j));
            }

            if (!KeysWithFullMix.empty()) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) = false", Mode));
            }

            if (!KeysWithScalarBoolean.empty()) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool) <> true", Mode));
            }

            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.shared == "shared_v"' RETURNING Bool) = Just(false))", Mode));

            if (!KeysWithIntUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    AddJErr(fmt::format("NOT JSON_VALUE(Text, '{0} $.u_{1} == {1}' RETURNING Bool)", Mode, k));
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1} == {1}' RETURNING Bool) <> true", Mode, k));
                }
            }

            if (!KeysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank == {1}' RETURNING Bool) = Just(false)", Mode, r));
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank == {1}' RETURNING Bool) <> Just(true)", Mode, r));
                }
            }
        }
    }

    void GenerateJsonPathMethods() {
        if (Opts.EnableJsonExists) {
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.ceiling() ? (@ == 10)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.floor() ? (@ != -1)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.abs() ? (@ != 1)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.*.abs() ? (@ != 1)')", Mode));

            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.abs().ceiling() ? (@ == 10)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.floor().abs() ? (@ != -1)')", Mode));
            AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.ceiling().abs() ? (@ == 10)')", Mode));

            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == "shared" && @.value == "shared_v")'))", Mode));
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == "rank" && @.value != null)'))", Mode));

            if (!KeysWithFlatObj.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.type() ? (@ == "object")'))", Mode));
            }

            if (!KeysWithUArr.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}.size() ? (@ == 3)')", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.*.size() ? (@ == 3)')", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}.size().abs() ? (@ == 3)')", Mode, PickFrom(KeysWithUArr)));
            }

            if (!KeysWithItems.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items.size() ? (@ == 2)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items.size().abs() ? (@ == 2)')", Mode));
            }

            if (!KeysWithFullMix.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_arr.size() ? (@ == 4)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n.ceiling() ? (@ == 10)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n.floor() ? (@ != -1)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n.abs() ? (@ != 1)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n.abs().ceiling() ? (@ == 10)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_arr.size().abs() ? (@ == 4)')", Mode));
            }

            if (!KeysWithEmptyContainers.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}.size() ? (@ == 0)')", Mode, PickFrom(KeysWithEmptyContainers)));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared.size() ? (@ == 1)')", Mode));
            }

            if (!KeysWithArrayOfArrays.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.size() ? (@ == 3)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[0].size() ? (@ == 2)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[2].size() ? (@ == 0)')", Mode));
            }

            if (!KeysWithArrayLiterals.empty() || !KeysWithArrayOfArrays.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.type() ? (@ == "array")'))", Mode));
            }

            if (!KeysWithScalarNull.empty() || !KeysWithFullMix.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared_null.type() ? (@ == "null")'))", Mode));
            }

            if (!KeysWithScalarBoolean.empty() || !KeysWithFullMix.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared_b.type() ? (@ == "boolean")'))", Mode));
            }

            if (!KeysWithScalarNumber.empty() || !KeysWithFlatObj.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.rank.type() ? (@ == "number")'))", Mode));
            }

            if (!KeysWithScalarString.empty() || !KeysWithFlatObj.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared.type() ? (@ == "string")'))", Mode));
            }

            if (Opts.EnableJsonPathPredicates) {
                if (!KeysWithNestedObj.empty()) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared.keyvalue() ? (@.name starts with "u_")'))", Mode));
                }
            }

            if (Opts.EnableRangeComparisons) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.ceiling() ? (@ > 10)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.floor() ? (@ >= 10)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.abs() ? (@ < 45)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.abs() ? (@ <= 40)')", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == "rank" && @.value >= 0)'))", Mode));

                if (Opts.EnablePassingVariables) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ > $num.double())' PASSING "9.5"u AS num))", Mode));
                }
            }

            if (Opts.EnablePassingVariables) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.abs().ceiling() ? (@ == $s)' PASSING 10 AS s)", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank.floor().abs() ? (@ != $s)' PASSING 10 AS s)", Mode));

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $s.double().floor())' PASSING "10.0"u AS s))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $s.double().ceiling())' PASSING "9.5"u AS s))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $s.double().abs())' PASSING "10.0"u AS s))", Mode));

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $num.double())' PASSING "10.0"u AS num))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.* ? (@ == $num.double())' PASSING "10.0"u AS num))", Mode));

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.items.size() ? (@ == $sz)' PASSING 2 AS sz))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == $field)' PASSING "shared"u AS field))", Mode));

                if (Opts.EnableSqlParameters) {
                    auto pNum = NewPname();
                    auto vNum = pNum.substr(1);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.rank ? (@ == ${1}.double())' PASSING {2} AS {1})", Mode, vNum, pNum),
                        [pNum](NYdb::TParamsBuilder& bld) { bld.AddParam(pNum).Utf8("10.5").Build(); });

                    auto pSize = NewPname();
                    auto vSize = pSize.substr(1);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items.size() ? (@ == ${1})' PASSING {2} AS {1})", Mode, vSize, pSize),
                        [pSize](NYdb::TParamsBuilder& bld) { bld.AddParam(pSize).Int64(2).Build(); });

                    auto pField = NewPname();
                    auto vField = pField.substr(1);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == ${1})' PASSING {2} AS {1})", Mode, vField, pField),
                        [pField](NYdb::TParamsBuilder& bld) { bld.AddParam(pField).Utf8("shared").Build(); });
                }
            }
        }

        if (Opts.EnableJsonValue) {
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = "null"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = "boolean"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = "number"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = "string"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = "array"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = "object"u)", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_arr.type()' RETURNING Utf8) = "array"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_null.type()' RETURNING Utf8) = "null"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_b.type()' RETURNING Utf8) = "boolean"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.rank.type()' RETURNING Utf8) = "number"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared.type()' RETURNING Utf8) = "string"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.*.type()' RETURNING Utf8) = "string"u)", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_arr.size()' RETURNING Int64) = 4", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.*.size()' RETURNING Int64) = 4", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $.items.size()' RETURNING Int64) = 2", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared.size()' RETURNING Int64) = 1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $[2].size()' RETURNING Int64) = 0", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.ceiling()' RETURNING Int64) = 10", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.floor()' RETURNING Int64) <> -1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.abs()' RETURNING Int64) <> -1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.ceiling()' RETURNING Int64) = 10", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.floor()' RETURNING Int64) <> -1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.abs()' RETURNING Int64) <> 1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.*.abs()' RETURNING Int64) <> 1", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.abs().ceiling()' RETURNING Int64) <> -1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.floor().abs()' RETURNING Int64) <> -1", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.ceiling().abs()' RETURNING Int64) = 10", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{} $.keyvalue() ? (@.name == \"rank\").value' RETURNING Int64) <> -1", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.keyvalue() ? (@.name == "shared").value.type()' RETURNING Utf8) = "string"u)", Mode));

            if (!KeysWithFullMix.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.abs().ceiling()' RETURNING Int64) = 10", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_arr.size().abs()' RETURNING Int64) = 4", Mode));
            }

            if (!KeysWithItems.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.items.size().abs()' RETURNING Int64) = 2", Mode));
            }

            if (!KeysWithUArr.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}.size()' RETURNING Int64) = 3", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*.size()' RETURNING Int64) = 3", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}.size().abs()' RETURNING Int64) = 3", Mode, PickFrom(KeysWithUArr)));
            }

            if (Opts.EnableRangeComparisons) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.ceiling()' RETURNING Int64) > 10", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.floor()' RETURNING Int64) >= 10", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.abs()' RETURNING Int64) < 45", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.abs()' RETURNING Int64) <= 40", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.ceiling()' RETURNING Int64) > 10", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.floor()' RETURNING Int64) >= 10", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.abs()' RETURNING Int64) < 45", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.shared_n.abs()' RETURNING Int64) <= 40", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.*.abs()' RETURNING Int64) <= 40", Mode));

                if (Opts.EnablePassingVariables) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ > $num.double())' PASSING "9.5"u AS num RETURNING Int64) <> -1)", Mode));
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.* ? (@ > $num.double())' PASSING "9.5"u AS num RETURNING Int64) <> -1)", Mode));
                }
            }

            if (Opts.EnablePassingVariables) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ == $s.double().floor())' PASSING "10.0"u AS s RETURNING Int64) = 10)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ == $s.double().ceiling())' PASSING "9.5"u AS s RETURNING Int64) = 10)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ == $num.double())' PASSING "10.0"u AS num RETURNING Int64) <> -1)", Mode));

                if (Opts.EnableSqlParameters) {
                    auto pNum = NewPname();
                    auto vNum = pNum.substr(1);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank ? (@ == ${1}.double())' PASSING {2} AS {1} RETURNING Int64) <> -1", Mode, vNum, pNum),
                        [pNum](NYdb::TParamsBuilder& bld) { bld.AddParam(pNum).Utf8("10.5").Build(); });

                    auto pSize = NewPname();
                    auto vSize = pSize.substr(1);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items.size() ? (@ == ${1})' PASSING {2} AS {1} RETURNING Int64) = 2", Mode, vSize, pSize),
                        [pSize](NYdb::TParamsBuilder& bld) { bld.AddParam(pSize).Int64(2).Build(); });

                    auto pField = NewPname();
                    auto vField = pField.substr(1);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.keyvalue() ? (@.name == ${1}).value.type()' PASSING {2} AS {1} RETURNING Utf8) = \"string\"u", Mode, vField, pField),
                        [pField](NYdb::TParamsBuilder& bld) { bld.AddParam(pField).Utf8("shared").Build(); });
                }
            }

            if (Opts.EnableSqlParameters) {
                for (size_t i = 0; i < 2; ++i) {
                    auto pn = NewPname();
                    std::string pv = (i == 0) ? "number" : "string";
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.type()' RETURNING Utf8) = {1})", Mode, pn),
                        [pn, pv](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(pv).Build(); });
                }
            }
        }
    }

    void GeneratePassingVariables() {
        if (Opts.EnableJsonExists) {
            for (size_t i = 0; i < 3; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var)", Mode, PickFrom(KeysWithIntUVal)));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var))", Mode));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.* == $var)' PASSING "shared_v"u AS var))", Mode));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithFlatObj);
                const auto g5k = "g5_" + std::to_string(k % 5);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == $v1 && @.{2} == $v2)' PASSING {1} AS v1, true AS v2)", Mode, k, g5k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == $v1 && @.shared == $v2)' PASSING {1} AS v1, "shared_v"u AS v2))", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= $lo && @.u_{1} <= $hi && @.shared == $sv)' PASSING {2} AS lo, {3} AS hi, "shared_v"u AS sv))", Mode, k, k - 1, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 2));
            }

            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ != $var)' PASSING 0 AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ < $lo || @ > $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k + 1, k + 10));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared ? (@.u_{1} == $v1 && @.g5_{2} == $v2)' PASSING {1} AS v1, "v"u AS v2))", Mode, k, k % 5));
                }
            }

            if (!KeysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithDeepNested);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c ? (@.u_{1} >= $lo && @.u_{1} <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.*.* ? (@.u_{1} >= $lo && @.u_{1} <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c ? (@.* >= $lo && @.u_{1} <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c ? (@.u_{1} >= $lo && @.* <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                }
            }

            if (!KeysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHomoArr);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*] ? (@.u_{1} == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 2));
                }
            }

            if (!KeysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*].k_a ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*].k_a ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));

                    if (Opts.EnableJsonPathPredicates) {
                        AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $[*].k_b ? (@ starts with $var)' PASSING "u_v"u AS var))", Mode));
                    }
                }
            }

            if (!KeysWithMixed.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithMixed);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == $var)' PASSING {2} AS var)", Mode, k % 5, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ != $var)' PASSING 0 AS var)", Mode, k % 5));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k % 5, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ < $lo || @ > $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k % 5, k + 1, k + 10));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep ? (@.v == $var)' PASSING {2} AS var)", Mode, k % 5, k));
                }
            }

            if (!KeysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithItems);
                    const auto uvk = "u_v_" + std::to_string(k);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items[*] ? (@.id == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.items[*] ? (@.id == $v1 && @.name == $v2)' PASSING {1} AS v1, "{2}"u AS v2))", Mode, k, uvk));
                }
            }

            if (!KeysWithFullMix.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    const auto uvk = "u_v_" + std::to_string(k);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ != $var)' PASSING 0 AS var)", Mode));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k - 1, k + 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ < $lo || @ > $hi)' PASSING {2} AS lo, {3} AS hi)", Mode, k, k + 1, k + 10));
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared_s ? (@ == $var)' PASSING "{1}"u AS var))", Mode, uvk));
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared_b ? (@ == $var)' PASSING true AS var))", Mode));
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared_n == $v1 && @.shared_b == $v2)' PASSING {1} AS v1, true AS v2))", Mode, k));
                }
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJErr(fmt::format("NOT JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var)", Mode, k));
                AddJErr(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) <> Just(true)", Mode, k));
            }

            AddJErr(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var) = Just(false))", Mode));
            AddJErr(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var) <> Just(true))", Mode));

            if (Opts.EnableSqlParameters) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    auto pn = NewPname();
                    auto vn = pn.substr(1);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == {2})' PASSING {2} AS {3})", Mode, k, pn, vn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    auto pn = NewPname();
                    auto vn = pn.substr(1);
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == {1})' PASSING {1} AS {2}))", Mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                }

                {
                    auto pn = NewPname();
                    auto vn = pn.substr(1);
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.* == {1})' PASSING {1} AS {2}))", Mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    auto p0 = NewPname();
                    auto p1 = NewPname();
                    auto v0 = p0.substr(1);
                    auto v1 = p1.substr(1);

                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {2} && @.shared == {3})' PASSING {2} AS {4}, {3} AS {5})", Mode, k, p0, p1, v0, v1),
                        [p0, p1, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(p0).Int64(k).Build();
                            bld.AddParam(p1).Utf8("shared_v").Build();
                        });
                }

                if (!KeysWithNestedObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithNestedObj);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == {2})' PASSING {2} AS {3})", Mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithNestedObj);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", Mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k - 1).Build();
                                bld.AddParam(phi).Int64(k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithNestedObj);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ < {2} || @ > {3})' PASSING {2} AS {4}, {3} AS {5})", Mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k + 1).Build();
                                bld.AddParam(phi).Int64(k + 10).Build();
                            });
                    }
                }

                if (!KeysWithDeepNested.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithDeepNested);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ == {2})' PASSING {2} AS {3})", Mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithDeepNested);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", Mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k - 1).Build();
                                bld.AddParam(phi).Int64(k + 1).Build();
                            });
                    }
                }

                if (!KeysWithMixed.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithMixed);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == {2})' PASSING {2} AS {3})", Mode, k % 5, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithMixed);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", Mode, k % 5, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k - 1).Build();
                                bld.AddParam(phi).Int64(k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithMixed);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ < {2} || @ > {3})' PASSING {2} AS {4}, {3} AS {5})", Mode, k % 5, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k + 1).Build();
                                bld.AddParam(phi).Int64(k + 10).Build();
                            });
                    }
                }

                if (!KeysWithItems.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithItems);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ == {1})' PASSING {1} AS {2})", Mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithItems);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items[*].* ? (@ == {1})' PASSING {1} AS {2})", Mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithItems);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ >= {1} && @ <= {2})' PASSING {1} AS {3}, {2} AS {4})", Mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k - 1).Build();
                                bld.AddParam(phi).Int64(k + 1).Build();
                            });
                    }
                }

                if (!KeysWithFullMix.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithFullMix);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ == {1})' PASSING {1} AS {2})", Mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithFullMix);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ >= {1} && @ <= {2})' PASSING {1} AS {3}, {2} AS {4})", Mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k - 1).Build();
                                bld.AddParam(phi).Int64(k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithFullMix);
                        auto plo = NewPname();
                        auto phi = NewPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ < {1} || @ > {2})' PASSING {1} AS {3}, {2} AS {4})", Mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64(k + 1).Build();
                                bld.AddParam(phi).Int64(k + 10).Build();
                            });
                    }

                    {
                        const ui64 k = PickFrom(KeysWithFullMix);
                        const auto uvk = "u_v_" + std::to_string(k);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared_s ? (@ == {1})' PASSING {1} AS {2})", Mode, pn, vn),
                            [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                    }
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithUArr);
                    auto pn = NewPname();
                    auto vn = pn.substr(1);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == {2})' PASSING {2} AS {3})", Mode, k, pn, vn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }
            }
        }

        if (Opts.EnableJsonValue) {
            AddJErr(fmt::format("NOT JSON_VALUE(Text, '{0} $.rank == $val' PASSING {1} AS val RETURNING Bool)", Mode, PickFrom(KeysWithFlatObj) % 50));
            AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank == $val' PASSING {1} AS val RETURNING Bool) != Just(true)", Mode, PickFrom(KeysWithFlatObj) % 50));

            AddJErr(fmt::format(R"(NOT JSON_VALUE(Text, '{0} $.shared == $val' PASSING "shared_v"u AS val RETURNING Bool))", Mode));
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared == $val' PASSING "shared_v"u AS val RETURNING Bool) != Just(true))", Mode));
        }
    }

    void GenerateSqlParameters() {
        if (Opts.EnableJsonValue) {
            for (size_t i = 0; i < 2; ++i) {
                auto pn = NewPname();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) = {1}", Mode, pn),
                    [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*' RETURNING Utf8) = {1}", Mode, pn),
                    [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                auto pn = NewPname();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {2}", Mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithStrUVal);
                auto pn = NewPname();
                const auto uvk = "u_v_" + std::to_string(k);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) = {2}", Mode, k, pn),
                    [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                auto pn = NewPname();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = {2}", Mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
            }

            {
                const ui64 k = PickFrom(KeysWithUArr);
                auto plo = NewPname();
                auto phi = NewPname();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, plo, phi),
                    [plo, phi, k](NYdb::TParamsBuilder& bld) {
                        bld.AddParam(plo).Int64(k - 1).Build();
                        bld.AddParam(phi).Int64(k + 1).Build();
                    });
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, plo, phi),
                    [plo, phi, k](NYdb::TParamsBuilder& bld) {
                        bld.AddParam(plo).Int64(k - 1).Build();
                        bld.AddParam(phi).Int64(k + 1).Build();
                    });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                auto pn = NewPname();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {2}", Mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
            }

            {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                auto plo = NewPname();
                auto phi = NewPname();
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, plo, phi),
                    [plo, phi, k](NYdb::TParamsBuilder& bld) {
                        bld.AddParam(plo).Int64(k - 1).Build();
                        bld.AddParam(phi).Int64(k + 1).Build();
                    });
            }

            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) = {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) > {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k - 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*.u_{1}' RETURNING Int64) > {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k - 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) < {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k + 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) >= {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 1).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 10).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.*' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 10).Build();
                        });
                }

                for (int j = 0; j < 5; ++j) {
                    auto pn = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared.g5_{1}' RETURNING Utf8) = {2})", Mode, j, pn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("v").Build(); });
                }
            }

            if (!KeysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithDeepNested);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) = {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithDeepNested);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 1).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithDeepNested);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 10).Build();
                        });
                }
            }

            if (!KeysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHomoArr);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) = {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithHomoArr);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 2).Build();
                        });
                }
            }

            if (!KeysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) = {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) = {1})", Mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 1).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithHeteroArr);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 10).Build();
                        });
                }
            }

            if (!KeysWithMixed.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) = {2}", Mode, k % 5, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) > {2}", Mode, k % 5, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k - 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) < {2}", Mode, k % 5, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k + 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) >= {2}", Mode, k % 5, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) <= {2}", Mode, k % 5, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k % 5, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 1).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithMixed);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k % 5, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 10).Build();
                        });
                }
            }

            if (!KeysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithItems);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) = {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithItems);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) > {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k - 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithItems);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) <= {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithItems);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.items[0].name' RETURNING Utf8) = {1})", Mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithItems);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 1).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithItems);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 5).Build();
                        });
                }
            }

            if (!KeysWithFullMix.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) = {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) > {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k - 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) < {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k + 1).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) >= {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) <= {1}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }

                {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k - 1).Build();
                            bld.AddParam(phi).Int64(k + 1).Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(k + 1).Build();
                            bld.AddParam(phi).Int64(k + 10).Build();
                        });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) = {1})", Mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }
            }
        }
    }

    void GenerateRangeComparisons() {
        if (Opts.EnableJsonExists) {
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ >= "s")'))", Mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} > {2})')", Mode, k, k - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= {1})')", Mode, PickFrom(KeysWithIntUVal)));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2})')", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.* ? (@ <= {1})')", Mode, PickFrom(KeysWithIntUVal)));
            }

            for (int lo : {10, 25}) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank > {})')", Mode, lo));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank >= {})')", Mode, lo));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= {1} && @.shared == "shared_v")'))", Mode, k));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2} && @.shared == "shared_v")'))", Mode, k, k + 1));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2} && @.* == "shared_v")'))", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} > {2} || @.shared == "shared_v")'))", Mode, k, k - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.u_{}[*] ? (@ > {})')", Mode, k, k - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithNestedObj);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ > {2})')", Mode, k, k - 1));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ < {2})')", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithDeepNested);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ >= {1})')", Mode, k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ < {2})')", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ >= {1})')", Mode, k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ < {2})')", Mode, k, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*].k_a ? (@ > {})')", Mode, PickFrom(KeysWithHeteroArr) - 1));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*].k_a ? (@ <= {})')", Mode, PickFrom(KeysWithHeteroArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithMixed);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= {2})')", Mode, k % 5, k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ < {2})')", Mode, k % 5, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*].id ? (@ > {})')", Mode, PickFrom(KeysWithItems) - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n ? (@ > {})')", Mode, PickFrom(KeysWithFullMix) - 1));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared_n ? (@ <= {})')", Mode, PickFrom(KeysWithFullMix)));
            }
        }

        if (Opts.EnableJsonValue) {
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) >= "s"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) < "z"u)", Mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > {2}", Mode, k, k - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) >= {1}", Mode, PickFrom(KeysWithIntUVal)));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) < {2}", Mode, k, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*' RETURNING Int64) < {1}", Mode, k + 1));
            }

            for (int lo : {5, 10, 15, 20, 25, 30}) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) > {}", Mode, lo));
            }

            for (int hi : {10, 20, 30, 40, 45}) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) < {}", Mode, hi));
            }

            for (int v : {10, 25, 35}) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) >= {}", Mode, v));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <= {}", Mode, v));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) > {2}", Mode, k, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) <= {1}", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) <= {1}", Mode, PickFrom(KeysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > {2}", Mode, k, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) < {2}", Mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithNestedObj);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) > {2}", Mode, k, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) < {2}", Mode, k, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) >= {1}", Mode, k));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithDeepNested);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) > {2}", Mode, k, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) <= {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) > {2}", Mode, k, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) >= {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHeteroArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) > {1}", Mode, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) <= {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithMixed);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) > {2}", Mode, k % 5, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) >= {2}", Mode, k % 5, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithItems);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) > {1}", Mode, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) <= {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithFullMix);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) > {1}", Mode, k - 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) < {1}", Mode, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) >= {1}", Mode, k));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) <= {1}", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode, PickFrom(KeysWithFlatObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64)", Mode, PickFrom(KeysWithFlatObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64)", Mode, PickFrom(KeysWithFlatObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode, PickFrom(KeysWithFlatObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Double) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Double)", Mode, PickFrom(KeysWithFlatObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Double) <= JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Double)", Mode, PickFrom(KeysWithFlatObj)));
            }

            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Double) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Double)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) <= JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8))", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) >= JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8))", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64) > JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Double) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Double)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.items[0].name' RETURNING Utf8) < JSON_VALUE(Text, '{0} $.items[1].name' RETURNING Utf8))", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)", Mode, PickFrom(KeysWithUArr)));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)", Mode, PickFrom(KeysWithUArr)));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64) > JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64)", Mode, PickFrom(KeysWithUArr)));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Double) < JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Double)", Mode, PickFrom(KeysWithUArr)));

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64)", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64)", Mode, PickFrom(KeysWithUArr)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double) <= JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double)", Mode, PickFrom(KeysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Double) >= JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Double)", Mode, PickFrom(KeysWithDeepNested)));
            }

            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Double) >= JSON_VALUE(Text, '{0} $.shared_n' RETURNING Double)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) <= JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8))", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) >= JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8))", Mode));

            AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) <= JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64)", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Double) >= JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Double)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) <= JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8))", Mode));

            AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) < JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool)", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) > JSON_VALUE(Text, '{0} $.g5_0' RETURNING Bool)", Mode));

            AddJErr(fmt::format("NOT JSON_VALUE(Text, '{0} $.rank > {1}' RETURNING Bool)", Mode, PickFrom(KeysWithFlatObj) % 50 - 1));
            AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank > {1}' RETURNING Bool) = Just(false)", Mode, PickFrom(KeysWithFlatObj) % 50 - 1));
        }
    }

    void GenerateBetween() {
        if (Opts.EnableJsonValue) {
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 10 AND 20", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 0 AND 25", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 20 AND 40", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) BETWEEN "a"u AND "z"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) NOT BETWEEN "t"u AND "z"u)", Mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*' RETURNING Int64) BETWEEN {1} AND {2}", Mode, k - 1, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, k + 2, k + 10));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, k + 1, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, k + 1, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithStrUVal);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) BETWEEN "a"u AND "z"u)", Mode, k));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) NOT BETWEEN "a"u AND "b"u)", Mode, k));
            }

            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) NOT BETWEEN 30 AND 50", Mode));
            AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) NOT BETWEEN 0 AND 5", Mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithNestedObj);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, k + 1, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithDeepNested);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, k + 2, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k, k - 1, k + 2));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k, k + 2, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHeteroArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) BETWEEN {1} AND {2}", Mode, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, k + 1, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithMixed);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) BETWEEN {2} AND {3}", Mode, k % 5, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) NOT BETWEEN {2} AND {3}", Mode, k % 5, k + 1, k + 10));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithItems);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) BETWEEN {1} AND {2}", Mode, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, k + 1, k + 5));
            }

            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = PickFrom(KeysWithFullMix);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) BETWEEN {1} AND {2}", Mode, k - 1, k + 1));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) NOT BETWEEN {1} AND {2}", Mode, k + 1, k + 10));
            }
        }
    }

    void GenerateInList() {
        if (Opts.EnableJsonValue) {
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) IN ("shared_v"u, "other"u))", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.*' RETURNING Utf8) IN ("shared_v"u, "other"u))", Mode));

            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) NOT IN ("shared_v"u, "other"u))", Mode));
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.*' RETURNING Utf8) NOT IN ("shared_v"u, "other"u))", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) IN ("shared_v"u, "other_v"u, "nope"u))", Mode));
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) NOT IN ("shared_v"u, "other_v"u, "nope"u))", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[1].name' RETURNING Utf8) IN ("shared_v"u, "nope"u))", Mode));
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.items[1].name' RETURNING Utf8) NOT IN ("shared_v"u, "nope"u))", Mode));

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) IN ("u_v_{1}"u, "nope"u))", Mode, PickFrom(KeysWithStrUVal)));
            }

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) IN ("u_v_{1}", "nope"u))", Mode, PickFrom(KeysWithStrUVal)));

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = (i < 2 || KeysWithBothSharedAndU.empty()) ? PickFrom(KeysWithIntUVal) : PickFrom(KeysWithBothSharedAndU);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}l)", Mode, k, k));
            }

            {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}l, {2}l, {3}l)", Mode, k, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}, {2}u, {3}l)", Mode, k, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN (Just({1}l), {2}l)", Mode, k, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithStrUVal);
                AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) NOT IN ("u_v_{1}"u, "nope"u))", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithIntUVal);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            if (!KeysWithBothSharedAndU.empty()) {
                const ui64 k = PickFrom(KeysWithBothSharedAndU);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k + 1, k + 2, k + 3));
            }

            {
                const ui64 k = PickFrom(KeysWithUArr);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithNestedObj);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (const int j : {0, 2, 4}) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}' RETURNING Utf8) IN ("v"u, "other"u))", Mode, j));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithNestedObj);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (const int j : {0, 2, 4}) {
                AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}' RETURNING Utf8) NOT IN ("v"u, "other"u))", Mode, j));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithDeepNested);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithDeepNested);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHeteroArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithHeteroArr);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) IN ("u_v_{1}"u, "nope"u))", Mode, k));
            }

            {
                const ui64 k = PickFrom(KeysWithHeteroArr);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithHeteroArr);
                AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].k_b' RETURNING Utf8) NOT IN ("u_v_{1}"u, "nope"u))", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithMixed);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) IN ({2}, {3}, {4})", Mode, k % 5, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithMixed);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) NOT IN ({2}, {3}, {4})", Mode, k % 5, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithItems);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithItems);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithFullMix);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithFullMix);
                AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) NOT IN ({1}, {2}, {3})", Mode, k, k + 1, k + 2));
            }

            {
                const ui64 k = PickFrom(KeysWithFullMix);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) IN ("u_v_{1}"u, "nope"u))", Mode, k));
                AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) NOT IN ("u_v_{1}"u, "nope"u))", Mode, k));
            }

            if (Opts.EnableSqlParameters) {
                {
                    auto plist = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) IN {1})", Mode, plist),
                        [plist](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plist).BeginList()
                                .AddListItem().Utf8("shared_v")
                                .AddListItem().Utf8("other")
                                .EndList()
                                .Build();
                        });
                }

                {
                    auto plist = NewPname();
                    AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) NOT IN {1})", Mode, plist),
                        [plist](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plist).BeginList()
                                .AddListItem().Utf8("__no_match_a")
                                .AddListItem().Utf8("__no_match_b")
                                .EndList()
                                .Build();
                        });
                }

                {
                    auto p1 = NewPname();
                    auto p2 = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) IN ({1}, {2}))", Mode, p1, p2),
                        [p1, p2](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(p1).Utf8("shared_v").Build();
                            bld.AddParam(p2).Utf8("other").Build();
                        });
                }

                {
                    auto p1 = NewPname();
                    auto p2 = NewPname();
                    AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) NOT IN ({1}, {2}))", Mode, p1, p2),
                        [p1, p2](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(p1).Utf8("__nm_a").Build();
                            bld.AddParam(p2).Utf8("__nm_b").Build();
                        });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    auto plist = NewPname();

                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN {2}", Mode, k, plist),
                        [plist, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plist).BeginList()
                                .AddListItem().Int64(k)
                                .AddListItem().Int64(k + 1)
                                .AddListItem().Int64(k + 2)
                                .EndList()
                                .Build();
                        });
                }

                if (!KeysWithStrUVal.empty()) {
                    const ui64 k = PickFrom(KeysWithStrUVal);
                    auto plist = NewPname();
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) IN {2})", Mode, k, plist),
                        [plist, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plist).BeginList()
                                .AddListItem().Utf8("u_v_" + std::to_string(k))
                                .AddListItem().Utf8("u_v_" + std::to_string(k + 1))
                                .EndList()
                                .Build();
                        });
                }

                if (!KeysWithStrUVal.empty()) {
                    const ui64 k = PickFrom(KeysWithStrUVal);
                    auto plist = NewPname();
                    AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) NOT IN {2})", Mode, k, plist),
                        [plist](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plist).BeginList()
                                .AddListItem().Utf8("__no_utf8_a")
                                .AddListItem().Utf8("__no_utf8_b")
                                .EndList()
                                .Build();
                        });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    auto p1 = NewPname();
                    auto p2 = NewPname();
                    auto p3 = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({2}, {3}, {4})", Mode, k, p1, p2, p3),
                        [p1, p2, p3, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(p1).Int64(k).Build();
                            bld.AddParam(p2).Int64(k + 1).Build();
                            bld.AddParam(p3).Int64(k + 2).Build();
                        });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    auto plist = NewPname();
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT IN {2}", Mode, k, plist),
                        [plist, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plist).BeginList()
                                .AddListItem().Int64(k + 1000000)
                                .AddListItem().Int64(k + 1000000 + 1)
                                .AddListItem().Int64(k + 1000000 + 2)
                                .EndList()
                                .Build();
                        });
                }

                {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    auto p1 = NewPname();
                    auto p2 = NewPname();
                    auto p3 = NewPname();
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT IN ({2}, {3}, {4})", Mode, k, p1, p2, p3),
                        [p1, p2, p3, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(p1).Int64(k + 1000000).Build();
                            bld.AddParam(p2).Int64(k + 1000000 + 1).Build();
                            bld.AddParam(p3).Int64(k + 1000000 + 2).Build();
                        });
                }
            }
        }
    }

    void GenerateJsonPathPredicates() {
        if (Opts.EnableJsonExists) {
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ starts with "shared")'))", Mode));
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ starts with "shared_v")'))", Mode));
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.* ? (@ starts with "shared_v")'))", Mode));

            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ like_regex "shared.*")'))", Mode));
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ like_regex "^shared_v$")'))", Mode));
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ like_regex "SHARED.*" flag "i")'))", Mode));
            AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.* ? (@ like_regex "SHARED.*" flag "i")'))", Mode));

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} starts with "u_v")'))", Mode, PickFrom(KeysWithStrUVal)));
            }

            for (size_t i = 0; i < 2; ++i) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.u_{1} ? (@ like_regex "u_v_.*")'))", Mode, PickFrom(KeysWithStrUVal)));
            }

            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $ ? ((@ == true) is unknown)')", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $ ? ((@ > 0) is unknown)')", Mode));
            AddJErr(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared ? ((@ == "shared_v") is unknown)'))", Mode));

            if (!KeysWithItems.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ starts with "u_v")'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ starts with "shared")'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.*[*].name ? (@ starts with "shared")'))", Mode));

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ like_regex "u_v_.*")'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.*[*].name ? (@ like_regex "u_v_.*")'))", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.id))')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.name))')", Mode));

                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.id) && @.id > 0)')", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.name) && @.name != "nope")'))", Mode));
            }

            if (!KeysWithFullMix.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared_s ? (@ starts with "u_v")'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared_s ? (@ like_regex "u_v_.*")'))", Mode));
            }

            if (!KeysWithScalarString.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@ like_regex "u_v_[0-9]+")'))", Mode));
            }

            if (!KeysWithScalarNumber.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@.type() == "number" && @ < 0)'))", Mode));
            }

            if (!KeysWithHeteroArr.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $[*].k_b ? (@ like_regex "u_v_[0-9]+")'))", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*] ? (exists(@.k_a))')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $[*] ? (exists(@.k_b))')", Mode));
            }

            if (!KeysWithHomoArr.empty()) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*] ? (@.u_{1} == {1})')", Mode, k));
            }

            if (!KeysWithArrayLiterals.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $[*] ? (@.type() == "string" && @ starts with "u_v")'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $[*] ? (@.type() == "string" && @ like_regex "shared.*")'))", Mode));
            }

            if (!KeysWithUArr.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared[*] ? (@ == true)'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared[*] ? (@ == null)'))", Mode));
            }

            if (!KeysWithEmptyContainers.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@.empty_key == null)'))", Mode));
            }

            if (!KeysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (exists(@.u_{}))')", Mode, PickFrom(KeysWithFlatObj)));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} == {1})')", Mode, PickFrom(KeysWithFlatObj)));
                }

                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (exists(@.shared))')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (exists(@.rank))')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (exists(@.nope_xyz))')", Mode));

                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (exists(@.shared) && @.shared == "shared_v")'))", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (exists(@.rank) && @.rank > 10)')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (exists(@.rank) && @.rank != -1)')", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@.shared == "shared_v" || @.rank > 0)'))", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    AddJErr(fmt::format("JSON_EXISTS(Text, '{} $ ? ((@.u_{} == {}) is unknown)')", Mode, k, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.* ? ((@ == {}) is unknown)')", Mode, k));
                }
            }

            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.shared ? (exists(@.u_{}))')", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.* ? (exists(@.u_{}))')", Mode, k));
                }
            }

            if (!KeysWithDeepNested.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.a ? (exists(@.b))')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.a.b ? (exists(@.c))')", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.a.* ? (exists(@.c))')", Mode));
            }

            if (!KeysWithMixed.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.shared[*] ? (exists(@.shared))'))", Mode));
            }

            if (Opts.EnablePassingVariables) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared ? (@ starts with $pfx)' PASSING "shared"u AS pfx))", Mode));
                AddJErr(fmt::format("JSON_EXISTS(Text, '{} $ ? ((@ == $var) is unknown)' PASSING true AS var)", Mode));

                if (!KeysWithItems.empty()) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.items[*].name ? (@ starts with $pfx)' PASSING "u_v"u AS pfx))", Mode));
                }

                if (!KeysWithFlatObj.empty()) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} >= $lo)' PASSING {1} AS lo)", Mode, k, k));
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (exists(@.shared) && @.shared starts with $pfx)' PASSING "shared"u AS pfx))", Mode));
                }

                if (Opts.EnableSqlParameters) {
                    auto pn = NewPname();
                    auto vn = pn.substr(1);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared ? (@ starts with {1})' PASSING {1} AS {2})", Mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared").Build(); });
                }
            }
        }

        if (Opts.EnableJsonValue) {
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared starts with "shared"' RETURNING Bool))", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.* starts with "shared"' RETURNING Bool))", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared ? (@ like_regex "shared.*")' RETURNING Utf8) = "shared_v"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.* like_regex "shared.*"' RETURNING Bool))", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared ? (@ like_regex "^shared_v$")' RETURNING Utf8) = "shared_v"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.* like_regex "^shared_v$"' RETURNING Bool))", Mode));

            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared ? (@ like_regex "SHARED.*" flag "i")' RETURNING Utf8) = "shared_v"u)", Mode));
            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.* like_regex "SHARED.*" flag "i"' RETURNING Bool))", Mode));

            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.shared ? ((@ == "shared_v") is unknown)' RETURNING Utf8) = "shared_v"u)", Mode));
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} ($.shared == "shared_v") is unknown' RETURNING Bool))", Mode));

            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank ? ((@ > 0) is unknown)' RETURNING Int64) = 0", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} ($.rank > 0) is unknown' RETURNING Bool)", Mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithStrUVal);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1} ? (@ like_regex "u_v_.*")' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = PickFrom(KeysWithStrUVal);
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1} ? (@ starts with "u_v")' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
            }

            if (!KeysWithScalarString.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $ ? (@ starts with "u_v")' RETURNING Utf8) = "u_v_{1}"u)", Mode, PickFrom(KeysWithScalarString)));
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $ starts with "u_v"' RETURNING Bool))", Mode, PickFrom(KeysWithScalarString)));
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.* starts with "u_v"' RETURNING Bool))", Mode, PickFrom(KeysWithScalarString)));

                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $ ? (@ like_regex "u_v_[0-9]+")' RETURNING Utf8) = "u_v_{1}"u)", Mode, PickFrom(KeysWithScalarString)));
                }
            }

            if (!KeysWithFullMix.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared_s ? (@ starts with "u_v")' RETURNING Utf8) <> "nope"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared_s starts with "u_v"' RETURNING Bool))", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.* starts with "u_v"' RETURNING Bool))", Mode));

                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared_s ? (@ like_regex "u_v_.*")' RETURNING Utf8) <> "nope"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared_s like_regex "u_v_.*"' RETURNING Bool))", Mode));
            }

            if (!KeysWithItems.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[*].name ? (@ starts with "u_v")' RETURNING Utf8) <> "nope"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[*].name starts with "u_v"' RETURNING Bool))", Mode));

                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[0] ? (exists(@.id)).id' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithItems)));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.items[0] ? (exists(@.id))' RETURNING Bool))", Mode, PickFrom(KeysWithItems)));

                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[0] ? (exists(@.name)).name' RETURNING Utf8) <> "nope"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} exists($.items[0].name)' RETURNING Bool))", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[*].* ? (exists(@.name))' RETURNING Bool))", Mode));

                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[*] ? (exists(@.id) && @.id == {1}).id' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithItems)));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[*] ? (exists(@.name) && @.name != "nope").name' RETURNING Utf8) <> "nope"u)", Mode));
            }

            if (!KeysWithHeteroArr.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $[*].k_b ? (@ like_regex "u_v_[0-9]+")' RETURNING Utf8) <> "nope"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $[*].k_b like_regex "u_v_[0-9]+"' RETURNING Bool))", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $[*].* like_regex "u_v_[0-9]+"' RETURNING Bool))", Mode));
            }

            if (!KeysWithScalarNumber.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (@.type() == "number" && @ < 0)' RETURNING Bool))", Mode));
            }

            if (!KeysWithHomoArr.empty()) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*] ? (@.u_{1} == {1})' RETURNING Bool)", Mode, k));
            }

            if (!KeysWithArrayLiterals.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $[*] ? (@.type() == "string" && @ like_regex "shared.*")' RETURNING Utf8) <> "nope"u)", Mode));
            }

            if (!KeysWithUArr.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.shared[*] ? (@ == true)' RETURNING Bool))", Mode));
            }

            if (!KeysWithEmptyContainers.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (@.empty_key == null)' RETURNING Bool))", Mode));
            }

            if (!KeysWithFlatObj.empty()) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (exists(@.shared)).shared' RETURNING Utf8) = "shared_v"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} exists($.shared)' RETURNING Bool))", Mode));

                AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (exists(@.rank)).rank' RETURNING Int64) <> -1", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} exists($.rank)' RETURNING Bool)", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.* ? (exists(@.rank))' RETURNING Bool)", Mode));

                AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (exists(@.rank) && @.rank != -1).rank' RETURNING Int64) <> -1", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (exists(@.shared) && @.shared == "shared_v").shared' RETURNING Utf8) = "shared_v"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (@.shared == "shared_v" || @.rank > 0)' RETURNING Bool))", Mode));

                {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    AddJErr(fmt::format("JSON_VALUE(Text, '{} $ ? ((@.u_{} == {}) is unknown).u_{}' RETURNING Int64) = 0", Mode, k, k, k));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.* ? ((@ == {}) is unknown).u_{}' RETURNING Int64) = 0", Mode, k, k));
                }
            }

            if (!KeysWithNestedObj.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared ? (exists(@.u_{1})).u_{1}' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithNestedObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} exists($.shared.u_{1})' RETURNING Bool)", Mode, PickFrom(KeysWithNestedObj)));
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.* ? (exists(@.u_{1}))' RETURNING Bool)", Mode, PickFrom(KeysWithNestedObj)));
            }

            if (Opts.EnableRangeComparisons) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $ ? (exists(@.rank) && @.rank > 10).rank' RETURNING Int64) > 10", Mode));
            }

            if (Opts.EnablePassingVariables) {
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared ? (@ starts with $pfx)' PASSING "shared"u AS pfx RETURNING Utf8) = "shared_v"u)", Mode));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared starts with $pfx' PASSING "shared"u AS pfx RETURNING Bool))", Mode));

                AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank ? ((@ == $var) is unknown)' PASSING 0 AS var RETURNING Int64) = 0", Mode));
                AddJErr(fmt::format("JSON_VALUE(Text, '{} ($.rank == $var) is unknown' PASSING 0 AS var RETURNING Bool)", Mode));

                if (Opts.EnableSqlParameters) {
                    auto pn = NewPname();
                    auto vn = pn.substr(1);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared ? (@ starts with {1})' PASSING {1} AS {2} RETURNING Utf8) = \"shared_v\"u", Mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared").Build(); });
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared starts with {1}' PASSING {1} AS {2} RETURNING Bool)", Mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared").Build(); });
                }
            }
        }
    }

    void GenerateComplexJsonPathFilters() {
        if (Opts.EnableJsonExists) {
            if (!KeysWithFlatObj.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (exists(@.shared) && @.rank != -1)'))", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} == {1})')", Mode, PickFrom(KeysWithFlatObj)));
                }
            }

            if (!KeysWithDeepNested.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.a ? (exists(@.b) && exists(@.b.c))')", Mode));
            }

            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared ? (exists(@.u_{1}) && @.u_{1} == {1})')", Mode, PickFrom(KeysWithNestedObj)));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.*.* ? (@ == {1})')", Mode, PickFrom(KeysWithNestedObj)));
                }
            }

            if (!KeysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $[*].* ? (@ == "u_v_{1}")'))", Mode, PickFrom(KeysWithHeteroArr)));
                }
            }

            if (!KeysWithHomoArr.empty()) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*] ? (@.u_{1} == {1} || @.u_{1} == {2})')", Mode, k, k + 1));
            }

            if (Opts.EnableRangeComparisons) {
                if (!KeysWithItems.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.*[*] ? (@.id > 0)')", Mode));
                }
            }

            if (Opts.EnableJsonPathMethods) {
                if (!KeysWithNestedObj.empty()) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $.shared ? (@.u_{1} == {1}).type() ? (@ == "object")'))", Mode, PickFrom(KeysWithNestedObj)));
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank ? (@ >= 0).abs() ? (@ < 50)')", Mode));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $.rank ? (@ >= 0).ceiling() ? (@ >= 0)')", Mode));
                    }
                }
            }

            if (Opts.EnableJsonPathPredicates) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.* ? ((@ starts with "x") is unknown)'))", Mode));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.* ? ((@ like_regex "x") is unknown)'))", Mode));
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $.* ? ((exists(@.nope_xyz)) is unknown)')", Mode));

                if (!KeysWithHomoArr.empty()) {
                    const ui64 k = PickFrom(KeysWithHomoArr);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*] ? (((exists(@.u_{1}) && @.u_{1} == {1})) || exists(@.shared))')", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $[*] ? (@.u_{1} == {2} || ((exists(@.u_{1})) && @.u_{1} == {1}))')", Mode, k, k + 1));
                }

                if (!KeysWithMixed.empty()) {
                    const ui64 k = PickFrom(KeysWithMixed);
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared[*] ? (((exists(@.u_{1}) && @.u_{1} == {1})) || exists(@.shared))')", Mode, k));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (((exists(@.g5_{1})) && exists(@.g5_{1}.deep.v)) || exists(@.shared))')", Mode, k % 5));
                }

                if (!KeysWithArrayLiterals.empty() || !KeysWithHeteroArr.empty()) {
                    AddJErr(fmt::format(R"(JSON_EXISTS(Text, '{} $[*] ? ((@ starts with "x") is unknown)'))", Mode));
                }

                if (!KeysWithItems.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.*[*] ? (exists(@.id) && @.id == {1})')", Mode, PickFrom(KeysWithItems)));

                    if (Opts.EnableRangeComparisons) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*] ? (@.id > 0 && exists(@.name))')", Mode));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.id)) ? (@.id > 0)')", Mode));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items[*] ? (((@.id > 0) && exists(@.name)) || (@.id < 0))')", Mode));
                    }
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (((exists(@.shared)) && (@.rank >= 0)) || (@.rank < -1))'))", Mode));
                    }
                }

                if (Opts.EnablePassingVariables) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $.* ? ((@ starts with $pfx) is unknown)' PASSING "x"u AS pfx))", Mode));

                    if (!KeysWithNestedObj.empty()) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $.shared ? (exists(@.u_{1}) && @.u_{1} == $val)' PASSING {1} AS val)", Mode, PickFrom(KeysWithNestedObj)));
                    }

                    if (Opts.EnableRangeComparisons) {
                        if (!KeysWithFlatObj.empty()) {
                            AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} >= $lo)' PASSING {1} AS lo)", Mode, PickFrom(KeysWithFlatObj)));
                        }
                    }
                }

                if (Opts.EnableJsonPathMethods) {
                    if (!KeysWithItems.empty()) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items ? (exists(@[*])).size() ? (@ == 2)')", Mode));
                    }
                }
            }
        }

        if (Opts.EnableJsonValue) {
            if (!KeysWithItems.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*[*] ? (@.id == {1}).id' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithItems)));
                AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.*[*] ? (@.id > 0).name' RETURNING Utf8) <> "nope"u)", Mode));
            }

            if (!KeysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.*.* ? (@ == {1})' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithNestedObj)));
                }
            }

            if (!KeysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $[*].* ? (@ == "u_v_{1}")' RETURNING Utf8) = "u_v_{1}"u)", Mode, PickFrom(KeysWithHeteroArr)));
                }
            }

            if (!KeysWithHomoArr.empty()) {
                const ui64 k = PickFrom(KeysWithHomoArr);
                AddJ(fmt::format("JSON_VALUE(Text, '{0} $[*] ? (@.u_{1} == {1} || @.u_{1} == {2}).u_{1}' RETURNING Int64) = {1}", Mode, k, k + 1));
            }

            if (Opts.EnableJsonPathMethods) {
                if (!KeysWithNestedObj.empty()) {
                    const ui64 k = PickFrom(KeysWithNestedObj);
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared ? (@.u_{1} == {1}).type()' RETURNING Utf8) = "object"u)", Mode, k));
                }

                if (Opts.EnableJsonPathPredicates) {
                    if (!KeysWithItems.empty()) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.items ? (exists(@[*])).size()' RETURNING Int64) = 2", Mode));
                    }
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank ? (@ >= 0).abs()' RETURNING Int64) < 50", Mode));
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank ? (@ >= 0).ceiling()' RETURNING Int64) >= 0", Mode));
                    }
                }
            }

            if (Opts.EnableJsonPathPredicates) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.* ? ((exists(@.nope_xyz)) is unknown)' RETURNING Bool)", Mode));

                if (!KeysWithFlatObj.empty()) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (exists(@.shared) && @.shared == "shared_v").shared' RETURNING Utf8) = "shared_v"u)", Mode));

                    for (size_t i = 0; i < 2; ++i) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} == {1}).rank' RETURNING Int64) >= 0", Mode, PickFrom(KeysWithFlatObj)));
                    }
                }

                if (!KeysWithDeepNested.empty()) {
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.a ? (exists(@.b) && exists(@.b.c)).b.c.u_{1}' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithDeepNested)));
                }

                if (!KeysWithHomoArr.empty()) {
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $[0] ? (((exists(@.u_{1}) && @.u_{1} == {1})) || exists(@.shared)).u_{1}' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithHomoArr)));
                }

                if (!KeysWithItems.empty()) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[*] ? (@.id > 0 && exists(@.name)).name' RETURNING Utf8) <> "nope"u)", Mode));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.items[*] ? (exists(@.id)) ? (@.id == {1}).id' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithItems)));

                    if (Opts.EnableRangeComparisons) {
                        AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $.items[*] ? (((@.id > 0) && exists(@.name)) || (@.id < 0)).name' RETURNING Utf8) <> "nope"u)", Mode));

                        if (!KeysWithFlatObj.empty()) {
                            AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $ ? (((exists(@.shared)) && (@.rank >= 0)) || (@.rank < -1)).shared' RETURNING Utf8) = "shared_v"u)", Mode));
                        }
                    }
                }

                if (!KeysWithNestedObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared ? (exists(@.u_{1}) && @.u_{1} == {1}).u_{1}' RETURNING Int64) = {1}", Mode, PickFrom(KeysWithNestedObj)));
                    }

                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.* ? ((@ starts with "x") is unknown).u_{1}' RETURNING Int64) = {1})", Mode, PickFrom(KeysWithNestedObj)));
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.* ? ((@ like_regex "x") is unknown).u_{1}' RETURNING Int64) = {1})", Mode, PickFrom(KeysWithNestedObj)));
                }

                if (!KeysWithMixed.empty()) {
                    const ui64 k = PickFrom(KeysWithMixed);
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared[1] ? (((exists(@.u_{1}) && @.u_{1} == {1})) || exists(@.shared)).shared[0]' RETURNING Int64) = 1", Mode, k));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $ ? (((exists(@.g5_{1})) && exists(@.g5_{1}.deep.v)) || exists(@.shared)).g5_{1}.deep.v' RETURNING Int64) = {2}", Mode, k % 5, k));
                }

                if (Opts.EnablePassingVariables) {
                    if (!KeysWithNestedObj.empty()) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.shared ? (exists(@.u_{1}) && @.u_{1} == $val).u_{1}' PASSING {1} AS val RETURNING Int64) = {1}", Mode, PickFrom(KeysWithNestedObj)));
                        AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $.* ? ((@ starts with $pfx) is unknown).u_{1}' PASSING "x"u AS pfx RETURNING Int64) = {1})", Mode, PickFrom(KeysWithNestedObj)));
                    }

                    if (Opts.EnableRangeComparisons) {
                        if (!KeysWithFlatObj.empty()) {
                            AddJ(fmt::format("JSON_VALUE(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} >= $lo).rank' PASSING {1} AS lo RETURNING Int64) >= 0", Mode, PickFrom(KeysWithFlatObj)));
                        }
                    }
                }
            }
        }
    }

    void GenerateJsonIsLiteral() {
        if (Opts.EnableJsonExists) {
            if (!KeysWithScalarNull.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (null == @)')", Mode));
            }

            if (!KeysWithScalarString.empty()) {
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@ == "u_v_{1}")'))", Mode, PickFrom(KeysWithScalarString)));
                AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@ != "nope")'))", Mode));
            }

            if (!KeysWithScalarNumber.empty()) {
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == {})')", Mode, PickFrom(KeysWithScalarNumber) - 1));
            }

            if (!KeysWithScalarDouble.empty()) {
                const ui64 k = PickFrom(KeysWithScalarDouble);
                const double dv = -static_cast<double>(k) - 0.5;
                AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == {})')", Mode, dv));
            }

            if (Opts.EnableRangeComparisons) {
                if (!KeysWithScalarInt.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ > {})')", Mode, PickFrom(KeysWithScalarInt) - 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ <= {})')", Mode, PickFrom(KeysWithScalarInt)));
                }

                if (!KeysWithScalarString.empty()) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@ >= "u_v")'))", Mode));
                }

                if (!KeysWithScalarDouble.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarDouble);
                    const double dv = -static_cast<double>(k) - 0.5;
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ < {})')", Mode, dv + 1.0));
                }
            }

            if (Opts.EnableJsonPathPredicates) {
                if (!KeysWithScalarString.empty()) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{} $ ? (@ starts with "u_v")'))", Mode));
                }
            }

            if (Opts.EnablePassingVariables) {
                if (!KeysWithScalarNull.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == $var)' PASSING NULL AS var)", Mode));
                }

                if (!KeysWithScalarInt.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == $var)' PASSING {} AS var)", Mode, PickFrom(KeysWithScalarInt)));
                }

                if (!KeysWithScalarString.empty()) {
                    AddJ(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@ == $var)' PASSING "u_v_{1}"u AS var))", Mode, PickFrom(KeysWithScalarString)));
                }

                if (Opts.EnableSqlParameters) {
                    if (!KeysWithScalarInt.empty()) {
                        const ui64 k = PickFrom(KeysWithScalarInt);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == {})' PASSING {} AS {})", Mode, pn, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }

                    if (!KeysWithScalarString.empty()) {
                        const ui64 k = PickFrom(KeysWithScalarString);
                        const auto uvk = "u_v_" + std::to_string(k);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@ == {})' PASSING {} AS {})", Mode, pn, pn, vn),
                            [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                    }
                }
            }
        }

        if (Opts.EnableJsonValue) {
            if (!KeysWithScalarString.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithScalarString);
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $' RETURNING Utf8) = "u_v_{1}"u)", Mode, k));
                }
            }

            if (!KeysWithScalarBoolean.empty()) {
                AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool)", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool) = true", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Bool) != false", Mode));
            }

            if (!KeysWithScalarInt.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) = {}", Mode, PickFrom(KeysWithScalarInt)));
                }
            }

            if (!KeysWithScalarDouble.empty()) {
                const ui64 k = PickFrom(KeysWithScalarDouble);
                const double dv = -static_cast<double>(k) - 0.5;
                AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Double) = {}", Mode, dv));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Float) = {}f", Mode, dv));
            }

            if (Opts.EnableRangeComparisons) {
                if (!KeysWithScalarString.empty()) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $' RETURNING Utf8) >= "u_v"u)", Mode));
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $' RETURNING Utf8) < "u_w"u)", Mode));
                }

                if (!KeysWithScalarInt.empty()) {
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) > {}", Mode, PickFrom(KeysWithScalarInt) - 1));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) <= {}", Mode, PickFrom(KeysWithScalarInt)));
                }

                if (!KeysWithScalarDouble.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarDouble);
                    const double dv = -static_cast<double>(k) - 0.5;
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Double) > {}", Mode, dv - 1.0));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Double) <= {}", Mode, dv + 1.0));
                }
            }

            if (Opts.EnableBetween) {
                if (!KeysWithScalarString.empty()) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $' RETURNING Utf8) BETWEEN "u_v"u AND "u_w"u)", Mode));
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{} $' RETURNING Utf8) NOT BETWEEN "a"u AND "b"u)", Mode));
                }

                if (!KeysWithScalarInt.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarInt);
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) BETWEEN {} AND {}", Mode, k - 1, k + 1));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) NOT BETWEEN {} AND {}", Mode, k + 1, k + 10));
                }

                if (!KeysWithScalarDouble.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarDouble);
                    const double dv = -static_cast<double>(k) - 0.5;
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Double) BETWEEN {} AND {}", Mode, dv - 1.0, dv + 1.0));
                }
            }

            if (Opts.EnableInList) {
                if (!KeysWithScalarString.empty()) {
                    AddJ(fmt::format(R"(JSON_VALUE(Text, '{0} $' RETURNING Utf8) IN ("u_v_{1}"u, "nope"u))", Mode, PickFrom(KeysWithScalarString)));
                    AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $' RETURNING Utf8) NOT IN ("u_v_{1}"u, "nope"u))", Mode, PickFrom(KeysWithScalarString)));
                }

                if (!KeysWithScalarInt.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarInt);
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) IN ({}, {}, {})", Mode, k, k + 1, k + 2));
                    const ui64 k2 = PickFrom(KeysWithScalarInt);
                    AddJErr(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) NOT IN ({}, {}, {})", Mode, k2, k2 + 1, k2 + 2));
                }

                if (!KeysWithScalarDouble.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarDouble);
                    const double dv = -static_cast<double>(k) - 0.5;
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Double) IN ({}, {})", Mode, dv, dv + 1.0));
                }
            }

            if (Opts.EnableSqlParameters) {
                if (!KeysWithScalarString.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarString);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Utf8) = {}", Mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }

                if (!KeysWithScalarInt.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarInt);
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) = {}", Mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });

                    const ui64 kBetween = PickFrom(KeysWithScalarInt);
                    auto plo = NewPname();
                    auto phi = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Int64) BETWEEN {} AND {}", Mode, plo, phi),
                        [plo, phi, kBetween](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64(kBetween - 1).Build();
                            bld.AddParam(phi).Int64(kBetween + 1).Build();
                        });
                }

                if (!KeysWithScalarDouble.empty()) {
                    const ui64 k = PickFrom(KeysWithScalarDouble);
                    const double dv = -static_cast<double>(k) - 0.5;
                    auto pn = NewPname();
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $' RETURNING Double) = {}", Mode, pn),
                        [pn, dv](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Double(dv).Build(); });
                }
            }
        }
    }

    void GenerateArithmeticOperators() {
        if (Opts.EnableJsonExists) {
            if (!KeysWithScalarInt.empty()) {
                const ui64 k = PickFrom(KeysWithScalarInt);
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (-$ == {1})')", Mode, -(int64_t)k));
                AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (+$ == {1})')", Mode, (int64_t)k));
            }

            if (!KeysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (-@.rank == {1})')", Mode, -r));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (+@.rank == {1})')", Mode, r));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (-@.u_{1} == {2})')", Mode, k, -(int64_t)k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank + 5 == {1})')", Mode, r + 5));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank - 1 == {1})')", Mode, r - 1));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank * 2 == {1})')", Mode, r * 2));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank % 5 == {1})')", Mode, r % 5));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? ((@.rank + 1) * 2 == {1})')", Mode, (r + 1) * 2));
                }

                {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (50 - @.rank == {1})')", Mode, 50 - r));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? ({1} + @.rank == {2})')", Mode, (int64_t)k, (int64_t)k + r));
                }

                for (int rDiv : {0, 5, 10, 15, 20}) {
                    if (!FlatObjByRank[rDiv].empty()) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank / 5 == {1})')", Mode, rDiv / 5));
                        break;
                    }
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} + @.rank == {2})')", Mode, k, k + r));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} - @.rank == {2})')", Mode, k, k - r));
                }
            }

            if (Opts.EnableRangeComparisons) {
                if (!KeysWithFlatObj.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (-@.rank < 0)')", Mode));
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (+@.rank >= 0)')", Mode));

                    {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank + 1 > {1})')", Mode, r));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank * 2 <= {1})')", Mode, r * 2));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank - 1 != {1})')", Mode, r));
                    }

                    {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} + @.rank > {2})')", Mode, k, k + r - 1));
                    }
                }
            }

            if (Opts.EnableJsonPathMethods) {
                if (!KeysWithFlatObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank.abs() + 1 == {1})')", Mode, r + 1));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (-@.rank.abs() == {1})')", Mode, -r));
                    }
                }

                if (!KeysWithItems.empty()) {
                    AddJ(fmt::format("JSON_EXISTS(Text, '{} $.items.size() ? (@ * 2 == 4)')", Mode));
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank.abs() - 1 >= 0)')", Mode));
                    }
                }
            }

            if (Opts.EnablePassingVariables) {
                if (!KeysWithFlatObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? (@.rank + $inc == {1})' PASSING 5 AS inc)", Mode, r + 5));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{0} $ ? ($base - @.rank == 0)' PASSING {1} AS base)", Mode, r));
                    }
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank * $factor > 0)' PASSING 2 AS factor)", Mode));
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank + $delta < 50)' PASSING 0 AS delta)", Mode));
                    }
                }

                if (Opts.EnableSqlParameters) {
                    if (!KeysWithFlatObj.empty()) {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_EXISTS(Text, '{} $ ? (@.rank + {} == {})' PASSING {} AS {})", Mode, pn, r + 1, pn, vn),
                            [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(1).Build(); });
                    }
                }
            }
        }

        if (Opts.EnableJsonValue) {
            if (!KeysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank + 5 == {1}' RETURNING Bool)", Mode, r + 5));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank - 1 == {1}' RETURNING Bool)", Mode, r - 1));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank * 2 == {1}' RETURNING Bool)", Mode, r * 2));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank % 5 == {1}' RETURNING Bool)", Mode, r % 5));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} -$.rank == {1}' RETURNING Bool)", Mode, -r));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} +$.rank == {1}' RETURNING Bool)", Mode, r));
                }

                {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1} + $.rank == {2}' RETURNING Bool)", Mode, k, k + r));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.u_{1} - $.rank == {2}' RETURNING Bool)", Mode, k, k - r));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} 50 - $.rank == {1}' RETURNING Bool)", Mode, 50 - r));
                }

                for (int rDiv : {0, 5, 10, 15, 20}) {
                    if (!FlatObjByRank[rDiv].empty()) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank / 5 == {1}' RETURNING Bool)", Mode, rDiv / 5));
                        break;
                    }
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + 5 = {1}", Mode, r + 5));
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) * 2 = {1}", Mode, r * 2));
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) - 1 = {1}", Mode, r - 1));
                    AddJErr(fmt::format("5 + JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = {1}", Mode, r + 5));
                }

                {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJErr(fmt::format(
                        "JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {2}",
                        Mode, k, k + r));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank ? (@ + 5 == {1})' RETURNING Int64) = {2}", Mode, r + 5, r));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank ? (-@ == {1})' RETURNING Int64) = {2}", Mode, -r, r));
                    AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank ? (@ * 2 == {1})' RETURNING Int64) = {2}", Mode, r * 2, r));
                }

                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) = -1", Mode));
                AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <> -999", Mode));
            }

            if (Opts.EnableRangeComparisons) {
                if (!KeysWithFlatObj.empty()) {
                    {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank ? (@ + 1 > {1})' RETURNING Int64) = {2}", Mode, r, r));
                    }

                    {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank + 1 > {1}' RETURNING Bool)", Mode, r));
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank * 2 <= {1}' RETURNING Bool)", Mode, r * 2));
                        AddJ(fmt::format("JSON_VALUE(Text, '{} -$.rank < 0' RETURNING Bool)", Mode));
                    }

                    AddJErr(fmt::format("-JSON_VALUE(Text, '{} $.rank' RETURNING Int64) < 0", Mode));
                    AddJErr(fmt::format("-JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <= -1", Mode));

                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) > -1", Mode));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) >= -10", Mode));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) < -1", Mode));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <= -1", Mode));
                }
            }

            if (Opts.EnableJsonPathMethods) {
                if (!KeysWithFlatObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank.abs() + 1 == {1}' RETURNING Bool)", Mode, r + 1));
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} -$.rank.abs() == {1}' RETURNING Bool)", Mode, -r));
                    }
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank.abs() - 1 >= 0' RETURNING Bool)", Mode));
                    }
                }
            }

            if (Opts.EnablePassingVariables) {
                if (!KeysWithFlatObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank + $inc == {1}' PASSING 5 AS inc RETURNING Bool)", Mode, r + 5));
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $base - $.rank == 0' PASSING {1} AS base RETURNING Bool)", Mode, r));
                    }
                }

                if (Opts.EnableRangeComparisons) {
                    if (!KeysWithFlatObj.empty()) {
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank * $factor > 0' PASSING 2 AS factor RETURNING Bool)", Mode));
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank + $delta < 50' PASSING 0 AS delta RETURNING Bool)", Mode));
                    }
                }

                if (Opts.EnableSqlParameters) {
                    if (!KeysWithFlatObj.empty()) {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank + {} == {}' PASSING {} AS {} RETURNING Bool)", Mode, pn, r + 1, pn, vn),
                            [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(1).Build(); });
                    }
                }
            }

            if (Opts.EnableBetween) {
                if (!KeysWithFlatObj.empty()) {
                    const ui64 k = PickFrom(KeysWithFlatObj);
                    const int64_t r = k % 50;
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) * 2 BETWEEN {1} AND {2}",
                        Mode, r * 2 - 1, r * 2 + 1));

                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN -5 AND 50", Mode));
                    AddJ(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) NOT BETWEEN -100 AND -1", Mode));
                }
            }

            if (Opts.EnableInList) {
                if (!KeysWithFlatObj.empty()) {
                    {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + 1 IN ({1}, -1, 999)", Mode, r + 1));
                    }

                    {
                        const ui64 k = PickFrom(KeysWithFlatObj);
                        const int64_t r = k % 50;
                        AddJ(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) IN ({1}, -1, -5)", Mode, r));
                        AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) NOT IN ({1}, -1, -5)", Mode, r));
                    }
                }
            }
        }
    }

    void GenerateSqlNullChecks() {
        if (Opts.EnableJsonExists) {
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') IS NULL", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') IS NOT NULL", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS NULL", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS NOT NULL", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.*') IS NULL", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.*') IS NOT NULL", Mode));

            if (Opts.EnablePassingVariables) {
                if (!KeysWithUKey.empty()) {
                    AddJErr(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) IS NULL", Mode, PickFrom(KeysWithUKey)));
                    AddJErr(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) IS NOT NULL", Mode, PickFrom(KeysWithUKey)));
                }
            }
        }

        if (Opts.EnableJsonValue) {
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) IS NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) IS NOT NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.nope_xyz' RETURNING Utf8) IS NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.nope_xyz' RETURNING Utf8) IS NOT NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.*' RETURNING Utf8) IS NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.*' RETURNING Utf8) IS NOT NULL", Mode));

            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS NOT NULL", Mode));

            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Double) IS NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Double) IS NOT NULL", Mode));

            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NULL", Mode));
            AddJErr(fmt::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NOT NULL", Mode));

            AddJErr(fmt::format("(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) = \"shared_v\"u) IS NULL", Mode));
            AddJErr(fmt::format("(JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) = \"shared_v\"u) IS NOT NULL", Mode));

            AddJErr(fmt::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = 10) IS NULL", Mode));
            AddJErr(fmt::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = 10) IS NOT NULL", Mode));
        }
    }

    void GenerateDistinctFrom() {
        if (Opts.EnableJsonExists) {
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') IS NOT DISTINCT FROM true", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') IS NOT DISTINCT FROM true", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS NOT DISTINCT FROM true", Mode));

            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') IS DISTINCT FROM true", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') IS DISTINCT FROM true", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS DISTINCT FROM true", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') IS NOT DISTINCT FROM false", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') IS NOT DISTINCT FROM false", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') IS DISTINCT FROM false", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $.shared') IS DISTINCT FROM false", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') IS DISTINCT FROM NULL", Mode));
            AddJErr(fmt::format("JSON_EXISTS(Text, '{} $') IS NOT DISTINCT FROM NULL", Mode));

            if (Opts.EnablePassingVariables) {
                if (!KeysWithIntUVal.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = PickFrom(KeysWithIntUVal);
                        AddJErr(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) IS NOT DISTINCT FROM true", Mode, k));
                    }
                }

                if (!KeysWithBothSharedAndU.empty()) {
                    const ui64 k = PickFrom(KeysWithBothSharedAndU);
                    AddJErr(fmt::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} && @.shared == "shared_v")') IS NOT DISTINCT FROM true)", Mode, k));
                }

                if (Opts.EnableSqlParameters) {
                    if (!KeysWithIntUVal.empty()) {
                        const ui64 k = PickFrom(KeysWithIntUVal);
                        auto pn = NewPname();
                        auto vn = pn.substr(1);
                        AddJErr(fmt::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == {2})' PASSING {2} AS {3}) IS NOT DISTINCT FROM true", Mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                    }
                }
            }
        }

        if (Opts.EnableJsonValue) {
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.shared' RETURNING Utf8) IS NOT DISTINCT FROM "shared_v"u)", Mode));
            AddJErr(fmt::format(R"(JSON_VALUE(Text, '{} $.*' RETURNING Utf8) IS NOT DISTINCT FROM "shared_v"u)", Mode));

            if (!KeysWithStrUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithStrUVal);
                    AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) IS NOT DISTINCT FROM "u_v_{1}"u)", Mode, k));
                }
            }

            for (size_t i = 0; i < 3; ++i) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS NOT DISTINCT FROM {}", Mode, Rng.Uniform(50)));
            }

            for (int j = 0; j < 5; ++j) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS NOT DISTINCT FROM true", Mode, j));
            }

            if (!KeysWithFullMix.empty()) {
                AddJErr(fmt::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NOT DISTINCT FROM true", Mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) IS NOT DISTINCT FROM {1}", Mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = PickFrom(KeysWithFullMix);
                    AddJErr(fmt::format(R"(JSON_VALUE(Text, '{0} $.shared_s' RETURNING Utf8) IS NOT DISTINCT FROM "u_v_{1}"u)", Mode, k));
                }
            }

            if (Opts.EnableSqlParameters) {
                {
                    auto pn = NewPname();
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.shared' RETURNING Utf8) IS DISTINCT FROM {1}", Mode, pn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                }

                if (!KeysWithIntUVal.empty()) {
                    const ui64 k = PickFrom(KeysWithIntUVal);
                    auto pn = NewPname();
                    AddJErr(fmt::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {2}", Mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(k).Build(); });
                }
            }
        }
    }

    void GenerateNonJsonFilters() {
        for (size_t i = 0; i < 3; ++i) {
            const ui64 k = Rows[Rng.Uniform(Rows.size())].Key;
            FilterAtoms.push_back(TAtom{
                .Sql = fmt::format("Key = {}", k),
                .AddParams = nullptr,
                .IsJsonIndexable = false});
        }

        {
            const ui64 mid = Rows.size() / 2 + 1;

            FilterAtoms.push_back(TAtom{
                .Sql = fmt::format("Key > {}", mid),
                .AddParams = nullptr,
                .IsJsonIndexable = false});

            FilterAtoms.push_back(TAtom{
                .Sql = fmt::format("Key < {}", mid),
                .AddParams = nullptr,
                .IsJsonIndexable = false});

            FilterAtoms.push_back(TAtom{
                .Sql = fmt::format("Key BETWEEN {} AND {}", mid - 5, mid + 5),
                .AddParams = nullptr,
                .IsJsonIndexable = false});
        }

        if (Opts.EnableSqlParameters) {
            const ui64 k = Rows[Rng.Uniform(Rows.size())].Key;
            auto pn = NewPname();
            FilterAtoms.push_back(TAtom{
                .Sql = fmt::format("Key = {}", pn),
                .AddParams = [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Uint64(k).Build(); },
                .IsJsonIndexable = false});
        }
    }

    std::vector<TBuiltPredicate> BuildResult(size_t maxCount) {
        std::vector<TBuiltPredicate> result;
        result.reserve(maxCount);

        auto pushAtom = [&](const TAtom& a) {
            if (result.size() >= maxCount) {
                return;
            }

            TBuiltPredicate p;
            p.Sql = a.Sql;
            p.ExpectExtractError = !a.IsJsonIndexable;
            if (a.AddParams) {
                NYdb::TParamsBuilder builder;
                a.AddParams(builder);
                p.Params = builder.Build();
            }

            result.push_back(std::move(p));
        };

        {
            std::vector<size_t> idx(JsonAtoms.size());
            std::iota(idx.begin(), idx.end(), 0);

            for (size_t i = idx.size() - 1; i > 0; --i) {
                std::swap(idx[i], idx[Rng.Uniform(i + 1)]);
            }

            const size_t budget = maxCount / 2;
            for (size_t i = 0; i < idx.size() && result.size() < budget; ++i) {
                pushAtom(JsonAtoms[idx[i]]);
            }
        }

        if (!JsonAtoms.empty()) {
            // J* AND J*
            if (Opts.EnableAndCombinations) {
                for (size_t i = 0; i < 10 && result.size() < maxCount * 3 / 4; ++i) {
                    const auto& a = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& b = JsonAtoms[Rng.Uniform(JsonAtoms.size())];

                    pushAtom(AndAtom(a, b));
                }
            }

            // J* OR J*
            if (Opts.EnableOrCombinations) {
                for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                    const auto& a = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& b = JsonAtoms[Rng.Uniform(JsonAtoms.size())];

                    pushAtom(OrAtom(a, b));
                }
            }

            if (Opts.EnableAndCombinations && Opts.EnableOrCombinations) {
                // J* AND J* OR J*
                for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                    const auto& a = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& b = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& c = JsonAtoms[Rng.Uniform(JsonAtoms.size())];

                    pushAtom(OrAtom(AndAtom(a, b), c));
                }

                // J* OR J* AND J*
                for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                    const auto& a = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& b = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& c = JsonAtoms[Rng.Uniform(JsonAtoms.size())];

                    pushAtom(AndAtom(OrAtom(a, b), c));
                }
            }

            // J* AND J* AND J*
            if (Opts.EnableAndCombinations) {
                for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                    const auto& a = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& b = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& c = JsonAtoms[Rng.Uniform(JsonAtoms.size())];

                    pushAtom(AndAtom(a, AndAtom(b, c)));
                }
            }

            // J* OR J* OR J*
            if (Opts.EnableOrCombinations) {
                for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                    const auto& a = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& b = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                    const auto& c = JsonAtoms[Rng.Uniform(JsonAtoms.size())];

                    pushAtom(OrAtom(a, OrAtom(b, c)));
                }
            }

            if (!FilterAtoms.empty()) {
                if (Opts.EnableAndCombinations) {
                    // J* AND non-J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(j, f));
                    }

                    // non-J* AND J* AND J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(AndAtom(f, j1), j2));
                    }

                    // J* AND non-J* AND J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(AndAtom(j1, f), j2));
                    }

                    // J* AND J* AND non-J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(AndAtom(j1, j2), f));
                    }
                }

                if (Opts.EnableOrCombinations) {
                    // J* OR non-J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(j, f));
                    }

                    // non-J* OR J* OR J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(OrAtom(f, j1), j2));
                    }

                    // J* OR non-J* OR J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(OrAtom(j1, f), j2));
                    }

                    // J* OR J* OR non-J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(OrAtom(j1, j2), f));
                    }
                }

                if (Opts.EnableAndCombinations && Opts.EnableOrCombinations) {
                    // J* AND J* OR non-J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(AndAtom(j1, j2), f));
                    }

                    // J* AND non-J* OR J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(AndAtom(j1, f), j2));
                    }

                    // non-J* AND J* OR J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(OrAtom(AndAtom(f, j1), j2));
                    }

                    // J* OR J* AND non-J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(OrAtom(j1, j2), f));
                    }

                    // J* OR non-J* AND J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(OrAtom(j1, f), j2));
                    }

                    // non-J* OR J* AND J*
                    for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                        const auto& j1 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& j2 = JsonAtoms[Rng.Uniform(JsonAtoms.size())];
                        const auto& f = FilterAtoms[Rng.Uniform(FilterAtoms.size())];

                        pushAtom(AndAtom(OrAtom(f, j1), j2));
                    }
                }
            }
        }

        return result;
    }

private:
    const TPredicateBuilderOptions& Opts;
    TMersenne<ui64> Rng;
    const std::string Mode;
    const std::vector<TGeneratedRow>& Rows;

    std::vector<TAtom> JsonAtoms;
    std::vector<TAtom> FilterAtoms;
    size_t ParamCount = 0;

    std::vector<ui64> KeysWithUKey;
    std::vector<ui64> KeysWithIntUVal;
    std::vector<ui64> KeysWithStrUVal;
    std::vector<ui64> KeysWithUArr;
    std::vector<ui64> KeysWithShared;
    std::vector<ui64> KeysWithFlatObj;
    std::vector<ui64> KeysWithBothSharedAndU;
    std::vector<ui64> KeysWithNestedObj;
    std::vector<ui64> KeysWithDeepNested;
    std::vector<ui64> KeysWithHomoArr;
    std::vector<ui64> KeysWithHeteroArr;
    std::vector<ui64> KeysWithMixed;
    std::vector<ui64> KeysWithItems;
    std::vector<ui64> KeysWithFullMix;
    std::vector<ui64> KeysWithEmptyContainers;
    std::vector<ui64> KeysWithArrayLiterals;
    std::vector<ui64> KeysWithArrayOfArrays;
    std::vector<ui64> KeysWithScalarNull;
    std::vector<ui64> KeysWithScalarBoolean;
    std::vector<ui64> KeysWithScalarNumber;
    std::vector<ui64> KeysWithScalarInt;
    std::vector<ui64> KeysWithScalarDouble;
    std::vector<ui64> KeysWithScalarString;
    std::array<std::vector<ui64>, 50> FlatObjByRank;
};

} // namespace

std::vector<TBuiltPredicate> TPredicateBuilder::BuildBatch(
    const TJsonCorpus& corpus, bool isStrict, size_t maxCount, ui64 seed,
    const TPredicateBuilderOptions& opts) const {
    return TPredicateBatchGenerator(corpus, isStrict, seed, opts).Build(maxCount);
}

} // namespace NKikimr::NKqp
