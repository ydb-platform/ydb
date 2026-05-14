#include "kqp_json_index_predicate.h"

#include <util/random/mersenne.h>

namespace NKikimr::NKqp {

namespace {

struct TAtom {
    std::string Sql;
    std::function<void(NYdb::TParamsBuilder&)> AddParams;
    bool IsJsonIndexable = true;
};

} // namespace

std::vector<TBuiltPredicate> TPredicateBuilder::BuildBatch(
    const TJsonCorpus& corpus, bool isStrict, size_t maxCount, ui64 seed,
    const TPredicateBuilderOptions& opts) const
{
    TMersenne<ui64> rng(seed);
    const std::string mode = isStrict ? "strict" : "lax";

    std::vector<ui64> keysWithUKey;
    std::vector<ui64> keysWithIntUVal;
    std::vector<ui64> keysWithStrUVal;
    std::vector<ui64> keysWithUArr;
    std::vector<ui64> keysWithShared;
    std::vector<ui64> keysWithFlatObj;
    std::vector<ui64> keysWithBothSharedAndU;
    std::vector<ui64> keysWithNestedObj;
    std::vector<ui64> keysWithDeepNested;
    std::vector<ui64> keysWithHomoArr;
    std::vector<ui64> keysWithHeteroArr;
    std::vector<ui64> keysWithMixed;
    std::vector<ui64> keysWithItems;
    std::vector<ui64> keysWithFullMix;
    std::vector<ui64> keysWithEmptyContainers;
    std::vector<ui64> keysWithArrayLiterals;
    std::vector<ui64> keysWithArrayOfArrays;
    std::vector<ui64> keysWithScalarNull;
    std::vector<ui64> keysWithScalarBoolean;
    std::vector<ui64> keysWithScalarNumber;
    std::vector<ui64> keysWithScalarInt;
    std::vector<ui64> keysWithScalarString;

    std::array<std::vector<ui64>, 50> flatObjByRank;

    const auto& rows = corpus.Rows();

    for (const auto& row : rows) {
        switch (row.Shape) {
            case EJsonShape::Scalar:
                switch (row.Key % 6) {
                    case 0:
                        keysWithScalarNull.push_back(row.Key);
                        break;
                    case 1:
                    case 2:
                        keysWithScalarBoolean.push_back(row.Key);
                        break;
                    case 3:
                        keysWithScalarNumber.push_back(row.Key);
                        keysWithScalarInt.push_back(row.Key);
                        break;
                    case 4:
                        keysWithScalarNumber.push_back(row.Key);
                        break;
                    case 5:
                        keysWithScalarString.push_back(row.Key);
                        break;
                    default:
                        break;
                }
                break;

            case EJsonShape::FlatObj:
                keysWithUKey.push_back(row.Key);
                keysWithIntUVal.push_back(row.Key);
                keysWithShared.push_back(row.Key);
                keysWithFlatObj.push_back(row.Key);
                keysWithBothSharedAndU.push_back(row.Key);
                flatObjByRank[row.Key % 50].push_back(row.Key);
                break;

            case EJsonShape::EmptyContainers:
                keysWithUKey.push_back(row.Key);
                keysWithShared.push_back(row.Key);
                keysWithEmptyContainers.push_back(row.Key);
                break;

            case EJsonShape::EmptyKey:
                keysWithUKey.push_back(row.Key);
                keysWithStrUVal.push_back(row.Key);
                break;

            case EJsonShape::ArrayLiterals:
                keysWithArrayLiterals.push_back(row.Key);
                break;

            case EJsonShape::ObjWithArray:
                keysWithUKey.push_back(row.Key);
                keysWithUArr.push_back(row.Key);
                keysWithShared.push_back(row.Key);
                keysWithBothSharedAndU.push_back(row.Key);
                break;

            case EJsonShape::ArrayOfArrays:
                keysWithArrayOfArrays.push_back(row.Key);
                break;

            case EJsonShape::NestedObj:
                keysWithNestedObj.push_back(row.Key);
                keysWithShared.push_back(row.Key);
                break;

            case EJsonShape::DeepNested:
                keysWithDeepNested.push_back(row.Key);
                break;

            case EJsonShape::HomogeneousArrayObjs:
                keysWithHomoArr.push_back(row.Key);
                break;

            case EJsonShape::HeterogeneousArrayObjs:
                keysWithHeteroArr.push_back(row.Key);
                break;

            case EJsonShape::Mixed:
                keysWithMixed.push_back(row.Key);
                keysWithShared.push_back(row.Key);
                break;

            case EJsonShape::ObjWithArrayObjs:
                keysWithItems.push_back(row.Key);
                break;

            case EJsonShape::FullLiteralMix:
                keysWithFullMix.push_back(row.Key);
                break;

            default:
                break;
        }
    }

    auto pickFrom = [&](const std::vector<ui64>& pool) -> ui64 {
        if (!pool.empty()) {
            return pool[rng.Uniform(pool.size())];
        }
        return rows[rng.Uniform(rows.size())].Key;
    };

    auto randKey = [&]() -> ui64 {
        return rows[rng.Uniform(rows.size())].Key;
    };

    auto mergeAdd = [](std::function<void(NYdb::TParamsBuilder&)> a, std::function<void(NYdb::TParamsBuilder&)> b)
        -> std::function<void(NYdb::TParamsBuilder&)>
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
    };

    auto andAtom = [&](const TAtom& a, const TAtom& b) -> TAtom {
        return TAtom{
            .Sql="(" + a.Sql + ") AND (" + b.Sql + ")",
            .AddParams=mergeAdd(a.AddParams, b.AddParams),
            .IsJsonIndexable=a.IsJsonIndexable || b.IsJsonIndexable
        };
    };

    auto orAtom = [&](const TAtom& a, const TAtom& b) -> TAtom {
        return TAtom{
            .Sql="(" + a.Sql + ") OR (" + b.Sql + ")",
            .AddParams=mergeAdd(a.AddParams, b.AddParams),
            .IsJsonIndexable=a.IsJsonIndexable && b.IsJsonIndexable
        };
    };

    size_t paramCount = 0;
    auto newPname = [&]() -> std::string {
        return "$p" + std::to_string(paramCount++);
    };

    std::vector<TAtom> jsonAtoms;
    std::vector<TAtom> filterAtoms;

    auto addJ = [&](std::string sql, std::function<void(NYdb::TParamsBuilder&)> addP = nullptr) {
        jsonAtoms.push_back(TAtom{
            .Sql=std::move(sql),
            .AddParams=std::move(addP),
            .IsJsonIndexable=true
        });
    };

    auto addJErr = [&](std::string sql, std::function<void(NYdb::TParamsBuilder&)> addP = nullptr) {
        jsonAtoms.push_back(TAtom{
            .Sql=std::move(sql),
            .AddParams=std::move(addP),
            .IsJsonIndexable=false
        });
    };

    if (opts.EnableJsonExists) {
        addJ(std::format("JSON_EXISTS(Text, '{} $')", mode));

        {
            for (size_t i = 0; i < 6; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}')", mode, pickFrom(keysWithUKey)));
            }

            for (size_t i = 0; i < 4; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}')", mode, randKey()));
            }

            addJ(std::format("JSON_EXISTS(Text, '{} $.shared')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.shared') = true", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.nope_xyz')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.*')", mode));

            for (int j = 0; j < 5; ++j) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.g5_{}')", mode, j));
            }
        }

        {
            for (size_t i = 0; i < 3; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[*]')", mode, pickFrom(keysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[0]')", mode, pickFrom(keysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[last]')", mode, pickFrom(keysWithUArr)));
            }

            addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[last - 1]')", mode, pickFrom(keysWithUArr)));
            addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[0 to 1]')", mode, pickFrom(keysWithUArr)));
            addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[0, 2]')", mode, pickFrom(keysWithUArr)));

            addJ(std::format("JSON_EXISTS(Text, '{} $.shared[*]')", mode));
        }

        {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == {1})')", mode, k));
            }

            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1})')", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == "shared_v")'))", mode));
            }

            for (size_t i = 0; i < 3; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank == {})')", mode, (int)rng.Uniform(50)));
            }

            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.* ? (@ == {1})')", mode, k));
            }
        }

        if (opts.EnableRangeComparisons) {
            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} > {2})')", mode, k, (int64_t)k - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= {1})')", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2})')", mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.* ? (@ <= {1})')", mode, k));
            }

            if (opts.EnableJsonPathPredicates) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.u_{1} ? (@ starts with "u_v")'))", mode, k));
                }
            }

            for (int lo : {10, 25}) {
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank > {})')", mode, lo));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank >= {})')", mode, lo));
            }
        }

        if (opts.EnablePassingVariables) {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var)", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var))", mode));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.* == $var)' PASSING "shared_v"u AS var))", mode));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithFlatObj);
                const auto g5k = "g5_" + std::to_string(k % 5);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == $v1 && @.{2} == $v2)' PASSING {1} AS v1, true AS v2)", mode, k, g5k));
            }

            if (opts.EnableSqlParameters) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    auto pn = newPname();
                    auto vn = pn.substr(1);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    auto pn = newPname();
                    auto vn = pn.substr(1);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == {1})' PASSING {1} AS {2}))", mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                }

                {
                    auto pn = newPname();
                    auto vn = pn.substr(1);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.* == {1})' PASSING {1} AS {2}))", mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    auto p0 = newPname();
                    auto p1 = newPname();
                    auto v0 = p0.substr(1);
                    auto v1 = p1.substr(1);

                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {2} && @.shared == {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k, p0, p1, v0, v1),
                        [p0, p1, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(p0).Int64((i64)k).Build();
                            bld.AddParam(p1).Utf8("shared_v").Build();
                        });
                }
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == $v1 && @.shared == $v2)' PASSING {1} AS v1, "shared_v"u AS v2))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= $lo && @.u_{1} <= $hi && @.shared == $sv)' PASSING {2} AS lo, {3} AS hi, "shared_v"u AS sv))", mode, k, k - 1, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == $var)' PASSING {1} AS var)", mode, k));
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 2));
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ != $var)' PASSING 0 AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ < $lo || @ > $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k + 1, k + 10));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared ? (@.u_{1} == $v1 && @.g5_{2} == $v2)' PASSING {1} AS v1, "v"u AS v2))", mode, k, k % 5));
                }
            }

            if (!keysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c ? (@.u_{1} >= $lo && @.u_{1} <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.a.*.* ? (@.u_{1} >= $lo && @.u_{1} <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c ? (@.* >= $lo && @.u_{1} <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c ? (@.u_{1} >= $lo && @.* <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                }
            }

            if (!keysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHomoArr);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $[*] ? (@.u_{1} == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 2));
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $[*].k_a ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $[*].k_a ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));

                    if (opts.EnableJsonPathPredicates) {
                        addJ(std::format(R"(JSON_EXISTS(Text, '{0} $[*].k_b ? (@ starts with $var)' PASSING "u_v"u AS var))", mode));
                    }
                }
            }

            if (!keysWithMixed.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithMixed);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == $var)' PASSING {2} AS var)", mode, k % 5, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ != $var)' PASSING 0 AS var)", mode, k % 5));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k % 5, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ < $lo || @ > $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k % 5, k + 1, k + 10));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep ? (@.v == $var)' PASSING {2} AS var)", mode, k % 5, k));
                }
            }

            if (!keysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    const auto uvk = "u_v_" + std::to_string(k);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*] ? (@.id == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.items[*] ? (@.id == $v1 && @.name == $v2)' PASSING {1} AS v1, "{2}"u AS v2))", mode, k, uvk));
                }
            }

            if (!keysWithFullMix.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    const auto uvk = "u_v_" + std::to_string(k);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ != $var)' PASSING 0 AS var)", mode));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ >= $lo && @ <= $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ < $lo || @ > $hi)' PASSING {2} AS lo, {3} AS hi)", mode, k, k + 1, k + 10));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared_s ? (@ == $var)' PASSING "{1}"u AS var))", mode, uvk));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared_b ? (@ == $var)' PASSING true AS var))", mode));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared_n == $v1 && @.shared_b == $v2)' PASSING {1} AS v1, true AS v2))", mode, k));
                }
            }

            if (opts.EnableSqlParameters) {
                if (!keysWithNestedObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ < {2} || @ > {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k + 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 10).Build();
                            });
                    }
                }

                if (!keysWithDeepNested.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }
                }

                if (!keysWithMixed.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithMixed);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == {2})' PASSING {2} AS {3})", mode, k % 5, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithMixed);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k % 5, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithMixed);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ < {2} || @ > {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k % 5, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k + 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 10).Build();
                            });
                    }
                }

                if (!keysWithItems.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithItems);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithItems);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*].* ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithItems);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ >= {1} && @ <= {2})' PASSING {1} AS {3}, {2} AS {4})", mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }
                }

                if (!keysWithFullMix.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithFullMix);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ >= {1} && @ <= {2})' PASSING {1} AS {3}, {2} AS {4})", mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        auto plo = newPname();
                        auto phi = newPname();
                        auto vlo = plo.substr(1);
                        auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ < {1} || @ > {2})' PASSING {1} AS {3}, {2} AS {4})", mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k + 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 10).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        const auto uvk = "u_v_" + std::to_string(k);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_s ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                    }
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    auto pn = newPname();
                    auto vn = pn.substr(1);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }
            }
        }

        {
            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} && @.shared == "shared_v")'))", mode, k));
            }

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.* == {1} && @.shared == "shared_v")'))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} || @.shared == "shared_v")'))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} || @.* == "shared_v")'))", mode, k));
            }

            if (opts.EnableRangeComparisons) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= {1} && @.shared == "shared_v")'))", mode, k));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2} && @.shared == "shared_v")'))", mode, k, k + 1));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2} && @.* == "shared_v")'))", mode, k, k + 1));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} > {2} || @.shared == "shared_v")'))", mode, k, k - 1));
                }
            }
        }

        {
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ == "shared_v")'))", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.shared ? (@ != null)')", mode));

            if (opts.EnableRangeComparisons) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ >= "s")'))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ >= "shared_v")'))", mode));
            }
        }

        {
            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[1]')", mode, pickFrom(keysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[*] ? (@ == {})')", mode, k, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == {2})')", mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.u_{1}[last] ? (@ == "u_v_{1}")'))", mode, k));
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[last - 1] ? (@ == {2})')", mode, k, k + 1));
            }

            {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[0 to 1] ? (@ == {1})')", mode, k));
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[0, 2] ? (@ == {1})')", mode, k));
            }

            if (opts.EnableRangeComparisons) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[*] ? (@ > {})')", mode, k, k - 1));
                    addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[0] ? (@ == {})')", mode, k, k));
                }
            }
        }

        {
            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.shared.u_{}')", mode, pickFrom(keysWithNestedObj)));
                }

                for (int j = 0; j < 5; ++j) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.shared.g5_{}')", mode, j));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == {1})')", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ > {2})')", mode, k, k - 1));
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ < {2})')", mode, k, k + 1));
                    }
                }
            }

            if (!keysWithDeepNested.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}')", mode, pickFrom(keysWithDeepNested)));
                }

                addJ(std::format("JSON_EXISTS(Text, '{} $.a.b.c')", mode));

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ >= {1})')", mode, k));
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ < {2})')", mode, k, k + 1));
                    }
                }
            }

            if (!keysWithHomoArr.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $[*].u_{}')", mode, pickFrom(keysWithHomoArr)));
                }

                addJ(std::format(R"(JSON_EXISTS(Text, '{} $[*].shared ? (@ == "v")'))", mode));

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHomoArr);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ >= {1})')", mode, k));
                        addJ(std::format("JSON_EXISTS(Text, '{0} $[*].u_{1} ? (@ < {2})')", mode, k, k + 2));
                    }
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJ(std::format("JSON_EXISTS(Text, '{} $[*].k_a ? (@ == {})')", mode, k));
                }

                addJ(std::format("JSON_EXISTS(Text, '{} $[*].k_b')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $[*].shared')", mode));

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJ(std::format("JSON_EXISTS(Text, '{} $[*].k_a ? (@ > {})')", mode, k - 1));
                        addJ(std::format("JSON_EXISTS(Text, '{} $[*].k_a ? (@ <= {})')", mode, k));
                    }
                }
            }

            if (!keysWithMixed.empty()) {
                for (int j = 0; j < 5; ++j) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.g5_{}.deep')", mode, j));
                    addJ(std::format("JSON_EXISTS(Text, '{} $.g5_{}.deep.v')", mode, j));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithMixed);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == {2})')", mode, k % 5, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithMixed);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= {2})')", mode, k % 5, k));
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ < {2})')", mode, k % 5, k + 1));
                    }
                }
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.items')", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJ(std::format("JSON_EXISTS(Text, '{} $.items[*].id ? (@ == {})')", mode, k));
                }

                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ == "shared_v")'))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.items[*].* ? (@ == "shared_v")'))", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.items[*].name ? (@ == "u_v_{1}")'))", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithItems);
                        addJ(std::format("JSON_EXISTS(Text, '{} $.items[*].id ? (@ > {})')", mode, k - 1));
                        addJ(std::format("JSON_EXISTS(Text, '{} $.items[0].id ? (@ == {})')", mode, k));
                    }
                }
            }

            if (!keysWithFullMix.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_s')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_b')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_null')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_arr')", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n ? (@ == {})')", mode, k));
                }

                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_b ? (@ == true)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_b ? (@ == false)')", mode));

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n ? (@ > {})')", mode, k - 1));
                        addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n ? (@ <= {})')", mode, k));
                    }
                }
            }
        }

        if (opts.EnableJsonPathMethods) {
            if (!keysWithFlatObj.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.type() ? (@ == "object")'))", mode));
            }

            if (!keysWithArrayLiterals.empty() || !keysWithArrayOfArrays.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.type() ? (@ == "array")'))", mode));
            }

            if (!keysWithScalarNull.empty() || !keysWithFullMix.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared_null.type() ? (@ == "null")'))", mode));
            }

            if (!keysWithScalarBoolean.empty() || !keysWithFullMix.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared_b.type() ? (@ == "boolean")'))", mode));
            }

            if (!keysWithScalarNumber.empty() || !keysWithFlatObj.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.rank.type() ? (@ == "number")'))", mode));
            }

            if (!keysWithScalarString.empty() || !keysWithFlatObj.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared.type() ? (@ == "string")'))", mode));
            }

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}.size() ? (@ == 3)')", mode, k));
                addJ(std::format("JSON_EXISTS(Text, '{0} $.*.size() ? (@ == 3)')", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.items.size() ? (@ == 2)')", mode));
            }

            if (!keysWithFullMix.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_arr.size() ? (@ == 4)')", mode));
            }

            if (!keysWithEmptyContainers.empty()) {
                const ui64 k = pickFrom(keysWithEmptyContainers);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}.size() ? (@ == 0)')", mode, k));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared.size() ? (@ == 1)')", mode));
            }

            if (!keysWithArrayOfArrays.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.size() ? (@ == 3)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $[0].size() ? (@ == 2)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $[2].size() ? (@ == 0)')", mode));
            }

            addJ(std::format("JSON_EXISTS(Text, '{} $.rank.ceiling() ? (@ == 10)')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.rank.floor() ? (@ != -1)')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.rank.abs() ? (@ != 1)')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.*.abs() ? (@ != 1)')", mode));

            if (!keysWithFullMix.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n.ceiling() ? (@ == 10)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n.floor() ? (@ != -1)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n.abs() ? (@ != -1)')", mode));
            }

            if (opts.EnableRangeComparisons) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.rank.ceiling() ? (@ > 10)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.rank.floor() ? (@ >= 10)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.rank.abs() ? (@ < 45)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.rank.abs() ? (@ <= 40)')", mode));
            }

            addJ(std::format("JSON_EXISTS(Text, '{} $.rank.abs().ceiling() ? (@ == 10)')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.rank.floor().abs() ? (@ != -1)')", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.rank.ceiling().abs() ? (@ == 10)')", mode));

            if (!keysWithFullMix.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_n.abs().ceiling() ? (@ == 10)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.shared_arr.size().abs() ? (@ == 4)')", mode));
            }

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}.size().abs() ? (@ == 3)')", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.items.size().abs() ? (@ == 2)')", mode));
            }

            if (opts.EnablePassingVariables) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $s.double().floor())' PASSING "10.0"u AS s))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $s.double().ceiling())' PASSING "9.5"u AS s))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $s.double().abs())' PASSING "10.0"u AS s))", mode));
            }

            addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == "shared" && @.value == "shared_v")'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == "rank" && @.value != null)'))", mode));

            if (opts.EnableRangeComparisons) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == "rank" && @.value >= 0)'))", mode));
            }

            if (!keysWithNestedObj.empty() && opts.EnableJsonPathPredicates) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared.keyvalue() ? (@.name starts with "u_")'))", mode));
            }

            if (opts.EnablePassingVariables) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ == $num.double())' PASSING "10.0"u AS num))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.* ? (@ == $num.double())' PASSING "10.0"u AS num))", mode));

                if (opts.EnableRangeComparisons) {
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.rank ? (@ > $num.double())' PASSING "9.5"u AS num))", mode));
                }

                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.items.size() ? (@ == $sz)' PASSING 2 AS sz))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == $field)' PASSING "shared"u AS field))", mode));
            }

            if (opts.EnableSqlParameters) {
                auto pNum = newPname();
                auto vNum = pNum.substr(1);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.rank ? (@ == ${1}.double())' PASSING {2} AS {1})", mode, vNum, pNum),
                    [pNum](NYdb::TParamsBuilder& bld) { bld.AddParam(pNum).Utf8("10.5").Build(); });

                auto pSize = newPname();
                auto vSize = pSize.substr(1);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.items.size() ? (@ == ${1})' PASSING {2} AS {1})", mode, vSize, pSize),
                    [pSize](NYdb::TParamsBuilder& bld) { bld.AddParam(pSize).Int64(2).Build(); });

                auto pField = newPname();
                auto vField = pField.substr(1);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.keyvalue() ? (@.name == ${1})' PASSING {2} AS {1})", mode, vField, pField),
                    [pField](NYdb::TParamsBuilder& bld) { bld.AddParam(pField).Utf8("shared").Build(); });
            }
        }

        {
            addJErr(std::format("NOT JSON_EXISTS(Text, '{} $')", mode));
            addJErr(std::format("NOT JSON_EXISTS(Text, '{} $.shared')", mode));
            addJErr(std::format("NOT JSON_EXISTS(Text, '{} $.nope_xyz')", mode));

            addJErr(std::format("JSON_EXISTS(Text, '{} $') = false", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') = false", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.nope_xyz') = false", mode));

            addJErr(std::format("JSON_EXISTS(Text, '{} $') <> true", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') <> true", mode));

            addJErr(std::format("JSON_EXISTS(Text, '{} $') = Just(false)", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') = Just(false)", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $') <> Just(true)", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') <> Just(true)", mode));

            if (!keysWithUKey.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUKey);
                    addJErr(std::format("NOT JSON_EXISTS(Text, '{} $.u_{}')", mode, k));
                }
            }

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.u_{}[*]') = false", mode, k));
            }

            if (!keysWithNestedObj.empty()) {
                const ui64 k = pickFrom(keysWithNestedObj);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.shared.u_{}') <> true", mode, k));
            }

            if (!keysWithDeepNested.empty()) {
                const ui64 k = pickFrom(keysWithDeepNested);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}') = Just(false)", mode, k));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}') <> Just(true)", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJErr(std::format("NOT JSON_EXISTS(Text, '{} $.items')", mode));
            }

            if (!keysWithIntUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    addJErr(std::format("NOT JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1})')", mode, k));
                    addJErr(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1})') <> true", mode, k));
                }
            }

            if (opts.EnablePassingVariables) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    addJErr(std::format("NOT JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var)", mode, k));
                    addJErr(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) <> Just(true)", mode, k));
                }

                addJErr(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var) = Just(false))", mode));
                addJErr(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var) <> Just(true))", mode));
            }
        }
    }

    if (opts.EnableJsonValue) {

        {
            addJ(std::format("JSON_VALUE(Text, '{} $.shared') = \"shared_v\"u", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.*') = \"shared_v\"u", mode));

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}') = \"u_v_{1}\"u", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = randKey();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}') = \"u_v_{1}\"u", mode, k));
            }

            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = randKey();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", mode, k));
            }

            for (int j = 0; j < 5; ++j) {
                addJ(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) = true", mode, j));
            }

            for (int j = 0; j < 5; ++j) {
                addJ(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) != false", mode, j));
            }

            for (int j = 0; j < 5; ++j) {
                addJ(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool)", mode, j));
            }

            for (size_t i = 0; i < 5; ++i) {
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) = {}", mode, (int)rng.Uniform(50)));
            }
        }


        if (opts.EnableRangeComparisons) {
            addJ(std::format("JSON_VALUE(Text, '{} $.shared') <> \"nope\"u", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.*') <> \"nope\"u", mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > {2}", mode, k, k - 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) >= {1}", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) < {2}", mode, k, k + 1));
                addJ(std::format("JSON_VALUE(Text, '{0} $.*' RETURNING Int64) < {1}", mode, k + 1));
            }

            for (int lo : {5, 10, 15, 20, 25, 30}) {
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) > {}", mode, lo));
            }

            for (int hi : {10, 20, 30, 40, 45}) {
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) < {}", mode, hi));
            }

            for (int v : {10, 25, 35}) {
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) >= {}", mode, v));
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <= {}", mode, v));
            }
        }


        if (opts.EnableBetween) {
            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                addJ(std::format("JSON_VALUE(Text, '{0} $.*' RETURNING Int64) BETWEEN {1} AND {2}", mode, k - 1, k + 1));
            }

            addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 10 AND 20", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 0 AND 25", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 20 AND 40", mode));
        }


        if (opts.EnableInList) {
            addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') IN ("shared_v"u, "other"u))", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{} $.*') IN ("shared_v"u, "other"u))", mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') IN ("u_v_{1}"u, "nope"u))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
            }

            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared') NOT IN ("shared_v"u, "other"u))", mode));
            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.*') NOT IN ("shared_v"u, "other"u))", mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') NOT IN ("u_v_{1}"u, "nope"u))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
            }
        }


        if (opts.EnableSqlParameters) {
            for (size_t i = 0; i < 2; ++i) {
                auto pn = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.shared') = {1}", mode, pn),
                    [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                addJ(std::format("JSON_VALUE(Text, '{0} $.*') = {1}", mode, pn),
                    [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                auto pn = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {2}", mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                auto pn = newPname();
                const auto uvk = "u_v_" + std::to_string(k);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Utf8) = {2}", mode, k, pn),
                    [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                auto pn = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = {2}", mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
            }

            {
                const ui64 k = pickFrom(keysWithUArr);
                auto plo = newPname();
                auto phi = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                    [plo, phi, k](NYdb::TParamsBuilder& bld) {
                        bld.AddParam(plo).Int64((i64)k - 1).Build();
                        bld.AddParam(phi).Int64((i64)k + 1).Build();
                    });
                addJ(std::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                    [plo, phi, k](NYdb::TParamsBuilder& bld) {
                        bld.AddParam(plo).Int64((i64)k - 1).Build();
                        bld.AddParam(phi).Int64((i64)k + 1).Build();
                    });
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                auto pn = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {2}", mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
            }

            {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                auto plo = newPname();
                auto phi = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                    [plo, phi, k](NYdb::TParamsBuilder& bld) {
                        bld.AddParam(plo).Int64((i64)k - 1).Build();
                        bld.AddParam(phi).Int64((i64)k + 1).Build();
                    });
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) = {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) > {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k - 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.*.u_{1}' RETURNING Int64) > {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k - 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) < {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k + 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) >= {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 10).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.*' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 10).Build();
                        });
                }

                for (int j = 0; j < 5; ++j) {
                    auto pn = newPname();
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared.g5_{1}') = {2})", mode, j, pn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("v").Build(); });
                }
            }

            if (!keysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) = {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 10).Build();
                        });
                }
            }

            if (!keysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHomoArr);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) = {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithHomoArr);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 2).Build();
                        });
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) = {1}", mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = newPname();
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') = {1})", mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 10).Build();
                        });
                }
            }

            if (!keysWithMixed.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) = {2}", mode, k % 5, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) > {2}", mode, k % 5, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k - 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) < {2}", mode, k % 5, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k + 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) >= {2}", mode, k % 5, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) <= {2}", mode, k % 5, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) BETWEEN {2} AND {3}", mode, k % 5, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k % 5, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 10).Build();
                        });
                }
            }

            if (!keysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) = {1}", mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithItems);auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) > {1}", mode, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k - 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithItems);auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) <= {1}", mode, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithItems);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = newPname();
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.items[0].name') = {1})", mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithItems);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithItems);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 5).Build();
                        });
                }
            }

            if (!keysWithFullMix.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) = {1}", mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) > {1}", mode, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k - 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) < {1}", mode, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k + 1).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) >= {1}", mode, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto pn = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) <= {1}", mode, pn),
                      [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto plo = newPname();
                    auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k + 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 10).Build();
                        });
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = newPname();
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') = {1})", mode, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }
            }
        }

        {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = {1}", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64) = {2}", mode, k, k + 1));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}[2]') = "u_v_{1}"u)", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}[last]') = "u_v_{1}"u)", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[last - 1]' RETURNING Int64) = {2}", mode, k, k + 1));
            }

            {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0 to 1] ? (@ == {1})' RETURNING Int64) = {1}", mode, k));
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0, 2] ? (@ == {1})' RETURNING Int64) = {1}", mode, k));
            }

            if (opts.EnableRangeComparisons) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) > {2}", mode, k, k - 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) <= {1}", mode, k));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) <= {1}", mode, k));
                }
            }

            if (opts.EnableBetween) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 2, k + 10));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                }
            }

            if (opts.EnableInList) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.*[0]' RETURNING Int64) IN ({1}, {2}, {3})", mode, k + 1, k + 2, k + 3));
                }

                {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                }
            }
        }

        {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", mode, k));
            }

            if (opts.EnableRangeComparisons) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > {2}", mode, k, k - 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) < {2}", mode, k, k + 1));
                }
            }

            if (opts.EnableBetween) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 1, k + 10));
                }
            }

            if (opts.EnableInList) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                }

                {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                }
            }
        }


        {
            if (opts.EnableRangeComparisons) {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') >= "s"u)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') < "z"u)", mode));
            }

            if (opts.EnableBetween) {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') BETWEEN "a"u AND "z"u)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') NOT BETWEEN "t"u AND "z"u)", mode));
            }

            if (opts.EnableInList) {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') IN ("shared_v"u, "other_v"u, "nope"u))", mode));
                addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared') NOT IN ("shared_v"u, "other_v"u, "nope"u))", mode));
            }
        }


        {
            if (opts.EnableBetween) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 1, k + 10));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') BETWEEN "a"u AND "z"u)", mode, k));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') NOT BETWEEN "a"u AND "b"u)", mode, k));
                }

                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) NOT BETWEEN 30 AND 50", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) NOT BETWEEN 0 AND 5", mode));
            }
        }

        {
            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) = {1}", mode, k));
                }

                for (int j = 0; j < 5; ++j) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}') = "v"u)", mode, j));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) > {2}", mode, k, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) < {2}", mode, k, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) >= {1}", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= {1}", mode, k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 1, k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    for (int j = 0; j < 5; ++j) {
                        addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}') IN ("v"u, "other"u))", mode, j));
                    }

                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    for (int j = 0; j < 5; ++j) {
                        addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}') NOT IN ("v"u, "other"u))", mode, j));
                    }
                }
            }

            if (!keysWithDeepNested.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) = {1}", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) > {2}", mode, k, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) <= {1}", mode, k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 2, k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }
                }
            }

            if (!keysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHomoArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) = {1}", mode, k));
                }

                addJ(std::format(R"(JSON_VALUE(Text, '{} $[*].shared') = "v"u)", mode));
                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHomoArr);
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) > {2}", mode, k, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) >= {1}", mode, k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHomoArr);
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 2));
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 2, k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHomoArr);
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    {
                        const ui64 k = pickFrom(keysWithHomoArr);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) = {1}", mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') = "u_v_{1}"u)", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) > {1}", mode, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) <= {1}", mode, k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) BETWEEN {1} AND {2}", mode, k - 1, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, k + 1, k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') IN ("u_v_{1}"u, "nope"u))", mode, k));
                    }

                    {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    {
                        const ui64 k = pickFrom(keysWithHeteroArr);
                        addJErr(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') NOT IN ("u_v_{1}"u, "nope"u))", mode, k));
                    }
                }
            }

            if (!keysWithMixed.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithMixed);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) = {2}", mode, k % 5, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithMixed);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) > {2}", mode, k % 5, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) >= {2}", mode, k % 5, k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithMixed);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) BETWEEN {2} AND {3}", mode, k % 5, k - 1, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k % 5, k + 1, k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithMixed);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) IN ({2}, {3}, {4})", mode, k % 5, k, k + 1, k + 2));
                    }

                    {
                        const ui64 k = pickFrom(keysWithMixed);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) NOT IN ({2}, {3}, {4})", mode, k % 5, k, k + 1, k + 2));
                    }
                }
            }

            if (!keysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) = {1}", mode, k));
                }

                addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[1].name') = "shared_v"u)", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.items[0].name') = "u_v_{1}"u)", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithItems);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) > {1}", mode, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) <= {1}", mode, k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithItems);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) BETWEEN {1} AND {2}", mode, k - 1, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, k + 1, k + 5));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithItems);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[1].name') IN ("shared_v"u, "nope"u))", mode));

                    {
                        const ui64 k = pickFrom(keysWithItems);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    addJErr(std::format(R"(JSON_VALUE(Text, '{} $.items[1].name') NOT IN ("shared_v"u, "nope"u))", mode));
                }
            }

            if (!keysWithFullMix.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) = {1}", mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') = "u_v_{1}"u)", mode, k));
                }

                addJ(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) = true", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) != false", mode));

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) > {1}", mode, k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) < {1}", mode, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) >= {1}", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) <= {1}", mode, k));
                    }

                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared_s') <> "nope"u)", mode));
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 3; ++i) {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) BETWEEN {1} AND {2}", mode, k - 1, k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) NOT BETWEEN {1} AND {2}", mode, k + 1, k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') IN ("u_v_{1}"u, "nope"u))", mode, k));
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) NOT IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') NOT IN ("u_v_{1}"u, "nope"u))", mode, k));
                    }
                }
            }
        }

        if (opts.EnableJsonPathMethods) {
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.type()') = "object"u)", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_arr.type()') = "array"u)", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_null.type()') = "null"u)", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_b.type()') = "boolean"u)", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.rank.type()') = "number"u)", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared.type()') = "string"u)", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.*.type()') = "string"u)", mode));

            addJ(std::format("JSON_VALUE(Text, '{} $.shared_arr.size()' RETURNING Int64) = 4", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.*.size()' RETURNING Int64) = 4", mode));

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}.size()' RETURNING Int64) = 3", mode, k));
                addJ(std::format("JSON_VALUE(Text, '{0} $.*.size()' RETURNING Int64) = 3", mode, k));
            }

            addJ(std::format("JSON_VALUE(Text, '{} $.items.size()' RETURNING Int64) = 2", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.shared.size()' RETURNING Int64) = 1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $[2].size()' RETURNING Int64) = 0", mode));

            addJ(std::format("JSON_VALUE(Text, '{} $.rank.ceiling()' RETURNING Int64) = 10", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank.floor()' RETURNING Int64) <> -1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank.abs()' RETURNING Int64) <> -1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.ceiling()' RETURNING Int64) = 10", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.floor()' RETURNING Int64) <> -1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.abs()' RETURNING Int64) <> 1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.*.abs()' RETURNING Int64) <> 1", mode));

            if (opts.EnableRangeComparisons) {
                addJ(std::format("JSON_VALUE(Text, '{} $.rank.ceiling()' RETURNING Int64) > 10", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.rank.floor()' RETURNING Int64) >= 10", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.rank.abs()' RETURNING Int64) < 45", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.rank.abs()' RETURNING Int64) <= 40", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.ceiling()' RETURNING Int64) > 10", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.floor()' RETURNING Int64) >= 10", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.abs()' RETURNING Int64) < 45", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.abs()' RETURNING Int64) <= 40", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.*.abs()' RETURNING Int64) <= 40", mode));
            }

            addJ(std::format("JSON_VALUE(Text, '{} $.rank.abs().ceiling()' RETURNING Int64) <> -1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank.floor().abs()' RETURNING Int64) <> -1", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank.ceiling().abs()' RETURNING Int64) = 10", mode));

            if (!keysWithFullMix.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_n.abs().ceiling()' RETURNING Int64) = 10", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.shared_arr.size().abs()' RETURNING Int64) = 4", mode));
            }

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}.size().abs()' RETURNING Int64) = 3", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{} $.items.size().abs()' RETURNING Int64) = 2", mode));
            }

            if (opts.EnablePassingVariables) {
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ == $s.double().floor())' PASSING "10.0"u AS s RETURNING Int64) = 10)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ == $s.double().ceiling())' PASSING "9.5"u AS s RETURNING Int64) = 10)", mode));
            }

            addJ(std::format("JSON_VALUE(Text, '{} $.keyvalue() ? (@.name == \"rank\").value' RETURNING Int64) <> -1", mode));
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.keyvalue() ? (@.name == "shared").value.type()') = "string"u)", mode));

            if (opts.EnablePassingVariables) {
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ == $num.double())' PASSING "10.0"u AS num RETURNING Int64) <> -1)", mode));

                if (opts.EnableRangeComparisons) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.rank ? (@ > $num.double())' PASSING "9.5"u AS num RETURNING Int64) <> -1)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.* ? (@ > $num.double())' PASSING "9.5"u AS num RETURNING Int64) <> -1)", mode));
                }
            }

            if (opts.EnableSqlParameters) {
                auto pNum = newPname();
                auto vNum = pNum.substr(1);
                addJ(std::format("JSON_VALUE(Text, '{0} $.rank ? (@ == ${1}.double())' PASSING {2} AS {1} RETURNING Int64) <> -1", mode, vNum, pNum),
                    [pNum](NYdb::TParamsBuilder& bld) { bld.AddParam(pNum).Utf8("10.5").Build(); });

                auto pSize = newPname();
                auto vSize = pSize.substr(1);
                addJ(std::format("JSON_VALUE(Text, '{0} $.items.size() ? (@ == ${1})' PASSING {2} AS {1} RETURNING Int64) = 2", mode, vSize, pSize),
                    [pSize](NYdb::TParamsBuilder& bld) { bld.AddParam(pSize).Int64(2).Build(); });

                auto pField = newPname();
                auto vField = pField.substr(1);
                addJ(std::format("JSON_VALUE(Text, '{0} $.keyvalue() ? (@.name == ${1}).value.type()' PASSING {2} AS {1}) = \"string\"u", mode, vField, pField),
                    [pField](NYdb::TParamsBuilder& bld) { bld.AddParam(pField).Utf8("shared").Build(); });
            }
        }

        {
            addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared') = JSON_VALUE(Text, '{0} $.shared'))", mode));
            addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode));
            addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Double) = JSON_VALUE(Text, '{0} $.rank' RETURNING Double)", mode));

            if (!keysWithStrUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}') = JSON_VALUE(Text, '{0} $.u_{1}')", mode, k));
                }
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode, k));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) != JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode, k));
                }

                addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) != JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode));

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithFlatObj);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Double) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Double)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Double) <= JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Double)", mode, k));
                    }

                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Double) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Double)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared') <= JSON_VALUE(Text, '{0} $.shared'))", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared') >= JSON_VALUE(Text, '{0} $.shared'))", mode));
                }
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) = JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)", mode));
                addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) != JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.items[0].name') != JSON_VALUE(Text, '{0} $.items[1].name'))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.items[1].name') = JSON_VALUE(Text, '{0} $.items[1].name'))", mode));

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64) > JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Double) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Double)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.items[0].name') < JSON_VALUE(Text, '{0} $.items[1].name'))", mode));
                }
            }

            if (!keysWithUArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64)", mode, k));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) != JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)", mode, k));

                    if (opts.EnableRangeComparisons) {
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64) > JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Double) < JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Double)", mode, k));
                    }
                }
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) = JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64)", mode, k));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double) = JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double)", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64)", mode, k));
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double) <= JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Double)", mode, k));
                    }
                }
            }

            if (!keysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) = JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64)", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Double) >= JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Double)", mode, k));
                    }
                }
            }

            if (!keysWithFullMix.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) = JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') = JSON_VALUE(Text, '{0} $.shared_s'))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared') != JSON_VALUE(Text, '{0} $.shared_s'))", mode));

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Double) >= JSON_VALUE(Text, '{0} $.shared_n' RETURNING Double)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') <= JSON_VALUE(Text, '{0} $.shared_s'))", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') >= JSON_VALUE(Text, '{0} $.shared_s'))", mode));
                }
            }

            if (!keysWithHeteroArr.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) = JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') = JSON_VALUE(Text, '{0} $[*].k_b'))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') != JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Utf8))", mode));

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) <= JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Double) >= JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Double)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') <= JSON_VALUE(Text, '{0} $[*].k_b'))", mode));
                }
            }

            for (int j = 0; j < 3; ++j) {
                addJErr(std::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) = JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool)", mode, j));
                addJErr(std::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) != JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool)", mode, j));
                addJErr(std::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) = JSON_VALUE(Text, '{0} $.g5_{2}' RETURNING Bool)", mode, j, (j + 1) % 5));
                addJErr(std::format("JSON_VALUE(Text, '{0} $.g5_{1}' RETURNING Bool) != JSON_VALUE(Text, '{0} $.g5_{2}' RETURNING Bool)", mode, j, (j + 2) % 5));
            }

            if (!keysWithFullMix.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) = JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool)", mode));
                addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) != JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool)", mode));
                addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) = JSON_VALUE(Text, '{0} $.g5_0' RETURNING Bool)", mode));

                if (opts.EnableRangeComparisons) {
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) < JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool)", mode));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_b' RETURNING Bool) > JSON_VALUE(Text, '{0} $.g5_0' RETURNING Bool)", mode));
                }
            }
        }

        {
            for (int j = 0; j < 2; ++j) {
                addJErr(std::format("NOT JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool)", mode, j));
            }

            if (!keysWithFullMix.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) = false", mode));
            }

            if (!keysWithScalarBoolean.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $' RETURNING Bool) <> true", mode));
            }

            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared == "shared_v"' RETURNING Bool) = Just(false))", mode));

            if (!keysWithIntUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    addJErr(std::format("NOT JSON_VALUE(Text, '{0} $.u_{1} == {1}' RETURNING Bool)", mode, k));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1} == {1}' RETURNING Bool) <> true", mode, k));
                }
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank == {1}' RETURNING Bool) = Just(false)", mode, r));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank == {1}' RETURNING Bool) <> Just(true)", mode, r));
                }

                if (opts.EnableRangeComparisons) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJErr(std::format("NOT JSON_VALUE(Text, '{0} $.rank > {1}' RETURNING Bool)", mode, r - 1));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank > {1}' RETURNING Bool) <> true", mode, r - 1));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank > {1}' RETURNING Bool) = Just(false)", mode, r - 1));
                }
            }

            if (opts.EnablePassingVariables && !keysWithFlatObj.empty()) {
                const ui64 k = pickFrom(keysWithFlatObj);
                const int64_t r = k % 50;
                addJErr(std::format("NOT JSON_VALUE(Text, '{0} $.rank == $val' PASSING {1} AS val RETURNING Bool)", mode, r));
                addJErr(std::format("JSON_VALUE(Text, '{0} $.rank == $val' PASSING {1} AS val RETURNING Bool) != Just(true)", mode, r));

                addJErr(std::format(R"(NOT JSON_VALUE(Text, '{0} $.shared == $val' PASSING "shared_v"u AS val RETURNING Bool))", mode));
                addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.shared == $val' PASSING "shared_v"u AS val RETURNING Bool) != Just(true))", mode));
            }
        }
    }

    if (opts.EnableJsonPathPredicates) {
      if (opts.EnableJsonExists) {
        {
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ starts with "shared")'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ starts with "shared_v")'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.* ? (@ starts with "shared_v")'))", mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} starts with "u_v")'))", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ starts with "u_v")'))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ starts with "shared")'))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.*[*].name ? (@ starts with "shared")'))", mode));
            }

            if (!keysWithFullMix.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared_s ? (@ starts with "u_v")'))", mode));
            }

            if (opts.EnablePassingVariables) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared ? (@ starts with $pfx)' PASSING "shared"u AS pfx))", mode));

                if (!keysWithItems.empty()) {
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.items[*].name ? (@ starts with $pfx)' PASSING "u_v"u AS pfx))", mode));
                }

                if (opts.EnableSqlParameters) {
                    auto pn = newPname();
                    auto vn = pn.substr(1);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared ? (@ starts with {1})' PASSING {1} AS {2})", mode, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared").Build(); });
                }
            }
        }

        {
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ like_regex "shared.*")'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ like_regex "^shared_v$")'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? (@ like_regex "SHARED.*" flag "i")'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.* ? (@ like_regex "SHARED.*" flag "i")'))", mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.u_{1} ? (@ like_regex "u_v_.*")'))", mode, k));
            }

            if (!keysWithScalarString.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $ ? (@ like_regex "u_v_[0-9]+")'))", mode));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.items[*].name ? (@ like_regex "u_v_.*")'))", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.*[*].name ? (@ like_regex "u_v_.*")'))", mode));
            }

            if (!keysWithHeteroArr.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $[*].k_b ? (@ like_regex "u_v_[0-9]+")'))", mode));
            }

            if (!keysWithFullMix.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.shared_s ? (@ like_regex "u_v_.*")'))", mode));
            }
        }

        {
            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (exists(@.u_{}))')", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{} $.* ? (exists(@.u_{}))')", mode, k));
                }

                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (exists(@.shared))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (exists(@.rank))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (exists(@.nope_xyz))')", mode));
            }

            if (!keysWithHeteroArr.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $[*] ? (exists(@.k_a))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $[*] ? (exists(@.k_b))')", mode));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.id))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.name))')", mode));
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_EXISTS(Text, '{} $.shared ? (exists(@.u_{}))')", mode, k));
                    addJ(std::format("JSON_EXISTS(Text, '{} $.* ? (exists(@.u_{}))')", mode, k));
                }
            }

            if (!keysWithDeepNested.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.a ? (exists(@.b))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.a.b ? (exists(@.c))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.a.* ? (exists(@.c))')", mode));
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} == {1})')", mode, k));
                }
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $ ? (exists(@.shared) && @.shared == "shared_v")'))", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (exists(@.rank) && @.rank > 10)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (exists(@.rank) && @.rank != -1)')", mode));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.id) && @.id > 0)')", mode));
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.name) && @.name != "nope")'))", mode));
            }

            if (opts.EnablePassingVariables && !keysWithFlatObj.empty()) {
                const ui64 k = pickFrom(keysWithFlatObj);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1}) && @.u_{1} >= $lo)' PASSING {1} AS lo)", mode, k, (int64_t)k));
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (exists(@.shared) && @.shared starts with $pfx)' PASSING "shared"u AS pfx))", mode));
            }
        }

        {
            addJErr(std::format("JSON_EXISTS(Text, '{} $ ? ((@ == true) is unknown)')", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $ ? ((@ > 0) is unknown)')", mode));
            addJErr(std::format(R"(JSON_EXISTS(Text, '{} $.shared ? ((@ == "shared_v") is unknown)'))", mode));

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJErr(std::format("JSON_EXISTS(Text, '{} $ ? ((@.u_{} == {}) is unknown)')", mode, k, k));
                    addJ(std::format("JSON_EXISTS(Text, '{} $.* ? ((@ == {}) is unknown)')", mode, k));
                }
            }

            if (opts.EnablePassingVariables) {
                addJErr(std::format("JSON_EXISTS(Text, '{} $ ? ((@ == $var) is unknown)' PASSING true AS var)", mode));
            }
        }
      }

      if (opts.EnableJsonValue) {
            {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared starts with "shared"' RETURNING Bool))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.* starts with "shared"' RETURNING Bool))", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1} ? (@ starts with "u_v")') = "u_v_{1}"u)", mode, k));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1} starts with "u_v"' RETURNING Bool))", mode, k));
                }

                if (!keysWithScalarString.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarString);
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $ ? (@ starts with "u_v")') = "u_v_{1}"u)", mode, k));
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $ starts with "u_v"' RETURNING Bool))", mode, k));
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $.* starts with "u_v"' RETURNING Bool))", mode, k));
                    }
                }

                if (!keysWithFullMix.empty()) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared_s ? (@ starts with "u_v")') <> "nope"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared_s starts with "u_v"' RETURNING Bool))", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.* starts with "u_v"' RETURNING Bool))", mode));
                }

                if (!keysWithItems.empty()) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[*].name ? (@ starts with "u_v")') <> "nope"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[*].name starts with "u_v"' RETURNING Bool))", mode));
                }

                if (opts.EnablePassingVariables) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared ? (@ starts with $pfx)' PASSING "shared"u AS pfx) = "shared_v"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared starts with $pfx' PASSING "shared"u AS pfx RETURNING Bool))", mode));

                    if (opts.EnableSqlParameters) {
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared ? (@ starts with {1})' PASSING {1} AS {2}) = \"shared_v\"u", mode, pn, vn),
                            [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared").Build(); });
                        addJ(std::format("JSON_VALUE(Text, '{0} $.shared starts with {1}' PASSING {1} AS {2} RETURNING Bool)", mode, pn, vn),
                            [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared").Build(); });
                    }
                }
            }

            {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared ? (@ like_regex "shared.*")') = "shared_v"u)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared like_regex "shared.*"' RETURNING Bool))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.* like_regex "shared.*"' RETURNING Bool))", mode));

                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared ? (@ like_regex "^shared_v$")') = "shared_v"u)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared like_regex "^shared_v$"' RETURNING Bool))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.* like_regex "^shared_v$"' RETURNING Bool))", mode));

                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared ? (@ like_regex "SHARED.*" flag "i")') = "shared_v"u)", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared like_regex "SHARED.*" flag "i"' RETURNING Bool))", mode));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.* like_regex "SHARED.*" flag "i"' RETURNING Bool))", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1} ? (@ like_regex "u_v_.*")') = "u_v_{1}"u)", mode, k));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1} like_regex "u_v_.*"' RETURNING Bool))", mode, k));
                }

                if (!keysWithScalarString.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarString);
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $ ? (@ like_regex "u_v_[0-9]+")') = "u_v_{1}"u)", mode, k));
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $ like_regex "u_v_[0-9]+"' RETURNING Bool))", mode, k));
                    }
                }

                if (!keysWithFullMix.empty()) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared_s ? (@ like_regex "u_v_.*")') <> "nope"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared_s like_regex "u_v_.*"' RETURNING Bool))", mode));
                }

                if (!keysWithHeteroArr.empty()) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $[*].k_b ? (@ like_regex "u_v_[0-9]+")') <> "nope"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $[*].k_b like_regex "u_v_[0-9]+"' RETURNING Bool))", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $[*].* like_regex "u_v_[0-9]+"' RETURNING Bool))", mode));
                }
            }

            {
                if (!keysWithFlatObj.empty()) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $ ? (exists(@.shared)).shared') = "shared_v"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} exists($.shared)' RETURNING Bool))", mode));

                    addJ(std::format("JSON_VALUE(Text, '{} $ ? (exists(@.rank)).rank' RETURNING Int64) <> -1", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} exists($.rank)' RETURNING Bool)", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} $.* ? (exists(@.rank))' RETURNING Bool)", mode));
                }

                if (!keysWithItems.empty()) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0] ? (exists(@.id)).id' RETURNING Int64) = {1}", mode, k));
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.items[0] ? (exists(@.id))' RETURNING Bool))", mode, k));

                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[0] ? (exists(@.name)).name') <> "nope"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} exists($.items[0].name)' RETURNING Bool))", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[*].* ? (exists(@.name))' RETURNING Bool))", mode));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared ? (exists(@.u_{1})).u_{1}' RETURNING Int64) = {1}", mode, k));
                    addJ(std::format("JSON_VALUE(Text, '{0} exists($.shared.u_{1})' RETURNING Bool)", mode, k));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.* ? (exists(@.u_{1}))' RETURNING Bool)", mode, k));
                }

                if (!keysWithFlatObj.empty()) {
                    addJ(std::format("JSON_VALUE(Text, '{} $ ? (exists(@.rank) && @.rank != -1).rank' RETURNING Int64) <> -1", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $ ? (exists(@.shared) && @.shared == "shared_v").shared') = "shared_v"u)", mode));

                    if (opts.EnableRangeComparisons) {
                        addJ(std::format("JSON_VALUE(Text, '{} $ ? (exists(@.rank) && @.rank > 10).rank' RETURNING Int64) > 10", mode));
                    }
                }

                if (!keysWithItems.empty()) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[*] ? (exists(@.id) && @.id == {1}).id' RETURNING Int64) = {1}", mode, k));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[*] ? (exists(@.name) && @.name != "nope").name') <> "nope"u)", mode));
                }
            }

            {
                addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared ? ((@ == "shared_v") is unknown)') = "shared_v"u)", mode));
                addJErr(std::format(R"(JSON_VALUE(Text, '{} ($.shared == "shared_v") is unknown' RETURNING Bool))", mode));

                addJErr(std::format("JSON_VALUE(Text, '{} $.rank ? ((@ > 0) is unknown)' RETURNING Int64) = 0", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} ($.rank > 0) is unknown' RETURNING Bool)", mode));

                if (!keysWithFlatObj.empty()) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJErr(std::format("JSON_VALUE(Text, '{} $ ? ((@.u_{} == {}) is unknown).u_{}' RETURNING Int64) = 0", mode, k, k, k));
                    addJ(std::format("JSON_VALUE(Text, '{} $.* ? ((@ == {}) is unknown).u_{}' RETURNING Int64) = 0", mode, k, k));
                }

                if (opts.EnablePassingVariables) {
                    addJErr(std::format("JSON_VALUE(Text, '{} $.rank ? ((@ == $var) is unknown)' PASSING 0 AS var RETURNING Int64) = 0", mode));
                    addJErr(std::format("JSON_VALUE(Text, '{} ($.rank == $var) is unknown' PASSING 0 AS var RETURNING Bool)", mode));
                }
            }
        }
    }

    if (opts.EnableComplexJsonPathFilters) {
        if (opts.EnableJsonExists) {
            if (!keysWithFlatObj.empty()) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{} $ ? (exists(@.shared)) ? (@.rank != -1)'))", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1})) ? (@.u_{1} == {1})')", mode, k));
                }
            }

            if (!keysWithDeepNested.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.a ? (exists(@.b)) ? (exists(@.b.c))')", mode));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.items[*] ? (@.id > 0) ? (exists(@.name))')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $.items[*] ? (exists(@.id)) ? (@.id > 0)')", mode));
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared ? (exists(@.u_{1})) ? (@.u_{1} == {1})')", mode, k));
                }
            }

            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.* ? ((@ starts with "x") is unknown)'))", mode));
            addJ(std::format(R"(JSON_EXISTS(Text, '{} $.* ? ((@ like_regex "x") is unknown)'))", mode));
            addJ(std::format("JSON_EXISTS(Text, '{} $.* ? ((exists(@.nope_xyz)) is unknown)')", mode));

            if (!keysWithArrayLiterals.empty() || !keysWithHeteroArr.empty()) {
                addJErr(std::format(R"(JSON_EXISTS(Text, '{} $[*] ? ((@ starts with "x") is unknown)'))", mode));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.*[*] ? (@.id > 0)')", mode));
                const ui64 k = pickFrom(keysWithItems);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.*[*] ? (exists(@.id) && @.id == {1})')", mode, k));
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.*.* ? (@ == {1})')", mode, k));
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $[*].* ? (@ == "u_v_{1}")'))", mode, k));
                }
            }

            if (opts.EnableJsonPathMethods) {
                if (!keysWithFlatObj.empty()) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.rank ? (@ >= 0).abs() ? (@ < 50)')", mode));
                    addJ(std::format("JSON_EXISTS(Text, '{} $.rank ? (@ >= 0).ceiling() ? (@ >= 0)')", mode));
                }

                if (!keysWithItems.empty()) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.items ? (exists(@[*])).size() ? (@ == 2)')", mode));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.shared ? (@.u_{1} == {1}).type() ? (@ == "object")'))", mode, k));
                }
            }

            if (opts.EnablePassingVariables) {
                if (!keysWithFlatObj.empty()) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (exists(@.u_{1})) ? (@.u_{1} >= $lo)' PASSING {1} AS lo)", mode, k));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.shared ? (exists(@.u_{1})) ? (@.u_{1} == $val)' PASSING {1} AS val)", mode, k));
                }

                addJ(std::format(R"(JSON_EXISTS(Text, '{} $.* ? ((@ starts with $pfx) is unknown)' PASSING "x"u AS pfx))", mode));
            }
        }

        if (opts.EnableJsonValue) {
            if (!keysWithFlatObj.empty()) {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $ ? (exists(@.shared)) ? (@.shared == "shared_v").shared') = "shared_v"u)", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $ ? (exists(@.u_{1})) ? (@.u_{1} == {1}).rank' RETURNING Int64) >= 0", mode, k));
                }
            }

            if (!keysWithDeepNested.empty()) {
                const ui64 k = pickFrom(keysWithDeepNested);
                addJ(std::format("JSON_VALUE(Text, '{0} $.a ? (exists(@.b)) ? (exists(@.b.c)).b.c.u_{1}' RETURNING Int64) = {1}", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.items[*] ? (@.id > 0) ? (exists(@.name)).name') <> "nope"u)", mode));

                const ui64 k = pickFrom(keysWithItems);
                addJ(std::format("JSON_VALUE(Text, '{0} $.items[*] ? (exists(@.id)) ? (@.id == {1}).id' RETURNING Int64) = {1}", mode, k));
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared ? (exists(@.u_{1})) ? (@.u_{1} == {1}).u_{1}' RETURNING Int64) = {1}", mode, k));
                }
            }

            if (!keysWithNestedObj.empty()) {
                const ui64 k = pickFrom(keysWithNestedObj);
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.* ? ((@ starts with "x") is unknown).u_{1}' RETURNING Int64) = {1})", mode, k));
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.* ? ((@ like_regex "x") is unknown).u_{1}' RETURNING Int64) = {1})", mode, k));
            }

            addJ(std::format("JSON_VALUE(Text, '{} $.* ? ((exists(@.nope_xyz)) is unknown)' RETURNING Bool)", mode));

            if (!keysWithItems.empty()) {
                const ui64 k = pickFrom(keysWithItems);
                addJ(std::format("JSON_VALUE(Text, '{0} $.*[*] ? (@.id == {1}).id' RETURNING Int64) = {1}", mode, k));
                addJ(std::format(R"(JSON_VALUE(Text, '{} $.*[*] ? (@.id > 0).name') <> "nope"u)", mode));
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.*.* ? (@ == {1})' RETURNING Int64) = {1}", mode, k));
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $[*].* ? (@ == "u_v_{1}")') = "u_v_{1}"u)", mode, k));
                }
            }

            if (opts.EnableJsonPathMethods) {
                if (!keysWithFlatObj.empty()) {
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank ? (@ >= 0).abs()' RETURNING Int64) < 50", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank ? (@ >= 0).ceiling()' RETURNING Int64) >= 0", mode));
                }

                if (!keysWithItems.empty()) {
                    addJ(std::format("JSON_VALUE(Text, '{} $.items ? (exists(@[*])).size()' RETURNING Int64) = 2", mode));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.shared ? (@.u_{1} == {1}).type()') = "object"u)", mode, k));
                }
            }

            if (opts.EnablePassingVariables) {
                if (!keysWithFlatObj.empty()) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $ ? (exists(@.u_{1})) ? (@.u_{1} >= $lo).rank' RETURNING Int64) >= 0", mode, k));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared ? (exists(@.u_{1})) ? (@.u_{1} == $val).u_{1}' PASSING {1} AS val RETURNING Int64) = {1}", mode, k));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $.* ? ((@ starts with $pfx) is unknown).u_{1}' PASSING "x"u AS pfx RETURNING Int64) = {1})", mode, k));
                }
            }
        }
    }

    if (opts.EnableJsonIsLiteral) {
        if (opts.EnableJsonExists) {
            if (!keysWithScalarNull.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == null)')", mode));
            }

            if (!keysWithScalarBoolean.empty()) {
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == true)')", mode));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == false)')", mode));
            }

            if (!keysWithScalarInt.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithScalarInt);
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == {})')", mode, (int64_t)k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ > {})')", mode, (int64_t)k - 1));
                        addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ <= {})')", mode, (int64_t)k));
                    }
                }

                if (opts.EnablePassingVariables) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == $var)' PASSING {} AS var)", mode, (int64_t)k));
                    }

                    if (opts.EnableSqlParameters) {
                        for (size_t i = 0; i < 2; ++i) {
                            const ui64 k = pickFrom(keysWithScalarInt);
                            auto pn = newPname();
                            auto vn = pn.substr(1);
                            addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == {})' PASSING {} AS {})", mode, pn, pn, vn),
                                [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                        }
                    }
                }
            }

            if (!keysWithScalarString.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithScalarString);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@ == "u_v_{1}")'))", mode, k));
                }

                if (opts.EnableJsonPathPredicates) {
                    addJ(std::format(R"(JSON_EXISTS(Text, '{} $ ? (@ starts with "u_v")'))", mode));
                }

                if (opts.EnableRangeComparisons) {
                    addJ(std::format(R"(JSON_EXISTS(Text, '{} $ ? (@ >= "u_v")'))", mode));
                }

                if (opts.EnablePassingVariables) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarString);
                        addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@ == $var)' PASSING "u_v_{1}"u AS var))", mode, k));
                    }

                    if (opts.EnableSqlParameters) {
                        for (size_t i = 0; i < 2; ++i) {
                            const ui64 k = pickFrom(keysWithScalarString);
                            const auto uvk = "u_v_" + std::to_string(k);
                            auto pn = newPname();
                            auto vn = pn.substr(1);
                            addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@ == {})' PASSING {} AS {})", mode, pn, pn, vn),
                                [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                        }
                    }
                }
            }
        }

        if (opts.EnableJsonValue) {
            if (!keysWithScalarString.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithScalarString);
                    addJ(std::format(R"(JSON_VALUE(Text, '{0} $') = "u_v_{1}"u)", mode, k));
                }

                if (opts.EnableRangeComparisons) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $') >= "u_v"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $') < "u_w"u)", mode));
                }

                if (opts.EnableBetween) {
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $') BETWEEN "u_v"u AND "u_w"u)", mode));
                    addJ(std::format(R"(JSON_VALUE(Text, '{} $') NOT BETWEEN "a"u AND "b"u)", mode));
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarString);
                        addJ(std::format(R"(JSON_VALUE(Text, '{0} $') IN ("u_v_{1}"u, "nope"u))", mode, k));
                    }

                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarString);
                        addJErr(std::format(R"(JSON_VALUE(Text, '{0} $') NOT IN ("u_v_{1}"u, "nope"u))", mode, k));
                    }
                }

                if (opts.EnableSqlParameters) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarString);
                        const auto uvk = "u_v_" + std::to_string(k);
                        auto pn = newPname();
                        addJ(std::format("JSON_VALUE(Text, '{} $') = {}", mode, pn),
                            [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                    }
                }
            }

            if (!keysWithScalarInt.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithScalarInt);
                    addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) = {}", mode, (int64_t)k));
                }

                if (opts.EnableRangeComparisons) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) > {}", mode, (int64_t)k - 1));
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) <= {}", mode, (int64_t)k));
                    }
                }

                if (opts.EnableBetween) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) BETWEEN {} AND {}", mode, (int64_t)k - 1, (int64_t)k + 1));
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) NOT BETWEEN {} AND {}", mode, (int64_t)k + 1, (int64_t)k + 10));
                    }
                }

                if (opts.EnableInList) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) IN ({}, {}, {})", mode, (int64_t)k, (int64_t)k + 1, (int64_t)k + 2));
                    }

                    {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        addJErr(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) NOT IN ({}, {}, {})", mode, (int64_t)k, (int64_t)k + 1, (int64_t)k + 2));
                    }
                }

                if (opts.EnableSqlParameters) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        auto pn = newPname();
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) = {}", mode, pn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithScalarInt);
                        auto plo = newPname();
                        auto phi = newPname();
                        addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) BETWEEN {} AND {}", mode, plo, phi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }
                }
            }

            if (!keysWithScalarBoolean.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Bool)", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Bool) = true", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $' RETURNING Bool) != false", mode));
            }
        }
    }

    if (opts.EnableArithmeticOperators) {
        if (opts.EnableJsonExists) {
            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (-@.rank == {1})')", mode, -r));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (+@.rank == {1})')", mode, r));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (-@.u_{1} == {2})')", mode, k, -(int64_t)k));
                }

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (-@.rank < 0)')", mode));
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (+@.rank >= 0)')", mode));
                }
            }

            if (!keysWithScalarInt.empty()) {
                const ui64 k = pickFrom(keysWithScalarInt);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (-$ == {1})')", mode, -(int64_t)k));
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (+$ == {1})')", mode, (int64_t)k));
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank + 5 == {1})')", mode, r + 5));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank - 1 == {1})')", mode, r - 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank * 2 == {1})')", mode, r * 2));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank % 5 == {1})')", mode, r % 5));
                }

                {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (50 - @.rank == {1})')", mode, 50 - r));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? ({1} + @.rank == {2})')", mode, (int64_t)k, (int64_t)k + r));
                }

                for (int rDiv : {0, 5, 10, 15, 20}) {
                    if (!flatObjByRank[rDiv].empty()) {
                        addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank / 5 == {1})')", mode, rDiv / 5));
                        break;
                    }
                }

                if (opts.EnableRangeComparisons) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank + 1 > {1})')", mode, r));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank * 2 <= {1})')", mode, r * 2));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank - 1 != {1})')", mode, r));
                }
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} + @.rank == {2})')", mode, k, k + r));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} - @.rank == {2})')", mode, k, k - r));
                }

                if (opts.EnableRangeComparisons) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} + @.rank > {2})')", mode, k, k + r - 1));
                }
            }

            if (opts.EnableJsonPathMethods && !keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank.abs() + 1 == {1})')", mode, r + 1));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (-@.rank.abs() == {1})')", mode, -r));
                }

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank.abs() - 1 >= 0)')", mode));
                }

                if (!keysWithItems.empty()) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $.items.size() ? (@ * 2 == 4)')", mode));
                }
            }

            if (opts.EnablePassingVariables && !keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.rank + $inc == {1})' PASSING 5 AS inc)", mode, r + 5));
                    addJ(std::format("JSON_EXISTS(Text, '{0} $ ? ($base - @.rank == 0)' PASSING {1} AS base)", mode, r));
                }

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank * $factor > 0)' PASSING 2 AS factor)", mode));
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank + $delta < 50)' PASSING 0 AS delta)", mode));
                }

                if (opts.EnableSqlParameters) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    auto pn = newPname();
                    auto vn = pn.substr(1);
                    addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank + {} == {})' PASSING {} AS {})", mode, pn, r + 1, pn, vn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(1).Build(); });
                }
            }
        }

        if (opts.EnableJsonValue) {
            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank + 5 == {1}' RETURNING Bool)", mode, r + 5));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank - 1 == {1}' RETURNING Bool)", mode, r - 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank * 2 == {1}' RETURNING Bool)", mode, r * 2));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank % 5 == {1}' RETURNING Bool)", mode, r % 5));
                    addJ(std::format("JSON_VALUE(Text, '{0} -$.rank == {1}' RETURNING Bool)", mode, -r));
                    addJ(std::format("JSON_VALUE(Text, '{0} +$.rank == {1}' RETURNING Bool)", mode, r));
                }

                {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1} + $.rank == {2}' RETURNING Bool)", mode, k, k + r));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1} - $.rank == {2}' RETURNING Bool)", mode, k, k - r));
                    addJ(std::format("JSON_VALUE(Text, '{0} 50 - $.rank == {1}' RETURNING Bool)", mode, 50 - r));
                }

                if (opts.EnableRangeComparisons) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank + 1 > {1}' RETURNING Bool)", mode, r));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank * 2 <= {1}' RETURNING Bool)", mode, r * 2));
                    addJ(std::format("JSON_VALUE(Text, '{} -$.rank < 0' RETURNING Bool)", mode));
                }
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank ? (@ + 5 == {1})' RETURNING Int64) = {2}", mode, r + 5, r));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank ? (-@ == {1})' RETURNING Int64) = {2}", mode, -r, r));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank ? (@ * 2 == {1})' RETURNING Int64) = {2}", mode, r * 2, r));
                }

                if (opts.EnableRangeComparisons) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank ? (@ + 1 > {1})' RETURNING Int64) = {2}", mode, r, r));
                }
            }

            if (opts.EnableJsonPathMethods && !keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank.abs() + 1 == {1}' RETURNING Bool)", mode, r + 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} -$.rank.abs() == {1}' RETURNING Bool)", mode, -r));
                }

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank.abs() - 1 >= 0' RETURNING Bool)", mode));
                }
            }

            if (opts.EnablePassingVariables && !keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank + $inc == {1}' PASSING 5 AS inc RETURNING Bool)", mode, r + 5));
                    addJ(std::format("JSON_VALUE(Text, '{0} $base - $.rank == 0' PASSING {1} AS base RETURNING Bool)", mode, r));
                }
            }

            if (!keysWithFlatObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + 5 = {1}", mode, r + 5));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) * 2 = {1}", mode, r * 2));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) - 1 = {1}", mode, r - 1));
                    addJErr(std::format("5 + JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = {1}", mode, r + 5));
                }

                {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJErr(std::format(
                        "JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {2}",
                        mode, k, k + r));
                }

                if (opts.EnableRangeComparisons) {
                    addJErr(std::format("-JSON_VALUE(Text, '{} $.rank' RETURNING Int64) < 0", mode));
                    addJErr(std::format("-JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <= -1", mode));
                }

                if (opts.EnableBetween) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) * 2 BETWEEN {1} AND {2}",
                        mode, r * 2 - 1, r * 2 + 1));
                }

                if (opts.EnableInList) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + 1 IN ({1}, -1, 999)", mode, r + 1));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) + 1 NOT IN ({1}, -1, 999)", mode, r + 1));
                }
            }

            if (!keysWithFlatObj.empty()) {
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) = -1", mode));
                addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <> -999", mode));

                if (opts.EnableRangeComparisons) {
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) > -1", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) >= -10", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) < -1", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) <= -1", mode));
                }

                if (opts.EnableBetween) {
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN -5 AND 50", mode));
                    addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) NOT BETWEEN -100 AND -1", mode));
                }

                if (opts.EnableInList) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    const int64_t r = k % 50;
                    addJ(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) IN ({1}, -1, -5)", mode, r));
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) NOT IN ({1}, -1, -5)", mode, r));
                }
            }
        }
    }

    if (opts.EnableSqlNullChecks) {
        if (opts.EnableJsonExists) {
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS NULL", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS NOT NULL", mode));

            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') IS NULL", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') IS NOT NULL", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS NULL", mode));

            if (!keysWithUKey.empty()) {
                const ui64 k = pickFrom(keysWithUKey);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.u_{}') IS NULL", mode, k));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.u_{}') IS NOT NULL", mode, k));
            }

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.u_{}[0]') IS NULL", mode, k));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.u_{}[*]') IS NOT NULL", mode, k));
            }

            if (!keysWithNestedObj.empty()) {
                const ui64 k = pickFrom(keysWithNestedObj);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.shared.u_{}') IS NULL", mode, k));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.shared.u_{}') IS NOT NULL", mode, k));
            }

            if (!keysWithDeepNested.empty()) {
                const ui64 k = pickFrom(keysWithDeepNested);
                addJErr(std::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}') IS NULL", mode, k));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.a.b.c.u_{}') IS NOT NULL", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJErr(std::format("JSON_EXISTS(Text, '{} $.items') IS NULL", mode));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.items[0].id') IS NOT NULL", mode));
            }

            if (!keysWithFullMix.empty()) {
                addJErr(std::format("JSON_EXISTS(Text, '{} $.shared_null') IS NULL", mode));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.shared_null') IS NOT NULL", mode));
                addJErr(std::format("JSON_EXISTS(Text, '{} $.shared_n') IS NOT NULL", mode));
            }

            if (opts.EnablePassingVariables) {
                const ui64 k = pickFrom(keysWithUKey);
                addJErr(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) IS NULL", mode, k));
                addJErr(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) IS NOT NULL", mode, k));
            }
        }

        if (opts.EnableJsonValue) {
            addJErr(std::format("JSON_VALUE(Text, '{} $.shared') IS NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.shared') IS NOT NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.nope_xyz') IS NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.*') IS NOT NULL", mode));

            if (!keysWithStrUVal.empty()) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}') IS NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}') IS NOT NULL", mode, k));
            }

            addJErr(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS NOT NULL", mode));

            if (!keysWithIntUVal.empty()) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}' RETURNING Int64) IS NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}' RETURNING Int64) IS NOT NULL", mode, k));
            }

            addJErr(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Double) IS NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Double) IS NOT NULL", mode));

            addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NOT NULL", mode));
            for (int j = 0; j < 5; ++j) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS NULL", mode, j));
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS NOT NULL", mode, j));
            }

            if (!keysWithUArr.empty()) {
                const ui64 k = pickFrom(keysWithUArr);
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}[0]' RETURNING Int64) IS NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}[0]' RETURNING Int64) IS NOT NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $.u_{}[2]') IS NULL", mode, k));
            }

            if (!keysWithNestedObj.empty()) {
                const ui64 k = pickFrom(keysWithNestedObj);
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared.u_{}' RETURNING Int64) IS NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared.u_{}' RETURNING Int64) IS NOT NULL", mode, k));
            }

            if (!keysWithDeepNested.empty()) {
                const ui64 k = pickFrom(keysWithDeepNested);
                addJErr(std::format("JSON_VALUE(Text, '{} $.a.b.c.u_{}' RETURNING Int64) IS NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $.a.b.c.u_{}' RETURNING Int64) IS NOT NULL", mode, k));
            }

            if (!keysWithItems.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.items[0].id' RETURNING Int64) IS NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.items[0].id' RETURNING Int64) IS NOT NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.items[0].name') IS NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.items[1].name') IS NOT NULL", mode));
            }

            if (!keysWithFullMix.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_n' RETURNING Int64) IS NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_n' RETURNING Int64) IS NOT NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_s') IS NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_null') IS NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_null') IS NOT NULL", mode));
            }

            if (!keysWithHeteroArr.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $[*].k_a' RETURNING Int64) IS NULL", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $[*].k_b') IS NOT NULL", mode));
            }

            if (!keysWithHomoArr.empty()) {
                const ui64 k = pickFrom(keysWithHomoArr);
                addJErr(std::format("JSON_VALUE(Text, '{} $[*].u_{}' RETURNING Int64) IS NULL", mode, k));
                addJErr(std::format("JSON_VALUE(Text, '{} $[*].shared') IS NOT NULL", mode));
            }

            if (!keysWithMixed.empty()) {
                const ui64 k = pickFrom(keysWithMixed);
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}.deep.v' RETURNING Int64) IS NULL", mode, k % 5));
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}.deep.v' RETURNING Int64) IS NOT NULL", mode, k % 5));
            }

            addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared') = \"shared_v\"u) IS NULL", mode));
            addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared') = \"shared_v\"u) IS NOT NULL", mode));
            addJErr(std::format("(JSON_VALUE(Text, '{0} $.nope_xyz') = \"x\"u) IS NULL", mode));

            addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = 10) IS NULL", mode));
            addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = 10) IS NOT NULL", mode));
            addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) != 10) IS NULL", mode));

            if (!keysWithIntUVal.empty()) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}) IS NULL", mode, k));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) != {1}) IS NOT NULL", mode, k));
            }

            if (opts.EnableRangeComparisons) {
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) > 10) IS NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) > 10) IS NOT NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) >= 10) IS NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) < 10) IS NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) <= 10) IS NOT NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Double) > 0.0) IS NULL", mode));

                addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared') >= \"s\"u) IS NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared') < \"z\"u) IS NOT NULL", mode));

                if (!keysWithIntUVal.empty()) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > {2}) IS NULL", mode, k, k - 1));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) <= {1}) IS NOT NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Double) >= {1}.0) IS NULL", mode, k));
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) > {2}) IS NULL", mode, k, k - 1));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) <= {1}) IS NOT NULL", mode, k));
                }

                if (!keysWithItems.empty()) {
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) > 0) IS NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) < 0) IS NOT NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Double) >= 0.0) IS NULL", mode));
                }

                if (!keysWithFullMix.empty()) {
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) > 0) IS NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) < 0) IS NOT NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared_s') >= \"a\"u) IS NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.shared_s') < \"z\"u) IS NOT NULL", mode));
                }

                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)) IS NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) = JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)) IS NOT NULL", mode));
                addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) != JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)) IS NULL", mode));

                if (!keysWithFlatObj.empty()) {
                    const ui64 k = pickFrom(keysWithFlatObj);
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)) IS NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)) IS NOT NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > JSON_VALUE(Text, '{0} $.rank' RETURNING Int64)) IS NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) <= JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64)) IS NOT NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64)) IS NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Double) >= JSON_VALUE(Text, '{0} $.rank' RETURNING Double)) IS NULL", mode, k));
                }

                if (!keysWithItems.empty()) {
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)) IS NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64)) IS NOT NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Int64) > JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64)) IS NULL", mode));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Double) < JSON_VALUE(Text, '{0} $.items[1].id' RETURNING Double)) IS NOT NULL", mode));
                }

                if (!keysWithUArr.empty()) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)) IS NULL", mode, k));
                    addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) < JSON_VALUE(Text, '{0} $.u_{1}[1]' RETURNING Int64)) IS NOT NULL", mode, k));
                }

                if (opts.EnableSqlParameters) {
                    {
                        auto pn = newPname();
                        addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) > {1}) IS NULL", mode, pn),
                            [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(10).Build(); });
                    }

                    {
                        auto pn = newPname();
                        addJErr(std::format("(JSON_VALUE(Text, '{0} $.rank' RETURNING Int64) >= {1}) IS NOT NULL", mode, pn),
                            [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64(0).Build(); });
                    }

                    if (!keysWithFlatObj.empty()) {
                        const ui64 k = pickFrom(keysWithFlatObj);
                        auto pn = newPname();
                        addJErr(std::format("(JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) > {2}) IS NULL", mode, k, pn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k - 1).Build(); });
                    }
                }
            }
        }
    }

    if (opts.EnableDistinctFrom) {
        if (opts.EnableJsonExists) {
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS NOT DISTINCT FROM true", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') IS NOT DISTINCT FROM true", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS NOT DISTINCT FROM true", mode));

            if (opts.EnablePassingVariables) {
                if (!keysWithIntUVal.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithIntUVal);
                        addJErr(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var) IS NOT DISTINCT FROM true", mode, k));
                    }
                }

                if (!keysWithBothSharedAndU.empty()) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJErr(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} && @.shared == "shared_v")') IS NOT DISTINCT FROM true)", mode, k));
                }

                if (opts.EnableSqlParameters) {
                    if (!keysWithIntUVal.empty()) {
                        const ui64 k = pickFrom(keysWithIntUVal);
                        auto pn = newPname();
                        auto vn = pn.substr(1);
                        addJErr(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == {2})' PASSING {2} AS {3}) IS NOT DISTINCT FROM true", mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }
                }
            }

            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS DISTINCT FROM true", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') IS DISTINCT FROM true", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.nope_xyz') IS DISTINCT FROM true", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS NOT DISTINCT FROM false", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') IS NOT DISTINCT FROM false", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS DISTINCT FROM false", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $.shared') IS DISTINCT FROM false", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS DISTINCT FROM NULL", mode));
            addJErr(std::format("JSON_EXISTS(Text, '{} $') IS NOT DISTINCT FROM NULL", mode));
        }

        if (opts.EnableJsonValue) {
            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared') IS NOT DISTINCT FROM "shared_v"u)", mode));
            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.*') IS NOT DISTINCT FROM "shared_v"u)", mode));

            if (!keysWithStrUVal.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') IS NOT DISTINCT FROM "u_v_{1}"u)", mode, k));
                }
            }

            if (!keysWithIntUVal.empty()) {
                for (size_t i = 0; i < 3; ++i) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }
            }

            for (size_t i = 0; i < 3; ++i) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS NOT DISTINCT FROM {}", mode, (int)rng.Uniform(50)));
            }

            for (int j = 0; j < 5; ++j) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS NOT DISTINCT FROM true", mode, j));
            }

            if (!keysWithFullMix.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NOT DISTINCT FROM true", mode));

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.shared_s') IS NOT DISTINCT FROM "u_v_{1}"u)", mode, k));
                }
            }

            if (!keysWithBothSharedAndU.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }
            }

            if (!keysWithUArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }
            }

            if (!keysWithNestedObj.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }

                for (int j = 0; j < 5; ++j) {
                    addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared.g5_{}') IS NOT DISTINCT FROM "v"u)", mode, j));
                }
            }

            if (!keysWithDeepNested.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }
            }

            if (!keysWithHomoArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHomoArr);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $[*].u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }
            }

            if (!keysWithHeteroArr.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    addJErr(std::format(R"(JSON_VALUE(Text, '{0} $[*].k_b') IS NOT DISTINCT FROM "u_v_{1}"u)", mode, k));
                }
            }

            if (!keysWithMixed.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithMixed);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) IS NOT DISTINCT FROM {2}", mode, k % 5, k));
                }
            }

            if (!keysWithItems.empty()) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, k));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithItems);
                    addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.items[0].name') IS NOT DISTINCT FROM "u_v_{1}"u)", mode, k));
                }
            }

            if (opts.EnableSqlParameters) {
                {
                    auto pn = newPname();
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared') IS NOT DISTINCT FROM {1}", mode, pn),
                        [pn](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8("shared_v").Build(); });
                }

                if (!keysWithIntUVal.empty()) {
                    const ui64 k = pickFrom(keysWithIntUVal);
                    auto pn = newPname();
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                if (!keysWithStrUVal.empty()) {
                    const ui64 k = pickFrom(keysWithStrUVal);
                    const auto uvk = "u_v_" + std::to_string(k);
                    auto pn = newPname();
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}') IS NOT DISTINCT FROM {2}", mode, k, pn),
                        [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                }

                if (!keysWithNestedObj.empty()) {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto pn = newPname();
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) IS NOT DISTINCT FROM {2}", mode, k, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }

                if (!keysWithFullMix.empty()) {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto pn = newPname();
                    addJErr(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) IS NOT DISTINCT FROM {1}", mode, pn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }
            }

            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared') IS DISTINCT FROM "shared_v"u)", mode));
            addJErr(std::format(R"(JSON_VALUE(Text, '{} $.shared') IS DISTINCT FROM "nope"u)", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) IS DISTINCT FROM 10", mode));

            addJErr(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) IS DISTINCT FROM NULL", mode));
            addJErr(std::format("JSON_VALUE(Text, '{} $' RETURNING Int64) IS NOT DISTINCT FROM NULL", mode));

            if (!keysWithStrUVal.empty()) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJErr(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') IS DISTINCT FROM "u_v_{1}"u)", mode, k));
            }

            if (!keysWithIntUVal.empty()) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJErr(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IS DISTINCT FROM {1}", mode, k));
            }

            for (int j = 0; j < 2; ++j) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS DISTINCT FROM true", mode, j));
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS NOT DISTINCT FROM false", mode, j));
                addJErr(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) IS DISTINCT FROM false", mode, j));
            }

            if (!keysWithFullMix.empty()) {
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS DISTINCT FROM true", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS NOT DISTINCT FROM false", mode));
                addJErr(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IS DISTINCT FROM false", mode));
            }

            if (!keysWithNestedObj.empty()) {
                const ui64 k = pickFrom(keysWithNestedObj);
                addJErr(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) IS DISTINCT FROM {1}", mode, k));
            }

            if (!keysWithItems.empty()) {
                const ui64 k = pickFrom(keysWithItems);
                addJErr(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) IS DISTINCT FROM {1}", mode, k));
            }
        }
    }

    if (opts.EnableNonJsonFilters) {
        for (size_t i = 0; i < 3; ++i) {
            const ui64 k = rows[rng.Uniform(rows.size())].Key;
            filterAtoms.push_back(TAtom{
                .Sql = std::format("Key = {}", k),
                .AddParams = nullptr,
                .IsJsonIndexable = false});
        }

        {
            const ui64 mid = rows.size() / 2 + 1;

            filterAtoms.push_back(TAtom{
                .Sql = std::format("Key > {}", mid),
                .AddParams = nullptr,
                .IsJsonIndexable = false});

            filterAtoms.push_back(TAtom{
                .Sql = std::format("Key < {}", mid),
                .AddParams = nullptr,
                .IsJsonIndexable = false});

            filterAtoms.push_back(TAtom{
                .Sql = std::format("Key BETWEEN {} AND {}", mid - 5, mid + 5),
                .AddParams = nullptr,
                .IsJsonIndexable = false});
        }

        if (opts.EnableSqlParameters) {
            const ui64 k = rows[rng.Uniform(rows.size())].Key;
            auto pn = newPname();
            filterAtoms.push_back(TAtom{
                .Sql = std::format("Key = {}", pn),
                .AddParams = [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Uint64(k).Build(); },
                .IsJsonIndexable = false});
        }
    }

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
        std::vector<size_t> idx(jsonAtoms.size());
        std::iota(idx.begin(), idx.end(), 0);

        for (size_t i = idx.size() - 1; i > 0; --i) {
            std::swap(idx[i], idx[rng.Uniform(i + 1)]);
        }

        const size_t budget = maxCount / 2;
        for (size_t i = 0; i < idx.size() && result.size() < budget; ++i) {
            pushAtom(jsonAtoms[idx[i]]);
        }
    }

    if (!jsonAtoms.empty()) {
        // J* AND J*
        if (opts.EnableAndCombinations) {
            for (size_t i = 0; i < 10 && result.size() < maxCount * 3 / 4; ++i) {
                const auto& a = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& b = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(andAtom(a, b));
            }
        }

        // J* OR J*
        if (opts.EnableOrCombinations) {
            for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                const auto& a = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& b = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(orAtom(a, b));
            }
        }

        if (opts.EnableAndCombinations && opts.EnableOrCombinations) {
            // J* AND J* OR J*
            for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                const auto& a = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& b = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& c = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(orAtom(andAtom(a, b), c));
            }

            // J* OR J* AND J*
            for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                const auto& a = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& b = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& c = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(andAtom(orAtom(a, b), c));
            }
        }

        // J* AND J* AND J*
        if (opts.EnableAndCombinations) {
            for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                const auto& a = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& b = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& c = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(andAtom(a, andAtom(b, c)));
            }
        }

        // J* OR J* OR J*
        if (opts.EnableOrCombinations) {
            for (size_t i = 0; i < 15 && result.size() < maxCount * 7 / 8; ++i) {
                const auto& a = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& b = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& c = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(orAtom(a, orAtom(b, c)));
            }
        }


        if (!filterAtoms.empty()) {
            if (opts.EnableAndCombinations) {
                // J* AND non-J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(j, f));
                }

                // non-J* AND J* AND J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(andAtom(f, j1), j2));
                }

                // J* AND non-J* AND J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(andAtom(j1, f), j2));
                }

                // J* AND J* AND non-J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(andAtom(j1, j2), f));
                }
            }

            if (opts.EnableOrCombinations) {
                // J* OR non-J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(j, f));
                }

                // non-J* OR J* OR J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(orAtom(f, j1), j2));
                }

                // J* OR non-J* OR J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(orAtom(j1, f), j2));
                }

                // J* OR J* OR non-J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(orAtom(j1, j2), f));
                }
            }

            if (opts.EnableAndCombinations && opts.EnableOrCombinations) {
                // J* AND J* OR non-J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(andAtom(j1, j2), f));
                }

                // J* AND non-J* OR J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(andAtom(j1, f), j2));
                }

                // non-J* AND J* OR J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(orAtom(andAtom(f, j1), j2));
                }

                // J* OR J* AND non-J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(orAtom(j1, j2), f));
                }

                // J* OR non-J* AND J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(orAtom(j1, f), j2));
                }

                // non-J* OR J* AND J*
                for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                    const auto& j1 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& j2 = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                    const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                    pushAtom(andAtom(orAtom(f, j1), j2));
                }
            }
        }
    }

    return result;
}

} // namespace NKikimr::NKqp
