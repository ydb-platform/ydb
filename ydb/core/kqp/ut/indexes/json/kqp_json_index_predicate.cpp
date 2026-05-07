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
    std::array<std::vector<ui64>, 50> flatObjByRank;
    std::vector<ui64> keysWithNestedObj;
    std::vector<ui64> keysWithDeepNested;
    std::vector<ui64> keysWithHomoArr;
    std::vector<ui64> keysWithHeteroArr;
    std::vector<ui64> keysWithMixed;
    std::vector<ui64> keysWithItems;
    std::vector<ui64> keysWithFullMix;

    const auto& rows = corpus.Rows();

    for (const auto& row : rows) {
        switch (row.Shape) {
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
                break;

            case EJsonShape::EmptyKey:
                keysWithUKey.push_back(row.Key);
                keysWithStrUVal.push_back(row.Key);
                break;

            case EJsonShape::ObjWithArray:
                keysWithUKey.push_back(row.Key);
                keysWithUArr.push_back(row.Key);
                keysWithShared.push_back(row.Key);
                keysWithBothSharedAndU.push_back(row.Key);
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

    if (opts.EnableJsonExists) {
        // JE empty
        {
            addJ(std::format("JSON_EXISTS(Text, '{} $')", mode));
        }

        // Member access JE
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

            for (int j = 0; j < 5; ++j) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.g5_{}')", mode, j));
            }
        }

        // Array access JE
        {
            for (size_t i = 0; i < 3; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[*]')", mode, pickFrom(keysWithUArr)));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format("JSON_EXISTS(Text, '{} $.u_{}[0]')", mode, pickFrom(keysWithUArr)));
            }

            addJ(std::format("JSON_EXISTS(Text, '{} $.shared[*]')", mode));
        }

        // Filter finished JE (equality)
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

            for (size_t i = 0; i < 3; ++i)
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank == {})')", mode, (int)rng.Uniform(50)));
        }

        // Filter not finished JE (range / starts with)
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
                const ui64 k = pickFrom(keysWithStrUVal);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $.u_{1} ? (@ starts with "u_v")'))", mode, k));
            }

            for (int lo : {10, 25}) {
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank > {})')", mode, lo));
                addJ(std::format("JSON_EXISTS(Text, '{} $ ? (@.rank >= {})')", mode, lo));
            }
        }

        // JE with PASSING variables (inline literals)
        if (opts.EnablePassingVariables) {
            for (size_t i = 0; i < 3; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1} ? (@ == $var)' PASSING {1} AS var)", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.shared == $var)' PASSING "shared_v"u AS var))", mode));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithFlatObj);
                const auto g5k = "g5_" + std::to_string(k % 5);
                addJ(std::format("JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == $v1 && @.{2} == $v2)' PASSING {1} AS v1, true AS v2)", mode, k, g5k));
            }

            // JE with PASSING + SQL parameters
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
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $[*].k_b ? (@ starts with $var)' PASSING "u_v"u AS var))", mode));
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

            // JE PASSING + SQL parameters
            if (opts.EnableSqlParameters) {
                if (!keysWithNestedObj.empty()) {
                    for (size_t i = 0; i < 2; ++i) {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        auto pn = newPname(); auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared.u_{1} ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithNestedObj);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
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
                        auto pn = newPname(); auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.a.b.c.u_{1} ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithDeepNested);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
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
                        auto pn = newPname(); auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ == {2})' PASSING {2} AS {3})", mode, k % 5, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithMixed);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.g5_{1}.deep.v ? (@ >= {2} && @ <= {3})' PASSING {2} AS {4}, {3} AS {5})", mode, k % 5, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithMixed);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
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
                        auto pn = newPname(); auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.items[*].id ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithItems);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
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
                        auto pn = newPname(); auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ >= {1} && @ <= {2})' PASSING {1} AS {3}, {2} AS {4})", mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k - 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 1).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        auto plo = newPname(); auto phi = newPname();
                        auto vlo = plo.substr(1); auto vhi = phi.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_n ? (@ < {1} || @ > {2})' PASSING {1} AS {3}, {2} AS {4})", mode, plo, phi, vlo, vhi),
                            [plo, phi, k](NYdb::TParamsBuilder& bld) {
                                bld.AddParam(plo).Int64((i64)k + 1).Build();
                                bld.AddParam(phi).Int64((i64)k + 10).Build();
                            });
                    }

                    {
                        const ui64 k = pickFrom(keysWithFullMix);
                        const auto uvk = "u_v_" + std::to_string(k);
                        auto pn = newPname(); auto vn = pn.substr(1);
                        addJ(std::format("JSON_EXISTS(Text, '{0} $.shared_s ? (@ == {1})' PASSING {1} AS {2})", mode, pn, vn),
                            [pn, uvk](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Utf8(uvk).Build(); });
                    }
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    auto pn = newPname(); auto vn = pn.substr(1);
                    addJ(std::format("JSON_EXISTS(Text, '{0} $.u_{1}[*] ? (@ == {2})' PASSING {2} AS {3})", mode, k, pn, vn),
                        [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
                }
            }
        }

        // JE combined path filters
        {
            for (size_t i = 0; i < 4; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} && @.shared == "shared_v")'))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithBothSharedAndU);
                addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} == {1} || @.shared == "shared_v")'))", mode, k));
            }

            if (opts.EnableRangeComparisons) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} >= {1} && @.shared == "shared_v")'))", mode, k));
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} < {2} && @.shared == "shared_v")'))", mode, k, k + 1));
                }

                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithBothSharedAndU);
                    addJ(std::format(R"(JSON_EXISTS(Text, '{0} $ ? (@.u_{1} > {2} || @.shared == "shared_v")'))", mode, k, k - 1));
                }
            }
        }

        // JE $.shared value filters
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
    }

    if (opts.EnableJsonValue) {
        // JV finished (equality)
        {
            addJ(std::format("JSON_VALUE(Text, '{} $.shared') = \"shared_v\"u", mode));

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

        // JV range comparisons (>, <, >=, <=)
        if (opts.EnableRangeComparisons) {
            addJ(std::format("JSON_VALUE(Text, '{} $.shared') <> \"nope\"u", mode));

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

        // JV BETWEEN
        if (opts.EnableBetween) {
            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
            }

            addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 10 AND 20", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 0 AND 25", mode));
            addJ(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN 20 AND 40", mode));
        }

        // JV IN
        if (opts.EnableInList) {
            addJ(std::format(R"(JSON_VALUE(Text, '{} $.shared') IN ("shared_v"u, "other"u))", mode));

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithStrUVal);
                addJ(std::format(R"(JSON_VALUE(Text, '{0} $.u_{1}') IN ("u_v_{1}"u, "nope"u))", mode, k));
            }

            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithIntUVal);
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
            }
        }

        // JV with SQL parameters
        if (opts.EnableSqlParameters) {
            for (size_t i = 0; i < 2; ++i) {
                auto pn = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.shared') = {1}", mode, pn),
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

            // JV SQL parameters
            for (size_t i = 0; i < 2; ++i) {
                const ui64 k = pickFrom(keysWithUArr);
                auto pn = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) = {2}", mode, k, pn),
                    [pn, k](NYdb::TParamsBuilder& bld) { bld.AddParam(pn).Int64((i64)k).Build(); });
            }

            {
                const ui64 k = pickFrom(keysWithUArr);
                auto plo = newPname(); auto phi = newPname();
                addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
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
                auto plo = newPname(); auto phi = newPname();
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
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithNestedObj);
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared.u_{1}' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, plo, phi),
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
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.a.b.c.u_{1}' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithDeepNested);
                    auto plo = newPname(); auto phi = newPname();
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
                    auto plo = newPname(); auto phi = newPname();
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
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $[*].k_a' RETURNING Int64) BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithHeteroArr);
                    auto plo = newPname(); auto phi = newPname();
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
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.g5_{1}.deep.v' RETURNING Int64) BETWEEN {2} AND {3}", mode, k % 5, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithMixed);
                    auto plo = newPname(); auto phi = newPname();
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
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.items[0].id' RETURNING Int64) BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithItems);
                    auto plo = newPname(); auto phi = newPname();
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
                    auto plo = newPname(); auto phi = newPname();
                    addJ(std::format("JSON_VALUE(Text, '{0} $.shared_n' RETURNING Int64) BETWEEN {1} AND {2}", mode, plo, phi),
                        [plo, phi, k](NYdb::TParamsBuilder& bld) {
                            bld.AddParam(plo).Int64((i64)k - 1).Build();
                            bld.AddParam(phi).Int64((i64)k + 1).Build();
                        });
                }

                {
                    const ui64 k = pickFrom(keysWithFullMix);
                    auto plo = newPname(); auto phi = newPname();
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

            if (opts.EnableRangeComparisons) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) > {2}", mode, k, k - 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) <= {1}", mode, k));
                }
            }

            if (opts.EnableBetween) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) BETWEEN {2} AND {3}", mode, k, k - 1, k + 1));
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) NOT BETWEEN {2} AND {3}", mode, k, k + 2, k + 10));
                }
            }

            if (opts.EnableInList) {
                for (size_t i = 0; i < 2; ++i) {
                    const ui64 k = pickFrom(keysWithUArr);
                    addJ(std::format("JSON_VALUE(Text, '{0} $.u_{1}[0]' RETURNING Int64) IN ({1}, {2}, {3})", mode, k, k + 1, k + 2));
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
            }
        }

        // JV $.shared range/BETWEEN/NOT BETWEEN/IN
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
            }
        }

        // JV NOT BETWEEN
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

                    addJ(std::format("JSON_VALUE(Text, '{} $.shared_b' RETURNING Bool) IN (true, false)", mode));
                }
            }
        }
    }

    // Non-J* filters
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
