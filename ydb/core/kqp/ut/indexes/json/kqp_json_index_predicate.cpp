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

    auto atom = [](std::string sql) -> TAtom {
        return TAtom{
            .Sql=std::move(sql),
            .AddParams=nullptr,
            .IsJsonIndexable=true
        };
    };

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

    // Fill J* result
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

    if (opts.EnableAndCombinations && opts.EnableJsonValue) {
        // JV AND JV
        for (size_t i = 0; i < 8 && result.size() < maxCount; ++i) {
            const int j = (int)rng.Uniform(50);
            if (flatObjByRank[j].empty()) {
                continue;
            }
            const ui64 k = flatObjByRank[j][rng.Uniform(flatObjByRank[j].size())];

            pushAtom(andAtom(
                atom(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) = {}", mode, j)),
                atom(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", mode, k))));
        }

        // JV AND JV BETWEEN
        if (opts.EnableBetween) {
            for (size_t i = 0; i < 6 && result.size() < maxCount; ++i) {
                const int lo = (int)rng.Uniform(35);
                const int hi = lo + 5 + (int)rng.Uniform(15);

                pushAtom(andAtom(
                    atom(std::format("JSON_VALUE(Text, '{} $.shared') = \"shared_v\"u", mode)),
                    atom(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN {} AND {}", mode, lo, hi))));
            }

            for (size_t i = 0; i < 4 && result.size() < maxCount; ++i) {
                const int j = (int)rng.Uniform(5);
                const int lo = (int)rng.Uniform(30);
                const int hi = lo + 10 + (int)rng.Uniform(15);

                pushAtom(andAtom(
                    atom(std::format("JSON_VALUE(Text, '{} $.g5_{}' RETURNING Bool) = true", mode, j)),
                    atom(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN {} AND {}", mode, lo, hi))));
            }
        }
    }

    if (opts.EnableAndCombinations && opts.EnableBetween && opts.EnableJsonValue && opts.EnableJsonExists) {
        // JV BETWEEN AND JE
        for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
            const ui64 k = pickFrom(keysWithFlatObj);
            const int rank = (int)(k % 50);
            const int lo = std::max(0, rank - 5);
            const int hi = std::min(49, rank + 5);

            pushAtom(andAtom(
                atom(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) BETWEEN {} AND {}", mode, lo, hi)),
                atom(std::format("JSON_EXISTS(Text, '{} $.u_{}')", mode, k))));
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

        // J* AND J* OR J*
        if (opts.EnableAndCombinations && opts.EnableOrCombinations) {
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

        // J* AND non-J*
        if (opts.EnableAndCombinations && !filterAtoms.empty()) {
            for (size_t i = 0; i < 10 && result.size() < maxCount; ++i) {
                const auto& j = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                pushAtom(andAtom(j, f));
            }
        }

        // J* OR non-J*
        if (opts.EnableOrCombinations && !filterAtoms.empty()) {
            for (size_t i = 0; i < 10 && result.size() < maxCount; ++i) {
                const auto& j = jsonAtoms[rng.Uniform(jsonAtoms.size())];
                const auto& f = filterAtoms[rng.Uniform(filterAtoms.size())];

                pushAtom(orAtom(j, f));
            }
        }

        // JV AND JV AND J*
        if (opts.EnableAndCombinations && opts.EnableJsonValue) {
            for (size_t i = 0; i < 5 && result.size() < maxCount; ++i) {
                const int j = (int)rng.Uniform(50);
                if (flatObjByRank[j].empty()) {
                    continue;
                }

                const ui64 k = flatObjByRank[j][rng.Uniform(flatObjByRank[j].size())];
                const auto& extra = jsonAtoms[rng.Uniform(jsonAtoms.size())];

                pushAtom(andAtom(
                    atom(std::format("JSON_VALUE(Text, '{} $.rank' RETURNING Int64) = {}", mode, j)),
                    andAtom(atom(std::format("JSON_VALUE(Text, '{0} $.u_{1}' RETURNING Int64) = {1}", mode, k)), extra)));
            }
        }
    }

    return result;
}

} // namespace NKikimr::NKqp
