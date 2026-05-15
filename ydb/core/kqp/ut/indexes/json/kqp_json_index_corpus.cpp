#include "kqp_json_index_corpus.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <util/random/mersenne.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

TString TJsonCorpus::SerializeJson(ui64 key, EJsonShape shape) {
    using NJson::TJsonValue;

    const std::string uk = "u_" + std::to_string(key);
    const std::string uvk = "u_v_" + std::to_string(key);
    const std::string g5k = "g5_" + std::to_string(key % 5);

    switch (shape) {
        case EJsonShape::Scalar: {
            TJsonValue v;
            switch (key % 6) {
                case 0:
                    v.SetType(NJson::JSON_NULL);
                    break;
                case 1:
                    v = TJsonValue(true);
                    break;
                case 2:
                    v = TJsonValue(false);
                    break;
                case 3:
                    v = TJsonValue(static_cast<long long>(key));
                    break;
                case 4:
                    v = TJsonValue(-static_cast<double>(key) - 0.5);
                    break;
                case 5:
                    v = TJsonValue(TString(uvk));
                    break;
            }
            return NJson::WriteJson(&v, /*formatOutput=*/false);
        }

        case EJsonShape::FlatObj: {
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue(uk, TJsonValue(static_cast<long long>(key)));
            v.InsertValue("shared", TJsonValue("shared_v"));
            v.InsertValue(g5k, TJsonValue(true));
            v.InsertValue("rank", TJsonValue(static_cast<long long>(key % 50)));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::EmptyContainers: {
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue(uk, TJsonValue(NJson::JSON_ARRAY));
            v.InsertValue("shared", TJsonValue(NJson::JSON_MAP));
            v.InsertValue("empty_key", TJsonValue(NJson::JSON_NULL));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::EmptyKey: {
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue("", TJsonValue(static_cast<long long>(key)));
            v.InsertValue(uk, TJsonValue(TString(uvk)));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::ArrayLiterals: {
            TJsonValue v(NJson::JSON_ARRAY);
            v.AppendValue(TJsonValue(NJson::JSON_NULL));
            v.AppendValue(TJsonValue(true));
            v.AppendValue(TJsonValue(false));
            v.AppendValue(TJsonValue(static_cast<long long>(key)));
            v.AppendValue(TJsonValue(TString(uvk)));
            v.AppendValue(TJsonValue("shared_v"));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::ObjWithArray: {
            TJsonValue arr(NJson::JSON_ARRAY);
            arr.AppendValue(TJsonValue(static_cast<long long>(key)));
            arr.AppendValue(TJsonValue(static_cast<long long>(key + 1)));
            arr.AppendValue(TJsonValue(TString(uvk)));
            TJsonValue sharedArr(NJson::JSON_ARRAY);
            sharedArr.AppendValue(TJsonValue(true));
            sharedArr.AppendValue(TJsonValue(NJson::JSON_NULL));
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue(uk, std::move(arr));
            v.InsertValue("shared", std::move(sharedArr));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::HomogeneousArrayObjs: {
            TJsonValue obj1(NJson::JSON_MAP);
            obj1.InsertValue(uk, TJsonValue(static_cast<long long>(key)));
            TJsonValue obj2(NJson::JSON_MAP);
            obj2.InsertValue(uk, TJsonValue(static_cast<long long>(key + 1)));
            TJsonValue obj3(NJson::JSON_MAP);
            obj3.InsertValue("shared", TJsonValue("v"));
            TJsonValue v(NJson::JSON_ARRAY);
            v.AppendValue(std::move(obj1));
            v.AppendValue(std::move(obj2));
            v.AppendValue(std::move(obj3));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::HeterogeneousArrayObjs: {
            TJsonValue obj1(NJson::JSON_MAP);
            obj1.InsertValue("k_a", TJsonValue(static_cast<long long>(key)));
            TJsonValue obj2(NJson::JSON_MAP);
            obj2.InsertValue("k_b", TJsonValue(TString(uvk)));
            TJsonValue obj3(NJson::JSON_MAP);
            obj3.InsertValue("shared", TJsonValue(NJson::JSON_NULL));
            TJsonValue v(NJson::JSON_ARRAY);
            v.AppendValue(std::move(obj1));
            v.AppendValue(std::move(obj2));
            v.AppendValue(std::move(obj3));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::NestedObj: {
            TJsonValue inner(NJson::JSON_MAP);
            inner.InsertValue(uk, TJsonValue(static_cast<long long>(key)));
            inner.InsertValue(g5k, TJsonValue("v"));
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue("shared", std::move(inner));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::DeepNested: {
            TJsonValue innermost(NJson::JSON_MAP);
            innermost.InsertValue(uk, TJsonValue(static_cast<long long>(key)));
            innermost.InsertValue("shared", TJsonValue(NJson::JSON_NULL));
            TJsonValue lc(NJson::JSON_MAP);
            lc.InsertValue("c", std::move(innermost));
            TJsonValue lb(NJson::JSON_MAP);
            lb.InsertValue("b", std::move(lc));
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue("a", std::move(lb));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::Mixed: {
            TJsonValue innerObj1(NJson::JSON_MAP);
            innerObj1.InsertValue(uk, TJsonValue(static_cast<long long>(key)));
            TJsonValue innerArr(NJson::JSON_ARRAY);
            innerArr.AppendValue(TJsonValue(static_cast<long long>(1)));
            innerArr.AppendValue(TJsonValue(static_cast<long long>(2)));
            innerArr.AppendValue(TJsonValue(static_cast<long long>(3)));
            TJsonValue innerObj2(NJson::JSON_MAP);
            innerObj2.InsertValue("shared", std::move(innerArr));
            TJsonValue sharedArr(NJson::JSON_ARRAY);
            sharedArr.AppendValue(std::move(innerObj1));
            sharedArr.AppendValue(std::move(innerObj2));
            TJsonValue deepInner(NJson::JSON_MAP);
            deepInner.InsertValue("v", TJsonValue(static_cast<long long>(key)));
            TJsonValue deep(NJson::JSON_MAP);
            deep.InsertValue("deep", std::move(deepInner));
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue("shared", std::move(sharedArr));
            v.InsertValue(g5k, std::move(deep));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::ArrayOfArrays: {
            TJsonValue sub1(NJson::JSON_ARRAY);
            sub1.AppendValue(TJsonValue(static_cast<long long>(key)));
            sub1.AppendValue(TJsonValue(static_cast<long long>(key + 1)));
            TJsonValue sub2(NJson::JSON_ARRAY);
            sub2.AppendValue(TJsonValue(static_cast<long long>(key + 2)));
            TJsonValue sub3(NJson::JSON_ARRAY);
            TJsonValue v(NJson::JSON_ARRAY);
            v.AppendValue(std::move(sub1));
            v.AppendValue(std::move(sub2));
            v.AppendValue(std::move(sub3));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::ObjWithArrayObjs: {
            TJsonValue item1(NJson::JSON_MAP);
            item1.InsertValue("id", TJsonValue(static_cast<long long>(key)));
            item1.InsertValue("name", TJsonValue(TString(uvk)));
            TJsonValue item2(NJson::JSON_MAP);
            item2.InsertValue("id", TJsonValue(static_cast<long long>(key + 1)));
            item2.InsertValue("name", TJsonValue("shared_v"));
            TJsonValue items(NJson::JSON_ARRAY);
            items.AppendValue(std::move(item1));
            items.AppendValue(std::move(item2));
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue("items", std::move(items));
            return NJson::WriteJson(&v, false);
        }

        case EJsonShape::FullLiteralMix: {
            TJsonValue arr(NJson::JSON_ARRAY);
            arr.AppendValue(TJsonValue(NJson::JSON_NULL));
            arr.AppendValue(TJsonValue(true));
            arr.AppendValue(TJsonValue(static_cast<long long>(key)));
            arr.AppendValue(TJsonValue(TString(uvk)));
            TJsonValue v(NJson::JSON_MAP);
            v.InsertValue("shared_n", TJsonValue(static_cast<long long>(key)));
            v.InsertValue("shared_s", TJsonValue(TString(uvk)));
            v.InsertValue("shared_b", TJsonValue(key % 2 == 0));
            v.InsertValue("shared_null", TJsonValue(NJson::JSON_NULL));
            v.InsertValue("shared_arr", std::move(arr));
            return NJson::WriteJson(&v, false);
        }

        default:
            Y_ABORT("Unexpected JSON shape");
    }
}

TGeneratedRow TJsonCorpus::MakeRow(ui64 key, EJsonShape shape) {
    TGeneratedRow row;
    row.Key = key;
    row.Shape = shape;
    if (shape == EJsonShape::SqlNull) {
        row.JsonText = std::nullopt;
    } else {
        row.JsonText = SerializeJson(key, shape);
    }
    return row;
}

TJsonCorpus::TJsonCorpus(TCorpusOptions opts) {
    TMersenne<ui64> rng(opts.Seed);

    std::vector<EJsonShape> shapes;
    shapes.reserve(opts.RowCount);

    static constexpr std::array<EJsonShape, kJsonCorpusNumShapes - 1> AllShapes = {
        EJsonShape::Scalar,
        EJsonShape::FlatObj,
        EJsonShape::EmptyContainers,
        EJsonShape::EmptyKey,
        EJsonShape::ArrayLiterals,
        EJsonShape::ObjWithArray,
        EJsonShape::HomogeneousArrayObjs,
        EJsonShape::HeterogeneousArrayObjs,
        EJsonShape::NestedObj,
        EJsonShape::DeepNested,
        EJsonShape::Mixed,
        EJsonShape::ArrayOfArrays,
        EJsonShape::ObjWithArrayObjs,
        EJsonShape::FullLiteralMix,
    };

    if (opts.IncludeAllShapes) {
        for (auto s : AllShapes) {
            shapes.push_back(s);
        }

        if (opts.IncludeNulls) {
            shapes.push_back(EJsonShape::SqlNull);
        }
    }

    const size_t numAllowed = opts.IncludeNulls ? kJsonCorpusNumShapes : (kJsonCorpusNumShapes - 1);

    while (shapes.size() < opts.RowCount) {
        shapes.push_back(static_cast<EJsonShape>(rng.Uniform(numAllowed)));
    }

    if (!shapes.empty()) {
        for (size_t i = shapes.size() - 1; i > 0; --i) {
            std::swap(shapes[i], shapes[rng.Uniform(i + 1)]);
        }
    }

    Rows_.reserve(opts.RowCount);
    for (size_t i = 0; i < opts.RowCount; ++i) {
        Rows_.push_back(MakeRow(static_cast<ui64>(i + 1), shapes[i]));
    }
}

void TJsonCorpus::UpsertRange(NYdb::NQuery::TQueryClient& db, std::string_view tableName,
    std::string_view jsonType, size_t offset, size_t count) const
{
    Y_ABORT_UNLESS(offset + count <= Rows_.size());
    constexpr size_t kBatchSize = 50;
    for (size_t start = offset; start < offset + count; start += kBatchSize) {
        const size_t end = std::min(start + kBatchSize, offset + count);
        UpsertBatch(db, tableName, jsonType, start, end);
    }
}

void TJsonCorpus::UpsertBatch(NYdb::NQuery::TQueryClient& db, std::string_view tableName,
    std::string_view jsonType, size_t from, size_t to) const
{
    std::string query = fmt::format("UPSERT INTO {} (Key, Text) VALUES\n", tableName);

    for (size_t i = from; i < to; ++i) {
        const auto& row = Rows_[i];

        std::string textVal;
        if (row.JsonText.has_value()) {
            std::string escaped;
            escaped.reserve(row.JsonText->size());
            for (char c : *row.JsonText) {
                if (c == '\'') {
                    escaped += "''";
                } else {
                    escaped += c;
                }
            }
            textVal = fmt::format("{}('{}')", jsonType, escaped);
        } else {
            textVal = "NULL";
        }

        query += fmt::format("  ({}, {}){}", row.Key, textVal, (i + 1 < to ? "," : ""));
    }

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace NKikimr::NKqp
