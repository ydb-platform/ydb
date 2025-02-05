#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/data_extractor.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

#include <regex>

Y_UNIT_TEST_SUITE(SubColumnsArrayAccessor) {
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow;
    using namespace NKikimr;

    std::string PrepareToCompare(const std::string& str) {
        return std::regex_replace(str, std::regex(" |\\n"), "");
    }

    TString PrintBinaryJsons(const std::shared_ptr<arrow::ChunkedArray>& array) {
        TStringBuilder sb;
        sb << "[";
        for (auto&& i : array->chunks()) {
            sb << "[";
            AFL_VERIFY(i->type()->id() == arrow::binary()->id());
            auto views = std::static_pointer_cast<arrow::BinaryArray>(i);
            for (ui32 r = 0; r < views->length(); ++r) {
                if (views->IsNull(r)) {
                    sb << "null";
                } else {
                    sb << NBinaryJson::SerializeToJson(TStringBuf(views->GetView(r).data(), views->GetView(r).size()));
                }
                if (r + 1 != views->length()) {
                    sb << ",";
                }
            }
            sb << "]";
        }
        sb << "]";
        return sb;
    }

    Y_UNIT_TEST(SlicesDef) {
        NSubColumns::TSettings settings(4, 1, 0);

        const std::vector<TString> jsons = {
            R"({"a" : 1, "b" : 1, "c" : "111"})",
            "null",
            R"({"a1" : 2, "b" : 2, "c" : "222"})",
            R"({"a" : 3, "b" : 3, "c" : "333"})",
            "null",
            R"({"a" : 5, "b1" : 5})",
        };

        TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder;
        ui32 idx = 0;
        for (auto&& i : jsons) {
            if (i != "null") {
                auto v = NBinaryJson::SerializeToBinaryJson(i);
                NBinaryJson::TBinaryJson* bJson = std::get_if<NBinaryJson::TBinaryJson>(&v);
                arrBuilder.AddRecord(idx, std::string_view(bJson->data(), bJson->size()));
            }
            ++idx;
        }
        auto bJsonArr = arrBuilder.Finish(jsons.size());
        auto arrData = TSubColumnsArray::Make(bJsonArr, std::make_shared<NSubColumns::TFirstLevelSchemaData>(), settings).DetachResult();
        Cerr << arrData->DebugJson() << Endl;
        AFL_VERIFY(PrintBinaryJsons(arrData->GetChunkedArray()) == R"([[{"a":"1","b":"1","c":"111"},null,{"a1":"2","b":"2","c":"222"},{"a":"3","b":"3","c":"333"},null,{"a":"5","b1":"5"}]])")(
                "string", PrintBinaryJsons(arrData->GetChunkedArray()));
        {
            auto arrSlice = arrData->ISlice(0, 6);
            AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":"1","b":"1","c":"111"},null,{"a1":"2","b":"2","c":"222"},{"a":"3","b":"3","c":"333"},null,{"a":"5","b1":"5"}]])")(
                    "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
        }
        {
            auto arrSlice = arrData->ISlice(0, 5);
            AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":"1","b":"1","c":"111"},null,{"a1":"2","b":"2","c":"222"},{"a":"3","b":"3","c":"333"},null]])")(
                "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
        }
        {
            auto arrSlice = arrData->ISlice(0, 0);
            AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([])")("string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[],"size":[],"key_names":[],"records":[]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[],"size":[],"key_names":[],"records":[]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
        }
        {
            auto arrSlice = arrData->ISlice(0, 2);
            AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":"1","b":"1","c":"111"},null]])")(
                "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[1],"size":[12],"key_names":["c"],"records":[1]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[1,1],"size":[1,1],"key_names":["a","b"],"records":[1,1]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
        }
        {
            auto arrSlice = arrData->ISlice(0, 3);
            AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":"1","b":"1","c":"111"},null,{"a1":"2","b":"2","c":"222"}]])")(
                "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[1],"size":[19],"key_names":["c"],"records":[2]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[1,1,1],"size":[1,1,2],"key_names":["a","a1","b"],"records":[1,1,2]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
        }
        {
            auto arrSlice = arrData->ISlice(3, 3);
            AFL_VERIFY(PrintBinaryJsons(arrSlice->GetChunkedArray()) == R"([[{"a":"3","b":"3","c":"333"},null,{"a":"5","b1":"5"}]])")(
                "string", PrintBinaryJsons(arrSlice->GetChunkedArray()));
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["columns_data"]["stats"].GetStringRobust() == R"({"accessor":[1],"size":[16],"key_names":["c"],"records":[1]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
            AFL_VERIFY(arrSlice->DebugJson()["internal"]["others_data"]["stats"].GetStringRobust() == R"({"accessor":[1,1,1],"size":[2,1,1],"key_names":["a","b","b1"],"records":[2,1,1]})")
            ("string", arrSlice->DebugJson().GetStringRobust());
        }
    }
};
