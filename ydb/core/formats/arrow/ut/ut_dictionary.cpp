#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/dictionary/conversion.h>
#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/batch.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>

Y_UNIT_TEST_SUITE(Dictionary) {

    using namespace NKikimr::NArrow;

    ui64 Test(NConstruction::IArrayBuilder::TPtr column, const arrow::ipc::IpcWriteOptions& options, const ui32 bSize) {
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(bSize);
        const TString data = NSerialization::TNativeSerializer(options).SerializeFull(batch);
        auto deserializedBatch = *NSerialization::TNativeSerializer().Deserialize(data);
        Y_ABORT_UNLESS(!!deserializedBatch);
        auto originalBatchTransformed = DictionaryToArray(batch);
        auto roundBatchTransformed = DictionaryToArray(deserializedBatch);
        const TString roundUnpacked = NSerialization::TNativeSerializer(options).SerializeFull(roundBatchTransformed);
        const TString roundTransformed = NSerialization::TNativeSerializer(options).SerializeFull(originalBatchTransformed);
        Y_ABORT_UNLESS(roundBatchTransformed->num_rows() == originalBatchTransformed->num_rows());
        Y_ABORT_UNLESS(roundUnpacked == roundTransformed);
        return data.size();
    }

    class TTestConfig {
    private:
        const ui32 BatchSize;
        const ui32 PoolSize;
        const ui32 StrLen;
        const arrow::Compression::type Codec;
        const bool Dict;
        arrow::ipc::IpcWriteOptions Options = arrow::ipc::IpcWriteOptions::Defaults();
        YDB_READONLY(ui64, Result, 0);
    public:
        TTestConfig(const ui32 batchSize, const ui32 poolSize, const ui32 strLen, const arrow::Compression::type codec, const bool dict)
            : BatchSize(batchSize)
            , PoolSize(poolSize)
            , StrLen(strLen)
            , Codec(codec)
            , Dict(dict)
        {
            Options.codec = *arrow::util::Codec::Create(Codec);
            NConstruction::IArrayBuilder::TPtr column;
            if (Dict) {
                column = std::make_shared<NConstruction::TDictionaryArrayConstructor<NConstruction::TStringPoolFiller>>(
                    "field", NConstruction::TStringPoolFiller(PoolSize, StrLen));
            } else {
                column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
                    "field", NConstruction::TStringPoolFiller(PoolSize, StrLen));
            }
            Result = Test(column, Options, BatchSize);
        }

        TString GetRowTitle() const {
            TStringBuilder sb;
            sb << (Options.codec ? Options.codec->name() : "NO_CODEC") << "(";
            sb << "poolsize=" << PoolSize << ";" << "keylen=" << StrLen << ")";
            return sb;
        }

        TString GetColTitle() const {
            TStringBuilder sb;
            sb << BatchSize << (Dict ? "d" : "") << ";";
            return sb;
        }
    };

    TString AlignString(const TString& base, const ui32 len) {
        if (base.size() > len) {
            return base.substr(0, len);
        } else if (base.size() < len) {
            auto result = base;
            for (ui32 i = 0; i < len - base.size(); ++i) {
                result += ' ';
            }
            return result;
        }
        return base;
    }

    Y_UNIT_TEST(Simple) {
        const std::vector<arrow::Compression::type> codecs = { arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD };
        std::vector<TTestConfig> configs;
        std::map<TString, std::map<TString, double>> testResults;
        for (auto&& codec : codecs) {
            for (auto bSize : { 10000, 100000 }) {
                for (auto pSize : { 1, 16, 64, 128, 512, 1024 }) {
                    for (auto&& strLen : { 1, 10, 16, 32, 64 }) {
                        configs.emplace_back(bSize, pSize, strLen, codec, false);
                        const auto col1 = configs.back().GetColTitle();
                        const auto val1 = configs.back().GetResult();
//                        testResults[configs.back().GetRowTitle()][configs.back().GetColTitle()] = configs.back().GetResult();
                        configs.emplace_back(bSize, pSize, strLen, codec, true);
                        const auto col2 = configs.back().GetColTitle();
                        const auto val2 = configs.back().GetResult();
//                        testResults[configs.back().GetRowTitle()][configs.back().GetColTitle()] = configs.back().GetResult();
                        testResults[configs.back().GetRowTitle()][col2 + "/" + col1] = 1.0 * val2 / val1;
                    }
                }
            }
        }
        std::map<TString, ui32> colLength;
        for (auto&& r : testResults) {
            colLength["names"] = std::max<ui32>(colLength["names"], r.first.size());
        }

        for (auto&& r : testResults) {
            for (auto&& [cName, cVal] : r.second) {
                colLength[cName] = std::max<ui32>(colLength[cName], ::ToString(cVal).size());
                colLength[cName] = std::max<ui32>(colLength[cName], cName.size());
            }
        }

        {
            TStringBuilder sb;
            sb << AlignString("", colLength["names"] + 5);
            for (auto&& c : testResults.begin()->second) {
                sb << AlignString(c.first, colLength[c.first] + 5);
            }
            Cerr << sb << Endl;
        }

        {
            for (auto&& r : testResults) {
                TStringBuilder sb;
                sb << AlignString(r.first, colLength["names"] + 5);
                for (auto&& c : r.second) {
                    sb << AlignString(::ToString(c.second), colLength[c.first] + 5);
                }
                Cerr << sb << Endl;
            }
        }
    }

    Y_UNIT_TEST(ComparePayloadAndFull) {
        const std::vector<arrow::Compression::type> codecs = { arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, };
        for (auto&& codec : codecs) {
            arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
            options.codec = *arrow::util::Codec::Create(codec);
            Cerr << (options.codec ? options.codec->name() : "NO_CODEC") << Endl;
            for (auto bSize : { 1000, 10000, 100000 }) {
                Cerr << "--" << bSize << Endl;
                for (auto pSize : { 1, 16, 64, 128, 512, 1024 }) {
                    Cerr << "----" << pSize << Endl;
                    for (auto&& strLen : { 1, 10, 16, 32, 64 }) {
                        Cerr << "------" << strLen << Endl;
                        ui64 bytesFull;
                        ui64 bytesPayload;
                        {
                            NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
                                "field", NConstruction::TStringPoolFiller(pSize, strLen)
                            );
                            std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(bSize);
                            const TString dataFull = NSerialization::TNativeSerializer(options).SerializeFull(batch);
                            const TString dataPayload = NSerialization::TNativeSerializer(options).SerializePayload(batch);
                            bytesFull = dataFull.size();
                            bytesPayload = dataPayload.size();
                        }
                        const double fraq = 1 - 1.0 * bytesPayload / bytesFull;
                        Cerr << "--------" << bytesPayload << " / " << bytesFull << " = " << 100 * fraq << "%" << Endl;
                        UNIT_ASSERT(fraq * 100 < 3);
                    }
                }
            }
        }
    }


};
