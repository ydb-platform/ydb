#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/dictionary/conversion.h>

Y_UNIT_TEST_SUITE(Dictionary) {

    using namespace NKikimr::NArrow;

    ui64 Test(NConstruction::IArrayBuilder::TPtr column, const arrow::ipc::IpcWriteOptions& options, const ui32 bSize) {
        std::shared_ptr<arrow::RecordBatch> batch = NConstruction::TRecordBatchConstructor({ column }).BuildBatch(bSize);
        const TString data = NSerialization::TFullDataSerializer(options).Serialize(batch);
        auto deserializedBatch = *NSerialization::TFullDataDeserializer().Deserialize(data);
        Y_VERIFY(!!deserializedBatch);
        auto originalBatchTransformed = DictionaryToArray(batch);
        auto roundBatchTransformed = DictionaryToArray(deserializedBatch);
        const TString roundUnpacked = NSerialization::TFullDataSerializer(options).Serialize(roundBatchTransformed);
        const TString roundTransformed = NSerialization::TFullDataSerializer(options).Serialize(originalBatchTransformed);
        Y_VERIFY(roundBatchTransformed->num_rows() == originalBatchTransformed->num_rows());
        Y_VERIFY(roundUnpacked == roundTransformed);
        return data.size();
    }

    Y_UNIT_TEST(Simple) {
        const std::vector<arrow::Compression::type> codecs = { arrow::Compression::UNCOMPRESSED, arrow::Compression::LZ4_FRAME, };
        for (auto&& codec : codecs) {
            arrow::ipc::IpcWriteOptions options = arrow::ipc::IpcWriteOptions::Defaults();
            options.codec = *arrow::util::Codec::Create(codec);
            Cerr << (options.codec ? options.codec->name() : "NO_CODEC") << Endl;
            for (auto bSize : { 100000 }) {
                Cerr << "--" << bSize << Endl;
                for (auto pSize : { 1, 16, 64, 128, 512, 1024 }) {
                    Cerr << "----" << pSize << Endl;
                    for (auto&& strLen : { 1, 10, 16, 32, 64 }) {
                        Cerr << "------" << strLen << Endl;
                        ui64 bytesDict;
                        ui64 bytesRaw;
                        {
                            NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TDictionaryArrayConstructor<NConstruction::TStringPoolFiller>>(
                                "field", NConstruction::TStringPoolFiller(pSize, strLen));
                            bytesDict = Test(column, options, bSize);
                        }
                        {
                            NConstruction::IArrayBuilder::TPtr column = std::make_shared<NConstruction::TSimpleArrayConstructor<NConstruction::TStringPoolFiller>>(
                                "field", NConstruction::TStringPoolFiller(pSize, strLen));
                            bytesRaw = Test(column, options, bSize);
                        }
                        Cerr << "--------" << bytesDict << " / " << bytesRaw << " = " << 1.0 * bytesDict / bytesRaw << Endl;
                    }
                }
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
                            const TString dataFull = NSerialization::TFullDataSerializer(options).Serialize(batch);
                            const TString dataPayload = NSerialization::TBatchPayloadSerializer(options).Serialize(batch);
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
