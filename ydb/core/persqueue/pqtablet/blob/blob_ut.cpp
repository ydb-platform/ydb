#include "blob_int.h"
#include "blob.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(BlobTest) {
    Y_UNIT_TEST(Flags_HasPartData) {
        TMessageFlags flags;

        flags.F.HasPartData = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 1);
    }

    Y_UNIT_TEST(Flags_HasWriteTimestamp) {
        TMessageFlags flags;

        flags.F.HasWriteTimestamp = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 2);
    }

    Y_UNIT_TEST(Flags_HasCreateTimestamp) {
        TMessageFlags flags;

        flags.F.HasCreateTimestamp = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 4);
    }

    Y_UNIT_TEST(Flags_HasUncompressedSize) {
        TMessageFlags flags;

        flags.F.HasUncompressedSize = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 8);
    }

    Y_UNIT_TEST(Flags_HasKinesisData) {
        TMessageFlags flags;

        flags.F.HasKinesisData = 1;
        UNIT_ASSERT_VALUES_EQUAL(flags.V, 16);
    }
}

static const ui8 HAS_PARTDATA = 1;
static const ui8 HAS_TS = 2;
static const ui8 HAS_TS2 = 4;
static const ui8 HAS_US = 8;
static const ui8 HAS_KINESIS = 16;

TClientBlob Deserialize(const char *data, ui32 size) {
    Y_ABORT_UNLESS(size > TClientBlob::OVERHEAD);
    ui32 totalSize = ReadUnaligned<ui32>(data);
    Cerr << "Deserialize: data of size: " << size
         << ", got total size: " << totalSize << Endl;
    Y_ABORT_UNLESS(size >= totalSize);
    const char *end = data + totalSize;
    data += sizeof(ui32);
    ui64 seqNo = ReadUnaligned<ui64>(data);
    data += sizeof(ui64);
    TMaybe<TPartData> partData;
    bool hasPartData = (data[0] & HAS_PARTDATA); // data[0] is mask
    bool hasTS = (data[0] & HAS_TS);
    bool hasTS2 = (data[0] & HAS_TS2);
    bool hasUS = (data[0] & HAS_US);
    bool hasKinesisData = (data[0] & HAS_KINESIS);

    ++data;
    TString partitionKey;
    TString explicitHashKey;

    if (hasPartData) {
        ui16 partNo = ReadUnaligned<ui16>(data);
        data += sizeof(ui16);
        ui16 totalParts = ReadUnaligned<ui16>(data);
        data += sizeof(ui16);
        ui32 totalSize = ReadUnaligned<ui32>(data);
        data += sizeof(ui32);
        partData = TPartData{partNo, totalParts, totalSize};
    }

    if (hasKinesisData) {
        ui8 keySize = ReadUnaligned<ui8>(data);
        data += sizeof(ui8);
        partitionKey = TString(data, keySize == 0 ? 256 : keySize);
        data += partitionKey.size();
        keySize = ReadUnaligned<ui8>(data);
        data += sizeof(ui8);
        explicitHashKey = TString(data, keySize);
        data += explicitHashKey.size();
    }

    TInstant writeTimestamp;
    TInstant createTimestamp;
    ui32 us = 0;
    if (hasTS) {
        writeTimestamp = TInstant::MilliSeconds(ReadUnaligned<ui64>(data));
        data += sizeof(ui64);
    }
    if (hasTS2) {
        createTimestamp = TInstant::MilliSeconds(ReadUnaligned<ui64>(data));
        data += sizeof(ui64);
    }
    if (hasUS) {
        us = ReadUnaligned<ui32>(data);
        data += sizeof(ui32);
    }

    Y_ABORT_UNLESS(data < end);
    ui16 sz = ReadUnaligned<ui16>(data);
    data += sizeof(ui16);
    Y_ABORT_UNLESS(data + sz <= end);
    TString sourceId(data, sz);
    data += sz;

    TString dt = (data != end) ? TString(data, end - data) : TString{};

    return TClientBlob(std::move(sourceId), seqNo, std::move(dt),
                       std::move(partData), writeTimestamp, createTimestamp, us,
                       std::move(partitionKey), std::move(explicitHashKey));
}
Y_UNIT_TEST_SUITE(SerializationCompat) {
    TClientBlob MakeClientBlob(ui64 size, ui64 seqNo, ui64 totalParts,
                               ui16 partNo, ui64 totalSize, ui64 partKeySize,
                               ui64 explisitHashKeySize) {
        TString data(size, 'a');
        TMaybe<TPartData> partData;
        if (totalParts) {
          partData = TPartData{static_cast<ui16>(partNo),
                               static_cast<ui16>(totalParts),
                               static_cast<ui32>(totalSize)};
        }
        return TClientBlob{"someSrcId",
                           seqNo,
                           std::move(data),
                           partData,
                           TInstant::Now(),
                           TInstant::Now(),
                           size ? size * 2 : 0,
                           partKeySize ? TString(partKeySize, 'a') : TString(),
                           explisitHashKeySize
                               ? TString(explisitHashKeySize, 'a')
                               : TString()};
    }
    Y_UNIT_TEST(SerializeAndDeserialize) {
        TVector<ui64> payloadSizes = {10, 0, 100, 512_KB};
        TVector<ui64> partsCount = {0, 1, 10, 40};
        TVector<ui64> totalSizes = {1, 0, 100, 512_KB, 1_MB, 10_MB};
        TVector<ui64> partKeySizes = {0, 100, 256};
        TVector<ui64> hashSizes = {1, 0, 100, 255};
        TVector<ui16> partNos = {0, 1, 10};

        ui64 seqNo = 1;
        TBuffer buffer;
        buffer.Reserve(8_MB);

        for (ui64 payloadSize : payloadSizes) {
            for (ui64 totalSize : totalSizes) {
                if (payloadSize > totalSize) {
                    continue;
                }
                for (ui64 partsCount : partsCount) {
                    if (partsCount < 2 && payloadSize < totalSize) {
                        continue;
                    }
                    for (ui16 partNo : partNos) {
                        if (partNo >= partsCount) {
                            continue;
                        }
                        for (ui64 partKeySize : partKeySizes) {
                            for (ui64 hashSize : hashSizes) {
                                if (hashSize && !partKeySize) {
                                    continue;
                                }

                                auto blob =
                                    MakeClientBlob(payloadSize, seqNo++, partsCount, partNo,
                                                totalSize, partKeySize, hashSize);

                                Cerr << "payloadSize " << payloadSize << " totalSize "
                                    << totalSize << " partsCount " << partsCount << " partKeySize "
                                    << partKeySize << " hashSize " << hashSize
                                    << " partNo " << partNo << Endl;

                                buffer.Clear();
                                Serialize(blob, buffer);

                                TClientBlob deserialized = Deserialize(buffer.data(), buffer.size());
                  }
                }
              }
            }
          }
        }
    }
}
}
