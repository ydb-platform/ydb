#include "blobstorage_synclogmsgreader.h"
#include "blobstorage_synclogmsgwriter.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/codecs/pfor_codec.h>
#include <util/system/unaligned_mem.h>
#include <map>

namespace NKikimr::NSyncLog {
Y_UNIT_TEST_SUITE(SyncLogBlock82) {
    template<typename TWriter>
    void CheckCodec() {
        const TBlobStorageGroupInfo group(TErasureType::Erasure8Plus2Block, 1, 12);
        TWriter writer;
        std::map<TLogoBlobID, ui64> expected;
        for (ui32 mask = 1; mask < 1024; ++mask) {
            const TLogoBlobID id(1, 1, mask, 0, 8193, 0);
            TBlobStorageGroupInfo::TVDiskIds disks;
            group.PickSubgroup(id.Hash(), &disks, nullptr);
            TIngress ingress;
            for (ui32 p = 0; p < 10; ++p) {
                if (mask & (1u << p)) {
                    for (ui32 handoff : {10, 11}) {
                        const auto part = TIngress::CreateIngressWOLocal(&group.GetTopology(), disks[handoff],
                            TLogoBlobID(id, p + 1));
                        UNIT_ASSERT(part);
                        ingress.Merge(*part);
                        if ((mask + p + handoff) % 3 == 0) {
                            ingress.DeleteHandoff(&group.GetTopology(), disks[handoff], TLogoBlobID(id, p + 1));
                        }
                    }
                }
            }
            char buffer[MaxRecFullSize];
            const ui32 size = TSerializeRoutines::SetLogoBlob(group.Type, buffer, mask, id, ingress);
            writer.Push(reinterpret_cast<const TRecordHdr*>(buffer), size);
            expected.emplace(id, ingress.Raw());
        }
        TString encoded;
        writer.Finish(&encoded);
        TFragmentReader reader(encoded);
        TString error;
        UNIT_ASSERT_C(reader.Check(error), error);
        ui32 seen = 0;
        reader.ForEach([&](const TLogoBlobRec* record) {
            const auto it = expected.find(record->LogoBlobID());
            UNIT_ASSERT(it != expected.end());
            UNIT_ASSERT_VALUES_EQUAL(record->Ingress.Raw(), it->second);
            expected.erase(it);
            ++seen;
        }, [](const TBlockRec*) { UNIT_FAIL("unexpected block"); },
            [](const TBarrierRec*) { UNIT_FAIL("unexpected barrier"); },
            [](const TBlockRecV2*) { UNIT_FAIL("unexpected block v2"); });
        UNIT_ASSERT_VALUES_EQUAL(seen, 1023);
        UNIT_ASSERT(expected.empty());
    }

    Y_UNIT_TEST(NaiveWideIngress) { CheckCodec<TNaiveFragmentWriter>(); }
    Y_UNIT_TEST(Lz4WideIngress) { CheckCodec<TLz4FragmentWriter>(); }
    Y_UNIT_TEST(OrderedLz4WideIngress) { CheckCodec<TOrderedLz4FragmentWriter>(); }
    Y_UNIT_TEST(CustomWideIngress) { CheckCodec<TCustomCodecFragmentWriter>(); }

    Y_UNIT_TEST(CustomCodecIntegerBoundaries) {
        // Raw codec values also cover bit patterns outside a particular ingress
        // geometry, including the value+1 boundary and full 64-bit words.
        for (bool high : {false, true}) {
            const TVector<ui64> values = high
                ? TVector<ui64>{0, 1, (1ull << 56) - 1, 1ull << 56, (1ull << 57) - 1,
                    1ull << 57, (1ull << 63) - 1, 1ull << 63, Max<ui64>()}
                : TVector<ui64>{0, 1, 0x1234, 1ull << 40, (1ull << 56) - 2};
            TRecordsWithSerial records;
            for (ui32 i = 0; i < 512; ++i) {
                records.LogoBlobs.emplace_back(TLogoBlobID(1, 1, i + 1, 0, 8193, 0),
                    values[i % values.size()], i);
            }
            TReorderCodec codec(TReorderCodec::EEncoding::Custom);
            const TString encoded = codec.Encode(records);
            TVector<ui64> raw;
            for (const auto& record : records.LogoBlobs) {
                raw.push_back(record.Ingress.Raw());
            }
            const char* pos = encoded.data() + sizeof(ui32);
            const char* end = encoded.data() + encoded.size();
            // Skip the six columns preceding ingress.
            for (ui32 column = 0; column < 6; ++column) {
                UNIT_ASSERT(pos + sizeof(ui32) <= end);
                const ui32 size = ReadUnaligned<ui32>(pos);
                pos += sizeof(ui32);
                UNIT_ASSERT(pos + size <= end);
                pos += size;
            }
            UNIT_ASSERT(pos + sizeof(ui32) <= end);
            const ui32 ingressSize = ReadUnaligned<ui32>(pos);
            pos += sizeof(ui32);
            UNIT_ASSERT(pos + ingressSize <= end);
            TBuffer expected;
            const TStringBuf rawBytes(reinterpret_cast<const char*>(raw.data()), raw.size() * sizeof(ui64));
            if (high) {
                expected.Append(ui8(-1));
                expected.Append(rawBytes.data(), rawBytes.size());
            } else {
                NCodecs::TPForCodec<ui64, false> legacyCodec;
                legacyCodec.Encode(rawBytes, expected);
            }
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(pos, ingressSize), TStringBuf(expected.data(), expected.size()));
            TRecordsWithSerial decoded;
            UNIT_ASSERT(codec.Decode(encoded.data(), end, decoded));
            UNIT_ASSERT_VALUES_EQUAL(decoded.LogoBlobs.size(), 512);
            for (ui32 i = 0; i < decoded.LogoBlobs.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(decoded.LogoBlobs[i].Ingress.Raw(), values[i % values.size()]);
                UNIT_ASSERT_VALUES_EQUAL(decoded.LogoBlobs[i].LogoBlobID().Step(), i + 1);
            }
        }
    }
}
}
