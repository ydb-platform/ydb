#include "blobstorage_blob.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    TIntrusivePtr<IContiguousChunk> AllocateArena() {
        return TRopeAlignedBuffer::Allocate(65536);
    }

    TRopeArena Arena(&AllocateArena);

    const TBlobStorageGroupType GType(TBlobStorageGroupType::ErasureNone);

    Y_UNIT_TEST_SUITE(TBlobStorageDiskBlob) {

        Y_UNIT_TEST(CreateFromDistinctParts) {
            for (bool addHeader1 : {true, false}) {
                for (bool addHeader2 : {true, false}) {
                    const ui8 totalParts = MaxTotalPartCount;
                    const ui32 partSize = 6;
                    const ui64 fullDataSize = partSize;
                    const char *data[totalParts] = {
                        "111111",
                        "222222",
                        "333333",
                        "444444",
                        "555555",
                        "666666",
                        "777777",
                    };

                    for (ui32 mask = 1; mask < (1 << totalParts); ++mask) {
                        std::array<TRope, MaxTotalPartCount> partsData;
                        ui32 numParts = 0;
                        NMatrix::TVectorType parts(0, totalParts);
                        for (ui8 i = 0; i < totalParts; ++i) {
                            if (mask >> i & 1) {
                                partsData[numParts++] = TRope(TString(data[i], partSize));
                                parts.Set(i);
                            }
                        }

                        TRope buffer = TDiskBlob::CreateFromDistinctParts(partsData.begin(), partsData.begin() + numParts, parts, fullDataSize, Arena, addHeader1);

                        TDiskBlobMerger m;
                        for (ui8 i = 0; i < totalParts; ++i) {
                            if (mask >> i & 1) {
                                const ui8 partId = i + 1;
                                TRope blobBuffer = TDiskBlob::Create(fullDataSize, partId, totalParts, TRope(TString(data[i], partSize)), Arena, addHeader2);
                                TDiskBlob blob(&blobBuffer, NMatrix::TVectorType::MakeOneHot(i, totalParts), GType, TLogoBlobID(0, 0, 0, 0, fullDataSize, 0));
                                m.Add(blob);
                            }
                        }

                        UNIT_ASSERT_EQUAL(buffer, m.CreateDiskBlob(Arena, addHeader1));
                    }
                }
            }
        }

        Y_UNIT_TEST(CreateIterate) {
            for (bool addHeader : {true, false}) {
                ui8 partId = 2;
                TString data("abcdefgh");
                TRope buf = TDiskBlob::Create(16, partId, 3, TRope(data), Arena, addHeader);
                NMatrix::TVectorType localParts(0, 3);
                localParts.Set(partId - 1);
                TDiskBlob blob(&buf, localParts, GType, TLogoBlobID(0, 0, 0, 0, data.size(), 0));
                for (TDiskBlob::TPartIterator it = blob.begin(), e = blob.end(); it != e; ++it) {
                    UNIT_ASSERT(it.GetPartId() == partId);
                    UNIT_ASSERT(it.GetPart().ConvertToString() == data);
                }
            }
        }

        Y_UNIT_TEST(Merge) {
            TString data("abcdefgh");

            for (bool addHeader1 : {true, false}) {
                for (bool addHeader2 : {true, false}) {
                    // blob1
                    ui8 partId1 = 1;
                    const TLogoBlobID id(0, 0, 0, 0, data.size(), 0);
                    TRope buf1 = TDiskBlob::Create(GType.PartSize(TLogoBlobID(id, partId1)), partId1, 8, TRope(data), Arena, addHeader1);
                    NMatrix::TVectorType localParts1(0, 3);
                    localParts1.Set(partId1 - 1);
                    TDiskBlob blob1(&buf1, localParts1, GType, id);

                    // blob2
                    ui8 partId2 = 3;
                    TRope buf2 = TDiskBlob::Create(GType.PartSize(TLogoBlobID(id, partId2)), partId2, 8, TRope(data), Arena, addHeader2);
                    NMatrix::TVectorType localParts2(0, 3);
                    localParts2.Set(partId2 - 1);
                    TDiskBlob blob2(&buf2, localParts2, GType, id);

                    // merge vars
                    TDiskBlobMerger merger;
                    TVector<ui8> ppp;
                    TVector<ui8> resPpp;
                    resPpp.push_back(1);
                    resPpp.push_back(3);
                    NMatrix::TVectorType resParts(0, 3);
                    resParts.Set(partId1 - 1);
                    resParts.Set(partId2 - 1);

                    // merge 1 (natural order)
                    {
                        UNIT_ASSERT(merger.Empty());
                        merger.Add(blob1);
                        UNIT_ASSERT(!merger.Empty());
                        merger.Add(blob2);
                        UNIT_ASSERT(!merger.Empty());
                        const TDiskBlob& blob = merger.GetDiskBlob();
                        UNIT_ASSERT(resParts == blob.GetParts());
                        for (TDiskBlob::TPartIterator it = blob.begin(), e = blob.end(); it != e; ++it) {
                            ppp.push_back(it.GetPartId());
                            UNIT_ASSERT_VALUES_EQUAL(blob.GetPartSize(it.GetPartId() - 1), data.size());
                            UNIT_ASSERT(it.GetPart().ConvertToString() == data);
                        }
                        UNIT_ASSERT(ppp == resPpp);
                    }

                    // clear
                    merger.Clear();
                    ppp.clear();

                    // merge 2 (reverse order)
                    {
                        UNIT_ASSERT(merger.Empty());
                        merger.Add(blob2);
                        UNIT_ASSERT(!merger.Empty());
                        merger.Add(blob1);
                        UNIT_ASSERT(!merger.Empty());
                        const TDiskBlob& blob = merger.GetDiskBlob();
                        UNIT_ASSERT(resParts == blob.GetParts());
                        for (TDiskBlob::TPartIterator it = blob.begin(), e = blob.end(); it != e; ++it) {
                            ppp.push_back(it.GetPartId());
                            UNIT_ASSERT(blob.GetPartSize(it.GetPartId() - 1) == data.size());
                            UNIT_ASSERT(it.GetPart().ConvertToString() == data);
                        }
                        UNIT_ASSERT(ppp == resPpp);
                    }
                }
            }
        }

        Y_UNIT_TEST(FilterMask) {
            for (bool addHeader1 : {true, false}) {
                for (bool addHeader2 : {true, false}) {
                    for (bool addHeader3 : {true, false}) {
                        const ui8 numParts = 6;
                        for (ui32 mask1 = 1; mask1 < (1 << numParts); ++mask1) {
                            for (ui32 mask2 = 1; mask2 < (1 << numParts); ++mask2) {
                                if (!(mask1 & mask2)) {
                                    continue;
                                }

                                NMatrix::TVectorType partsToStore(0, numParts);
                                for (ui8 i = 0; i < numParts; ++i) {
                                    if (mask2 >> i & 1) {
                                        partsToStore.Set(i);
                                    }
                                }

                                TDiskBlobMergerWithMask m;
                                m.SetFilterMask(partsToStore);
                                for (ui8 i = 0; i < numParts; ++i) {
                                    if (mask1 >> i & 1) {
                                        NMatrix::TVectorType v(0, numParts);
                                        v.Set(i);
                                        TRope buffer = TDiskBlob::Create(8, i + 1, numParts, TRope(Sprintf("%08x", i)), Arena, addHeader1);
                                        m.Add(TDiskBlob(&buffer, v, GType, TLogoBlobID(0, 0, 0, 0, 8, 0)));
                                    }
                                }

                                TDiskBlobMerger m2;
                                for (ui8 i = 0; i < numParts; ++i) {
                                    if ((mask1 & mask2) >> i & 1) {
                                        NMatrix::TVectorType v(0, numParts);
                                        v.Set(i);
                                        TRope buffer = TDiskBlob::Create(8, i + 1, numParts, TRope(Sprintf("%08x", i)), Arena, addHeader2);
                                        m2.Add(TDiskBlob(&buffer, v, GType, TLogoBlobID(0, 0, 0, 0, 8, 0)));
                                    }
                                }

                                UNIT_ASSERT_EQUAL(m.CreateDiskBlob(Arena, addHeader3), m2.CreateDiskBlob(Arena, addHeader3));
                            }
                        }
                    }
                }
            }
        }
    }

} // NKikimr
