#include "erasure_rope.h"
#include "ut_util.h"


namespace NKikimr {
namespace NErasureRope {

TRope GenerateRandomRope(size_t dataSize) {
    NPrivate::TMersenne64 randGen(Seed());
    return TRopeHelpers::RopeFromStringMemcpy(GenerateRandomString(randGen, dataSize));
}

void TestMissingPartWithRandomData(TRopeErasureType &groupType, ui32 *missingPartIdx, ui32 missingParts,
                        ui32 dataSize, bool isRestoreParts,bool isRestoreFullData,
                        TString &info, std::function<TRope(size_t)> ropeGen = GenerateRandomRope) {

    ui32 partMask = ~(ui32)0;
    for (ui32 i = 0; i < missingParts; ++i) {
        partMask &= ~(ui32)(1ul << missingPartIdx[i]);
    }

    TString mode = Sprintf(" restoreParts=%s restoreFullData=%s ",
            (isRestoreParts ? "true" : "false"),
            (isRestoreFullData ? "true" : "false"));
    VERBOSE_COUT(" dataSize# " << dataSize << Endl);

    TRope testRope = ropeGen(dataSize);
    TString testString = testRope.ConvertToString();

    // Split the data into parts
    TDataPartSet partSet;
    groupType.SplitData(TRopeErasureType::CrcModeNone, testRope, partSet);
    ui64 partSize = groupType.PartSize(TRopeErasureType::CrcModeNone, dataSize);
    for (ui32 part = 0; part < groupType.TotalPartCount(); ++part) {
        UNIT_ASSERT_EQUAL(partSize, partSet.Parts[part].size());
    }

    // Save the original parts for the future checks
    TDataPartSet originalPartSet = partSet;

    // Remove the 'missing' parts
    partSet.PartsMask &= partMask;
    for (ui32 i = 0; i < missingParts; ++i) {
        partSet.Parts[missingPartIdx[i]].clear();
    }
    // Restore the data
    TRope restoredRope;
    groupType.RestoreData(TRopeErasureType::CrcModeNone, partSet, restoredRope,
            isRestoreParts, isRestoreFullData, isRestoreParts);
    TString restoredString = restoredRope.ConvertToString();
    // Make sure the restored data matches the original
    TString errorInfo = Sprintf("dataSize=%d partMask=0x%x", dataSize, partMask);
    if (isRestoreFullData) {
        UNIT_ASSERT_EQUAL_C(testString.size(), restoredString.size(), errorInfo);
        UNIT_ASSERT_EQUAL(testString, restoredString);
    }

    if (isRestoreParts) {
        for (ui32 idx = 0; idx < missingParts; ++idx) {
            if (missingPartIdx[idx] < partSet.Parts.size()) {
                UNIT_ASSERT_EQUAL_C(partSet.Parts[missingPartIdx[idx]].size(),
                        originalPartSet.Parts[missingPartIdx[idx]].size(), info + errorInfo);
                ui32 size = (ui32)originalPartSet.Parts[missingPartIdx[idx]].size();
                TString restored = partSet.Parts[missingPartIdx[idx]].OwnedRope.ConvertToString();
                TString original = originalPartSet.Parts[missingPartIdx[idx]].OwnedRope.ConvertToString();
                for (ui32 i = 0; i < size; ++i) {
                    UNIT_ASSERT_EQUAL_C(restored[i], original[i],
                            (info + errorInfo + mode + Sprintf(" (part %d byte %d)", missingPartIdx[idx], i)));
                }
            }
        }
    }
}

template <ui32 maxMissingParts>
void TestAllLossesDifferentSizes(TRopeErasureType &groupType, ui32 maxParts,
                        std::function<TRope(size_t)> ropeGen = GenerateRandomRope) {
    for (ui32 missingParts = 0; missingParts <= maxMissingParts; ++missingParts) {
        ui32 missingPartIdx[maxMissingParts];
        GenFirstCombination(&missingPartIdx[0], missingParts);
        ui32 maxMissingVariants = Fact(maxParts)/Fact(missingParts)/Fact(maxParts-missingParts);
        //printf("k=%u, n=%u,  variants=%u\n", missingParts, maxParts, maxMissingVariants);
        for (ui32 missingVariant = 0; missingVariant < maxMissingVariants; ++missingVariant) {
            VERBOSE_COUT(PrintArr(missingPartIdx, missingParts));
            for (ui32 dataSize = 1; dataSize < 600; ++dataSize) {
                VERBOSE_COUT("dataSize# " << dataSize << Endl);
                for (ui32 type = 0; type < 3; ++type) {
                    bool isRestoreParts = false;
                    bool isRestoreFullData = false;
                    switch (type) {
                        case 0:
                            isRestoreParts = true;
                            isRestoreFullData = true;
                            break;
                        case 1:
                            isRestoreFullData = true;
                            break;
                        case 2:
                            isRestoreParts = true;
                            break;
                    }
                    TStringStream info;
                    info << "Type# " << groupType.ToString() << " ";
                    info << "maxMissingParts# " << maxMissingParts << " ";
                    info << "missingVariant# " << missingVariant << " ";
                    info << "dataSize# " << dataSize << " ";
                    info << "case# " << BoolToStr(isRestoreParts) << "," << BoolToStr(isRestoreFullData) << " ";
                    VERBOSE_COUT(info.Str() << Endl);
                    TestMissingPartWithRandomData(groupType, missingPartIdx, missingParts, dataSize,
                            isRestoreParts, isRestoreFullData, info.Str(), ropeGen);
                }
            } // dataSize
            GenNextCombination(&missingPartIdx[0], missingParts, maxParts);
        }
    } // missingVariant
}

Y_UNIT_TEST_SUITE(TErasureTypeTest) {
// Test if new version is capable to restore data splited by current version (which is right by definition)
    Y_UNIT_TEST(isSplittedDataEqualsToOldVerion) {
        TVector<TVector<ui8>> dataPool {
                        {49,184,130,19,181,231,130},

                        {249,122,57,146,140,30,69,51,88,81,92,29,220,192,18,14,195,162,244,139,59,141,161,14,
                        202,194,28,123,179,195,60,101,56,157,176,150,23,105,123,62,101,19,56,168,222,81,172,
                        251,199,223,85,60,99,184,45,90,84,68,1,131,199,36,64,103,150,221,18,236,86,15,142},

                        {46,173,157,247,36,205,150,116,82,10,212,7,45,29,93,90,49,233,170,207,198,219,215,
                        187,220,220,48,228,83,53,50,37,153,214,149,28,231,171,92,176,230,139,168,126,
                        138,227,106,92,38,23,87,62,20,192,151,15,170,34,248,199,220,250,108,47,54,217,36,
                        56,146,224,21,148,133,155,49,199,101,250,173,93,104,205,67,222,132,104,187,231,53,
                        206,247,46,22,73,11,70,87,124,4,242,9,165,99,82,83,40,165,55,53,187,238,96,248,16,
                        103,197,132,216,107,191,229,140,90,129,81,63,232,85,19,232,59,96,193,5,133,139,251,
                        148,144,0,147,22,247,36,221,244,117,144,98,173,40} };
        TVector<TVector<TVector<ui8>>> partsPool {
            {
                {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {49,184,130,19,181,231,130,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {49,184,130,19,181,231,130,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,49,184,130,19,181,231,130,0, },
            },{
                {249,122,57,146,140,30,69,51,88,81,92,29,220,192,18,14,195,162,244,139,59,141,161,14,202,
                                                                                   194,28,123,179,195,60,101,},
                {56,157,176,150,23,105,123,62,101,19,56,168,222,81,172,251,199,223,85,60,99,184,45,90,84,
                                                                                   68,1,131,199,36,64,103,},
                {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},

                {150,221,18,236,86,15,142,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {87,58,155,232,205,120,176,13,61,66,100,181,2,145,190,245,4,125,161,183,88,53,140,84,
                                                                           158,134,29,248,116,231,124,2,},
                {173,62,56,17,75,58,5,84,52,136,237,8,12,141,41,87,242,245,205,160,34,248,77,146,207,
                                                                                     132,90,40,65,80,223,88,},
            },{
                {46,173,157,247,36,205,150,116,82,10,212,7,45,29,93,90,49,233,170,207,198,219,215,187,220,220,
                    48,228,83,53,50,37,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {153,214,149,28,231,171,92,176,230,139,168,126,138,227,106,92,38,23,87,62,20,192,151,15,170,
                    34,248,199,220,250,108,47,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {54,217,36,56,146,224,21,148,133,155,49,199,101,250,173,93,104,205,67,222,132,104,187,231,53,
                    206,247,46,22,73,11,70,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,},
                {87,124,4,242,9,165,99,82,83,40,165,55,53,187,238,96,248,16,103,197,132,216,107,191,229,140,
                    90,129,81,63,232,85,19,232,59,96,193,5,133,139,251,148,144,0,147,22,247,36,221,244,117,144,
                    98,173,40,0,0,0,0,0,0,0,0,0,},
                {214,222,40,33,88,35,188,2,98,50,232,137,247,191,116,59,135,35,217,234,210,171,144,236,166,
                    188,101,140,200,185,189,25,19,232,59,96,193,5,133,139,251,148,144,0,147,22,247,36,221,
                    244,117,144,98,173,40,0,0,0,0,0,0,0,0,0,},
                {114,180,19,50,219,117,207,37,191,151,5,180,246,160,208,23,112,124,56,167,179,241,145,219,
                    185,235,76,193,70,131,82,141,38,96,229,144,241,187,223,36,251,148,144,0,147,22,247,36,
                    251,148,144,0,147,22,247,36,232,124,171,96,82,19,114,175,},
            }
        };
        TRopeErasureType type(TRopeErasureType::EErasureSpecies::Erasure4Plus2Block);
        for (ui32 variant = 0; variant < dataPool.size(); ++variant) {
            TVector<ui8> &data = dataPool[variant];
            TVector<TVector<ui8>> &expectedParts = partsPool[variant];
            TString testString;
            testString.resize(data.size());
            for (ui32 i = 0; i < testString.size(); ++i) {
                testString[i] = (char)data[i];
            }
            TDataPartSet partSet;
            type.SplitData(TRopeErasureType::CrcModeNone, TRopeHelpers::RopeFromStringMemcpy(testString), partSet);
            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_EQUAL_C(partSet.Parts[i].size(), expectedParts[i].size(), Sprintf("%lu == %lu",
                                                                partSet.Parts[i].size(), expectedParts[i].size()));
                for (ui32 j = 0; j < partSet.Parts[i].size(); ++j) {
                    UNIT_ASSERT_EQUAL( (ui8)partSet.Parts[i].FastViewer.At8(j), expectedParts[i][j]);
                }
            }
        }
    }

    Y_UNIT_TEST(SpecialTest) {

    }

    Y_UNIT_TEST(TestTRopeErasureType) {
        ui64 dataSize = 1024;
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::ErasureMirror3);

        // for full coverage
        // TRopeErasureType::ErasureSpeciesName(TRopeErasureType::ErasureSpeciesCount);

        UNIT_ASSERT_EQUAL(dataSize, groupType.PartUserSize(dataSize));
    }

    Y_UNIT_TEST(TestEo) {
        ui32 species = (ui32)TRopeErasureType::Erasure4Plus2Block;
        {
            TRopeErasureType groupType((TRopeErasureType::EErasureSpecies)species);

            ui32 startingDataSize = 248;

            ui32 dataSize = startingDataSize;
            {
                const ui32 maxMissingParts = 4;
                ui32 missingPartIdx[maxMissingParts];
                for (ui32 i = 0; i < maxMissingParts; ++i) {
                    missingPartIdx[i] = groupType.TotalPartCount();
                }
                missingPartIdx[0] = 2;
                missingPartIdx[1] = 3;

                ui32 maxMissingPartsTolerable = groupType.TotalPartCount() - groupType.MinimalRestorablePartCount();
                {
                    ui32 partMask = ~(ui32)0;
                    for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                        partMask &= ~(ui32)(1 << missingPartIdx[idx]);
                    }
                    char mask[33];
                    for (ui32 idx = 0; idx < 32; ++idx) {
                        mask[idx] = (partMask & ((1ul << 31) >> idx)) ? '1' : '0';
                    }
                    mask[32] = 0;

                    TString errorInfo = Sprintf("species=%d (%s) dataSize=%d partMask=0x%x (%s)", species,
                            TRopeErasureType::ErasureSpeciesName(species).c_str(), dataSize, partMask, mask);

                    TString testString;
                    testString.resize(dataSize);
                    for (ui32 i = 0; i < testString.size(); ++i) {
                        ui32 col = (i / 8) % 4;
                        ui32 row = (i / (2 * 8 * 4)) % 4;
                        ui8 val = ui8(1 << col) | ui8(1 << (row + 4));
                        ((char*)testString.data())[i] = val;
                    }
                    TDataPartSet partSet;
                    try {
                        groupType.SplitData(TRopeErasureType::CrcModeNone, TRopeHelpers::RopeFromStringMemcpy(testString), partSet);
                    } catch (yexception ex) {
                        ex << " [in SplitData while testing " << errorInfo << "]";
                        throw ex;
                    }

                    ui64 partSize = groupType.PartSize(TRopeErasureType::CrcModeNone, dataSize);
                    for (ui32 part = 0; part < groupType.TotalPartCount(); ++part) {
                        UNIT_ASSERT_EQUAL_C(partSize, partSet.Parts[part].size(), errorInfo);
                    }

                    TDataPartSet originalPartSet = partSet;

                    // Restore full data
                    for (int type = 0; type < 1; ++type) {
                        bool isRestoreFullData = false;
                        bool isRestoreParts = false;
                        switch (type) {
                            case 0:
                                isRestoreFullData = true;
                                break;
                            case 1:
                                isRestoreParts = true;
                                break;
                            case 2:
                                isRestoreFullData = true;
                                isRestoreParts = true;
                                break;
                            default:
                                Y_ABORT();
                        }

                        partSet = originalPartSet;
                        for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                            if (missingPartIdx[idx] < partSet.Parts.size()) {
                                partSet.PartsMask &= partMask;
                                partSet.Parts[missingPartIdx[idx]].clear();
                            }
                        }

                        TString mode = Sprintf(" restoreParts=%s restoreFullData=%s ",
                            (isRestoreParts ? "true" : "false"),
                            (isRestoreFullData ? "true" : "false"));

                        TRope restoredRope;
                        try {
                            groupType.RestoreData(TRopeErasureType::CrcModeNone, partSet, restoredRope,
                                    isRestoreParts, isRestoreFullData, isRestoreParts);
                        } catch (yexception ex) {
                            ex << " [in RestoreData while testing " << errorInfo << mode << "]";
                            throw ex;
                        }
                        TString restoredString = restoredRope.ConvertToString();

                        VERBOSE_COUT("testing " << errorInfo << mode << " (full data)" << Endl);
                        if (isRestoreFullData) {
                            UNIT_ASSERT_EQUAL_C(testString.size(), restoredString.size(), errorInfo);
                            for (ui32 i = 0; i < testString.size(); ++i) {
                                UNIT_ASSERT_EQUAL_C(((char*)testString.data())[i], ((char*)restoredString.data())[i],
                                    (errorInfo + mode + " (full data)"));
                                if (((char*)testString.data())[i] != ((char*)restoredString.data())[i]) {
                                    VERBOSE_COUT("mismatch " << errorInfo << mode << " (full data)" << Endl);
                                    break;
                                }
                            }
                        }
                        if (isRestoreParts) {
                            for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                                if (missingPartIdx[idx] < partSet.Parts.size()) {
                                    UNIT_ASSERT_EQUAL_C(partSet.Parts[missingPartIdx[idx]].size(),
                                        originalPartSet.Parts[missingPartIdx[idx]].size(), errorInfo);
                                    ui32 size = (ui32)originalPartSet.Parts[missingPartIdx[idx]].size();
                                    TString restored = partSet.Parts[missingPartIdx[idx]].OwnedRope.ConvertToString();
                                    TString original = originalPartSet.Parts[missingPartIdx[idx]].OwnedRope.ConvertToString();
                                    for (ui32 i = 0; i < size; ++i) {
                                        UNIT_ASSERT_EQUAL_C(restored[i], original[i],
                                            (errorInfo + mode + Sprintf(" (part %d byte %d)", missingPartIdx[idx], i)));
                                        if (restored[i] != original[i]) {
                                           VERBOSE_COUT(" wrong part " << errorInfo << mode <<
                                                   Sprintf(" (part %d byte %d)", missingPartIdx[idx], i) << Endl);
                                           break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Mirror tests
    Y_UNIT_TEST(TestMirror3LossOfAllPossible3) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::ErasureMirror3);
        constexpr ui32 maxMissingParts = 2;
        constexpr ui32 maxParts = 1 + 2;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

    // Block tests
    Y_UNIT_TEST(TestBlock31LossOfAllPossible1) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure3Plus1Block);
        constexpr ui32 maxMissingParts = 1;
        constexpr ui32 maxParts = 3 + 1;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

    Y_UNIT_TEST(TestBlock42LossOfAllPossible2) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure4Plus2Block);
        // Specify
        constexpr ui32 maxMissingParts = 2;
        constexpr ui32 maxParts = 4 + 2;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

//    Y_UNIT_TEST(TestBlock42Not8AlignedSizes) {
//        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure4Plus2Block);
//        constexpr ui32 maxMissingParts = 2;
//        constexpr ui32 maxParts = 4 + 2;
//
//        {
//            auto ropeGen = [](size_t dataSize) {
//                TRope rope;
//                for (size_t i = 0; i < dataSize; ++i) {
//                    TString base = GenerateRandomString(1);
//                    rope.Insert(rope.End(), TRopeHelpers::RopeFromStringReference(base));
//                }
//                Y_ABORT_UNLESS(rope.GetSize() == dataSize);
//                return rope;
//            };
//            TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts, ropeGen);
//        }
//
//        {
//            auto ropeGen = [](size_t dataSize) {
//                TRope rope;
//                for (size_t i = 1; i < dataSize && rope.GetSize() + i <= dataSize; ++i) {
//                    TString base = GenerateRandomString(i);
//                    rope.Insert(rope.End(), TRopeHelpers::RopeFromStringReference(base));
//                }
//                if (rope.GetSize() < dataSize) {
//                    TString base = GenerateRandomString(dataSize - rope.GetSize());
//                    rope.Insert(rope.End(), TRopeHelpers::RopeFromStringReference(base));
//                }
//                Y_ABORT_UNLESS(rope.GetSize() == dataSize);
//                return rope;
//            };
//            TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts, ropeGen);
//        }
//
//        {
//            NPrivate::TMersenne64 randGen(Seed());
//            auto ropeGen = [&randGen](size_t dataSize) {
//                ui32 maxSize = (ui32) std::sqrt(dataSize) + 1;
//                maxSize = std::min(dataSize, (size_t) maxSize);
//                TRope rope;
//                while (rope.GetSize() < dataSize) {
//                    ui64 size = (ui64) randGen.GenRand() % maxSize;
//                    if (rope.GetSize() + size <= dataSize) {
//                        TString base = GenerateRandomString(size);
//                        rope.Insert(rope.End(), TRopeHelpers::RopeFromStringReference(base));
//                    } else {
//                        TString base = GenerateRandomString(dataSize - rope.GetSize());
//                        rope.Insert(rope.End(), TRopeHelpers::RopeFromStringReference(base));
//                    }
//                }
//                Y_ABORT_UNLESS(rope.GetSize() == dataSize);
//                return rope;
//            };
//            TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts, ropeGen);
//        }
//    }

    Y_UNIT_TEST(TestBlock32LossOfAllPossible2) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure3Plus2Block);
        constexpr ui32 maxMissingParts = 2;
        constexpr ui32 maxParts = 3 + 2;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

    Y_UNIT_TEST(TestBlock43LossOfAllPossible3) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure4Plus3Block);
        constexpr ui32 maxMissingParts = 3;
        constexpr ui32 maxParts = 4 + 3;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

    Y_UNIT_TEST(TestBlock33LossOfAllPossible3) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure3Plus3Block);
        constexpr ui32 maxMissingParts = 3;
        constexpr ui32 maxParts = 3 + 3;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

    Y_UNIT_TEST(TestBlock23LossOfAllPossible3) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure2Plus3Block);
        constexpr ui32 maxMissingParts = 3;
        constexpr ui32 maxParts = 2 + 3;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }

    Y_UNIT_TEST(TestBlock22LossOfAllPossible2) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::EErasureSpecies::Erasure2Plus2Block);
        constexpr ui32 maxMissingParts = 2;
        constexpr ui32 maxParts = 2 + 2;
        TestAllLossesDifferentSizes<maxMissingParts>(groupType, maxParts);
    }


    void TestErasureMain(ui32 dataSize, TRopeErasureType::ECrcMode crcMode, ui32 species, const TRope& inp = TRope()) {
        TRopeErasureType groupType((TRopeErasureType::EErasureSpecies)species);
        TString erasureName = TRopeErasureType::ErasureName[species];

        const ui32 maxMissingParts = 4;
        ui32 missingPartIdx[maxMissingParts];
        for (ui32 i = 0; i < maxMissingParts; ++i) {
            missingPartIdx[i] = groupType.TotalPartCount();
        }

        ui32 maxMissingPartsTolerable = groupType.TotalPartCount() - groupType.MinimalRestorablePartCount();
        bool isComplete = false;
        while (!isComplete) {
            ui32 partMask = ~(ui32)0;
            for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                partMask &= ~(ui32)(1 << missingPartIdx[idx]);
            }
            char mask[33];
            for (ui32 idx = 0; idx < 32; ++idx) {
                mask[idx] = (partMask & ((1ul << 31) >> idx)) ? '1' : '0';
            }
            mask[32] = 0;

            TString errorInfo = Sprintf("crcMode=%d species=%d (%s) dataSize=%d partMask=0x%x (%s)",
                                        (i32)crcMode, species, TRopeErasureType::ErasureSpeciesName(species).c_str(),
                                        dataSize, partMask, mask);

            TString testString;


            TRope inputBuffer;
            if (inp.GetSize() == dataSize) {
                inputBuffer = inp;
                testString = inp.ConvertToString();
            } else {
                testString.resize(dataSize);
                for (ui32 i = 0; i < testString.size(); ++i) {
                    ((char*)testString.data())[i] = (char)(i % 10) + '0';
                }

                inputBuffer = TRopeHelpers::RopeFromStringMemcpy(testString);
            }

            TDataPartSet partSet;
            try {
                VERBOSE_COUT("SplitData " << errorInfo << Endl);
                groupType.SplitData(crcMode, inputBuffer, partSet);
            } catch (yexception ex) {
                ex << " [in SplitData while testing " << errorInfo << "]";
                throw ex;
            }

            ui64 partSize = groupType.PartSize(crcMode, dataSize);
            for (ui32 part = 0; part < groupType.TotalPartCount(); ++part) {
                UNIT_ASSERT_EQUAL_C(partSize, partSet.Parts[part].size(), errorInfo);
                UNIT_ASSERT(CheckCrcAtTheEnd(crcMode, partSet.Parts[part].OwnedRope));
            }

            TDataPartSet originalPartSet = partSet;

            // Restore full data
            for (int type = 0; type < 5; ++type) {
                bool isRestoreFullData = false;
                bool isRestoreParts = false;
                bool isRestoreParityParts = false;
                switch (type) {
                    case 0:
                        isRestoreFullData = true;
                        break;
                    case 1:
                        isRestoreParts = true;
                        break;
                    case 2:
                        isRestoreFullData = true;
                        isRestoreParts = true;
                        break;
                    case 3:
                        isRestoreParts = true;
                        isRestoreParityParts = true;
                        break;
                    case 4:
                        isRestoreFullData = true;
                        isRestoreParts = true;
                        isRestoreParityParts = true;
                        break;
                    default:
                        Y_ABORT();
                }
                partSet = originalPartSet;
                partSet.ResetWithFullCopy();
                for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                    if (missingPartIdx[idx] < partSet.Parts.size()) {
                        partSet.PartsMask &= partMask;
                        partSet.Parts[missingPartIdx[idx]].clear();
                    }
                }
                partSet.FullDataFragment.UninitializedOwnedWhole(dataSize);


                TString mode = Sprintf(" restoreParts=%s isRestoreParityParts=%s restoreFullData=%s ",
                                       (isRestoreParts ? "true" : "false"),
                                       (isRestoreParityParts ? "true" : "false"),
                                       (isRestoreFullData ? "true" : "false"));

                VERBOSE_COUT("RestoreData " << errorInfo << Endl);
                TRope restoredRope;
                try {
                    groupType.RestoreData(crcMode, partSet, restoredRope,
                                          isRestoreParts, isRestoreFullData, isRestoreParityParts);
                } catch (yexception ex) {
                    ex << " [in RestoreData while testing " << errorInfo << mode << "]";
                    throw ex;
                }
                for (ui32 part = 0; part < groupType.TotalPartCount(); ++part) {
                    if (part < groupType.DataParts() && isRestoreParts) {
                        Y_ABORT_UNLESS(CheckCrcAtTheEnd(crcMode, partSet.Parts[part].OwnedRope));
                    } else if (part >= groupType.DataParts() && isRestoreParityParts) {
                        Y_ABORT_UNLESS(CheckCrcAtTheEnd(crcMode, partSet.Parts[part].OwnedRope));
                    }
                }
                TString restoredString = restoredRope.ConvertToString();
                VERBOSE_COUT("testing " << errorInfo << mode << " (full data)" << Endl);
                if (isRestoreFullData) {
                    UNIT_ASSERT_EQUAL_C(testString.size(), restoredString.size(), errorInfo);
                    for (ui32 i = 0; i < testString.size(); ++i) {
                        UNIT_ASSERT_EQUAL_C(((char*)testString.data())[i], ((char*)restoredString.data())[i],
                                            (errorInfo + erasureName + mode + " (full data)"));
                    }
                }
                if (isRestoreParts) {
                    for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                        ui32 missingIdx = missingPartIdx[idx];
                        if (missingIdx < partSet.Parts.size() &&
                            (isRestoreParityParts || missingIdx < groupType.DataParts())) {
                            UNIT_ASSERT_EQUAL_C(partSet.Parts[missingIdx].size(),
                                                originalPartSet.Parts[missingIdx].size(), errorInfo);
                            ui32 size = (ui32)originalPartSet.Parts[missingIdx].size();
                            if (size) {
                                TString restored = partSet.Parts[missingIdx].OwnedRope.ConvertToString();
                                TString original = originalPartSet.Parts[missingIdx].OwnedRope.ConvertToString();
                                for (ui32 i = 0; i < size; ++i) {
                                    UNIT_ASSERT_EQUAL_C(restored[i], original[i],
                                                        (errorInfo + erasureName + mode +
                                                         Sprintf(" (part idx# %d of %d byte i# %d size# %d restored# %d original# %d)",
                                                                 missingIdx, (ui32)groupType.TotalPartCount(), i, size, (ui32)restored[i], (ui32)original[i])));
                                }
                            } else {
                                UNIT_ASSERT(partSet.Parts[missingIdx].size() == 0);
                                UNIT_ASSERT(originalPartSet.Parts[missingIdx].size() == 0);
                            }
                        }
                    }
                }
            }

            if (maxMissingPartsTolerable == 0) {
                isComplete = true;
            }
            for (ui32 idx = maxMissingPartsTolerable - 1; idx != (ui32)-1; --idx) {
                missingPartIdx[idx]--;
                if (missingPartIdx[idx] != (ui32)-1) {
                    break;
                }
                if (idx == 0) {
                    isComplete = true;
                }
                missingPartIdx[idx] = groupType.TotalPartCount() - 1;
            }
        } // while !isComplete
    }

    void TestErasure(TRopeErasureType::ECrcMode crcMode, ui32 species, ui32 dataSizeOffset = 0, ui32 dataSizeStep = 1) {
        ui32 startingDataSize = 0;
        TRopeErasureType groupType((TRopeErasureType::EErasureSpecies)species);
        ui32 maxDataSize = groupType.MinimalBlockSize() * 8;

        for (ui32 dataSize = startingDataSize + dataSizeOffset; dataSize < maxDataSize; dataSize += dataSizeStep) {
            TestErasureMain(dataSize, crcMode, species);
        }
    }

    void TestErasure(TRopeErasureType::ECrcMode crcMode, ui32 species, const TVector<ui32>& sizes) {
        for (ui32 dataSize : sizes) {
            TestErasureMain(dataSize, crcMode, species);
        }
    }

    ui32 SpeciesForTest[] {
        TRopeErasureType::Erasure4Plus2Block,  // 1
        TRopeErasureType::Erasure2Plus2Block,  // 2
        TRopeErasureType::Erasure3Plus2Block,  // 3
        TRopeErasureType::Erasure4Plus3Block,  // 4
        TRopeErasureType::Erasure3Plus1Block,  // 1
        TRopeErasureType::Erasure2Plus3Block,  // 2
        TRopeErasureType::Erasure3Plus3Block,  // 3
        TRopeErasureType::ErasureNone,         // 4
        TRopeErasureType::ErasureMirror3,      // 1
        TRopeErasureType::ErasureMirror3Plus2, // 2
        TRopeErasureType::ErasureMirror3dc     // 3
    };

    ui32 NumberOfTestSpecies = sizeof(SpeciesForTest) / sizeof(ui32);

    Y_UNIT_TEST(TestAllSpeciesCrcWhole1of4) {
        for (ui32 species = 0; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species]);
        }
    }

    Y_UNIT_TEST(TestAllSpeciesCrcWhole2of4) {
        for (ui32 species = 1; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species]);
        }
    }

    Y_UNIT_TEST(TestAllSpeciesCrcWhole3of4) {
        for (ui32 species = 2; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species]);
        }
    }

    Y_UNIT_TEST(TestAllSpeciesCrcWhole4of4_p1of4) {
        for (ui32 species = 3; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species], 0, 4);
        }
    }

    Y_UNIT_TEST(TestAllSpeciesCrcWhole4of4_p2of4) {
        for (ui32 species = 3; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species], 1, 4);
        }
    }

    Y_UNIT_TEST(TestAllSpeciesCrcWhole4of4_p3of4) {
        for (ui32 species = 3; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species], 2, 4);
        }
    }

    Y_UNIT_TEST(TestAllSpeciesCrcWhole4of4_p4of4) {
        for (ui32 species = 3; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeWholePart, SpeciesForTest[species], 3, 4);
        }
    }

    Y_UNIT_TEST(TestBigSizes) {
        TVector<ui32> sizes{1234567};
        for (ui32 species = 0; species < NumberOfTestSpecies; ++species) {
            TestErasure(TRopeErasureType::CrcModeNone, SpeciesForTest[species], sizes);
        }
    }

    Y_UNIT_TEST(TestAllSpecies1of2) {
        for (ui32 species = 0; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeNone, SpeciesForTest[species]);
        }
    }

    Y_UNIT_TEST(TestAllSpecies2of4) {
        for (ui32 species = 1; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeNone, SpeciesForTest[species]);
        }
    }

    Y_UNIT_TEST(TestAllSpecies3of4) {
        for (ui32 species = 2; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeNone, SpeciesForTest[species]);
        }
    }

    Y_UNIT_TEST(TestAllSpecies4of4) {
        for (ui32 species = 3; species < NumberOfTestSpecies; species += 4) {
            TestErasure(TRopeErasureType::CrcModeNone, SpeciesForTest[species]);
        }
    }



    Y_UNIT_TEST(TestBlockByteOrder) {
        ui32 species = (ui32)TRopeErasureType::Erasure4Plus2Block;
        TRopeErasureType groupType((TRopeErasureType::EErasureSpecies)species);
        TString erasureName = TRopeErasureType::ErasureName[species];

        for (ui32 dataSize = 0; dataSize <= 256; ++dataSize) {
            TString testString;
            testString.resize(dataSize);
            for (ui32 i = 0; i < testString.size(); ++i) {
                ((ui8*)testString.data())[i] = (ui8)i;
            }
            TDataPartSet partSet;
            groupType.SplitData(TRopeErasureType::CrcModeNone, TRopeHelpers::RopeFromStringMemcpy(testString), partSet);
            for (ui32 p = 0; p < groupType.DataParts(); ++p) {
                auto &part = partSet.Parts[p];
                VERBOSE_COUT("Part# " << p << " Size# " << part.size() << " Data# ");
                if (part.size() == 0) {
                    VERBOSE_COUT(" --- ");
                } else {
                    ui32 begin = (ui32)*(ui8*)part.GetDataAt(0);
                    ui32 prev = (ui32)*(ui8*)part.GetDataAt(0);
                    for (ui32 i = 1; i < part.size(); ++i) {
                        ui32 cur = (ui32)*(ui8*)part.GetDataAt(i);
                        if (cur == prev + 1) {
                            prev = cur;
                        } else {
                            if (begin == prev) {
                                VERBOSE_COUT(begin << " ");
                            } else {
                                VERBOSE_COUT(begin << ".." << prev << " ");
                            }
                            begin = cur;
                            prev = cur;
                        }
                    }
                    if (begin == prev) {
                        VERBOSE_COUT(begin << " ");
                    } else {
                        VERBOSE_COUT(begin << ".." << prev << " ");
                    }
                }
                VERBOSE_COUT("  ");
            }
            VERBOSE_COUT(Endl);
        }
    }


    void TestBlock42PartialRestore(ui32 missingVariant) {
        // Set up the erasure
        TRopeErasureType groupType(TRopeErasureType::Erasure4Plus2Block);

        // Specify the missing part indexes
        const ui32 maxMissingParts = 2;
        ui32 missingPartsToTest[] = {0, 1, 2, 4, 3, 5, 4, 5};
        ui32 missingPartIdx[maxMissingParts];
        missingPartIdx[0] = missingPartsToTest[missingVariant * 2];
        missingPartIdx[1] = missingPartsToTest[missingVariant * 2 + 1];
        ui32 partMask = ~(ui32) 0;
        partMask &= ~(ui32) (1 << missingPartIdx[0]);
        partMask &= ~(ui32) (1 << missingPartIdx[1]);

        // Prepare the test data
        TString testString;
        for (ui32 dataSize = 1; dataSize < 600; ++dataSize) {
            if (dataSize > 128) {
                dataSize += 6;
            }
            VERBOSE_COUT( "variant# " << missingVariant << " dataSize# " << dataSize << Endl);
            testString.resize(dataSize);
            for (ui32 i = 0; i < testString.size(); ++i) {
                ((char *) testString.data())[i] = (char) (i % 10) + '0';
            }

            // Split the data into parts
            TDataPartSet partSet;
            groupType.SplitData(TRopeErasureType::CrcModeNone, TRopeHelpers::RopeFromStringMemcpy(testString), partSet);
            ui64 partSize = groupType.PartSize(TRopeErasureType::CrcModeNone, dataSize);
            for (ui32 part = 0; part < groupType.TotalPartCount(); ++part) {
                UNIT_ASSERT_EQUAL(partSize, partSet.Parts[part].size());
            }

            // Save the original parts for the future checks
            TDataPartSet originalPartSet = partSet;
            originalPartSet.ResetWithFullCopy();


            // TODO: Test different offsets and sizes
            for (ui64 partialSize = 1; partialSize < Min((ui32) dataSize, (ui32) 512); ++partialSize) {
                VERBOSE_COUT( "partialSize# " << partialSize << Endl);
                ui64 partialShiftSpecials[] = {95, 96, 97, 127, 128, 129, 159, 160, 161, 191, 192, 193,
                                                223, 224, 225, 254, 255, 256, 257,
                                                510, 511, 512, 513, 514, 333, 364, 173,
                                                dataSize - 4, dataSize - 3, dataSize - 2, dataSize - 1, dataSize};
                ui64 specialCount = sizeof(partialShiftSpecials) / sizeof(partialShiftSpecials[0]);
                ui64 normalCount = 70;
                ui64 totalCount = normalCount + specialCount;

                for (ui64 caseIdx = 0; caseIdx < totalCount; ++caseIdx) {
                    ui64 partialShift = caseIdx < normalCount ?
                        caseIdx : partialShiftSpecials[caseIdx - normalCount];
                    if (partialShift + partialSize >= dataSize) {
                        continue;
                    }
                    VERBOSE_COUT( "partialShift# " << partialShift << Endl);
                    partSet = originalPartSet;

                    ui64 shift = Max<ui64>();
                    ui64 size = Max<ui64>();

                    ui64 needBegin = Max<ui64>();
                    ui64 needEnd = Max<ui64>();

                    TBlockSplitRange range1;
                    groupType.BlockSplitRange(TRopeErasureType::CrcModeNone, dataSize, partialShift,
                            partialShift + partialSize, &range1);
                    for (ui32 partIdx = range1.BeginPartIdx; partIdx < range1.EndPartIdx; ++partIdx) {
                        TPartOffsetRange &r = range1.PartRanges[partIdx];
                        if (shift == Max<ui64>() || shift > r.AlignedWholeBegin) {
                            shift = r.AlignedWholeBegin;
                        }
                        if (size == Max<ui64>() || size < r.AlignedWholeBegin + r.AlignedEnd - r.AlignedBegin) {
                            size = r.AlignedWholeBegin + r.AlignedEnd - r.AlignedBegin;
                        }
                        if (needBegin == Max<ui64>() || needBegin > r.AlignedBegin) {
                            needBegin = r.AlignedBegin;
                        }
                        if (needEnd == Max<ui64>() || needEnd < r.AlignedEnd) {
                            needEnd = r.AlignedEnd;
                        }
                    }
                    if (size > dataSize) {
                        size = dataSize;
                    }
                    size -= shift;

                    ui64 partSize = groupType.PartSize(TRopeErasureType::CrcModeNone, dataSize);
                    for (ui32 idx = 0; idx < partSet.Parts.size(); ++idx) {
                        ui32 cutBegin = Min(partSize, needBegin);
                        ui32 cutSize = Min(partSize, needEnd) - cutBegin;
                        partSet.Parts[idx].ReferenceTo(TRopeHelpers::RopeFromStringMemcpy(
                                partSet.Parts[idx].OwnedRope.ConvertToString().substr(
                                        cutBegin, cutSize)), cutBegin, cutSize, partSize);
                    }

                    // Remove the 'missing' parts
                    partSet.PartsMask &= partMask;
                    for (ui32 i = 0; i < 2; ++i) {
                        ui32 idx = missingPartIdx[i];
                        ui32 cutBegin = Min(partSize, needBegin);
                        ui32 cutSize = Min(partSize, needEnd) - cutBegin;
                        partSet.Parts[idx].clear();
                        TString tmp = TString::Uninitialized(cutSize);
                        partSet.Parts[idx].ReferenceTo(TRopeHelpers::RopeFromStringMemcpy(tmp), cutBegin, cutSize, partSize);
                    }

                    // Restore the data
                    TString restoredString;
                    groupType.RestoreData(TRopeErasureType::CrcModeNone, partSet, true, false, true);

                    TBlockSplitRange range;
                    groupType.BlockSplitRange(TRopeErasureType::CrcModeNone, dataSize, shift, shift + size, &range);
                    for (ui32 partIdx = range.BeginPartIdx; partIdx < range.EndPartIdx; ++partIdx) {
                        TPartOffsetRange &partRange = range.PartRanges[partIdx];
                        if (partRange.Begin != partRange.End) {
                            // Make sure the restored data matches the original
                            ui64 checkSize = partRange.End - partRange.AlignedBegin;
                            UNIT_ASSERT(testString.size() >= partRange.AlignedWholeBegin + checkSize);
                            UNIT_ASSERT_C(partSet.Parts[partIdx].Offset <= partRange.AlignedBegin,
                                    "missingVariant# " << missingVariant
                                    << " dataSize# " << dataSize
                                    << " partialSize# " << partialSize
                                    << " partialShift# " << partialShift
                                    << " Offset# " << partSet.Parts[partIdx].Offset
                                    << " alignedBegin# " << partRange.AlignedBegin);
                            const char *expected = testString.data() + partRange.AlignedWholeBegin;
                            TString string = partSet.Parts[partIdx].OwnedRope.ConvertToString();
                            const char* actual = string.data() + partRange.AlignedBegin - partSet.Parts[partIdx].Offset;
                            UNIT_ASSERT(memcmp(expected, actual, checkSize) == 0);
                        }
                    }

                } // partialShift
            } // partialSize
        } // dataSize
    }

    Y_UNIT_TEST(TestBlock42PartialRestore0) {
        TestBlock42PartialRestore(0);
    }

    Y_UNIT_TEST(TestBlock42PartialRestore1) {
        TestBlock42PartialRestore(1);
    }

    Y_UNIT_TEST(TestBlock42PartialRestore2) {
        TestBlock42PartialRestore(2);
    }

    Y_UNIT_TEST(TestBlock42PartialRestore3) {
        TestBlock42PartialRestore(3);
    }
}

} // namespace NKikimr
} // NErasureRope
