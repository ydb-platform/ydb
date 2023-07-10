#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/util/lz4_data_generator.h>
#include <util/system/hp_timer.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(ErasureBrandNew) {

    Y_UNIT_TEST(Block42_encode) {
        TErasureType erasure(TErasureType::Erasure4Plus2Block);

        std::vector<TString> buffers;
        ui64 totalSize = 0;
        for (ui32 iter = 0; iter < 10000; ++iter) {
            const ui32 length = 1 + RandomNumber(100000u);
            buffers.push_back(FastGenDataForLZ4(length, iter));
            totalSize += length;
        }

        std::vector<TDataPartSet> parts1;
        parts1.reserve(buffers.size());
        std::vector<std::array<TRope, 6>> parts2;
        parts2.reserve(buffers.size());

        THPTimer timer;
        for (const auto& buffer : buffers) {
            erasure.SplitData(TErasureType::CrcModeNone, buffer, parts1.emplace_back());
        }
        TDuration period1 = TDuration::Seconds(timer.PassedReset());

        for (const auto& buffer : buffers) {
            ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, TRope(buffer), parts2.emplace_back());
        }
        TDuration period2 = TDuration::Seconds(timer.PassedReset());

        Cerr << "totalSize# " << totalSize
            << " period1# " << period1
            << " period2# " << period2
            << " MB/s1# " << (1.0 * totalSize / period1.MicroSeconds())
            << " MB/s2# " << (1.0 * totalSize / period2.MicroSeconds())
            << " factor# " << (1.0 * period1.MicroSeconds() / period2.MicroSeconds())
            << Endl;

        for (ui32 i = 0; i < buffers.size(); ++i) {
            auto& p1 = parts1[i];
            auto& p2 = parts2[i];

            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(p1.Parts[i].OwnedString.size(), p2[i].size());
                UNIT_ASSERT_EQUAL(p1.Parts[i].OwnedString, p2[i]);
            }
        }
    }

    Y_UNIT_TEST(Block42_chunked) {
        TErasureType erasure(TErasureType::Erasure4Plus2Block);

        for (ui32 iter = 0; iter < 10000; ++iter) {
            const ui32 length = 1 + RandomNumber(100000u);
            TString buffer = FastGenDataForLZ4(length, iter);
            UNIT_ASSERT_VALUES_EQUAL(buffer.size(), length);

            TRope rope;
            size_t offset = 0;
            while (offset < length) {
                size_t chunkLen = 1 + RandomNumber<size_t>(length - offset);
                auto chunk = TRcBuf::Uninitialized(chunkLen);
                memcpy(chunk.GetDataMut(), buffer.data() + offset, chunkLen);
                rope.Insert(rope.End(), std::move(chunk));
                offset += chunkLen;
            }
            UNIT_ASSERT_VALUES_EQUAL(rope.size(), length);
            UNIT_ASSERT_VALUES_EQUAL(rope.ConvertToString().size(), buffer.size());
            UNIT_ASSERT_EQUAL(rope.ConvertToString(), buffer);

            TDataPartSet p1;
            erasure.SplitData(TErasureType::CrcModeNone, buffer, p1);

            std::array<TRope, 6> p2;
            ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, rope, p2);

            std::array<TRope, 6> p3;
            TErasureSplitContext ctx = TErasureSplitContext::Init(32 * (1 + RandomNumber(1000u)));
            while (!ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, rope, p3, &ctx)) {}

            for (ui32 i = 0; i < 6; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(p1.Parts[i].OwnedString.size(), p2[i].size());
                UNIT_ASSERT_EQUAL(p1.Parts[i].OwnedString, p2[i]);
                UNIT_ASSERT_EQUAL(p2[i], p3[i]);
            }
        }
    }

    Y_UNIT_TEST(Block42_restore) {
        TErasureType erasure(TErasureType::Erasure4Plus2Block);

        for (ui32 length = 1; length < 4096; ++length) {
            TString buffer = FastGenDataForLZ4(length, length);
            
            std::array<TRope, 6> parts;
            ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, TRope(buffer), parts);

            for (i32 i = -1; i < 6; ++i) {
                for (i32 j = -1; j < 6; ++j) {
                    if (i == j) {
                        continue;
                    }
                    std::vector<bool> opts1(1, false);
                    if (i != -1) {
                        opts1.push_back(true);
                    }
                    std::vector<bool> opts2(1, false);
                    if (j != -1) {
                        opts2.push_back(true);
                    }

                    for (bool restoreI : opts1) {
                        for (bool restoreJ : opts2) {
                            for (bool needWhole : {true, false}) {
                                const ui32 clearMask = (i != -1 ? 1 << i : 0) | (j != -1 ? 1 << j : 0);
                                const ui32 restoreMask = (restoreI ? 1 << i : 0) | (restoreJ ? 1 << j : 0);
                                if (!needWhole && !restoreMask) {
                                    continue;
                                }

                                TString iter = TStringBuilder()
                                    << "i# " << i
                                    << " j# " << j
                                    << " restoreI# " << restoreI
                                    << " restoreJ# " << restoreJ
                                    << " needWhole# " << needWhole
                                    << " clearMask# " << clearMask
                                    << " restoreMask# " << restoreMask;

                                std::array<TRope, 6> copy;
                                for (ui32 k = 0; k < 6; ++k) {
                                    if (~clearMask >> k & 1) {
                                        copy[k] = parts[k];
                                    }
                                }

                                TRope whole;
                                ErasureRestore(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, buffer.size(),
                                    needWhole ? &whole : nullptr, copy, restoreMask, 0, false);
                                for (ui32 k = 0; k < 6; ++k) {
                                    TString restored = copy[k].ConvertToString();
                                    TString reference = parts[k].ConvertToString();
                                    if ((clearMask & ~restoreMask) >> k & 1) {
                                        UNIT_ASSERT(copy[k].size() == parts[k].size() || !copy[k]);
                                        if (copy[k].size() == parts[k].size()) {
                                            UNIT_ASSERT_EQUAL(copy[k], parts[k]);
                                        }
                                        continue;
                                    }
                                    UNIT_ASSERT_VALUES_EQUAL_C(restored.size(), reference.size(), iter << " k# " << k);
                                    for (ui32 z = 0; z < restored.size(); ++z) {
                                        if (restored[z] != reference[z]) {
                                            Cerr << "difference at " << z << Endl;
                                            break;
                                        }
                                    }
                                    UNIT_ASSERT_EQUAL_C(restored, reference, iter << " k# " << k);
                                }
                                if (needWhole) {
                                    UNIT_ASSERT_EQUAL_C(whole.ConvertToString(), buffer, iter);
                                }

                                ui32 partLen = parts[0].size();

                                if (!needWhole) {
                                    auto doTry = [&](ui32 offset, ui32 len) {
                                        if (offset < partLen && len <= partLen - offset) {
                                            TString xiter = TStringBuilder() << iter
                                                << " offset# " << offset << " len# " << len;
                                            std::array<TRope, 6> copy;
                                            for (ui32 k = 0; k < 6; ++k) {
                                                if (~clearMask >> k & 1) {
                                                    copy[k] = {parts[k].Position(offset), parts[k].Position(offset + len)};
                                                    UNIT_ASSERT_VALUES_EQUAL_C(copy[k].size(), len, xiter << " k# " << k);
                                                }
                                            }
                                            ErasureRestore(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block,
                                                buffer.size(), nullptr, copy, restoreMask, offset, true);
                                            for (ui32 k = 0; k < 6; ++k) {
                                                TString restored = copy[k].ConvertToString();
                                                TString reference = parts[k].ConvertToString().substr(offset, len);
                                                if ((clearMask & ~restoreMask) >> k & 1) {
                                                    UNIT_ASSERT(copy[k].size() == parts[k].size() || !copy[k]);
                                                    if (copy[k].size() == parts[k].size()) {
                                                        UNIT_ASSERT_EQUAL(copy[k], parts[k]);
                                                    }
                                                    continue;
                                                }
                                                UNIT_ASSERT_VALUES_EQUAL_C(restored.size(), reference.size(), xiter << " k# " << k);
                                                UNIT_ASSERT_EQUAL_C(restored, reference, xiter << " k# " << k);
                                            }
                                        }
                                    };

                                    doTry(0, partLen);
                                    doTry(32, partLen - 32);
                                    doTry(64, partLen - 64);
                                    doTry(0, 32);
                                    doTry(32, 32);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(Block42_restore_benchmark) {
        TErasureType erasure(TErasureType::Erasure4Plus2Block);

        ui64 totalSize = 0;
        std::vector<std::tuple<TRope, std::array<TRope, 6>, ui32, ui32>> parts;
        std::vector<TDataPartSet> restored1;
        std::vector<std::array<TRope, 6>> restored2;

        for (ui32 iter = 0; iter < 10000; ++iter) {
            const ui32 length = 1 + RandomNumber(100000u);
            TString buffer = FastGenDataForLZ4(length, iter);
            std::array<TRope, 6> p;
            ErasureSplit(TErasureType::CrcModeNone, TErasureType::Erasure4Plus2Block, TRope(buffer), p);
            for (TRope& r : p) {
                r.Compact();
            }
            parts.emplace_back(TRope(buffer), std::move(p), RandomNumber(6u), RandomNumber(6u));
            totalSize += buffer.size();
        }

        restored1.reserve(parts.size());
        restored2.reserve(parts.size());

        THPTimer timer;
        for (const auto& [rope, p, i, j] : parts) {
            TDataPartSet& parts = restored1.emplace_back();
            parts.PartsMask = 63 & ~(1 << i) & ~(1 << j);
            parts.FullDataSize = rope.size();
            parts.Parts.resize(6);
            for (ui32 k = 0; k < 6; ++k) {
                if (k != i && k != j) {
                    parts.Parts[k].ReferenceTo(p[k]);
                }
            }
            erasure.RestoreData(TErasureType::CrcModeNone, parts, true, false, true);
        }
        TDuration period1 = TDuration::Seconds(timer.PassedReset());

        for (const auto& [rope, p, i, j] : parts) {
            std::array<TRope, 6>& copy = restored2.emplace_back();
            for (ui32 k = 0; k < 6; ++k) {
                if (k != i && k != j) {
                    copy[k] = p[k];
                }
            }
            ErasureRestore(TErasureType::CrcModeNone, erasure, rope.size(), nullptr, copy, 1 << i | 1 << j, 0, false);
            UNIT_ASSERT_EQUAL(copy, p);
        }
        TDuration period2 = TDuration::Seconds(timer.PassedReset());

        Cerr << "totalSize# " << totalSize
            << " period1# " << period1
            << " period2# " << period2
            << " MB/s1# " << (1.0 * totalSize / period1.MicroSeconds())
            << " MB/s2# " << (1.0 * totalSize / period2.MicroSeconds())
            << " factor# " << (1.0 * period1.MicroSeconds() / period2.MicroSeconds())
            << Endl;
    }

}
