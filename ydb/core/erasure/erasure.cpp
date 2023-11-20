#include "erasure.h"

#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/digest/crc32c/crc32c.h>

#define MAX_TOTAL_PARTS 8
#define MAX_LINES_IN_BLOCK 8

#define IS_VERBOSE 0
#define IS_TRACE 0

#if IS_VERBOSE
#   include <util/stream/str.h>
#   define VERBOSE_COUT(a) \
       Cerr << a

static TString DebugFormatBits(ui64 value) {
    TStringStream s;
    for (size_t i = 7; i >=4; --i) {
        s << ((value >> i) & 1);
    }
    s << "_";
    for (size_t i = 3; i <= 3; --i) {
        s << ((value >> i) & 1);
    }
    return s.Str();
}
#else
#   define VERBOSE_COUT(a)  \
       do {                 \
       } while (false)
#endif

#if IS_TRACE
#   define TRACE(a) \
       Cerr << a
#else
#   define TRACE(a)  \
       do {                 \
       } while (false)
#endif

namespace NKikimr {

static void Refurbish(TRope &str, ui64 size, ui64 headroom = 0, ui64 tailroom = 0) {
    if (str.size() != size) {
        str = TRope(TRcBuf::Uninitialized(size, headroom, tailroom));
    }
}

static void Refurbish(TPartFragment &fragment, ui64 size, ui64 headroom = 0, ui64 tailroom = 0) {
    if (fragment.size() != size) {
        TRACE("Refurbish fragment size# " << fragment.size() << " to size# " << size << Endl);
        fragment.UninitializedOwnedWhole(size, headroom, tailroom);
    }
}

const char *TErasureType::ErasureSpeciesToStr(TErasureType::EErasureSpecies es) {
    switch (es) {
        case ErasureNone:           return "None";
        case ErasureMirror3:        return "Mirror3";
        case Erasure3Plus1Block:    return "3Plus1Block";
        case Erasure3Plus1Stripe:   return "3Plus1Stripe";
        case Erasure4Plus2Block:    return "4Plus2Block";
        case Erasure3Plus2Block:    return "3Plus2Block";
        case Erasure4Plus2Stripe:   return "4Plus2Stripe";
        case Erasure3Plus2Stripe:   return "3Plus2Stripe";
        case ErasureMirror3Plus2:   return "Mirror3Plus2";
        case ErasureMirror3dc:      return "Mirror3dc";
        case Erasure4Plus3Block:    return "4Plus3Block";
        case Erasure4Plus3Stripe:   return "4Plus3Stripe";
        case Erasure3Plus3Block:    return "3Plus3Block";
        case Erasure3Plus3Stripe:   return "3Plus3Stripe";
        case Erasure2Plus3Block:    return "2Plus3Block";
        case Erasure2Plus3Stripe:   return "2Plus3Stripe";
        case Erasure2Plus2Block:    return "2Plus2Block";
        case Erasure2Plus2Stripe:   return "2Plus2Stripe";
        case ErasureMirror3of4:     return "ErasureMirror3of4";
        default:                    return "UNKNOWN";
    }
}

struct TErasureParameters {
    TErasureType::EErasureFamily ErasureFamily;
    ui32 DataParts; // for parity - number of data parts, for mirror - 1
    ui32 ParityParts; // for parity - number of parity parts (1 | 2 | 3), for mirror - number of additional copies
    ui32 Prime; // for parity - smallest prime number >= DataParts, for mirror - 1
};

static const std::array<TErasureParameters, TErasureType::ErasureSpeciesCount> ErasureSpeciesParameters{{
    {TErasureType::ErasureMirror,  1, 0, 1} // 0 = ErasureSpicies::ErasureNone
    ,{TErasureType::ErasureMirror, 1, 2, 1} // 1 = ErasureSpicies::ErasureMirror3
    ,{TErasureType::ErasureParityBlock,  3, 1, 3} // 2 = ErasureSpicies::Erasure3Plus1Block
    ,{TErasureType::ErasureParityStripe, 3, 1, 3} // 3 = ErasureSpicies::Erasure3Plus1Stipe
    ,{TErasureType::ErasureParityBlock,  4, 2, 5} // 4 = ErasureSpicies::Erasure4Plus2Block
    ,{TErasureType::ErasureParityBlock,  3, 2, 3} // 5 = ErasureSpicies::Erasure3Plus2Block
    ,{TErasureType::ErasureParityStripe, 4, 2, 5} // 6 = ErasureSpicies::Erasure4Plus2Stipe
    ,{TErasureType::ErasureParityStripe, 3, 2, 3} // 7 = ErasureSpicies::Erasure3Plus2Stipe
    ,{TErasureType::ErasureMirror,       1, 2, 1} // 8 = ErasureSpicies::ErasureMirror3Plus2
    ,{TErasureType::ErasureMirror,       1, 2, 1} // 9 = ErasureSpicies::ErasureMirror3dc
    ,{TErasureType::ErasureParityBlock,  4, 3, 5} // 10 = ErasureSpicies::Erasure4Plus3Block
    ,{TErasureType::ErasureParityStripe, 4, 3, 5} // 11 = ErasureSpicies::Erasure4Plus3Stripe
    ,{TErasureType::ErasureParityBlock,  3, 3, 3} // 12 = ErasureSpicies::Erasure3Plus3Block
    ,{TErasureType::ErasureParityStripe, 3, 3, 3} // 13 = ErasureSpicies::Erasure3Plus3Stripe
    ,{TErasureType::ErasureParityBlock,  2, 3, 3} // 14 = ErasureSpicies::Erasure2Plus3Block
    ,{TErasureType::ErasureParityStripe, 2, 3, 3} // 15 = ErasureSpicies::Erasure2Plus3Stripe
    ,{TErasureType::ErasureParityBlock,  2, 2, 3} // 16 = ErasureSpicies::Erasure2Plus2Block
    ,{TErasureType::ErasureParityStripe, 2, 2, 3} // 17 = ErasureSpicies::Erasure2Plus2Stripe
    ,{TErasureType::ErasureMirror,       1, 2, 1} // 18 = ErasureSpicies::ErasureMirror3of4
}};

void PadAndCrcAtTheEnd(char *data, ui64 dataSize, ui64 bufferSize) {
    ui64 marginSize = bufferSize - dataSize - sizeof(ui32);
    if (marginSize) {
        memset(data + dataSize, 0, marginSize);
    }
    ui32 hash = Crc32c(data, dataSize);
    memcpy(data + bufferSize - sizeof(ui32), &hash, sizeof(ui32));
}

bool CheckCrcAtTheEnd(TErasureType::ECrcMode crcMode, const TContiguousSpan& buf) {
    switch (crcMode) {
    case TErasureType::CrcModeNone:
        return true;
    case TErasureType::CrcModeWholePart:
        if (buf.size() == 0) {
                return true;
        } else {
            Y_ABORT_UNLESS(buf.size() > sizeof(ui32), "Error in CheckWholeBlobCrc: blob part size# %" PRIu64
                    " is less then crcSize# %" PRIu64, (ui64)buf.size(), (ui64)sizeof(ui32));
            ui32 crc = Crc32c(buf.data(), buf.size() - sizeof(ui32));
            ui32 expectedCrc = ReadUnaligned<ui32>(buf.data() + buf.size() - sizeof(ui32));
            return crc == expectedCrc;
        }
    }
    ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
}

bool CheckCrcAtTheEnd(TErasureType::ECrcMode crcMode, const TRope& rope) {
    switch (crcMode) {
        case TErasureType::CrcModeNone:
            return true;
        case TErasureType::CrcModeWholePart:
            if (rope.IsEmpty()) {
                return true;
            } else {
                Y_ABORT_UNLESS(rope.size() > sizeof(ui32), "Error in CheckWholeBlobCrc: blob part size# %" PRIu64
                        " is less then crcSize# %" PRIu64, (ui64)rope.size(), (ui64)sizeof(ui32));

                size_t bytesRemain = rope.size() - sizeof(ui32);
                ui32 crc = 0;

                auto iter = rope.begin();
                while (bytesRemain) {
                    const char *data = iter.ContiguousData();
                    const size_t size = std::min(bytesRemain, iter.ContiguousSize());
                    crc = Crc32cExtend(crc, data, size);
                    iter += size;
                    bytesRemain -= size;
                }

                ui32 expectedCrc;
                iter.ExtractPlainDataAndAdvance(&expectedCrc, sizeof(expectedCrc));

                return crc == expectedCrc;
            }
    }
    ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
}

class TBlockParams {
public:
    ui64 DataSize;
    ui64 PartUserSize;
    ui64 PartContainerSize;
    ui32 DataParts;

    ui64 BlockSize; // Whole data is split into blocks of BlockSize bytes
    ui64 ColumnSize; // Each block consists of DataParts columns, each containing ColumnSize bytes
    ui64 WholeColumns;
    ui64 LineCount;

    ui64 SmallPartColumns;
    ui64 LargePartColumns;

    ui64 LastPartTailSize;

    ui64 SmallPartSize;
    ui64 LargePartSize;

    ui32 FirstSmallPartIdx;

    ui32 TotalParts;
    ui64 WholeBlocks; // Data consists of (WholeBlocks * BlockSize + TailSize) bytes
    ui32 TailSize;

    ui32 Prime;
    TErasureType::ECrcMode CrcMode;

    using TBufferDataPart = TStackVec<ui64*, MAX_TOTAL_PARTS>;
    TBufferDataPart BufferDataPart;
    char *Data;

    // Maximum blocks to be split during one run in incremental mode
    static constexpr ui64 IncrementalSplitMaxBlocks = 1024;

    TBlockParams(TErasureType::ECrcMode crcMode, const TErasureType &type, ui64 dataSize) {
        DataSize = dataSize;
        PartUserSize = type.PartUserSize(dataSize);
        PartContainerSize = type.PartSize(crcMode, dataSize);
        DataParts = type.DataParts();

        BlockSize = type.MinimalBlockSize();
        ColumnSize = BlockSize / DataParts;
        WholeColumns = dataSize / ColumnSize;
        LineCount = ColumnSize / sizeof(ui64);

        SmallPartColumns = WholeColumns / DataParts;
        LargePartColumns = SmallPartColumns + 1;

        LastPartTailSize = DataSize - WholeColumns * ColumnSize;

        SmallPartSize = SmallPartColumns * ColumnSize;
        LargePartSize = LargePartColumns * ColumnSize;

        FirstSmallPartIdx = WholeColumns % DataParts;

        TotalParts = type.TotalPartCount();
        WholeBlocks = DataSize / BlockSize;
        TailSize = (ui32)(DataSize % BlockSize);

        Prime = type.Prime();
        CrcMode = crcMode;

        Data = nullptr;
    }

    template <bool isStripe>
    void PrepareInputDataPointers(char* data) {
        if (isStripe) {
            Data = data;
        } else {
            //
            // All data is a matrix. Matrix cell is ColumnSize bytes.
            // Each part is a matrix column (continuous in memory).
            // Each block is a matrix row (not continuous in memory).
            // Cells are numerated as they appear in memory.
            //
            //   1 5 9 C   <--   4 required pointers (beginning of each data part)
            //   2 6 A D
            //   3 7 B E
            //   4 8   *
            //
            // There are two large parts (1st and 2nd), followed by two small parts (3rd and 4th).
            // Large part is exactly ColumnSize bytes larger than small part.
            // The last part is special, it can contain tail less than ColumnSize bytes (shown as *)
            //

            BufferDataPart.resize(DataParts);
            for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
                BufferDataPart[i] = (ui64*)(data + i * LargePartSize);
            }
            for (ui32 i = FirstSmallPartIdx; i < DataParts; ++i) {
                BufferDataPart[i] = (ui64*)(data + FirstSmallPartIdx * LargePartSize +
                        (i - FirstSmallPartIdx) * SmallPartSize);
            }
        }
    }

    template <bool isStripe>
    void XorSplitWhole(char* data, TBufferDataPart &bufferDataPart, TDataPartSet &outPartSet, ui64 writePosition, ui32 blocks) {
        ui64 readPosition = 0;
        VERBOSE_COUT("XorSplitWhole:" << Endl);
        for (ui64 blockIdx = 0; blockIdx < blocks; ++blockIdx) {
            for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                ui64 xored = 0;
                for (ui32 part = 0; part < DataParts; ++part) {
                    ui64 sourceData;
                    if (isStripe) {
                        sourceData = *((ui64*)data + readPosition + part);
                    } else {
                        sourceData = bufferDataPart[part][readPosition];
                    }
                    xored ^= sourceData;
                    VERBOSE_COUT(DebugFormatBits(sourceData) << ", ");
                    *(ui64*)(outPartSet.Parts[part].GetDataAt(writePosition)) = sourceData;
                }
                *(ui64*)(outPartSet.Parts[DataParts].GetDataAt(writePosition)) = xored;
                VERBOSE_COUT(DebugFormatBits(xored) << Endl);
                writePosition += sizeof(ui64);
                if (isStripe) {
                    readPosition += DataParts;
                } else {
                    ++readPosition;
                }
            }
        }
        VERBOSE_COUT(Endl);
    }

    template <bool isStripe>
    void XorSplit(TDataPartSet &outPartSet) {
        VERBOSE_COUT("XorSplit:" << Endl);
        // Write data and parity
        XorSplitWhole<isStripe>(Data, BufferDataPart, outPartSet, 0ull, WholeBlocks);

        // Use the remaining parts to fill in the last block
        // Write the tail of the data
        if (TailSize) {
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            PrepareLastBlockData<isStripe>(lastBlockSource, bufferDataPart);

            XorSplitWhole<isStripe>(lastBlockSource, bufferDataPart, outPartSet, WholeBlocks * ColumnSize, 1);
        }
    }

#if IS_VERBOSE
#   define VERBOSE_COUT_BLOCK(IS_FULL_DATA, FULL_DATA_ELEM, PART_ELEM, COL_M, COL_M1) \
    do { \
        for (ui32 row = 0; row < LineCount; ++row) { \
            VERBOSE_COUT(Endl); \
            for (ui32 col = 0; col < DataParts; ++col) { \
                if (IS_FULL_DATA) { \
                    VERBOSE_COUT(DebugFormatBits(FULL_DATA_ELEM(row, col)) << ", "); \
                } else { \
                    VERBOSE_COUT(DebugFormatBits(PART_ELEM(row, col)) << ", "); \
                } \
            } \
            VERBOSE_COUT(DebugFormatBits(COL_M(row)) << ", "); \
            VERBOSE_COUT(DebugFormatBits(COL_M1(row))); \
        } \
        VERBOSE_COUT(Endl); \
    } while (false)
#   define VERBOSE_COUT_BLOCK_M2(IS_FULL_DATA, FULL_DATA_ELEM, PART_ELEM, COL_M, COL_M1, COL_M2) \
    do { \
        for (ui32 row = 0; row < LineCount; ++row) { \
            VERBOSE_COUT(Endl); \
            for (ui32 col = 0; col < DataParts; ++col) { \
                if (IS_FULL_DATA) { \
                    VERBOSE_COUT(DebugFormatBits(FULL_DATA_ELEM(row, col)) << ", "); \
                } else { \
                    VERBOSE_COUT(DebugFormatBits(PART_ELEM(row, col)) << ", "); \
                } \
            } \
            VERBOSE_COUT(DebugFormatBits(COL_M(row)) << ", "); \
            VERBOSE_COUT(DebugFormatBits(COL_M1(row)) << ", "); \
            VERBOSE_COUT(DebugFormatBits(COL_M2(row))); \
        } \
        VERBOSE_COUT(Endl); \
    } while (false)
#else
#   define VERBOSE_COUT_BLOCK(IS_FULL_DATA, FULL_DATA_ELEM, PART_ELEM, COL_M, COL_M1) \
    do { \
    } while (false)
#   define VERBOSE_COUT_BLOCK_M2(IS_FULL_DATA, FULL_DATA_ELEM, PART_ELEM, COL_M, COL_M1, COL_M2) \
    do { \
    } while (false)
#endif


    template <bool isStripe, bool isFromDataParts>
    void EoSplitWhole(char *data, TBufferDataPart &bufferDataPart, TDataPartSet &outPartSet, ui64 writePosition, ui64 firstBlock, ui64 lastBlock) {
        const ui32 lastPartIdx = DataParts + 1;
        const ui32 m = Prime;
        for (ui64 blockIdx = firstBlock; blockIdx != lastBlock; ++blockIdx) {

#define IN_EL_STRIPE(row, column) *((ui64*)data + (blockIdx * LineCount + (row)) * DataParts + (column))
#define IN_EL_BLOCK(row, column) bufferDataPart[column][blockIdx * LineCount + (row)]
#define OUT_EL(row, column) *((ui64*)(outPartSet.Parts[column].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define OUT_M(row) *((ui64*)(outPartSet.Parts[DataParts].GetDataAt(writePosition + (row) *sizeof(ui64))))
#define OUT_M1(row) *((ui64*)(outPartSet.Parts[lastPartIdx].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define IN_EL(row, column) (isFromDataParts ?\
    OUT_EL((row), (column)) :\
    (isStripe ? IN_EL_STRIPE((row), (column)) : IN_EL_BLOCK((row), (column))))

            if (isStripe) {
                VERBOSE_COUT_BLOCK(true, IN_EL_STRIPE, IN_EL_STRIPE, OUT_M, OUT_M1);
            } else {
                VERBOSE_COUT_BLOCK(true, IN_EL_BLOCK, IN_EL_BLOCK, OUT_M, OUT_M1);
            }
            ui64 adj = 0;
            const ui32 mint = (m - 2 < LineCount ? 1 : m - 2 - LineCount);
            VERBOSE_COUT("mint = " << mint << " m - 1 - t = " << (m - 1 - mint) << Endl);
            for (ui32 t = mint; t < DataParts; ++t) {
                adj ^= IN_EL(m - 1 - t, t);
                VERBOSE_COUT("s: " << adj << " el[" << (m - 1 - t) << ", " << t << "]: " <<
                    DebugFormatBits(IN_EL(m - 1 - t, t)) << Endl);
            }
            for (ui32 l = 0; l < LineCount; ++l) {
                ui64 sourceData = IN_EL(l, 0);
                OUT_M1(l) = adj ^ sourceData;
                OUT_M(l) = sourceData;
                if (!isFromDataParts) {
                    OUT_EL(l, 0) = sourceData;
                }
            }
            for (ui32 t = 1; t < DataParts; ++t) {
                for (ui32 l = 0; l < LineCount; ++l) {
                    ui64 sourceData = IN_EL(l, t);
                    OUT_M(l) ^= sourceData;
                    if (!isFromDataParts) {
                        OUT_EL(l, t) = sourceData;
                    }
                    VERBOSE_COUT("OUT_M(" << l << ") = " << DebugFormatBits(OUT_M(l)) << Endl);
                }
            }
            for (ui32 t = 1; t < DataParts; ++t) {
                for (ui32 l = 0; l < LineCount - t; ++l) {
                    ui32 row = l + t;
                    OUT_M1(row) ^= IN_EL(l, t);
                    VERBOSE_COUT(DebugFormatBits(IN_EL(row, t)) << Endl);
                }
                for (ui32 l = LineCount - t + 1; l < LineCount; ++l) {
                    ui32 row = l + t - m;
                    OUT_M1(row) ^= IN_EL(l, t);
                    VERBOSE_COUT(DebugFormatBits(IN_EL(row, t)) << Endl);
                }
            }
            VERBOSE_COUT_BLOCK(true, OUT_EL, OUT_EL, OUT_M, OUT_M1);
#undef IN_EL
#undef OUT_M1
#undef OUT_M
#undef OUT_EL
#undef IN_EL_BLOCK
#undef IN_EL_STRIPE
            writePosition += ColumnSize;
        }
    }

    template <bool isStripe, bool isFromDataParts>
    void StarSplitWhole(char *data, TBufferDataPart &bufferDataPart, TDataPartSet &outPartSet,
            ui64 writePosition, ui32 blocks) {
        const ui32 m = Prime;
#define IN_EL_STRIPE(row, column) *((ui64*)data + (blockIdx * LineCount + (row)) * DataParts + (column))
#define IN_EL_BLOCK(row, column) bufferDataPart[column][blockIdx * LineCount + (row)]
#define IN_EL_SB(row, column) (isStripe ? IN_EL_STRIPE(row, column) : IN_EL_BLOCK(row, column))
#define OUT_EL(row, column) *((ui64*)(outPartSet.Parts[column].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define IN_EL(row, column) (isFromDataParts ? OUT_EL(row, column) : IN_EL_SB(row, column))
#define OUT_M(row) *((ui64*)(outPartSet.Parts[DataParts].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define OUT_M1(row) *((ui64*)(outPartSet.Parts[DataParts + 1].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define OUT_M2(row) *((ui64*)(outPartSet.Parts[DataParts + 2].GetDataAt(writePosition + (row) * sizeof(ui64))))
        for (ui64 blockIdx = 0; blockIdx < blocks; ++blockIdx) {
            if (isStripe) {
                VERBOSE_COUT_BLOCK(true, IN_EL_STRIPE, IN_EL_STRIPE, OUT_M, OUT_M1);
            } else {
                VERBOSE_COUT_BLOCK(true, IN_EL_BLOCK, IN_EL_BLOCK, OUT_M, OUT_M1);
            }
            ui64 s1 = 0;
            const ui32 mint = (m - 2 < LineCount ? 1 : m - 2 - LineCount);
            VERBOSE_COUT("mint = " << mint << " m - 1 - t = " << (m - 1 - mint) << Endl);
            for (ui32 t = mint; t < DataParts; ++t) {
                s1 ^= IN_EL(m - 1 - t, t);
                VERBOSE_COUT("s1: " << s1 << " el[" << (m - 1 - t) << ", " << t << "]: " <<
                    DebugFormatBits(isStripe ? IN_EL_STRIPE(m - 1 - t, t): IN_EL_BLOCK(m - 1 - t, t)) << Endl);
            }
            ui64 s2 = 0;
            for (ui32 t = 1; t < DataParts; ++t) {
                s2 ^= IN_EL(t - 1, t);
                VERBOSE_COUT("s2: " << s2 << " el[" << (t - 1) << ", " << t << "]: " <<
                    DebugFormatBits(IN_EL(t - 1, t)) << Endl);
            }
            for (ui32 l = 0; l < LineCount; ++l) {
                ui64 dataIN_EL = IN_EL(l, 0);
                OUT_M(l) = dataIN_EL;
                OUT_M1(l) = s1 ^ dataIN_EL;
                OUT_M2(l) = s2 ^ dataIN_EL;
                if (!isFromDataParts) {
                    OUT_EL(l, 0) = dataIN_EL;
                }
            }
            for (ui32 t = 1; t < DataParts; ++t) {
                for (ui32 l = 0; l < LineCount; ++l) {
                    ui64 dataIN_EL = IN_EL(l, t);
                    ui32 row1 = (l + t) % m;
                    OUT_M(l) ^= dataIN_EL;
                    if (row1 < LineCount) {
                        OUT_M1(row1) ^= dataIN_EL;
                        VERBOSE_COUT(IN_EL(row1, t) << Endl);
                    }
                    ui32 row2 = (m + l - t) % m;
                    if (row2 < LineCount) {
                        OUT_M2(row2) ^= dataIN_EL;
                        VERBOSE_COUT(IN_EL(row2, t) << Endl);
                    }
                    if (!isFromDataParts) {
                        OUT_EL(l, t) = dataIN_EL;
                    }
                }
            }
#if IS_VERBOSE
            for (ui32 l = 0; l < LineCount; ++l) {
                VERBOSE_COUT("OUT_M1(" << l << ") = " << DebugFormatBits(OUT_M1(l)) << Endl);
            }
            VERBOSE_COUT_BLOCK_M2(true, OUT_EL, OUT_EL, OUT_M, OUT_M1, OUT_M2);
#endif
            writePosition += ColumnSize;
        }
#undef OUT_M2
#undef OUT_M1
#undef OUT_M
#undef OUT_EL
#undef IN_EL
#undef IN_EL_BLOCK
#undef IN_EL_STRIPE
    }

    template<bool isStripe>
    void PrepareLastBlockData(char *lastBlockSource, TBufferDataPart &bufferDataPart) {
        if (isStripe) {
            memcpy(lastBlockSource, Data + WholeBlocks * BlockSize, TailSize);
            memset(lastBlockSource + TailSize, 0, BlockSize - TailSize);
        } else {
            bufferDataPart.resize(DataParts);
            for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
                bufferDataPart[i] = (ui64*)(lastBlockSource + i * ColumnSize);
                memcpy(bufferDataPart[i], reinterpret_cast<const char*>(BufferDataPart[i]) + WholeBlocks * ColumnSize,
                        ColumnSize);
            }
            for (ui32 i = FirstSmallPartIdx; i < DataParts - 1; ++i) {
                bufferDataPart[i] = (ui64*)(lastBlockSource + i * ColumnSize);
                memset(bufferDataPart[i], 0, ColumnSize);
            }
            bufferDataPart[DataParts - 1] = (ui64*)(lastBlockSource + (DataParts - 1) * ColumnSize);
            char *lastColumnData = reinterpret_cast<char*>(bufferDataPart[DataParts - 1]);
            if (LastPartTailSize) {
                memcpy(lastColumnData,
                    reinterpret_cast<const char*>(BufferDataPart[DataParts - 1]) + WholeBlocks * ColumnSize,
                    LastPartTailSize);
            }
            memset(lastColumnData + LastPartTailSize, 0, ColumnSize - LastPartTailSize);
        }
    }

    template <bool isStripe>
    void PrepareLastBlockPointers(char *lastBlockSource, TBufferDataPart &bufferDataPart) {
        if (!isStripe) {
            bufferDataPart.resize(DataParts);
            for (ui32 i = 0; i < DataParts; ++i) {
                bufferDataPart[i] = (ui64*)(lastBlockSource + i * ColumnSize);
            }
        }
    }

    template <bool isStripe>
    void PlaceLastBlock(TBufferDataPart &bufferDataPart, char *lastBlock) {
        if (isStripe) {
            memcpy(Data + WholeBlocks * BlockSize, lastBlock, TailSize);
        } else {
            for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
                memcpy(reinterpret_cast<char*>(BufferDataPart[i]) + WholeBlocks * ColumnSize,
                    bufferDataPart[i], ColumnSize);
            }
            memcpy(reinterpret_cast<char*>(BufferDataPart[DataParts - 1]) + WholeBlocks * ColumnSize,
                bufferDataPart[DataParts - 1], LastPartTailSize);
        }
    }

    template <bool isStripe, bool isFromDataParts>
    void StarSplit(TDataPartSet &outPartSet) {
        // Use all whole columns of all the parts
        StarSplitWhole<isStripe, isFromDataParts>(Data, BufferDataPart, outPartSet, 0ull, WholeBlocks);

        // Use the remaining parts to fill in the last block
        // Write the tail of the data
        if (TailSize) {
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            if (!isFromDataParts) {
                PrepareLastBlockData<isStripe>(lastBlockSource, bufferDataPart);
            }

            StarSplitWhole<isStripe, isFromDataParts>(lastBlockSource, bufferDataPart, outPartSet, WholeBlocks * ColumnSize, 1);
        }
    }

    template <bool isStripe, bool isFromDataParts, bool isIncremental = false>
    void EoSplit(TDataPartSet &outPartSet) {
        ui64 readPosition = isIncremental? ColumnSize * outPartSet.CurBlockIdx: 0;
        ui64 firstBlock = isIncremental? outPartSet.CurBlockIdx: 0;
        ui64 lastBlock = isIncremental? Min(WholeBlocks, firstBlock + IncrementalSplitMaxBlocks): WholeBlocks;
        outPartSet.CurBlockIdx = lastBlock;
        bool hasTail = TailSize;

        if (isFromDataParts) {
            Y_ABORT_UNLESS(outPartSet.Parts[0].Offset % ColumnSize == 0);
            readPosition = outPartSet.Parts[0].Offset;
            lastBlock = Min(WholeBlocks - readPosition / ColumnSize, outPartSet.Parts[0].Size / ColumnSize);
            hasTail = TailSize && (outPartSet.Parts[0].Size + readPosition > WholeBlocks * ColumnSize);
        }
        TRACE(__LINE__ << " EoSplit readPosition# " << readPosition
                << " wholeBlocks# " << wholeBlocks
                << " hasTail# " << hasTail
                << " fullDataSize# " << outPartSet.FullDataSize
                << Endl);
        // Use all whole columns of all the parts
        EoSplitWhole<isStripe, isFromDataParts>(Data, BufferDataPart, outPartSet, readPosition, firstBlock, lastBlock);

        // Use the remaining parts to fill in the last block
        // Write the tail of the data
        if (hasTail && outPartSet.IsSplitDone()) {
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            if (!isFromDataParts) {
                PrepareLastBlockData<isStripe>(lastBlockSource, bufferDataPart);
            }

            EoSplitWhole<isStripe, isFromDataParts>(lastBlockSource, bufferDataPart, outPartSet,
                    WholeBlocks * ColumnSize, 0, 1);
        }
    }

    void GlueBlockParts(char* dst, const TDataPartSet& partSet) const {
        if (LargePartSize) {
            for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
                memcpy(dst + i * LargePartSize,
                        partSet.Parts[i].GetDataAt(0),
                        LargePartSize);
            }
            if (SmallPartSize) {
                for (ui32 i = FirstSmallPartIdx; i < DataParts - 1; ++i) {
                    memcpy(dst + (LargePartSize * FirstSmallPartIdx) +
                            (i - FirstSmallPartIdx) * SmallPartSize,
                            partSet.Parts[i].GetDataAt(0), SmallPartSize);
                }
            }
        }
        if (SmallPartSize + LastPartTailSize) {
            ui64 offset = LargePartSize * FirstSmallPartIdx + ((DataParts - 1) - FirstSmallPartIdx) * SmallPartSize;
            memcpy(dst + offset,
                    partSet.Parts[DataParts - 1].GetDataAt(0),
                    SmallPartSize + LastPartTailSize);
        }
        return;
    }

    // s = a[(m + missingDataPartIdx - 1) % m][m + 1];
    // for (l = 0; l < m; ++l) {
    //    s ^= a[(m + missingDataPartIdx - l - 1) % m][l];
    // }
    // for (k = 0; k < m - 1; ++k) {
    //    ui64 res = s;
    //    for (l = 0; l < missingDataPartIdx; ++l) {
    //        res ^= a[(m + k + missingDataPartIdx - l) % m][l];
    //    }
    //    for (l = missingDataPartIdx + 1; l < m; ++l) {
    //        res ^= a[(m + k + missingDataPartIdx - l) % m][l];
    //    }
    //    a[k][missingDataPartIdx] = res;
    // }
    template <bool isStripe, bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
    void EoDiagonalRestorePartWhole(char *data, TBufferDataPart &bufferDataPart, TDataPartSet &partSet, ui64 readPosition,
            ui32 beginBlockIdx, ui32 endBlockIdx, ui32 missingDataPartIdx) {
        ui32 lastColumn = reversed ? DataParts + 2 : DataParts + 1;
        const ui32 m = Prime;
        // Use all whole columns of all the parts
        for (ui64 blockIdx = beginBlockIdx; blockIdx < endBlockIdx; ++blockIdx) {
#define RIGHT_ROW(row) (reversed ? LineCount - 1 - (row) : (row))
#define OUT_EL_BLOCK(row, column) bufferDataPart[column][blockIdx * LineCount + RIGHT_ROW(row)]
#define OUT_EL_STRIPE(row, column) *((ui64*)data + (blockIdx * LineCount + RIGHT_ROW(row)) * DataParts + (column))
#define IN_EL(row, column) *((ui64*)(partSet.Parts[column].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define IN_M(row) *((ui64*)(partSet.Parts[DataParts].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define IN_M12(row) *((ui64*)(partSet.Parts[lastColumn].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
            VERBOSE_COUT_BLOCK(true, IN_EL, IN_EL, IN_M, IN_M12);
            ui64 s = 0;
            ui32 colLimit = DataParts;
            ui32 rowLimit = LineCount;
            {
                ui32 idx = (m + missingDataPartIdx - 1) % m;
                if (idx < rowLimit) {
                    s = IN_M12(idx);
                    VERBOSE_COUT("s(" << idx << ", m1): " << DebugFormatBits(s) << Endl);
                }
            }
            for (ui32 l = 0; l < colLimit; ++l) {
                ui32 idx = (m + missingDataPartIdx - l - 1) % m;
                if (idx < LineCount) {
                    ui64 value = IN_EL(idx, l);
                    s ^= value;
                    if (restoreFullData) {
                        VERBOSE_COUT("a [" << idx << ", " << l << "] = " << DebugFormatBits(value) << Endl);
                        if (isStripe) {
                            OUT_EL_STRIPE(idx, l) = value;
                        } else {
                            OUT_EL_BLOCK(idx, l) = value;
                        }
                    }
                }
            }
            VERBOSE_COUT("s: " << DebugFormatBits(s) << Endl);
            for (ui32 k = 0; k < LineCount; ++k) {
                ui64 res = s;
                for (ui32 l = 0; l < missingDataPartIdx; ++l) {
                    ui32 idx = (m + k + missingDataPartIdx - l) % m;
                    if (idx < LineCount) {
                        ui64 value = IN_EL(idx, l);
                        res ^= value;
                        if (restoreFullData) {
                            VERBOSE_COUT("b [" << idx << ", " << l << "] = " << DebugFormatBits(value) << Endl);
                            if (isStripe) {
                                OUT_EL_STRIPE(idx, l) = value;
                            } else {
                                OUT_EL_BLOCK(idx, l) = value;
                            }
                        }
                    }
                }
                for (ui32 l = missingDataPartIdx + 1; l < colLimit; ++l) {
                    ui32 idx = (m + k + missingDataPartIdx - l) % m;
                    if (idx < LineCount) {
                        ui64 value = IN_EL(idx, l);
                        res ^= value;
                        if (restoreFullData) {
                            VERBOSE_COUT("c [" << idx << ", " << l << "] = " << DebugFormatBits(value) << Endl);
                            if (isStripe) {
                                OUT_EL_STRIPE(idx, l) = value;
                            } else {
                                OUT_EL_BLOCK(idx, l) = value;
                            }
                        }
                    }
                }
                ui32 idx = (m + k + missingDataPartIdx) % m;
                if (idx < LineCount) {
                    VERBOSE_COUT("idx = " << idx);
                    res ^= IN_M12(idx); // This is missing in the article!
                }
                if (restoreFullData) {
                    VERBOSE_COUT("out [" << k << ", " << missingDataPartIdx << "] = " << DebugFormatBits(res) << Endl);
                    if (isStripe) {
                        OUT_EL_STRIPE(k, missingDataPartIdx) = res;
                    } else {
                        OUT_EL_BLOCK(k, missingDataPartIdx) = res;
                    }
                }
                if (restoreParts) {
                    IN_EL(k, missingDataPartIdx) = res;
                    if (restoreParityParts) {
                        ui64 tmp = 0;
                        for (ui32 l = 0; l < DataParts; ++l) {
                            tmp ^= IN_EL(k, l);
                        }
                        IN_M(k) = tmp;
                    }
                }
            }
            if (isStripe) {
                VERBOSE_COUT_BLOCK(restoreFullData, OUT_EL_STRIPE, IN_EL, IN_M, IN_M12);
            } else {
                VERBOSE_COUT_BLOCK(restoreFullData, OUT_EL_BLOCK, IN_EL, IN_M, IN_M12);
            }
#undef IN_M12
#undef IN_M
#undef IN_EL
#undef OUT_EL_BLOCK
#undef OUT_EL_STRIPE
#undef RIGHT_ROW

            readPosition += ColumnSize;
        }
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
    void EoDiagonalRestorePart(TDataPartSet& partSet, ui32 missingDataPartIdx) {
        TRACE("Line# " << __LINE__ << " Diagonal restore: LineCount=" << LineCount << Endl);

        TRACE("EoDiagonalRestorePart fullSize# " << partSet.FullDataSize
                << " partSet p0 Size# " << partSet.Parts[0].Size
                << " p1 Size# " << partSet.Parts[1].Size
                << " p2 Size# " << partSet.Parts[2].Size << Endl);
        ui32 presentPartIdx = (missingDataPartIdx == 0 ? 1 : 0);
        Y_ABORT_UNLESS(partSet.Parts[presentPartIdx].Offset % ColumnSize == 0);
        ui64 readPosition = partSet.Parts[presentPartIdx].Offset;
        ui64 wholeBlocks = Min(WholeBlocks - readPosition / ColumnSize, partSet.Parts[presentPartIdx].Size / ColumnSize);

        ui64 beginBlock = readPosition / ColumnSize;
        ui64 endBlock = beginBlock + wholeBlocks;
        TRACE("wholeBlocks# " << wholeBlocks << " blockSize# " << BlockSize << Endl);

        EoDiagonalRestorePartWhole<isStripe, restoreParts, restoreFullData, reversed, restoreParityParts>(
                Data, BufferDataPart, partSet, readPosition, beginBlock, endBlock, missingDataPartIdx);

        // Read the tail of the data
        if (TailSize && (partSet.Parts[presentPartIdx].Size + readPosition > WholeBlocks * ColumnSize)) {
            TRACE("EoDiagonalRestorePart tail" << Endl);
            char lastBlock[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers<isStripe>(lastBlock, bufferDataPart);

            EoDiagonalRestorePartWhole<isStripe, restoreParts, restoreFullData, reversed, restoreParityParts>(lastBlock, bufferDataPart,
                            partSet, WholeBlocks * ColumnSize, 0, 1, missingDataPartIdx);

            if (restoreFullData) {
                PlaceLastBlock<isStripe>(bufferDataPart, lastBlock);
            }
        }
        if (restoreParts && missingDataPartIdx < partSet.Parts.size()) {
            PadAndCrcPart(partSet, missingDataPartIdx);
        }
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarMainRestorePartsWholeSymmetric(char *data, TBufferDataPart &bufferDataPart, TDataPartSet& partSet,
                ui64 readPosition, ui32 endBlockIdx, ui32 missingDataPartIdxA, ui32 missingDataPartIdxB,
                ui32 missingDataPartIdxC) {
        VERBOSE_COUT("Start of StarMainRestorePartsWholeSymmetric for blocks "  << missingDataPartIdxA
                                        << " " << missingDataPartIdxB << " " <<missingDataPartIdxC << Endl);
        // Notation used in this function is taken from article
        // Cheng Huang, Lihao Xu (2005, 4th USENIX Conf.) - STAR: An Efficient Coding Scheme...
        ui64 readPositionStart = readPosition;
        const ui32 m = Prime;
        const ui32 r = missingDataPartIdxA;
        const ui32 s = missingDataPartIdxB;
        const ui32 t = missingDataPartIdxC;
        const ui32 dr = (m + s - r) % m;
        // Use all whole columns of all the parts
#define OUT_EL_BLOCK(row, column) bufferDataPart[column][blockIdx * LineCount + (row)]
#define OUT_EL_STRIPE(row, column) *((ui64*)data + (blockIdx * LineCount + (row)) * DataParts + (column))
#define OUT_EL(row, column) (isStripe ? OUT_EL_STRIPE(row, column) : OUT_EL_BLOCK(row, column))
#define IN_EL(row, column) *((ui64*)(partSet.Parts[column].GetDataAt(readPosition + (row) * sizeof(ui64))))
#define IN_M(row) *((ui64*)(partSet.Parts[DataParts].GetDataAt(readPosition + (row) * sizeof(ui64))))
#define IN_M1(row) *((ui64*)(partSet.Parts[DataParts + 1].GetDataAt(readPosition + (row) * sizeof(ui64))))
#define IN_M2(row) *((ui64*)(partSet.Parts[DataParts + 2].GetDataAt(readPosition + (row) * sizeof(ui64))))
        for (ui64 blockIdx = 0; blockIdx < endBlockIdx; ++blockIdx) {
            VERBOSE_COUT_BLOCK_M2(true, IN_EL, IN_EL, IN_M, IN_M1, IN_M2);
            // 1) Adjusters recovery   adj0 is for S0
            ui64 adj0 = 0;
            ui64 adj1 = 0;
            ui64 adj2 = 0;
            for (ui32 i = 0; i < LineCount; ++i) {
                adj0 ^= IN_M(i);
                adj1 ^= IN_M1(i);
                adj2 ^= IN_M2(i);
            }
            adj1 = adj0 ^ adj1;
            adj2 = adj0 ^ adj2;
            // 2) Syndrome calculation
            ui64 s0[MAX_LINES_IN_BLOCK];
            ui64 s1[MAX_LINES_IN_BLOCK];
            ui64 s2[MAX_LINES_IN_BLOCK];
            ui32 row;
            for (ui32 i = 0; i < LineCount; ++i) {
                s0[i] = IN_M(i);
                s1[i] = IN_M1(i) ^ adj1;
                s2[i] = IN_M2(i) ^ adj2;
                VERBOSE_COUT("IN_M[" << i << "] = " << DebugFormatBits(IN_M(i)) << ", ");
                VERBOSE_COUT("IN_M1[" << i << "] ^ adj1 = " << DebugFormatBits(IN_M1(i) ^ adj1) << ", ");
                VERBOSE_COUT("IN_M2[" << i << "] ^ adj2 = " << DebugFormatBits(IN_M2(i) ^ adj2) << Endl);
            }
            s0[m - 1] = 0;
            s1[m - 1] = adj1;
            s2[m - 1] = adj2;
            for (ui32 j = 0; j < DataParts; ++j) {
                if (j == r || j == s || j == t) {
                    continue;
                }
                for (ui32 i = 0; i < LineCount; ++i) {
                    ui64 data_tmp = IN_EL(i, j);
                    if (restoreFullData) {
                        OUT_EL(i, j) = data_tmp;
                    }
                    s0[i] ^= data_tmp;
                    row = (i + j) % m;
                    if (row < m) {
                        s1[row] ^= IN_EL(i, j);
                    }
                    row = (m + i - j) % m;
                    if (row < m) {
                        s2[row] ^= IN_EL(i, j);
                        VERBOSE_COUT("s2[" << i << "] ^= IN_EL(" << row << "," << j << ");" << Endl;);

                    }
                }
            }
            for (ui32 i = 0; i < m; ++i) {
                VERBOSE_COUT("s0[" << i << "] = " << DebugFormatBits(s0[i]) << ", ");
                VERBOSE_COUT("s1[" << i << "] = " << DebugFormatBits(s1[i]) << ", ");
                VERBOSE_COUT("s2[" << i << "] = " << DebugFormatBits(s2[i]) << Endl);
            }
            // 3) Compute all rows in s
            ui32 row1 = (m - 1 + r) % m;
            ui32 row2 = (m + m - 1 - 2*dr - r) % m;
            ui32 row01 = (m + row1 - r) % m;
            ui32 row02 = (row2 + r) % m;
            ui64 res = 0;
            for (ui32 i = 0; i < LineCount; ++i) {
                res = s0[row01] ^ s1[row1] ^ s0[row02] ^ s2[row2] ^ res;
                if (restoreFullData) {
                    OUT_EL(row02, s) = res;
                }
                IN_EL(row02, s) = res;
                VERBOSE_COUT("IN_EL(" << row02 << ", " << s << ") = " << DebugFormatBits(res) << Endl);
                row1 = (m + row1 - 2*dr) % m;
                row2 = (m + row2 - 2*dr) % m;
                row01 = (m + row1 - r) % m;
                row02 = (row2 + r) % m;
            }
            VERBOSE_COUT_BLOCK_M2(true, IN_EL, IN_EL, IN_M, IN_M1, IN_M2);
            readPosition += ColumnSize;
        }
        VERBOSE_COUT("End of StarMainRestorePartsWholeSymmetric" << Endl);
        EoMainRestorePartsWhole<isStripe, restoreParts, restoreFullData, false, restoreParityParts>(data, bufferDataPart,
                                        partSet, readPositionStart, endBlockIdx, Min(r, t), Max(r,t));
#undef IN_M2
#undef IN_M1
#undef IN_M
#undef IN_EL
#undef OUT_EL
#undef OUT_EL_BLOCK
#undef OUT_EL_STRIPE
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarRestoreHorizontalPartWhole(char *data, TBufferDataPart &bufferDataPart, TDataPartSet& partSet,
                ui64 readPosition, ui32 endBlockIdx, ui32 missingDataPartIdxA, ui32 missingDataPartIdxB) {
        VERBOSE_COUT("Start of StarRestoreHorizontalPartWhole for blocks "
                        << missingDataPartIdxA << " " << missingDataPartIdxB << Endl);
        // Notation ised in this function is taken from article
        // Cheng Huang, Lihao Xu (2005, 4th USENIX Conf.) - STAR: An Efficient Coding Scheme...
        ui64 readPositionStart = readPosition;
        const ui32 m = Prime;
        const ui32 r = missingDataPartIdxA;
        const ui32 s = missingDataPartIdxB;
        const ui32 dr = (m + s - r) % m;
        // Use all whole columns of all the parts
#define OUT_EL_BLOCK(row, column) bufferDataPart[column][blockIdx * LineCount + (row)]
#define OUT_EL_STRIPE(row, column) *((ui64*)data + (blockIdx * LineCount + (row)) * DataParts + (column))
#define OUT_EL(row, column) (isStripe ? OUT_EL_STRIPE(row, column) : OUT_EL_BLOCK(row, column))
#define IN_EL(row, column) *((ui64*)(partSet.Parts[column].GetDataAt(readPosition + (row) * sizeof(ui64))))
#define IN_M(row) *((ui64*)(partSet.Parts[DataParts].GetDataAt(readPosition + (row) * sizeof(ui64))))
#define IN_M1(row) *((ui64*)(partSet.Parts[DataParts + 1].GetDataAt(readPosition + (row) * sizeof(ui64))))
#define IN_M2(row) *((ui64*)(partSet.Parts[DataParts + 2].GetDataAt(readPosition + (row) * sizeof(ui64))))
        for (ui64 blockIdx = 0; blockIdx < endBlockIdx; ++blockIdx) {
            VERBOSE_COUT_BLOCK_M2(true, IN_EL, IN_EL, IN_M, IN_M1, IN_M2);
            // 1) Adjusters recovery
            ui64 adj12 = 0;
            for (ui32 i = 0; i < LineCount; ++i) {
                adj12 ^= IN_M1(i) ^ IN_M2(i);
            }
            VERBOSE_COUT("adj12# " << DebugFormatBits(adj12) << Endl);
            // 2) Syndrome calculation
            ui64 s1[MAX_LINES_IN_BLOCK];
            ui64 s2[MAX_LINES_IN_BLOCK];
            //ui32 row_adj;
            for (ui32 i = 0; i < LineCount; ++i) {
                IN_M(i) = 0;
                s1[i] = IN_M1(i);
                s2[i] = IN_M2(i);
            }
            s1[m - 1] = 0;
            s2[m - 1] = 0;
            ui32 row;
            for (ui32 j = 0; j < DataParts; ++j) {
                if (j == r || j == s) {
                    continue;
                }
                for (ui32 i = 0; i < LineCount; ++i) {
                        ui64 data_tmp = IN_EL(i, j);
                        IN_M(i) ^= data_tmp; // Store horizontal syndrome directly in M-column
                        if (restoreFullData) {
                            OUT_EL(i, j) = data_tmp;
                        }
                        row = (i + j) % m;
                        s1[row] ^= data_tmp;
                        row = (m + i - j) % m;
                        s2[row] ^= data_tmp;
                }
            }
            for (ui32 i = 0; i < m; ++i) {
                VERBOSE_COUT("s1[" << i << "] = " << DebugFormatBits(s1[i]) << ", ");
                VERBOSE_COUT("s2[" << i << "] = " << DebugFormatBits(s2[i]) << Endl);
            }
            // 3) Compute all row pairs
            ui32 row1 = (m - 1 + r) % m;
            ui32 row2 = (m + m - 1 - r - dr) % m;
            ui32 row3 = (row2 + r) % m;
            ui64 res = 0;
            for (ui32 i = 0; i < LineCount; ++i) {
                res = s1[row1] ^ s2[row2] ^ adj12 ^ res;
                IN_M(row3) ^= res;
                VERBOSE_COUT("IN_M(" << row3 << ") = " << DebugFormatBits(IN_M(row3)) << Endl);
                //row1 = (m + row1 - dr) % m;
                VERBOSE_COUT("row1,2,3# " << row1 << " " << row2 << " " << row3 << Endl);
                row1 = (m + row1 - dr) % m;
                row2 = (m + row2 - dr) % m;
                row3 = (m + row3 - dr) % m;
            }
            VERBOSE_COUT_BLOCK_M2(true, IN_EL, IN_EL, IN_M, IN_M1, IN_M2);
            readPosition += ColumnSize;
        }
        VERBOSE_COUT("End of StarRestoreHorizontalPartWhole" << Endl);
        EoMainRestorePartsWhole<isStripe, restoreParts, restoreFullData, false, restoreParityParts>(data, bufferDataPart,
                                        partSet, readPositionStart, endBlockIdx, r, s);
#undef IN_M2
#undef IN_M1
#undef IN_M
#undef IN_EL
#undef OUT_EL
#undef OUT_EL_BLOCK
#undef OUT_EL_STRIPE
    }


    template <bool isStripe, bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
    void EoMainRestorePartsWhole(char *data, TBufferDataPart &bufferDataPart, TDataPartSet& partSet, ui64 readPosition,
            ui32 endBlockIdx, ui32 missingDataPartIdxA, ui32 missingDataPartIdxB) {
        VERBOSE_COUT("Start of EoMainRestorePartsWhole" << Endl);
        ui32 lastColumn = reversed ? DataParts + 2 : DataParts + 1;
        const ui32 m = Prime;
        // Use all whole columns of all the parts
#define RIGHT_ROW(row) (reversed ? LineCount - 1 - (row) : (row))
#define OUT_EL_BLOCK(row, column) bufferDataPart[column][blockIdx * LineCount + RIGHT_ROW(row)]
#define OUT_EL_STRIPE(row, column) *((ui64*)data + (blockIdx * LineCount + RIGHT_ROW(row)) * DataParts + (column))
#define OUT_EL(row, column) (isStripe ? OUT_EL_STRIPE((row), column) : OUT_EL_BLOCK((row), (column)))
#define IN_EL(row, column) *((ui64*)(partSet.Parts[column].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define IN_M(row) *((ui64*)(partSet.Parts[DataParts].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define IN_M12(row) *((ui64*)(partSet.Parts[lastColumn].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
        for (ui64 blockIdx = 0; blockIdx < endBlockIdx; ++blockIdx) {
            VERBOSE_COUT_BLOCK(true, IN_EL, IN_EL, IN_M, IN_M12);
            // compute diagonal partiy s
            ui64 s = 0;
            ui64 s0[MAX_LINES_IN_BLOCK];
            for (ui32 l = 0; l < LineCount; ++l) {
                ui64 tmp = IN_M(l);
                s0[l] = tmp;
                s ^= tmp;
                s ^= IN_M12(l);
                VERBOSE_COUT("Diag [l,m] s:" << DebugFormatBits(s) << Endl);
            }

            // compute horizontal syndromes s0
            for (ui32 t = 0; t < DataParts; ++t) {
                if (t == missingDataPartIdxA || t == missingDataPartIdxB) {
                    continue;
                }
                for (ui32 l = 0; l < LineCount; ++l) {
                    ui64 val = IN_EL(l, t);
                    s0[l] ^= val;
                    if (restoreFullData) {
                        OUT_EL(l, t) = val;
                    }
                }
            }

            // compute diagonal syndromes s1
            ui64 s1[MAX_LINES_IN_BLOCK];
            for (ui32 u = 0; u < m; ++u) {
                s1[u] = s;
                VERBOSE_COUT("S1 = s = " << DebugFormatBits(s1[u]) << Endl);
                if (u < LineCount) {
                    s1[u] ^= IN_M12(u);
                    VERBOSE_COUT("S1 ^= a[" << u << ", m+1] = " << DebugFormatBits(s1[u]) << Endl);
                }
                for (ui32 l = 0; l < missingDataPartIdxA; ++l) {
                    ui32 idx = (m + u - l) % m;
                    if (idx < LineCount) {
                        ui64 val = IN_EL(idx, l);
                        s1[u] ^= val;
                    }
                    VERBOSE_COUT("S1 ^= a[" << idx << ", " << l << "] = " << DebugFormatBits(s1[u]) << Endl);
                }
                for (ui32 l = missingDataPartIdxA + 1; l < missingDataPartIdxB; ++l) {
                    ui32 idx = (m + u - l) % m;
                    if (idx < LineCount) {
                        ui64 val = IN_EL(idx, l);
                        s1[u] ^= val;
                    }
                    VERBOSE_COUT("S1 ^= a[" << idx << ", " << l << "] = " << DebugFormatBits(s1[u]) << Endl);
                }
                for (ui32 l = missingDataPartIdxB + 1; l < DataParts; ++l) {
                    ui32 idx = (m + u - l) % m;
                    if (idx < LineCount) {
                        ui64 val = IN_EL(idx, l);
                        s1[u] ^= val;
                    }
                    VERBOSE_COUT("S1 ^= a[" << idx << ", " << l << "] = " << DebugFormatBits(s1[u]) << Endl);
                }
                VERBOSE_COUT("S1[" << u << "] = " << DebugFormatBits(s1[u]) << Endl);
            }

            s = (m - (missingDataPartIdxB - missingDataPartIdxA) - 1) % m;
            ui64 aVal = 0;
            do {
                if (s < LineCount) {
                    ui64 bVal = s1[(missingDataPartIdxB + s) % m];
                    VERBOSE_COUT("bVal = s1[" << ((missingDataPartIdxB + s ) % m) << "] = " << DebugFormatBits(bVal)
                        << Endl);
                    ui32 bRow = (m + s + (missingDataPartIdxB - missingDataPartIdxA)) % m;
                    if (bRow < LineCount) {
                        VERBOSE_COUT("read [" << bRow << ", " << missingDataPartIdxA << "] = ");
                        bVal ^= aVal;
                        if (restoreParts) {
                            VERBOSE_COUT("i " << DebugFormatBits(IN_EL(bRow, missingDataPartIdxA)) << Endl);
                        } else {
                            VERBOSE_COUT("o " << DebugFormatBits(OUT_EL_STRIPE(bRow,missingDataPartIdxA)) << Endl);
                        }
                    }
                    if (restoreParts) {
                        IN_EL(s, missingDataPartIdxB) = bVal;
                        VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxB << "] = " << DebugFormatBits(bVal)
                            << Endl);
                    }
                    if (restoreFullData) {
                        OUT_EL(s, missingDataPartIdxB) = bVal;
                        VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxB << "] = " << DebugFormatBits(bVal)
                            << Endl);
                    }

                    aVal = s0[s];
                    VERBOSE_COUT("aVal = s0[" << s << "] = " << DebugFormatBits(aVal) << Endl);
                    VERBOSE_COUT("read [" << s << ", " << missingDataPartIdxB << "] = ");
                    aVal ^= bVal;
                    if (restoreParts) {
                        VERBOSE_COUT("i " << DebugFormatBits(IN_EL(s,missingDataPartIdxB)) << Endl);
                    } else {
                        VERBOSE_COUT("o " << DebugFormatBits(OUT_EL_STRIPE(s,missingDataPartIdxB)) << Endl);
                    }

                    if (restoreParts) {
                        IN_EL(s, missingDataPartIdxA) = aVal;
                        VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxA << "] = " << DebugFormatBits(bVal)
                            << Endl);
                    }
                    if (restoreFullData) {
                        OUT_EL(s, missingDataPartIdxA) = aVal;
                        VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxA << "] = " << DebugFormatBits(bVal)
                            << Endl);
                    }
                }

                s = (m + s - (missingDataPartIdxB - missingDataPartIdxA)) % m;
            } while (s != m - 1);
                VERBOSE_COUT_BLOCK(restoreFullData, OUT_EL, IN_EL, IN_M, IN_M12);
#undef IN_M12
#undef IN_M
#undef IN_EL
#undef OUT_EL_BLOCK
#undef OUT_EL_STRIPE
            readPosition += ColumnSize;
        }
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarRestoreHorizontalPart(TDataPartSet& partSet, ui32 missingDataPartIdxA,
                                        ui32 missingDataPartIdxB) {
        // Read data and parity
        VERBOSE_COUT("StarRestoreHorizontalPart for " << missingDataPartIdxA << " " << missingDataPartIdxB << Endl);
        StarRestoreHorizontalPartWhole<isStripe, restoreParts, restoreFullData, restoreParityParts>(Data, BufferDataPart,
                    partSet, 0ull, WholeBlocks, missingDataPartIdxA, missingDataPartIdxB);

        if (TailSize) {
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers<isStripe>(lastBlockSource, bufferDataPart);

            StarRestoreHorizontalPartWhole<isStripe, restoreParts, restoreFullData, restoreParityParts>(lastBlockSource,
                        bufferDataPart, partSet, WholeBlocks * ColumnSize, 1, missingDataPartIdxA,
                        missingDataPartIdxB);

            if (restoreFullData) {
                PlaceLastBlock<isStripe>(bufferDataPart, lastBlockSource);
            }
        }
        if (restoreParts) {
            if (missingDataPartIdxA < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxA);
            }
            if (missingDataPartIdxB < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxB);
            }
        }
    }


    template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarMainRestorePartsSymmetric(TDataPartSet& partSet, ui32 missingDataPartIdxA,
                                        ui32 missingDataPartIdxB, ui32 missingDataPartIdxC) {
        // Read data and parity
        VERBOSE_COUT("StarMainRestorePartsSymmetric" << Endl);
        StarMainRestorePartsWholeSymmetric<isStripe, restoreParts, restoreFullData, restoreParityParts>(Data, BufferDataPart,
                    partSet, 0ull, WholeBlocks, missingDataPartIdxA, missingDataPartIdxB, missingDataPartIdxC);

        if (TailSize) {
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers<isStripe>(lastBlockSource, bufferDataPart);

            StarMainRestorePartsWholeSymmetric<isStripe, restoreParts, restoreFullData, restoreParityParts>(lastBlockSource,
                        bufferDataPart, partSet, WholeBlocks * ColumnSize, 1, missingDataPartIdxA,
                        missingDataPartIdxB, missingDataPartIdxC);

            if (restoreFullData) {
                PlaceLastBlock<isStripe>(bufferDataPart, lastBlockSource);
            }
        }
        if (restoreParts) {
            if (missingDataPartIdxA < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxA);
            }
            if (missingDataPartIdxB < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxB);
            }
            if (missingDataPartIdxC < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxC);
            }
        }
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
    void EoMainRestoreParts(TDataPartSet& partSet, ui32 missingDataPartIdxA, ui32 missingDataPartIdxB) {
        // Read data and parity
        VERBOSE_COUT("EoMainRestorePart" << Endl);
        TRACE("EoMainRestorePart fullSize# " << partSet.FullDataSize
                << " partSet p0 Size# " << partSet.Parts[0].Size
                << " p1 Size# " << partSet.Parts[1].Size
                << " p2 Size# " << partSet.Parts[2].Size << Endl);
        ui32 presentPartIdx = (missingDataPartIdxA == 0 ?
                (missingDataPartIdxB == 1 ? 2 : 1) : \
                (missingDataPartIdxB == 0 ? 1 : 0));
        Y_ABORT_UNLESS(partSet.Parts[presentPartIdx].Offset % ColumnSize == 0);
        ui64 readPosition = partSet.Parts[presentPartIdx].Offset;
        ui64 wholeBlocks = Min(WholeBlocks - readPosition / ColumnSize, partSet.Parts[presentPartIdx].Size / ColumnSize);

        TRACE("wholeBlocks# " << wholeBlocks << " blockSize# " << BlockSize << Endl);
        EoMainRestorePartsWhole<isStripe, restoreParts, restoreFullData, reversed, restoreParityParts>(Data, BufferDataPart,
                partSet, readPosition, wholeBlocks, missingDataPartIdxA, missingDataPartIdxB);

        if (TailSize && (partSet.Parts[presentPartIdx].Size + readPosition > WholeBlocks * ColumnSize)) {
            TRACE("EoMainRestoreParts restore tail" << Endl);
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers<isStripe>(lastBlockSource, bufferDataPart);

            EoMainRestorePartsWhole<isStripe, restoreParts, restoreFullData, reversed, restoreParityParts>(lastBlockSource,
                    bufferDataPart, partSet, WholeBlocks * ColumnSize, 1, missingDataPartIdxA, missingDataPartIdxB);

            if (restoreFullData) {
                PlaceLastBlock<isStripe>(bufferDataPart, lastBlockSource);
            }
        }

        if (restoreParts) {
            if (missingDataPartIdxA < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxA);
            }
            if (missingDataPartIdxB < partSet.Parts.size()) {
                PadAndCrcPart(partSet, missingDataPartIdxB);
            }
        }
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void XorRestorePartWhole(char* data, TBufferDataPart &bufferDataPart, TDataPartSet& partSet,
            ui64 readPosition, ui32 beginBlockIdx, ui32 endBlockIdx, ui32 missingDataPartIdx) {
        VERBOSE_COUT("XorRestorePartWhole: read:" << readPosition << " LineCount: " << LineCount << Endl);
        ui64 writePosition = 0;
        for (ui64 blockIdx = beginBlockIdx; blockIdx < endBlockIdx; ++blockIdx) {
#if IS_VERBOSE
            for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                for (ui32 part = 0; part <= DataParts; ++part) {
                    if (part != missingDataPartIdx) {
                        ui64 partData = *reinterpret_cast<const ui64*>(partSet.Parts[part].GetDataAt(readPosition +
                                lineIdx * sizeof(ui64)));
                        VERBOSE_COUT(DebugFormatBits(partData) << ", ");
                    } else {
                        VERBOSE_COUT(", ");
                    }
                }
                VERBOSE_COUT(Endl);
            }
            VERBOSE_COUT(Endl);
#endif
            for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                ui64 restoredData = 0;
                ui64 *destination;
                if (isStripe) {
                    destination = (ui64*)data + writePosition;
                }
                for (ui32 part = 0; part < DataParts; ++part) {
                    if (part != missingDataPartIdx) {
                        ui64 partData = *reinterpret_cast<const ui64*>(partSet.Parts[part].GetDataAt(readPosition));
                        restoredData ^= partData;
                        if (restoreFullData) {
                            if (isStripe) {
                                destination[part] = partData;
                            } else {
                                bufferDataPart[part][writePosition] = partData;
                            }
                        }
                    }
                }
                if (missingDataPartIdx < DataParts) {
                    ui64 partData = *reinterpret_cast<const ui64*>(partSet.Parts[DataParts].GetDataAt(readPosition));
                    restoredData ^= partData;
                    if (restoreFullData) {
                        if (isStripe) {
                            destination[missingDataPartIdx] = restoredData;
                        } else {
                            bufferDataPart[missingDataPartIdx][writePosition] = restoredData;
                        }
                    }
                    if (restoreParts) {
                        *reinterpret_cast<ui64*>(partSet.Parts[missingDataPartIdx].GetDataAt(readPosition)) =
                            restoredData;
                    }
                } else if (restoreParts && missingDataPartIdx == DataParts) {
                    *reinterpret_cast<ui64*>(partSet.Parts[DataParts].GetDataAt(readPosition)) = restoredData;
                }
                readPosition += sizeof(ui64);
                if (restoreFullData) {
                    if (isStripe) {
                        writePosition += DataParts;
                    } else {
                        ++writePosition;
                    }
                }
            }
#if IS_VERBOSE
            VERBOSE_COUT("Out: " << Endl);
            for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                for (ui32 part = 0; part <= DataParts; ++part) {
                    ui64 partData = *reinterpret_cast<const ui64*>(
                        partSet.Parts[part].GetDataAt(readPosition - ColumnSize + lineIdx * sizeof(ui64)));
                    VERBOSE_COUT(DebugFormatBits(partData) << ", ");
                }
                VERBOSE_COUT(Endl);
            }
            VERBOSE_COUT(Endl);
#endif
        }
    }

    template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void XorRestorePart(TDataPartSet &partSet, ui32 missingDataPartIdx) {
        // Read data and parity
        VERBOSE_COUT("XorRestorePart" << Endl);
        TRACE("XorRestorePart partSet p0 Size# " << partSet.Parts[0].Size
                << " p1 Size# " << partSet.Parts[1].Size << Endl);
        ui32 presentPartIdx = (missingDataPartIdx == 0 ? 1 : 0);
        Y_ABORT_UNLESS(partSet.Parts[presentPartIdx].Offset % ColumnSize == 0);
        ui64 readPosition = partSet.Parts[presentPartIdx].Offset;
        ui64 beginBlockIdx = readPosition / ColumnSize;
        ui64 wholeBlocks = Min(WholeBlocks - readPosition / ColumnSize, partSet.Parts[presentPartIdx].Size / ColumnSize);
        TRACE("XorRestore beginBlockIdx# " << beginBlockIdx << " wholeBlocks# " << wholeBlocks << Endl);
        XorRestorePartWhole<isStripe, restoreParts, restoreFullData, restoreParityParts>(Data, BufferDataPart, partSet, readPosition,
            beginBlockIdx, beginBlockIdx + wholeBlocks, missingDataPartIdx);

        if (TailSize && (partSet.Parts[presentPartIdx].Size + readPosition > WholeBlocks * ColumnSize)) {
            TRACE("Restore tail, restoreFullData# " << restoreFullData << " restoreParts# " << restoreParts << Endl);
            char lastBlockSource[MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64)] = {};
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers<isStripe>(lastBlockSource, bufferDataPart);

            XorRestorePartWhole<isStripe, restoreParts, restoreFullData, restoreParityParts>(lastBlockSource, bufferDataPart,
                partSet, WholeBlocks * ColumnSize, WholeBlocks, WholeBlocks + 1, missingDataPartIdx);

            if (restoreFullData) {
                PlaceLastBlock<isStripe>(bufferDataPart, lastBlockSource);
            }
        }

        if (restoreParts && missingDataPartIdx < partSet.Parts.size()) {
            if (restoreParityParts || missingDataPartIdx < DataParts) {
                PadAndCrcPart(partSet, missingDataPartIdx);
            }
        }
    }

    void PadAndCrcPart(TDataPartSet &inOutPartSet, ui32 partIdx) {
        if (inOutPartSet.IsFragment) {
            return;
        }
        switch (CrcMode) {
        case TErasureType::CrcModeNone:
            return;
        case TErasureType::CrcModeWholePart:
            if (DataSize) {
                PadAndCrcAtTheEnd(inOutPartSet.Parts[partIdx].GetDataAt(0), PartUserSize, PartContainerSize);
            } else {
                memset(inOutPartSet.Parts[partIdx].GetDataAt(0), 0, PartContainerSize);
            }
            return;
        }
        ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)CrcMode;
    }
};

void PadAndCrcParts(TErasureType::ECrcMode crcMode, const TBlockParams &p, TDataPartSet &inOutPartSet) {
    if (inOutPartSet.IsFragment) {
        return;
    }
    switch (crcMode) {
    case TErasureType::CrcModeNone:
        return;
    case TErasureType::CrcModeWholePart:
        if (p.DataSize) {
            for (ui32 i = 0; i < p.TotalParts; ++i) {
                PadAndCrcAtTheEnd(inOutPartSet.Parts[i].GetDataAt(0), p.PartUserSize, p.PartContainerSize);
            }
        } else {
            for (ui32 i = 0; i < p.TotalParts; ++i) {
                memset(inOutPartSet.Parts[i].GetDataAt(0), 0, p.PartContainerSize);
            }
        }
        return;
    }
    ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
}

template <bool isStripe>
void StarBlockSplit(TErasureType::ECrcMode crcMode, const TErasureType &type, TRope &buffer,
        TDataPartSet &outPartSet) {
    TBlockParams p(crcMode, type, buffer.size());

    // Prepare input data pointers
    p.PrepareInputDataPointers<isStripe>(buffer.UnsafeGetContiguousSpanMut().data());

    outPartSet.FullDataSize = buffer.size();
    outPartSet.PartsMask = ~((~(ui32)0) << p.TotalParts);
    outPartSet.Parts.resize(p.TotalParts);
    for (ui32 i = 0; i < p.TotalParts; ++i) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(outPartSet.Parts[i], p.PartContainerSize);
    }
    outPartSet.MemoryConsumed = p.TotalParts * outPartSet.Parts[0].MemoryConsumed();

    p.StarSplit<isStripe, false>(outPartSet);
    PadAndCrcParts(crcMode, p, outPartSet);
}

template <bool isStripe>
void EoBlockSplit(TErasureType::ECrcMode crcMode, const TErasureType &type, TRope &buffer,
        TDataPartSet &outPartSet) {
    TBlockParams p(crcMode, type, buffer.size());

    // Prepare input data pointers
    p.PrepareInputDataPointers<isStripe>(buffer.UnsafeGetContiguousSpanMut().data());

    // Prepare if not yet
    if (!outPartSet.IsSplitStarted()) {
        outPartSet.StartSplit(p.WholeBlocks);

        outPartSet.FullDataSize = buffer.size();
        outPartSet.PartsMask = ~((~(ui32)0) << p.TotalParts);
        outPartSet.Parts.resize(p.TotalParts);
        for (ui32 i = 0; i < p.TotalParts; ++i) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(outPartSet.Parts[i], p.PartContainerSize);
        }
        outPartSet.MemoryConsumed = p.TotalParts * outPartSet.Parts[0].MemoryConsumed();
    }

    p.EoSplit<isStripe, false, true>(outPartSet);

    // Finalize if split has been done to completion
    if (outPartSet.IsSplitDone()) {
        PadAndCrcParts(crcMode, p, outPartSet);
    }
}

template <bool isStripe>
void XorBlockSplit(TErasureType::ECrcMode crcMode, const TErasureType &type, TRope& buffer,
        TDataPartSet& outPartSet) {
    TBlockParams p(crcMode, type, buffer.size());

    // Prepare input data pointers
    p.PrepareInputDataPointers<isStripe>(buffer.UnsafeGetContiguousSpanMut().data());

    outPartSet.FullDataSize = buffer.size();
    outPartSet.PartsMask = ~((~(ui32)0) << p.TotalParts);
    outPartSet.Parts.resize(p.TotalParts);
    for (ui32 i = 0; i < p.TotalParts; ++i) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(outPartSet.Parts[i], p.PartContainerSize);
    }
    outPartSet.MemoryConsumed = p.TotalParts * outPartSet.Parts[0].MemoryConsumed();

    p.XorSplit<isStripe>(outPartSet);
    PadAndCrcParts(crcMode, p, outPartSet);
}

template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
void EoBlockRestore(TErasureType::ECrcMode crcMode, const TErasureType &type, TDataPartSet& partSet) {
    TRope &outBuffer = partSet.FullDataFragment.OwnedString;
    ui32 totalParts = type.TotalPartCount();
    Y_ABORT_UNLESS(partSet.Parts.size() >= totalParts);

    ui32 missingDataPartIdxA = totalParts;
    ui32 missingDataPartIdxB = totalParts;
    ui32 missingDataPartCount = 0;
    ui64 expectedPartSize = type.PartSize(crcMode, partSet.FullDataSize);
    ui32 i = 0;
    for (; i < totalParts; ++i) {
        if (!(partSet.PartsMask & (1 << i))) {
            missingDataPartIdxA = i;
            ++missingDataPartCount;
            break;
        } else {
            Y_ABORT_UNLESS(partSet.Parts[i].size() == expectedPartSize, "partSet.Parts[%" PRIu32 "].size(): %" PRIu64
                " expectedPartSize: %" PRIu64 " erasure: %s partSet.FullDataSize: %" PRIu64,
                (ui32)i, (ui64)partSet.Parts[i].size(), expectedPartSize, type.ErasureName[type.GetErasure()].data(),
                (ui64)partSet.FullDataSize);
        }
    }
    ++i;
    for (; i < totalParts; ++i) {
        if (!(partSet.PartsMask & (1 << i))) {
            missingDataPartIdxB = i;
            ++missingDataPartCount;
        } else {
            Y_ABORT_UNLESS(partSet.Parts[i].size() == expectedPartSize, "partSet.Parts[%" PRIu32 "].size()# %" PRIu32
                " != expectedPartSize# %" PRIu32 " erasure: %s partSet.FullDataSize: %" PRIu64,
                (ui32)i, (ui32)partSet.Parts[i].size(), (ui32)expectedPartSize, type.ErasureName[type.GetErasure()].data(),
                (ui64)partSet.FullDataSize);
        }
    }
    Y_ABORT_UNLESS(missingDataPartCount <= 2);

    ui64 dataSize = partSet.FullDataSize;
    if (restoreParts) {
        if (missingDataPartIdxA != totalParts) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(partSet.Parts[missingDataPartIdxA], expectedPartSize);
        }
        if (missingDataPartIdxB != totalParts) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(partSet.Parts[missingDataPartIdxB], expectedPartSize);
        }
    }
    if (restoreFullData) {
        Refurbish(outBuffer, dataSize);
    } else if (missingDataPartCount == 0) {
        return;
    }

    if (missingDataPartCount == 2) {
        VERBOSE_COUT("missing parts " << missingDataPartIdxA << " and " << missingDataPartIdxB << Endl);
    } else if (missingDataPartCount == 1) {
        VERBOSE_COUT("missing part " << missingDataPartIdxA << Endl);
    }
    TBlockParams p(crcMode, type, dataSize);

    // Restore the fast way if all data parts are present
    if (missingDataPartCount == 0 ||
                (!restoreParts && missingDataPartIdxA >= p.TotalParts - 2)) {
        VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
        if (isStripe) {
            p.PrepareInputDataPointers<isStripe>(outBuffer.GetContiguousSpanMut().data());
            p.XorRestorePart<isStripe, false, true, false>(partSet, p.DataParts);
        } else {
            p.GlueBlockParts(outBuffer.GetContiguousSpanMut().data(), partSet);
        }
        return;
    }

    // Prepare output data pointers
    if (restoreFullData) {
        p.PrepareInputDataPointers<isStripe>(outBuffer.GetContiguousSpanMut().data());
    }

    // Consider failed disk cases
    // a) < m
    // b) m
    //    'xor-restore'
    // d) m, m+1
    //    TODO: 1-pass
    //    just glue the data
    //    use 'eo split' to restore the remaining parts
    // f) <m, m+1
    //    use 'xor-restore' to restore the data
    //    TODO: use 2-nd part of 'eo-split' to restore m+1 part
    //    TODO: 1-pass
    if (missingDataPartIdxA <= p.DataParts && missingDataPartIdxB >= p.TotalParts - 1) {
        TRope temp;
        TRope &buffer = restoreFullData ? outBuffer : temp;
        if (!restoreFullData && restoreParts && missingDataPartIdxB == p.TotalParts - 1) {
            // The (f1) case, but no full data needed, only parts
            TRACE("case# f1" << Endl);
            VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
            if (isStripe) {
                Refurbish(buffer, dataSize);
                p.PrepareInputDataPointers<isStripe>(buffer.GetContiguousSpanMut().data());
            }
            p.XorRestorePart<isStripe, true, false, false>(partSet, missingDataPartIdxA);
            TRACE("case# f1 split" << Endl);
            p.EoSplit<isStripe, true>(partSet);
            p.PadAndCrcPart(partSet, missingDataPartIdxA);
            p.PadAndCrcPart(partSet, missingDataPartIdxB);
        } else {
            // Cases (a), (b) and (d2), case (f2) with full data and maybe parts needed
            TRACE("case# a b d2 f2" << Endl);
            VERBOSE_COUT(__LINE__ << " of " << __FILE__ << " missing " << missingDataPartIdxA << Endl);
            p.XorRestorePart<isStripe, restoreParts, restoreFullData, restoreParityParts>(partSet, missingDataPartIdxA);
            if (restoreParts && missingDataPartIdxB == p.TotalParts - 1 && restoreParityParts) {
                // The (d2a) or (f2a) case with full data and parts needed
                TRACE("case# d2a f2a" << Endl);
                VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
                p.EoSplit<isStripe, true>(partSet);
                p.PadAndCrcPart(partSet, missingDataPartIdxB);
            }
            if (restoreParts) {
                p.PadAndCrcPart(partSet, missingDataPartIdxA);
            }
        }
        return;
    }

    // c) m+1
    //    TODO: use 2-nd part of 'eo-split' to restore m+1 part, while glueing the data
    //    TODO: 1-pass
    //    just glue the data
    //    use 'eo split' to restore the missing part
    if (missingDataPartIdxA == p.TotalParts - 1 && missingDataPartIdxB == p.TotalParts) {
        TRACE("case# c" << Endl);
        VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
        TRope temp;
        TRope &buffer = restoreFullData ? outBuffer : temp;
        if (!restoreFullData) {
            TRACE(__LINE__ << Endl);
            if (!restoreParityParts) {
                TRACE(__LINE__ << Endl);
                return;
            }
            TRACE(__LINE__ << Endl);
            if (isStripe) {
                Refurbish(buffer, dataSize);
            }
        }
        if (isStripe) {
            TRACE(__LINE__ << Endl);
            p.PrepareInputDataPointers<isStripe>(buffer.GetContiguousSpanMut().data());
            p.XorRestorePart<isStripe, false, true, false>(partSet, p.DataParts);
        } else {
            TRACE(__LINE__ << Endl);
            if (restoreFullData) {
                p.GlueBlockParts(buffer.GetContiguousSpanMut().data(), partSet);
            }
        }
        if (restoreParts) {
            TRACE(__LINE__ << Endl);
            VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
            p.EoSplit<isStripe, true>(partSet);
            p.PadAndCrcPart(partSet, missingDataPartIdxA);
        }
        return;
    }

    // e) <m, m
    //    TODO: 1-pass
    //    use diagonal-sums to restore the data
    //    use 'xor restore' with 'restore part' to restore m part
    if (missingDataPartIdxA < p.DataParts && missingDataPartIdxB == p.DataParts) {
        TRACE(__LINE__ << " of " << __FILE__ << " case# e restore part missing# " << missingDataPartIdxA << ", " << missingDataPartIdxB <<
            " restoreParts# " << restoreParts
            << " restoreParityParts# " << restoreParityParts
            << " restoreFullData# " << restoreFullData << Endl);
        p.EoDiagonalRestorePart<isStripe, restoreParts, restoreFullData, false, restoreParityParts>(partSet, missingDataPartIdxA);
        if (restoreParts) {
            p.PadAndCrcPart(partSet, missingDataPartIdxA);
            if (restoreParityParts) {
                p.PadAndCrcPart(partSet, missingDataPartIdxB);
            }
        }
        return;
    }

    // g) <m, <m
    //    the main case :(
    TRACE("case# g" << Endl);
    VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
    Y_ABORT_UNLESS(missingDataPartIdxA < p.DataParts && missingDataPartIdxB < p.DataParts);
    p.EoMainRestoreParts<isStripe, restoreParts, restoreFullData, false, restoreParityParts>(partSet, missingDataPartIdxA,
            missingDataPartIdxB);
    if (restoreParts) {
        p.PadAndCrcPart(partSet, missingDataPartIdxA);
        p.PadAndCrcPart(partSet, missingDataPartIdxB);
    }
}

// restorePartiyParts may be set only togehter with restore parts
template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
void StarBlockRestore(TErasureType::ECrcMode crcMode, const TErasureType &type, TDataPartSet& partSet) {
    TRope &outBuffer = partSet.FullDataFragment.OwnedString;

    ui32 totalParts = type.TotalPartCount();
    Y_ABORT_UNLESS(partSet.Parts.size() == totalParts);

    ui32 missingDataPartIdxA = totalParts;
    ui32 missingDataPartIdxB = totalParts;
    ui32 missingDataPartIdxC = totalParts;
    ui32 missingDataPartCount = 0;
    ui64 expectedPartSize = type.PartSize(crcMode, partSet.FullDataSize); // ???
    ui32 i = 0;
    for (; i < totalParts; ++i) {
        if (!(partSet.PartsMask & (1 << i))) {
            missingDataPartIdxA = i;
            ++missingDataPartCount;
            break;
        } else {
            Y_ABORT_UNLESS(partSet.Parts[i].size() == expectedPartSize, "partSet.Parts[%" PRIu32 "].size(): %" PRIu64
                " expectedPartSize: %" PRIu64 " erasure: %s partSet.FullDataSize: %" PRIu64,
                (ui32)i, (ui64)partSet.Parts[i].size(), expectedPartSize, type.ErasureName[type.GetErasure()].data(),
                (ui64)partSet.FullDataSize);
        }
    }
    ++i;
    for (; i < totalParts; ++i) {
        if (!(partSet.PartsMask & (1 << i))) {
            missingDataPartIdxB = i;
            ++missingDataPartCount;
            break;
        } else {
            Y_ABORT_UNLESS(partSet.Parts[i].size() == expectedPartSize, "partSet.Parts[%" PRIu32 "].size()# %" PRIu32
                " != expectedPartSize# %" PRIu32 " erasure: %s partSet.FullDataSize: %" PRIu64,
                (ui32)i, (ui32)partSet.Parts[i].size(), (ui32)expectedPartSize, type.ErasureName[type.GetErasure()].data(),
                (ui64)partSet.FullDataSize);
        }
    }
    ++i;
    for (; i < totalParts; ++i) {
        if (!(partSet.PartsMask & (1 << i))) {
            missingDataPartIdxC = i;
            ++missingDataPartCount;
            break;
        } else {
            Y_ABORT_UNLESS(partSet.Parts[i].size() == expectedPartSize, "partSet.Parts[%" PRIu32 "].size()# %" PRIu32
                " != expectedPartSize# %" PRIu32 " erasure: %s partSet.FullDataSize: %" PRIu64,
                (ui32)i, (ui32)partSet.Parts[i].size(), (ui32)expectedPartSize, type.ErasureName[type.GetErasure()].data(),
                (ui64)partSet.FullDataSize);
        }
    }
    Y_ABORT_UNLESS(missingDataPartCount <= 3);

    if (restoreParts) {
        if (missingDataPartIdxA != totalParts) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(partSet.Parts[missingDataPartIdxA], expectedPartSize);
        }
        if (missingDataPartIdxB != totalParts) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(partSet.Parts[missingDataPartIdxB], expectedPartSize);
        }
        if (missingDataPartIdxC != totalParts) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(partSet.Parts[missingDataPartIdxC], expectedPartSize);
        }
    }
    if (missingDataPartCount == 3) {
        VERBOSE_COUT("missing parts " << missingDataPartIdxA << " and " << missingDataPartIdxB <<
            " and " << missingDataPartIdxC << Endl);
    } else if (missingDataPartCount == 2) {
        VERBOSE_COUT("missing parts " << missingDataPartIdxA << " and " << missingDataPartIdxB << Endl);
    } else if (missingDataPartCount == 1) {
        VERBOSE_COUT("missing part " << missingDataPartIdxA << Endl);
    }

    ui64 dataSize = partSet.FullDataSize;
    TBlockParams p(crcMode, type, dataSize);
    if (restoreFullData) {
        Refurbish(outBuffer, dataSize);
        p.PrepareInputDataPointers<isStripe>(outBuffer.GetContiguousSpanMut().data());
    } else if (missingDataPartCount == 0) {
        return;
    }

    // Restore the fast way if all data parts are present
    if (missingDataPartCount == 0 ||
            (!restoreParts && missingDataPartIdxA >= p.DataParts)) {
        VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
        if (isStripe) {
            p.PrepareInputDataPointers<isStripe>(outBuffer.GetContiguousSpanMut().data());
            p.XorRestorePart<isStripe, false, true, false>(partSet, p.DataParts);
        } else {
            p.GlueBlockParts(outBuffer.GetContiguousSpanMut().data(), partSet);
        }
        return;
    }


    // All possible failures of 2 disks which EVENODD capable to handle
    if (missingDataPartCount <= 2 && missingDataPartIdxA != p.TotalParts - 1
            && missingDataPartIdxB != p.TotalParts - 1) {
        if (p.DataParts == 4) {
            TErasureType typeEO(TErasureType::EErasureSpecies::Erasure4Plus2Block);
            EoBlockRestore<isStripe, restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 3) {
            TErasureType typeEO(TErasureType::EErasureSpecies::Erasure3Plus2Block);
            EoBlockRestore<isStripe, restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 2) {
            TErasureType typeEO(TErasureType::EErasureSpecies::Erasure2Plus2Block);
            EoBlockRestore<isStripe, restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        }
        return;
    }
    if (missingDataPartIdxA == p.TotalParts - 1
            || missingDataPartIdxB == p.TotalParts - 1
            || missingDataPartIdxC == p.TotalParts - 1) {
        // Possible combinations handled in this branch
        // '+' stands for part, which is present for sure,
        // '-' stands for part, which is missing for sure,
        // series of 0, 1 and 2 means that there are n missing parts in this region
        // 0 0 0 0   0 0 -    or     1 1 1 1   1 1 -    or     2 2 2 2   2 2 -
        if (p.DataParts == 4) {
            TErasureType typeEO(TErasureType::EErasureSpecies::Erasure4Plus2Block);
            EoBlockRestore<isStripe, restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 3) {
            TErasureType typeEO(TErasureType::EErasureSpecies::Erasure3Plus2Block);
            EoBlockRestore<isStripe, restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 2) {
            TErasureType typeEO(TErasureType::EErasureSpecies::Erasure2Plus2Block);
            EoBlockRestore<isStripe, restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        }
        if (restoreParts) {
            if (restoreParityParts) {
                p.StarSplit<isStripe, true>(partSet);
            }
            if (missingDataPartIdxA < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxA);
            }
            if (missingDataPartIdxB < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxB);
            }
            if (missingDataPartIdxC < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxC);
            }
        }
        return;
    }
    // There are remain only cases with missingDataPartCount == 3
    if ( missingDataPartIdxC == p.DataParts + 1) {
        if (missingDataPartIdxB < p.DataParts) {
            // 2 2 2 2   + - +
            // "It can be decoded with slightly modification of the EVENODD decoding" (c)
            p.EoMainRestoreParts<isStripe, restoreParts, restoreFullData, true, restoreParityParts>(partSet, missingDataPartIdxA,
                    missingDataPartIdxB);
        } else {
            // 1 1 1 1   - - +
            p.EoDiagonalRestorePart<isStripe, restoreParts, restoreFullData, true, restoreParityParts>(partSet, missingDataPartIdxA);
        }
        if (restoreParts) {
            if (restoreParityParts) {
                p.StarSplit<isStripe, !restoreFullData>(partSet);
            }
            if (missingDataPartIdxA < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxA);
            }
            if (missingDataPartIdxB < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxB);
            }
            if (missingDataPartIdxC < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxC);
            }
        }
        return;
    }
    if (missingDataPartIdxC == p.DataParts) {
        // 2 2 2 2   - + +
        if (! restoreParts) {
            TRACE("Line# " << __LINE__ << Endl);
            Refurbish(partSet.Parts[missingDataPartIdxC], expectedPartSize);
        }
        p.StarRestoreHorizontalPart<isStripe, restoreParts, restoreFullData, restoreParityParts>(partSet,
                                                    missingDataPartIdxA, missingDataPartIdxB);
        if (restoreParts) {
            if (missingDataPartIdxA < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxA);
            }
            if (missingDataPartIdxB < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxB);
            }
            if (missingDataPartIdxC < (restoreParityParts ? p.TotalParts : p.DataParts)) {
                p.PadAndCrcPart(partSet, missingDataPartIdxC);
            }
        }
        return;
    }

    VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
    Y_ABORT_UNLESS(missingDataPartIdxA < p.DataParts && missingDataPartIdxB < p.DataParts
                    && missingDataPartIdxC < p.DataParts);
    // Two possible cases:
    //     - Symmetric
    //     - Asymmetric
    // But for m = 5 it is always possible to change asymmetric to symmetric by shifting
    ui32 m = ErasureSpeciesParameters[TErasureType::EErasureSpecies::Erasure4Plus3Block].Prime;
    while ((m + missingDataPartIdxB - missingDataPartIdxA) % m != (m + missingDataPartIdxC - missingDataPartIdxB) % m ) {
        ui32 tmp = missingDataPartIdxA;
        missingDataPartIdxA = missingDataPartIdxB;
        missingDataPartIdxB = missingDataPartIdxC;
        missingDataPartIdxC = tmp;
    }
    if (! restoreParts) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(partSet.Parts[missingDataPartIdxB], expectedPartSize);
    }
    p.StarMainRestorePartsSymmetric<isStripe, restoreParts, restoreFullData, restoreParityParts>(partSet,
                                missingDataPartIdxA, missingDataPartIdxB, missingDataPartIdxC);
    if (restoreParts) {
        if (missingDataPartIdxA < (restoreParityParts ? p.TotalParts : p.DataParts)) {
            p.PadAndCrcPart(partSet, missingDataPartIdxA);
        }
        if (missingDataPartIdxB < (restoreParityParts ? p.TotalParts : p.DataParts)) {
            p.PadAndCrcPart(partSet, missingDataPartIdxB);
        }
        if (missingDataPartIdxC < (restoreParityParts ? p.TotalParts : p.DataParts)) {
            p.PadAndCrcPart(partSet, missingDataPartIdxC);
        }
    }
}

template <bool isStripe, bool restoreParts, bool restoreFullData, bool restoreParityParts>
void XorBlockRestore(TErasureType::ECrcMode crcMode, const TErasureType &type, TDataPartSet &partSet) {
    TRope &outBuffer = partSet.FullDataFragment.OwnedString;
    ui32 totalParts = type.TotalPartCount();
    Y_ABORT_UNLESS(partSet.Parts.size() == totalParts,
        "partSet.Parts.size(): %" PRIu64 " totalParts: %" PRIu32 " erasure: %s",
        (ui64)partSet.Parts.size(), (ui32)totalParts, type.ErasureName[type.GetErasure()].data());

    ui32 missingDataPartIdx = totalParts;
    ui32 missingDataPartCount = 0;
    ui64 expectedPartSize = type.PartSize(crcMode, partSet.FullDataSize);
    for (ui32 i = 0; i < totalParts; ++i) {
        if (!(partSet.PartsMask & (1 << i))) {
            missingDataPartIdx = i;
            ++missingDataPartCount;
        } else {
            Y_ABORT_UNLESS(partSet.Parts[i].size() == expectedPartSize, "partSet.Parts[%" PRIu32 "].size(): %" PRIu64
                " expectedPartSize: %" PRIu64 " erasure: %s partSet.FullDataSize: %" PRIu64,
                (ui32)i, (ui64)partSet.Parts[i].size(), expectedPartSize, type.ErasureName[type.GetErasure()].data(),
                (ui64)partSet.FullDataSize);
        }
    }
    Y_ABORT_UNLESS(missingDataPartCount <= 1);

    ui64 dataSize = partSet.FullDataSize;
    if (restoreParts && missingDataPartIdx != totalParts) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(partSet.Parts[missingDataPartIdx], partSet.Parts[missingDataPartIdx == 0 ? 1 : 0].size());
    }
    if (restoreFullData) {
        Refurbish(outBuffer, dataSize);
    } else if (missingDataPartCount == 0) {
        return;
    }

    TBlockParams p(crcMode, type, dataSize);

    // Restore the fast way if all data parts are present
    if (missingDataPartCount == 0 ||
            (missingDataPartCount == 1 && !restoreParts && missingDataPartIdx == p.TotalParts - 1)) {
        if (isStripe) {
            p.PrepareInputDataPointers<isStripe>(outBuffer.GetContiguousSpanMut().data());
            p.XorRestorePart<isStripe, false, true, false>(partSet, p.DataParts);
        } else {
            p.GlueBlockParts(outBuffer.GetContiguousSpanMut().data(), partSet);
        }
        return;
    }
    // Prepare output data pointers
    if (restoreFullData) {
        p.PrepareInputDataPointers<isStripe>(outBuffer.GetContiguousSpanMut().data());
    }

    p.XorRestorePart<isStripe, restoreParts, restoreFullData, restoreParityParts>(partSet, missingDataPartIdx);
}

const std::array<TString, TErasureType::ErasureSpeciesCount> TErasureType::ErasureName{{
    "none",
    "mirror-3",
    "block-3-1",
    "stripe-3-1",
    "block-4-2",
    "block-3-2",
    "stripe-4-2",
    "stripe-3-2",
    "mirror-3-2",
    "mirror-3-dc",
    "block-4-3",
    "stripe-4-3",
    "block-3-3",
    "stripe-3-3",
    "block-2-3",
    "stripe-2-3",
    "block-2-2",
    "stripe-2-2",
    "mirror-3of4",
}};

TErasureType::EErasureFamily TErasureType::ErasureFamily() const {
    const TErasureParameters &erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.ErasureFamily;
}

ui32 TErasureType::ParityParts() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.ParityParts;
}

ui32 TErasureType::DataParts() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.DataParts;
}

ui32 TErasureType::TotalPartCount() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.DataParts + erasure.ParityParts;
}

ui32 TErasureType::MinimalRestorablePartCount() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.DataParts;
}

ui32 TErasureType::ColumnSize() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TErasureType::ErasureMirror:
        return 1;
    case TErasureType::ErasureParityStripe:
    case TErasureType::ErasureParityBlock:
        if (erasure.ParityParts == 1) {
            return sizeof(ui64);
        }
        return (erasure.Prime - 1) * sizeof(ui64);
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}
/*
ui32 TErasureType::PartialRestoreStep() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureMirror:
            return 1;
        case TErasureType::ErasureParityStripe:
            if (erasure.ParityParts == 1) {
                return erasure.DataParts * sizeof(ui64);
            }
            return erasure.DataParts * (erasure.Prime - 1) * sizeof(ui64);
        case TErasureType::ErasureParityBlock:
            if (erasure.ParityParts == 1) {
                return sizeof(ui64);
            }
            return (erasure.Prime - 1) * sizeof(ui64);
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}*/

ui32 TErasureType::MinimalBlockSize() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TErasureType::ErasureMirror:
        return 1;
    case TErasureType::ErasureParityStripe:
    case TErasureType::ErasureParityBlock:
        if (erasure.ParityParts == 1) {
            return erasure.DataParts * sizeof(ui64);
        }
        if (erasure.ParityParts == 2) {
            return (erasure.Prime - 1) * erasure.DataParts * sizeof(ui64);
        }
        if (erasure.ParityParts == 3) {
            return (erasure.Prime - 1) * erasure.DataParts * sizeof(ui64);
        }
        ythrow TWithBackTrace<yexception>() << "Unsupported partiy part count = " << erasure.ParityParts <<
                " for ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}

ui64 TErasureType::PartUserSize(ui64 dataSize) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TErasureType::ErasureMirror:
        return dataSize;
    case TErasureType::ErasureParityStripe:
    case TErasureType::ErasureParityBlock:
        {
            ui32 blockSize = MinimalBlockSize();
            ui64 dataSizeBlocks = (dataSize + blockSize - 1) / blockSize;
            ui64 partSize = dataSizeBlocks * sizeof(ui64) * (erasure.ParityParts == 1 ? 1 : (erasure.Prime - 1));
            return partSize;
        }
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}

ui64 TErasureType::PartSize(ECrcMode crcMode, ui64 dataSize) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TErasureType::ErasureMirror:
        switch (crcMode) {
        case CrcModeNone:
            return dataSize;
        case CrcModeWholePart:
            if (dataSize) {
                return dataSize + sizeof(ui32);
            } else {
                return 0;
            }
        }
        ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
    case TErasureType::ErasureParityStripe:
    case TErasureType::ErasureParityBlock:
        {
            ui32 blockSize = MinimalBlockSize();
            ui64 dataSizeBlocks = (dataSize + blockSize - 1) / blockSize;
            ui64 partSize = dataSizeBlocks * sizeof(ui64) * (erasure.ParityParts == 1 ? 1 : (erasure.Prime - 1));
            switch (crcMode) {
            case CrcModeNone:
                return partSize;
            case CrcModeWholePart:
                return partSize + sizeof(ui32);
            }
            ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
        }
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}

ui64 TErasureType::SuggestDataSize(ECrcMode crcMode, ui64 partSize, bool roundDown) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TErasureType::ErasureMirror:
        switch (crcMode) {
        case CrcModeNone:
            return partSize;
        case CrcModeWholePart:
            if (partSize < sizeof(ui32)) {
                return 0;
            }
            return partSize - sizeof(ui32);
        }
        ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
    case TErasureType::ErasureParityStripe:
    case TErasureType::ErasureParityBlock:
        {
            ui64 combinedDataSize = 0;
            switch (crcMode) {
            case CrcModeNone:
                if (partSize == 0) {
                    return 0;
                }
                combinedDataSize = partSize * erasure.DataParts;
                break;
            case CrcModeWholePart:
                if (partSize < sizeof(ui32)) {
                    return 0;
                }
                combinedDataSize = (partSize - sizeof(ui32)) * erasure.DataParts;
            }
            if (combinedDataSize == 0) {
                ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
            }
            ui32 blockSize = MinimalBlockSize();
            ui64 dataSizeBlocks = (combinedDataSize + (roundDown ? 0 :  blockSize - 1)) / blockSize;
            return dataSizeBlocks * blockSize;
        }
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}

ui32 TErasureType::Prime() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.Prime;
}

// Block consists of columns.
// block = [column1, column2, ... ,columnN], where N == erasure.DataParts
//
// Input partitioning:
// | large, ... | small, ... | small + tail |

bool TErasureType::IsSinglePartRequest(ui32 fullDataSize, ui32 shift, ui32 size,
        ui32 &outPartIdx) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureParityBlock: {
            if (fullDataSize == 0) {
                return false;
            }
            if (size == 0) {
                size = fullDataSize - shift;
            }
            ui64 firstPartOffset = 0;
            ui64 firstPartIdx = BlockSplitPartIndex(shift, fullDataSize, firstPartOffset);

            ui64 lastPartOffset = 0;
            ui64 lastPartIdx = (BlockSplitPartIndex(shift + size, fullDataSize, lastPartOffset));

            if (firstPartIdx == lastPartIdx) {
                outPartIdx = (ui32)firstPartIdx;
                return true;
            }
            return false;
        }
        case TErasureType::ErasureParityStripe:
            return false;
        case TErasureType::ErasureMirror:
            return false;
        default:
            ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
}

bool TErasureType::IsPartialDataRequestPossible() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureParityBlock:
            // FIXME
            return false;
        case TErasureType::ErasureParityStripe:
            return true;
        case TErasureType::ErasureMirror:
            return true;
        default:
            ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
}

bool TErasureType::IsUnknownFullDataSizePartialDataRequestPossible() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureParityBlock:
            return false;
        case TErasureType::ErasureParityStripe:
            return true;
        case TErasureType::ErasureMirror:
            return true;
        default:
            ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
    //return false;
}

void TErasureType::AlignPartialDataRequest(ui64 shift, ui64 size, ui64 fullDataSize, ui64 &outShift,
        ui64 &outSize) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    ui64 blockSize = MinimalBlockSize();
    ui64 columnSize = blockSize / erasure.DataParts;

    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureParityBlock:
        {
            if (size == 0) {
                size = fullDataSize - shift;
            }
            ui64 firstPartOffset = 0;
            ui64 firstPartIdx = BlockSplitPartIndex(shift, fullDataSize, firstPartOffset);

            ui64 lastPartOffset = 0;
            ui64 lastPartIdx = BlockSplitPartIndex(shift + size, fullDataSize, lastPartOffset);

            // TODO: Consider data on the edge of 2 parts ( data ..... ...xx x.... => request x..xx of each part )
            if (firstPartIdx == lastPartIdx) {
                outShift = (firstPartOffset / columnSize) * columnSize;
                outSize = ((lastPartOffset + columnSize - 1) / columnSize) * columnSize - outShift;
                break;
            }

            outShift = 0;
            outSize = 0;
            break;
        }
        case TErasureType::ErasureParityStripe:
        {
            ui64 beginBlockIdx = (shift / blockSize);
            outShift = beginBlockIdx * columnSize;
            if (size == 0) {
                outSize = 0;
            } else {
                ui64 endBlockIdx = ((shift + size + blockSize - 1) / blockSize);
                outSize = (endBlockIdx - beginBlockIdx) * columnSize;
            }
            break;
        }
        case TErasureType::ErasureMirror:
        {
            outShift = shift;
            outSize = size;
            break;
        }
        default:
            outShift = shift;
            outSize = size;
            ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
            break;
    }
    return;
}

void TErasureType::BlockSplitRange(ECrcMode crcMode, ui64 blobSize, ui64 wholeBegin, ui64 wholeEnd,
        TBlockSplitRange *outRange) const {
    Y_ABORT_UNLESS(wholeBegin <= wholeEnd && outRange, "wholeBegin# %" PRIu64 " wholeEnd# %" PRIu64 " outRange# %" PRIu64,
            wholeBegin, wholeEnd, (ui64)(intptr_t)outRange);
    Y_UNUSED(crcMode);
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    const ui64 blockSize = MinimalBlockSize();
    const ui64 dataParts = erasure.DataParts;
    const ui64 columnSize = blockSize / dataParts;

    const ui64 wholeColumns = blobSize / columnSize;

    const ui64 smallPartColumns = wholeColumns / dataParts;
    const ui64 largePartColumns = smallPartColumns + 1;

    const ui64 smallPartSize = smallPartColumns * columnSize;
    const ui64 largePartSize = largePartColumns * columnSize;

    const ui32 firstSmallPartIdx = wholeColumns % dataParts;
    const ui64 lastPartSize = blobSize
        - largePartSize * firstSmallPartIdx
        - smallPartSize * (dataParts - firstSmallPartIdx - 1);
    const ui64 alignedLastPartSize = ((lastPartSize + columnSize - 1) / columnSize) * columnSize;

    const ui64 alignedPartSize = Max<ui64>(alignedLastPartSize, (firstSmallPartIdx ? largePartSize : smallPartSize));

    outRange->BeginPartIdx = Max<ui64>();
    outRange->EndPartIdx = Max<ui64>();
    outRange->PartRanges.resize(dataParts);
    ui64 partBeginWholeOffset = 0;
    ui64 partEndWholeOffset = 0;
    ui32 partIdx = 0;
    for (; partIdx < firstSmallPartIdx; ++partIdx) {
        TPartOffsetRange &out = outRange->PartRanges[partIdx];
        partEndWholeOffset = partBeginWholeOffset + largePartSize;
        if (!largePartSize) {
            out.Reset();
        } else {
            // pb----pe pb-----pe pb-----pe pb----pe pb-----pe
            //              wb----------------we
            // wb < pe && we > pb
            if (wholeBegin < partEndWholeOffset && wholeEnd > partBeginWholeOffset) {
                if (wholeBegin >= partBeginWholeOffset) {
                    // first part
                    outRange->BeginPartIdx = partIdx;
                    out.Begin = wholeBegin - partBeginWholeOffset;
                    out.AlignedBegin = (out.Begin / columnSize) * columnSize;
                    out.WholeBegin = wholeBegin;
                    out.AlignedWholeBegin = wholeBegin + out.AlignedBegin - out.Begin;
                } else {
                    // not first part
                    out.Begin = 0;
                    out.AlignedBegin = 0;
                    out.WholeBegin = partBeginWholeOffset;
                    out.AlignedWholeBegin = partBeginWholeOffset;
                }

                if (wholeEnd <= partEndWholeOffset) {
                    // last part
                    outRange->EndPartIdx = partIdx + 1;
                    out.End = wholeEnd - partBeginWholeOffset;
                    out.AlignedEnd = ((out.End + columnSize - 1) / columnSize) * columnSize;
                    out.WholeEnd = wholeEnd;
                } else {
                    // not last part
                    out.End = largePartSize;
                    out.AlignedEnd = largePartSize;
                    out.WholeEnd = partEndWholeOffset;
                }
            } else {
                out.Reset();
            }
        }
        partBeginWholeOffset = partEndWholeOffset;
    }
    for (; partIdx < dataParts - 1; ++partIdx) {
        TPartOffsetRange &out = outRange->PartRanges[partIdx];
        partEndWholeOffset = partBeginWholeOffset + smallPartSize;
        if (!smallPartSize) {
            out.Reset();
        } else {
            // pb----pe pb-----pe pb-----pe pb----pe pb-----pe
            //              wb----------------we
            // wb < pe && we > pb
            if (wholeBegin < partEndWholeOffset && wholeEnd > partBeginWholeOffset) {
                if (wholeBegin >= partBeginWholeOffset) {
                    // first part
                    outRange->BeginPartIdx = partIdx;
                    out.Begin = wholeBegin - partBeginWholeOffset;
                    out.AlignedBegin = (out.Begin / columnSize) * columnSize;
                    out.WholeBegin = wholeBegin;
                    out.AlignedWholeBegin = wholeBegin + out.AlignedBegin - out.Begin;
                } else {
                    // not first part
                    out.Begin = 0;
                    out.AlignedBegin = 0;
                    out.WholeBegin = partBeginWholeOffset;
                    out.AlignedWholeBegin = partBeginWholeOffset;
                }

                if (wholeEnd <= partEndWholeOffset) {
                    // last part
                    outRange->EndPartIdx = partIdx + 1;
                    out.End = wholeEnd - partBeginWholeOffset;
                    out.AlignedEnd = ((out.End + columnSize - 1) / columnSize) * columnSize;
                    out.WholeEnd = wholeEnd;
                } else {
                    // not last part
                    out.End = smallPartSize;
                    // Align up to the large part size for restoration purposes
                    out.AlignedEnd = alignedPartSize;
                    out.WholeEnd = partEndWholeOffset;
                }
            } else {
                out.Reset();
            }
        }
        partBeginWholeOffset = partEndWholeOffset;
    }
    // last part
    {
        TPartOffsetRange &out = outRange->PartRanges[partIdx];
        partEndWholeOffset = blobSize;
        if (!lastPartSize) {
            out.Reset();
        } else {
            // pb----pe pb-----pe pb-----pe pb----pe pb-----pe
            //              wb----------------we
            // wb < pe && we > pb
            if (wholeBegin < partEndWholeOffset && wholeEnd > partBeginWholeOffset) {
                if (wholeBegin >= partBeginWholeOffset) {
                    // first part
                    outRange->BeginPartIdx = partIdx;
                    out.Begin = wholeBegin - partBeginWholeOffset;
                    out.AlignedBegin = (out.Begin / columnSize) * columnSize;
                    out.WholeBegin = wholeBegin;
                    out.AlignedWholeBegin = wholeBegin + out.AlignedBegin - out.Begin;
                } else {
                    // not first part
                    out.Begin = 0;
                    out.AlignedBegin = 0;
                    out.WholeBegin = partBeginWholeOffset;
                    out.AlignedWholeBegin = partBeginWholeOffset;
                }

                if (wholeBegin >= partBeginWholeOffset || wholeEnd < partEndWholeOffset) {
                    // part of the last part
                    out.End = wholeEnd - partBeginWholeOffset;
                    out.AlignedEnd = ((out.End + columnSize - 1) / columnSize) * columnSize;
                    out.WholeEnd = wholeEnd;
                } else {
                    // up to the end
                    out.End = lastPartSize;
                    out.AlignedEnd = alignedPartSize;
                    out.WholeEnd = partEndWholeOffset;
                }
                // It IS the LAST data part of the blob
                outRange->EndPartIdx = partIdx + 1;
            } else {
                out.Reset();
            }
        }
    }
    Y_DEBUG_ABORT_UNLESS(outRange->EndPartIdx != Max<ui64>());
}

ui32 TErasureType::BlockSplitPartIndex(ui64 offset, ui64 dataSize, ui64 &outPartOffset) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    ui64 blockSize = MinimalBlockSize();
    ui64 columnSize = blockSize / erasure.DataParts;
    ui64 wholeColumns = dataSize / columnSize;

    ui64 smallPartColumns = wholeColumns / erasure.DataParts;
    ui64 largePartColumns = smallPartColumns + 1;

    ui64 smallPartSize = smallPartColumns * columnSize;
    ui64 largePartSize = largePartColumns * columnSize;

    ui32 firstSmallPartIdx = wholeColumns % erasure.DataParts;

    ui64 firstSmallPartOffset = firstSmallPartIdx * largePartSize;
    if (offset < firstSmallPartOffset) {
        ui64 index = offset / largePartSize;
        outPartOffset = offset - index * largePartSize;
        return (ui32)index;
    }
    ui64 lastPartOffset = firstSmallPartOffset + smallPartSize * (erasure.DataParts - firstSmallPartIdx - 1);
    if (offset < lastPartOffset) {
        offset -= firstSmallPartOffset;
        ui64 smallIndex = offset / smallPartSize;
        outPartOffset = offset - smallIndex * smallPartSize;
        return (ui32)(smallIndex + firstSmallPartIdx);
    }
    outPartOffset = offset - lastPartOffset;
    return (erasure.DataParts - 1);
}

ui64 TErasureType::BlockSplitWholeOffset(ui64 dataSize, ui64 partIdx, ui64 offset) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    ui64 blockSize = MinimalBlockSize();
    ui64 columnSize = blockSize / erasure.DataParts;
    ui64 wholeColumns = dataSize / columnSize;

    ui64 smallPartColumns = wholeColumns / erasure.DataParts;
    ui64 largePartColumns = smallPartColumns + 1;

    ui64 smallPartSize = smallPartColumns * columnSize;
    ui64 largePartSize = largePartColumns * columnSize;

    ui32 firstSmallPartIdx = wholeColumns % erasure.DataParts;
    if (partIdx < firstSmallPartIdx) {
        return largePartSize * partIdx + offset;
    }
    if (partIdx + 1 < erasure.DataParts) {
        return largePartSize * firstSmallPartIdx + smallPartSize * (partIdx - firstSmallPartIdx) + offset;
    }
    return largePartSize * firstSmallPartIdx + smallPartSize * (erasure.DataParts - firstSmallPartIdx - 1) + offset;
}

ui64 TErasureType::BlockSplitPartUsedSize(ui64 dataSize, ui32 partIdx) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    ui64 blockSize = MinimalBlockSize();
    ui64 columnSize = blockSize / erasure.DataParts;
    ui64 wholeColumns = dataSize / columnSize;

    ui64 smallPartColumns = wholeColumns / erasure.DataParts;
    ui64 largePartColumns = smallPartColumns + 1;

    ui64 smallPartSize = smallPartColumns * columnSize;
    ui64 largePartSize = largePartColumns * columnSize;

    ui32 firstSmallPartIdx = wholeColumns % erasure.DataParts;
    if (partIdx < firstSmallPartIdx) {
        return largePartSize;
    }
    if (partIdx + 1 < erasure.DataParts) {
        return smallPartSize;
    }
    ui64 lastPartSize = dataSize
        - largePartSize * firstSmallPartIdx
        - smallPartSize * (erasure.DataParts - firstSmallPartIdx - 1);
    return lastPartSize;
}

void MirrorSplit(TErasureType::ECrcMode crcMode, const TErasureType &type, TRope& buffer,
        TDataPartSet& outPartSet) {
    outPartSet.FullDataSize = buffer.size();
    outPartSet.Parts.resize(type.TotalPartCount());
    ui32 parityParts = type.ParityParts();
    switch (crcMode) {
    case TErasureType::CrcModeNone:
        for (ui32 partIdx = 0; partIdx <= parityParts; ++partIdx) {
            outPartSet.Parts[partIdx].ReferenceTo(buffer);
            outPartSet.PartsMask |= (1 << partIdx);
        }
        outPartSet.MemoryConsumed = buffer.capacity(); //FIXME(innokentii) check how MemoryConsumed actually used
        return;
    case TErasureType::CrcModeWholePart:
        {
            ui64 partSize = type.PartSize(crcMode, buffer.size());
            TRcBuf part(buffer);
            part.GrowBack(partSize - buffer.GetSize());
            char *dst = part.GetContiguousSpanMut().data();
            if (buffer.size() || part.size()) {
                Y_ABORT_UNLESS(part.size() >= buffer.size() + sizeof(ui32), "Part size too small, buffer size# %" PRIu64
                        " partSize# %" PRIu64, (ui64)buffer.size(), (ui64)partSize);
                PadAndCrcAtTheEnd(dst, buffer.size(), part.size());
            }
            for (ui32 partIdx = 0; partIdx <= parityParts; ++partIdx) {
                outPartSet.Parts[partIdx].ReferenceTo(part);
                outPartSet.PartsMask |= (1 << partIdx);
            }
            outPartSet.MemoryConsumed = part.GetOccupiedMemorySize(); //FIXME(innokentii) check how MemoryConsumed actually used
        }
        return;
    }
    ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
}

template <bool restoreParts, bool restoreFullData>
void MirrorRestore(TErasureType::ECrcMode crcMode, const TErasureType &type, TDataPartSet& partSet) {
    ui32 totalParts = type.TotalPartCount();
    for (ui32 partIdx = 0; partIdx < totalParts; ++partIdx) {
        if (partSet.PartsMask & (1 << partIdx)) {
            if (restoreParts) {
                for (ui32 i = 0; i < totalParts; ++i) {
                    if (!(partSet.PartsMask & (1 << i))) {
                        partSet.Parts[i] = partSet.Parts[partIdx];
                    }
                }
                partSet.MemoryConsumed = partSet.Parts[partIdx].MemoryConsumed();
            }
            if (restoreFullData) {
                switch (crcMode) {
                    case TErasureType::CrcModeNone:
                        partSet.FullDataFragment.ReferenceTo(partSet.Parts[partIdx].OwnedString);
                        return;
                    case TErasureType::CrcModeWholePart:
                        TRope outBuffer = partSet.Parts[partIdx].OwnedString;
                        outBuffer.GetContiguousSpanMut(); // Detach
                        if(outBuffer.size() != partSet.FullDataSize) {
                            TRcBuf newOutBuffer(outBuffer);
                            Y_ABORT_UNLESS(outBuffer.size() >= partSet.FullDataSize, "Unexpected outBuffer.size# %" PRIu64
                                    " fullDataSize# %" PRIu64, (ui64)outBuffer.size(), (ui64)partSet.FullDataSize);
                            newOutBuffer.TrimBack(partSet.FullDataSize); // To pad with zeroes!
                            outBuffer = TRope(newOutBuffer);
                        }
                        partSet.FullDataFragment.ReferenceTo(outBuffer);
                        return;
                }
                ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
            }
            return;
        }
    }
    ythrow TWithBackTrace<yexception>() << "No data in part set";


}
static void VerifyPartSizes(TDataPartSet& partSet, size_t definedPartEndIdx) {
    size_t partSize = partSet.Parts[0].size();
    for (size_t idx = 0; idx < partSet.Parts.size(); ++idx) {
        Y_ABORT_UNLESS(partSet.Parts[idx].size() == partSize);
        if (partSize && idx < definedPartEndIdx) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(partSet.Parts[idx].GetDataAt(partSet.Parts[idx].Offset),
                    partSet.Parts[idx].Size);
        }
    }
}

void TErasureType::SplitData(ECrcMode crcMode, TRope& buffer, TDataPartSet& outPartSet) const {
    outPartSet.ResetSplit();
    do {
        IncrementalSplitData(crcMode, buffer, outPartSet);
    } while (!outPartSet.IsSplitDone());
}

void TErasureType::IncrementalSplitData(ECrcMode crcMode, TRope& buffer, TDataPartSet& outPartSet) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureMirror:
            MirrorSplit(crcMode, *this, buffer, outPartSet);
            break;
        case TErasureType::ErasureParityStripe:
            switch (erasure.ParityParts) {
                case 1:
                    XorBlockSplit<true>(crcMode, *this, buffer, outPartSet);
                    break;
                case 2:
                    EoBlockSplit<true>(crcMode, *this, buffer, outPartSet);
                    break;
                case 3:
                    StarBlockSplit<true>(crcMode, *this, buffer, outPartSet);
                    break;
                default:
                    ythrow TWithBackTrace<yexception>() << "Unsupported number of parity parts: "
                        << erasure.ParityParts;
                    break;
            }
            break;
        case TErasureType::ErasureParityBlock:
            switch (erasure.ParityParts) {
                case 1:
                    XorBlockSplit<false>(crcMode, *this, buffer, outPartSet);
                    break;
                case 2:
                    EoBlockSplit<false>(crcMode, *this, buffer, outPartSet);
                    break;
                case 3:
                    StarBlockSplit<false>(crcMode, *this, buffer, outPartSet);
                    break;
                default:
                    ythrow TWithBackTrace<yexception>() << "Unsupported number of parity parts: "
                        << erasure.ParityParts;
                    break;
            }
            break;
        default:
            ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
            break;
    }
    if (outPartSet.IsSplitDone()) {
        VerifyPartSizes(outPartSet, Max<size_t>());
    }
}

void MirrorSplitDiff(const TErasureType &type, const TVector<TDiff> &diffs, TPartDiffSet& outDiffSet) {
    outDiffSet.PartDiffs.resize(type.TotalPartCount());
    ui32 parityParts = type.ParityParts();
    for (ui32 partIdx = 0; partIdx <= parityParts; ++partIdx) {
        outDiffSet.PartDiffs[partIdx].Diffs = diffs;
    }
}

void EoBlockSplitDiff(TErasureType::ECrcMode crcMode, const TErasureType &type, ui32 dataSize, const TVector<TDiff> &diffs, TPartDiffSet& outDiffSet) {
    TBlockParams p(crcMode, type, dataSize);
    outDiffSet.PartDiffs.resize(type.TotalPartCount());
    ui32 dataParts = type.DataParts();

    ui32 partOffset = 0;
    ui32 diffIdx = 0;
    for (ui32 partIdx = 0; partIdx < dataParts && diffIdx < diffs.size(); ++partIdx) {
        ui32 nextOffset = partOffset;
        if (partIdx < p.FirstSmallPartIdx) {
            nextOffset += p.LargePartSize;
        } else {
            nextOffset += p.SmallPartSize;
        }
        if (partIdx + 1 == dataParts) {
            nextOffset = dataSize;
        }
        if (partOffset == nextOffset) {
            continue;
        }
        TPartDiff &part = outDiffSet.PartDiffs[partIdx];

        while (diffIdx < diffs.size()) {
            const TDiff &diff = diffs[diffIdx];
            ui32 lineOffset = diff.Offset % sizeof(ui64);
            ui32 diffEnd = diff.Offset + diff.GetDiffLength();

            if (diff.Offset <= partOffset && diffEnd >= partOffset) {
                ui32 diffEndForThisPart = Min(diffEnd, nextOffset);
                ui32 diffShift = partOffset - diff.Offset;

                ui32 bufferSize = diffEndForThisPart - partOffset;
                Y_ABORT_UNLESS(bufferSize);
                Y_VERIFY_S(diffShift + bufferSize <= diff.Buffer.size(), "diffShift# " << diffShift
                        << " bufferSize# " << bufferSize << " diff.GetDiffLength()# " << diff.GetDiffLength());
                TRcBuf newBuffer(diff.Buffer);
                newBuffer.TrimBack(bufferSize + diffShift);
                newBuffer.TrimFront(bufferSize);
                part.Diffs.emplace_back(newBuffer, 0, false, true);

                if (diffEnd <= nextOffset) {
                    diffIdx++;
                } else {
                    break;
                }
            } else if (diffEnd <= nextOffset) {
                TRcBuf buffer;
                ui32 bufferSize = 0;
                if (lineOffset && !diff.IsAligned) {
                    bufferSize = diff.GetDiffLength() + lineOffset;
                    buffer = diff.Buffer;
                    buffer.GrowFront(lineOffset); // FIXME(innokentii) should the [0..lineOffset) be zeroed?
                } else {
                    buffer = diff.Buffer;
                    bufferSize = diff.Buffer.size();
                }
                Y_ABORT_UNLESS(bufferSize);
                part.Diffs.emplace_back(buffer, diff.Offset - partOffset, false, true);
                diffIdx++;
            } else if (diff.Offset < nextOffset) {
                ui32 bufferSize = nextOffset - diff.Offset + lineOffset;
                TRcBuf newBuffer = diff.Buffer;
                newBuffer.GrowFront(lineOffset); // FIXME(innokentii) should the [0..lineOffset) be zeroed?
                newBuffer.TrimBack(bufferSize);
                Y_ABORT_UNLESS(bufferSize);
                part.Diffs.emplace_back(newBuffer, diff.Offset - partOffset, false, true);
                break;
            } else {
                break;
            }
        }

        partOffset = nextOffset;
    }
}

void TErasureType::SplitDiffs(ECrcMode crcMode, ui32 dataSize, const TVector<TDiff> &diffs, TPartDiffSet& outDiffSet) const {
    Y_ABORT_UNLESS(crcMode == CrcModeNone, "crc's not implemented");
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];

    // change crc part only in during of applying diffs
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureMirror:
            MirrorSplitDiff(*this, diffs, outDiffSet);
            break;
        case TErasureType::ErasureParityStripe:
            Y_ABORT("Not implemented");
            break;
        case TErasureType::ErasureParityBlock:
            Y_ABORT_UNLESS(erasure.ParityParts == 2, "Other is not implemented");
            EoBlockSplitDiff(crcMode, *this, dataSize, diffs, outDiffSet);
            break;
    }
}

template <typename Bucket>
void XorCpy(Bucket *dest, const Bucket *orig, const Bucket *diff, ui32 count) {
    for (ui32 idx = 0; idx < count; ++idx) {
        dest[idx] = ReadUnaligned<Bucket>(&orig[idx]) ^ ReadUnaligned<Bucket>(&diff[idx]);
    }
}

void MakeEoBlockXorDiff(TErasureType::ECrcMode crcMode, const TErasureType &type, ui32 dataSize,
        const ui8 *src, const TVector<TDiff> &inDiffs, TVector<TDiff> *outDiffs)
{
    Y_ABORT_UNLESS(crcMode == TErasureType::CrcModeNone, "crc's not implemented");
    TBlockParams p(crcMode, type, dataSize);

    for (const TDiff &diff : inDiffs) {
        const ui8 *diffBufferBytes = reinterpret_cast<const ui8*>(diff.Buffer.data());

        ui32 lineOffset = diff.Offset % sizeof(ui64);
        ui32 lowerStartPos = diff.Offset - lineOffset;
        ui32 upperStartPos = lowerStartPos + (lineOffset ? sizeof(ui64) : 0);

        ui32 end = diff.Offset + diff.GetDiffLength();
        ui32 endLineOffset = end % sizeof(ui64);
        ui32 lowerEndPos = end - endLineOffset;
        ui32 upperEndPos = lowerEndPos + (endLineOffset ? sizeof(ui64) : 0);

        ui32 bufferSize = upperEndPos - lowerStartPos;
        Y_ABORT_UNLESS(bufferSize);
        TRcBuf xorDiffBuffer = TRcBuf::Uninitialized(bufferSize); // FIXME(innokentii) candidate
        ui8 *xorDiffBufferBytes = reinterpret_cast<ui8*>(const_cast<char*>(xorDiffBuffer.data()));

        if (lowerEndPos == lowerStartPos) {
            ui64 &val = *reinterpret_cast<ui64*>(xorDiffBufferBytes);
            val = 0;
            ui32 byteCount = diff.GetDiffLength();
            XorCpy(xorDiffBufferBytes + lineOffset, src + diff.Offset, diffBufferBytes + lineOffset, byteCount);
            outDiffs->emplace_back(xorDiffBuffer, diff.Offset, true, true);
            continue;
        }

        if (lineOffset) {
            ui64 &val = *reinterpret_cast<ui64*>(xorDiffBufferBytes);
            val = 0;
            ui32 byteCount = Min<ui32>(sizeof(ui64) - lineOffset, diff.GetDiffLength());
            XorCpy(xorDiffBufferBytes + lineOffset, src + diff.Offset, diffBufferBytes + lineOffset, byteCount);
        }

        ui32 firstLine = lowerStartPos / sizeof(ui64);
        ui32 lineStart = upperStartPos / sizeof(ui64);
        ui32 lineEnd = lowerEndPos / sizeof(ui64);
        ui64 *xorUI64 = reinterpret_cast<ui64*>(xorDiffBufferBytes) + (lineStart - firstLine);
        const ui64 *srcUI64 = reinterpret_cast<const ui64*>(src) + lineStart;
        const ui64 *diffUI64 = reinterpret_cast<const ui64*>(diffBufferBytes)+ (lineStart - firstLine);
        ui32 countUI64 = lineEnd - lineStart;
        XorCpy(xorUI64, srcUI64, diffUI64, countUI64);

        if (endLineOffset) {
            ui64 &val = reinterpret_cast<ui64*>(xorDiffBufferBytes)[lineEnd - firstLine];
            val = 0;
            ui32 diffOffset = lowerEndPos - lowerStartPos;
            XorCpy(xorDiffBufferBytes + diffOffset, src + lowerEndPos, diffBufferBytes + diffOffset, endLineOffset);
        }
        outDiffs->emplace_back(xorDiffBuffer, diff.Offset, true, true);
    }
}

void TErasureType::MakeXorDiff(ECrcMode crcMode, ui32 dataSize, const ui8 *src,
        const TVector<TDiff> &inDiffs, TVector<TDiff> *outDiffs) const
{
    Y_ABORT_UNLESS(crcMode == CrcModeNone, "crc's not implemented");
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureMirror:
            Y_ABORT("unreachable");
            break;
        case TErasureType::ErasureParityStripe:
            Y_ABORT("Not implemented");
            break;
        case TErasureType::ErasureParityBlock:
            Y_ABORT_UNLESS(erasure.ParityParts == 2, "Other is not implemented");
            MakeEoBlockXorDiff(crcMode, *this, dataSize, src, inDiffs, outDiffs);
            break;
    }
}

void TErasureType::ApplyDiff(ECrcMode crcMode, ui8 *dst, const TVector<TDiff> &diffs) const {
    Y_ABORT_UNLESS(crcMode == CrcModeNone, "crc's not implemented");
    for (auto &diff : diffs) {
        memcpy(dst + diff.Offset, diff.GetDataBegin(), diff.GetDiffLength());
    }
}

template <bool forSecondParity>
void ApllyXorDiffForEoBlock(const TBlockParams &p, ui64 *buffer, const ui64 *diff, ui64 adj,
        ui32 begin, ui32 end, ui32 diffOffset, ui8 fromPart = 0)
 {
    ui32 blockBegin = begin / p.LineCount;
    ui32 blockEnd = (end + p.LineCount - 1) / p.LineCount;

    Y_ABORT_UNLESS(!forSecondParity || (blockBegin == 0 && blockEnd == 1));

    if (forSecondParity && adj) {
        for (ui32 idx = 0; idx < p.LineCount; ++idx) {
            buffer[idx] ^= adj;
        }
    }

    Y_ABORT_UNLESS(begin < end);
    if constexpr (forSecondParity) {
        ui32 lineBorder = p.LineCount - fromPart;

        if (begin < lineBorder) {
            ui32 bufferBegin = begin + fromPart;
            ui32 diffBegin = begin - diffOffset;
            ui32 count = Min(lineBorder, end) - begin;
            XorCpy(buffer + bufferBegin, buffer + bufferBegin, diff + diffBegin, count);
        }

        if (end > lineBorder + 1) {
            ui32 currentBegin = Max(lineBorder + 1, begin);
            ui32 bufferBegin = currentBegin + fromPart - p.Prime;
            ui32 count = end - currentBegin;
            ui32 diffBegin = currentBegin - diffOffset;
            XorCpy(buffer + bufferBegin, buffer + bufferBegin, diff + diffBegin, count);
        }
    } else {
        ui32 count = end - begin;
        ui32 diffBegin = begin - diffOffset;
        XorCpy(buffer + begin, buffer + begin, diff + diffBegin, count);
    }
}

void ApplyEoBlockXorDiffForFirstParityPart(TErasureType::ECrcMode crcMode, const TErasureType &type, ui32 dataSize,
        ui64 *dst, const TVector<TDiff> &xorDiffs)
{
    Y_ABORT_UNLESS(crcMode == TErasureType::CrcModeNone, "crc's not implemented");
    TBlockParams p(crcMode, type, dataSize);

    for (const TDiff &diff : xorDiffs) {
        Y_ABORT_UNLESS(diff.Buffer.size() % sizeof(ui64) == 0);
        Y_ABORT_UNLESS(diff.IsXor && diff.IsAligned);
        const ui64 *diffBuffer = reinterpret_cast<const ui64*>(diff.GetBufferBegin());
        ui32 diffOffset = diff.Offset / sizeof(ui64);
        ui32 offset = diff.Offset / sizeof(ui64);
        ui32 lineCount = diff.Buffer.size() / sizeof(ui64);
        Y_ABORT_UNLESS(diff.Offset < type.PartSize(crcMode, dataSize));
        Y_ABORT_UNLESS(diff.Offset + diff.GetDiffLength() <= type.PartSize(crcMode, dataSize));
        ApllyXorDiffForEoBlock<false>(p, dst, diffBuffer, 0, offset, offset + lineCount, diffOffset);
    }
}

void ApplyXorForSecondParityPart(const TBlockParams &p, ui64 *startBufferBlock, const ui64 *startDiffBlock,
        ui32 begin, ui32 end, ui8 fromPart, ui32 diffOffset)
{
    ui32 m = p.Prime;
    const ui32 mint = (m - 2 < p.LineCount ? 1 : m - 2 - p.LineCount);
    ui64 adj = 0;
    ui32 adjRelatedBytesBegin = m - 1 - fromPart;
    bool isAdjChanged = (fromPart >= mint)
            && (adjRelatedBytesBegin >= begin)
            && (adjRelatedBytesBegin < end);

    if (isAdjChanged) {
        adj = startDiffBlock[adjRelatedBytesBegin - diffOffset];
    }

    ApllyXorDiffForEoBlock<true>(p, startBufferBlock, startDiffBlock, adj, begin, end, diffOffset, fromPart);
}

void ApplyEoBlockXorDiffForSecondParityPart(TErasureType::ECrcMode crcMode, const TErasureType &type, ui32 dataSize,
        ui8 fromPart, ui64 *dst, const TVector<TDiff> &xorDiffs)
{
    Y_ABORT_UNLESS(crcMode == TErasureType::CrcModeNone, "crc's not implemented");
    TBlockParams p(crcMode, type, dataSize);

    ui64 bytesInBlock = p.LineCount * sizeof(ui64);

    for (const TDiff &diff : xorDiffs) {
        Y_ABORT_UNLESS(diff.Buffer.size() % sizeof(ui64) == 0);
        Y_ABORT_UNLESS(diff.IsXor && diff.IsAligned);

        const ui64 *diffBuffer = reinterpret_cast<const ui64*>(diff.GetBufferBegin());

        ui32 firstBlock = diff.Offset / bytesInBlock * p.LineCount;
        ui32 begin = diff.Offset / sizeof(ui64);
        ui32 end = begin + diff.Buffer.size() / sizeof(ui64);
        ui32 endBlock = (end + p.LineCount - 1) / p.LineCount * p.LineCount;

        for (ui32 idx = firstBlock; idx < endBlock; idx += p.LineCount) {
            ui32 lineBegin = Max(idx, begin) - idx;
            ui32 lineEnd = Min<ui32>(idx + p.LineCount, end) - idx;
            if (idx == firstBlock) {
                ui32 diffOffset = begin - idx;
                ApplyXorForSecondParityPart(p, dst + idx, diffBuffer, lineBegin, lineEnd, fromPart, diffOffset);
            } else {
                ui32 diffOffset = idx - begin;
                ApplyXorForSecondParityPart(p, dst + idx, diffBuffer + diffOffset, lineBegin, lineEnd, fromPart, 0);
            }
        }
    }
}

void TErasureType::ApplyXorDiff(ECrcMode crcMode, ui32 dataSize, ui8 *dst,
        const TVector<TDiff> &diffs, ui8 fromPart, ui8 toPart) const
{
    Y_ABORT_UNLESS(crcMode == CrcModeNone, "crc's not implemented");
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureMirror:
            Y_ABORT("unreachable");
            break;
        case TErasureType::ErasureParityStripe:
            Y_ABORT("Not implemented");
            break;
        case TErasureType::ErasureParityBlock:
            Y_ABORT_UNLESS(erasure.ParityParts == 2, "Other is not implemented");
            ui64 *lineDst = reinterpret_cast<ui64*>(dst);
            if (toPart + 1 != TotalPartCount()) {
                ApplyEoBlockXorDiffForFirstParityPart(crcMode, *this, dataSize, lineDst, diffs);
            } else {
                ApplyEoBlockXorDiffForSecondParityPart(crcMode, *this, dataSize, fromPart, lineDst, diffs);
            }
            break;
    }
}

void TErasureType::RestoreData(ECrcMode crcMode, TDataPartSet& partSet, TRope& outBuffer, bool restoreParts,
        bool restoreFullData, bool restoreParityParts) const {
    partSet.FullDataFragment.ReferenceTo(outBuffer);
    RestoreData(crcMode, partSet, restoreParts, restoreFullData, restoreParityParts);
    outBuffer = partSet.FullDataFragment.OwnedString;
}

void TErasureType::RestoreData(ECrcMode crcMode, TDataPartSet& partSet, bool restoreParts, bool restoreFullData,
        bool restoreParityParts) const {
    if (restoreParityParts) {
        restoreParts = true;
    }
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    ui32 totalParts = TotalPartCount();
    if (partSet.Parts.size() != totalParts) {
        ythrow TWithBackTrace<yexception>() << "Incorrect partSet size, received " << partSet.Parts.size()
            << " while expected " << (erasure.DataParts + erasure.ParityParts);
    }
    Y_DEBUG_ABORT_UNLESS(restoreFullData || restoreParts);
    Y_DEBUG_ABORT_UNLESS(erasure.Prime <= MAX_LINES_IN_BLOCK);
    switch (erasure.ErasureFamily) {
        case TErasureType::ErasureMirror:
            if (restoreParts) {
                if (restoreFullData) {
                    MirrorRestore<true, true>(crcMode, *this, partSet);
                } else {
                    MirrorRestore<true, false>(crcMode, *this, partSet);
                }
                VerifyPartSizes(partSet, Max<size_t>());
            } else if (restoreFullData) {
                MirrorRestore<false, true>(crcMode, *this, partSet);
            }
            if (restoreFullData) {
                Y_ABORT_UNLESS(partSet.FullDataSize == partSet.FullDataFragment.PartSize,
                        "Incorrect data part size = %" PRIu64 ", expected size = %" PRIu64,
                        (ui64)partSet.FullDataFragment.PartSize, (ui64)partSet.FullDataSize);
            }
            break;
        case TErasureType::ErasureParityStripe:
            switch (erasure.ParityParts) {
                case 1:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                XorBlockRestore<true, true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                XorBlockRestore<true, true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                XorBlockRestore<true, true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                XorBlockRestore<true, true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        XorBlockRestore<true, false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                case 2:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                EoBlockRestore<true, true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                EoBlockRestore<true, true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                EoBlockRestore<true, true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                EoBlockRestore<true, true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        EoBlockRestore<true, false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                case 3:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                StarBlockRestore<true, true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                StarBlockRestore<true, true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                StarBlockRestore<true, true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                StarBlockRestore<true, true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        StarBlockRestore<true, false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                default:
                    ythrow TWithBackTrace<yexception>() << "Unsupported number of parity parts: "
                        << erasure.ParityParts;
                    break;
            }
            break;
        case TErasureType::ErasureParityBlock:
            switch (erasure.ParityParts) {
                case 1:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                XorBlockRestore<false, true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                XorBlockRestore<false, true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                XorBlockRestore<false, true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                XorBlockRestore<false, true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        XorBlockRestore<false, false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                case 2:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                EoBlockRestore<false, true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                EoBlockRestore<false, true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                EoBlockRestore<false, true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                EoBlockRestore<false, true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        EoBlockRestore<false, false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                case 3:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                // isStripe, restoreParts, restoreFullData, restoreParityParts
                                StarBlockRestore<false, true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                StarBlockRestore<false, true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                StarBlockRestore<false, true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                StarBlockRestore<false, true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        StarBlockRestore<false, false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                default:
                    ythrow TWithBackTrace<yexception>() << "Unsupported number of parity parts: "
                        << erasure.ParityParts;
                    break;
            }
            break;
    }
}

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TErasureType::EErasureSpecies, stream, value) {
    stream << NKikimr::TErasureType::ErasureSpeciesToStr(value);
}
