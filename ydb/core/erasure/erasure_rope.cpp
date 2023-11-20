#include "erasure_rope.h"

#include <util/generic/yexception.h>
#include <util/system/unaligned_mem.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

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
namespace NErasureRope {

static void Refurbish(TRope &str, ui64 size) {
    if (str.GetSize() != size) {
        str = TRopeHelpers::RopeUninitialized(size);
    }
}

static void Refurbish(TPartFragment &fragment, ui64 size) {
    if (fragment.size() != size) {
        TRACE("Refurbish fragment size# " << fragment.size() << " to size# " << size << Endl);
        fragment.UninitializedOwnedWhole(size);
    }
}

const char *TRopeErasureType::ErasureSpeciesToStr(TRopeErasureType::EErasureSpecies es) {
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
        case ErasureMirror3of4:     return "Mirror3of4";
        default:                    return "UNKNOWN";
    }
}

struct TErasureParameters {
    TRopeErasureType::EErasureFamily ErasureFamily;
    ui32 DataParts; // for parity - number of data parts, for mirror - 1
    ui32 ParityParts; // for parity - number of parity parts (1 | 2 | 3), for mirror - number of additional copies
    ui32 Prime; // for parity - smallest prime number >= DataParts, for mirror - 1
};

static const std::array<TErasureParameters, TRopeErasureType::ErasureSpeciesCount> ErasureSpeciesParameters{{
    {TRopeErasureType::ErasureMirror,  1, 0, 1} // 0 = ErasureSpicies::ErasureNone
    ,{TRopeErasureType::ErasureMirror, 1, 2, 1} // 1 = ErasureSpicies::ErasureMirror3
    ,{TRopeErasureType::ErasureParityBlock,  3, 1, 3} // 2 = ErasureSpicies::Erasure3Plus1Block
    ,{TRopeErasureType::ErasureParityStripe, 3, 1, 3} // 3 = ErasureSpicies::Erasure3Plus1Stipe
    ,{TRopeErasureType::ErasureParityBlock,  4, 2, 5} // 4 = ErasureSpicies::Erasure4Plus2Block
    ,{TRopeErasureType::ErasureParityBlock,  3, 2, 3} // 5 = ErasureSpicies::Erasure3Plus2Block
    ,{TRopeErasureType::ErasureParityStripe, 4, 2, 5} // 6 = ErasureSpicies::Erasure4Plus2Stipe
    ,{TRopeErasureType::ErasureParityStripe, 3, 2, 3} // 7 = ErasureSpicies::Erasure3Plus2Stipe
    ,{TRopeErasureType::ErasureMirror,       1, 2, 1} // 8 = ErasureSpicies::ErasureMirror3Plus2
    ,{TRopeErasureType::ErasureMirror,       1, 2, 1} // 9 = ErasureSpicies::ErasureMirror3dc
    ,{TRopeErasureType::ErasureParityBlock,  4, 3, 5} // 10 = ErasureSpicies::Erasure4Plus3Block
    ,{TRopeErasureType::ErasureParityStripe, 4, 3, 5} // 11 = ErasureSpicies::Erasure4Plus3Stripe
    ,{TRopeErasureType::ErasureParityBlock,  3, 3, 3} // 12 = ErasureSpicies::Erasure3Plus3Block
    ,{TRopeErasureType::ErasureParityStripe, 3, 3, 3} // 13 = ErasureSpicies::Erasure3Plus3Stripe
    ,{TRopeErasureType::ErasureParityBlock,  2, 3, 3} // 14 = ErasureSpicies::Erasure2Plus3Block
    ,{TRopeErasureType::ErasureParityStripe, 2, 3, 3} // 15 = ErasureSpicies::Erasure2Plus3Stripe
    ,{TRopeErasureType::ErasureParityBlock,  2, 2, 3} // 16 = ErasureSpicies::Erasure2Plus2Block
    ,{TRopeErasureType::ErasureParityStripe, 2, 2, 3} // 17 = ErasureSpicies::Erasure2Plus2Stripe
    ,{TRopeErasureType::ErasureMirror,       1, 2, 1} // 18 = ErasureSpicies::ErasureMirror3of4
}};

void PadAndCrcAtTheEnd(TRopeHelpers::Iterator data, ui64 dataSize, ui64 bufferSize) {
    ui64 marginSize = bufferSize - dataSize - sizeof(ui32);
    if (marginSize) {
        TRopeUtils::Memset(data + dataSize, 0, marginSize);
    }
    ui32 hash = TRopeHelpers::GetCrc32c(data, dataSize);
    TRopeUtils::Memcpy(data + (bufferSize - sizeof(ui32)), (const char *)&hash, sizeof(ui32));
}

bool CheckCrcAtTheEnd(TRopeErasureType::ECrcMode crcMode, const TRope& buf) {
    switch (crcMode) {
    case TRopeErasureType::CrcModeNone:
        return true;
    case TRopeErasureType::CrcModeWholePart:
        if (buf.GetSize() == 0) {
                return true;
        } else {
            Y_ABORT_UNLESS(buf.GetSize() >= sizeof(ui32), "Error in CheckWholeBlobCrc: blob part size# %" PRIu64
                    " is less then crcSize# %" PRIu64, (ui64)buf.GetSize(), (ui64)sizeof(ui32));
            ui32 crc = TRopeHelpers::GetCrc32c(buf.Begin(), buf.GetSize() - sizeof(ui32));
            TString expectedStringCrc = TRope(buf.Begin() + buf.GetSize() - sizeof(ui32), buf.End()).ConvertToString();
            ui32 expectedCrc = ReadUnaligned<ui32>(expectedStringCrc.data());
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
    ui64 WholeBlocks; // Data consists of (WhloeBlocks * BlockSize + TailSize) bytes
    ui32 TailSize;

    ui32 Prime;
    TRopeErasureType::ECrcMode CrcMode;

    using TBufferDataPart = TStackVec<TRopeHelpers::TRopeFastView, MAX_TOTAL_PARTS>;
    TBufferDataPart BufferDataPart;
    TRopeHelpers::TRopeFastView Data;

    TBlockParams(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, ui64 dataSize) {
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
    }

    inline void PrepareInputDataPointers(TRopeHelpers::Iterator data) {
        BufferDataPart.resize(DataParts);
        for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
            BufferDataPart[i] = TRopeHelpers::TRopeFastView(data);
            data += LargePartSize;
        }
        for (ui32 i = FirstSmallPartIdx; i < DataParts; ++i) {
            BufferDataPart[i] = TRopeHelpers::TRopeFastView(data);
            data += SmallPartSize;
        }
    }

    inline void XorSplitWhole(TBufferDataPart &bufferDataPart,
            TDataPartSet &outPartSet, ui64 writePosition, ui32 blocks) {
        ui64 readPosition = 0;
        VERBOSE_COUT("XorSplitWhole:" << Endl);
        for (ui64 blockIdx = 0; blockIdx < blocks; ++blockIdx) {
            for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                ui64 xored = 0;
                for (ui32 part = 0; part < DataParts; ++part) {
                    ui64 sourceData;
                    sourceData = bufferDataPart[part].At64(readPosition);
                    xored ^= sourceData;
                    VERBOSE_COUT(DebugFormatBits(sourceData) << ", ");
                    *(ui64*)(outPartSet.Parts[part].GetDataAt(writePosition)) = sourceData;
                }
                *(ui64*)(outPartSet.Parts[DataParts].GetDataAt(writePosition)) = xored;
                VERBOSE_COUT(DebugFormatBits(xored) << Endl);
                writePosition += sizeof(ui64);
                ++readPosition;
            }
        }
        VERBOSE_COUT(Endl);
    }

    inline void XorSplit(TDataPartSet &outPartSet) {
        VERBOSE_COUT("XorSplit:" << Endl);
        // Write data and parity
        XorSplitWhole(BufferDataPart, outPartSet, 0ull, WholeBlocks);

        // Use the remaining parts to fill in the last block
        // Write the tail of the data
        if (TailSize) {
            TRope lastBlockSource = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            PrepareLastBlockData(lastBlockSource.Begin(), bufferDataPart);

            XorSplitWhole(bufferDataPart, outPartSet, WholeBlocks * ColumnSize, 1);
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



    template <bool isFastMode, class TData>
    class EoView {
    private:
        TData& Data;
        ui64 Offset;

    public:
        EoView(TData& data, ui64 offset)
                : Data(data)
                , Offset(offset) {
        }

        ui64& At(ui64 row, ui64 column) {
            if constexpr (std::is_same_v<TDataPartSet, TData>) {
                if constexpr (isFastMode) {
                    return *(ui64*)(Data.Parts[column].FastDataPtrAt(Offset + row * sizeof(ui64)));
                } else {
                    return *(ui64*)(Data.Parts[column].GetDataAt(Offset + row * sizeof(ui64)));
                }
            } else {
                return *(ui64*) (Data[column] + row * sizeof(ui64));
            }
        }
    };

    template <bool isFastMode, class TData>
    void EoSplitLoop(TDataPartSet& outPartSet, ui64 writePosition, ui32 blocks, TData& inPartSet) {

        const ui32 m = Prime;


        for (ui64 blockIdx = 0; blockIdx < blocks; ++blockIdx) {

            EoView<isFastMode, TData> in(inPartSet, writePosition);
            EoView<isFastMode, TDataPartSet> out(outPartSet, writePosition);

            VERBOSE_COUT_BLOCK(true, IN_EL_BLOCK, IN_EL_BLOCK, OUT_M, OUT_M1);

            ui64 adj = 0;
            const ui32 mint = (m - 2 < LineCount ? 1 : m - 2 - LineCount);
            VERBOSE_COUT("mint = " << mint << " m - 1 - t = " << (m - 1 - mint) << Endl);
            for (ui32 t = mint; t < DataParts; ++t) {
                adj ^= in.At(m - 1 - t, t);
                VERBOSE_COUT("s: " << adj << " el[" << (m - 1 - t) << ", " << t << "]: " <<
                                   DebugFormatBits(in.At(m - 1 - t, t)) << Endl);
            }
            for (ui32 l = 0; l < LineCount; ++l) {
                ui64 sourceData = in.At(l, 0);
                out.At(l, DataParts + 1) = adj ^ sourceData;
                out.At(l, DataParts) = sourceData;
            }
            for (ui32 t = 1; t < DataParts; ++t) {
                for (ui32 l = 0; l < LineCount; ++l) {
                    out.At(l, DataParts) ^= in.At(l, t);
                    VERBOSE_COUT("OUT_M(" << l << ") = " <<
                              DebugFormatBits(out.At(l, DataParts)) << Endl);
                }
            }
            for (ui32 t = 1; t < DataParts; ++t) {
                for (ui32 l = 0; l < LineCount - t; ++l) {
                    ui32 row = l + t;
                    out.At(row, DataParts + 1) ^= in.At(l, t);
                    VERBOSE_COUT(DebugFormatBits(in.At(row, t)) << Endl);
                }
                for (ui32 l = LineCount - t + 1; l < LineCount; ++l) {
                    ui32 row = l + t - m;
                    out.At(row, DataParts + 1) ^= in.At(l, t);
                    VERBOSE_COUT(DebugFormatBits(in.At(row, t)) << Endl);
                }
            }

            VERBOSE_COUT_BLOCK(true, OUT_EL, OUT_EL, OUT_M, OUT_M1);
            writePosition += ColumnSize;
        }
    }

    void EoSplitWhole(TDataPartSet &outPartSet, ui64 writePosition, ui32 blocks) {
        for (ui64 blockIdx = 0; blockIdx < blocks; ) {
            ui64 contiguousSize = PartContainerSize;
            for (ui32 i = 0; i < DataParts + 2; ++i) {
                contiguousSize = Min(contiguousSize, outPartSet.Parts[i].GetContiguousSize(writePosition));
            }
            ui64 contiguousBlocks = contiguousSize / ColumnSize;
            contiguousBlocks = Min(contiguousBlocks, blocks - blockIdx);

            EoSplitLoop<true>(outPartSet, writePosition, contiguousBlocks, outPartSet);
            writePosition += contiguousBlocks * ColumnSize;
            blockIdx += contiguousBlocks;

            if (blockIdx == blocks) {
                break;
            }

            bool isAligned = true;
            for (ui32 i = 0; i < DataParts; ++i) {
                ui64 currentSize = outPartSet.Parts[i].GetContiguousSize(writePosition);
                if (currentSize < ColumnSize && currentSize % sizeof(ui64)) {
                    isAligned = false;
                    break;
                }
            }

            if (isAligned) {
                EoSplitLoop<false>(outPartSet, writePosition, 1u, outPartSet);
            } else {
                char buffer[MAX_TOTAL_PARTS][(MAX_TOTAL_PARTS - 2) * sizeof(ui64)];
                for (ui32 i = 0; i < DataParts; ++i) {
                    TRopeUtils::Memcpy(buffer[i], outPartSet.Parts[i].FastViewer.GetCurrent(writePosition), ColumnSize);
                }
                EoSplitLoop<false>(outPartSet, writePosition, 1u, buffer);
            }

            writePosition += ColumnSize;
            blockIdx += 1;
        }
    }

    template <bool isFromDataParts>
    void StarSplitWhole(TBufferDataPart &bufferDataPart, TDataPartSet &outPartSet,
            ui64 writePosition, ui32 blocks) {
        const ui32 m = Prime;
#define IN_EL_SB(row, column) bufferDataPart[column].At64(blockIdx * LineCount + (row))
#define OUT_EL(row, column) *((ui64*)(outPartSet.Parts[column].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define IN_EL(row, column) (isFromDataParts ? OUT_EL(row, column) : IN_EL_SB(row, column))
#define OUT_M(row) *((ui64*)(outPartSet.Parts[DataParts].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define OUT_M1(row) *((ui64*)(outPartSet.Parts[DataParts + 1].GetDataAt(writePosition + (row) * sizeof(ui64))))
#define OUT_M2(row) *((ui64*)(outPartSet.Parts[DataParts + 2].GetDataAt(writePosition + (row) * sizeof(ui64))))
        for (ui64 blockIdx = 0; blockIdx < blocks; ++blockIdx) {
            VERBOSE_COUT_BLOCK(true, IN_EL_BLOCK, IN_EL_BLOCK, OUT_M, OUT_M1);
            ui64 s1 = 0;
            const ui32 mint = (m - 2 < LineCount ? 1 : m - 2 - LineCount);
            VERBOSE_COUT("mint = " << mint << " m - 1 - t = " << (m - 1 - mint) << Endl);
            for (ui32 t = mint; t < DataParts; ++t) {
                s1 ^= IN_EL(m - 1 - t, t);
                VERBOSE_COUT("s1: " << s1 << " el[" << (m - 1 - t) << ", " << t << "]: " <<
                    DebugFormatBits(IN_EL_BLOCK(m - 1 - t, t)) << Endl);
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
    }

    void PrepareLastBlockData(TRopeHelpers::Iterator lastBlockSource, TBufferDataPart &bufferDataPart) {
        bufferDataPart.resize(DataParts);
        for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
            bufferDataPart[i] = TRopeHelpers::TRopeFastView(lastBlockSource);
            TRopeUtils::Memcpy(bufferDataPart[i].GetBegin(), BufferDataPart[i].GetBegin() + WholeBlocks * ColumnSize,
                    ColumnSize);
            lastBlockSource += ColumnSize;
        }
        for (ui32 i = FirstSmallPartIdx; i < DataParts - 1; ++i) {
            bufferDataPart[i] = TRopeHelpers::TRopeFastView(lastBlockSource);
            TRopeUtils::Memset(bufferDataPart[i].GetBegin(), 0, ColumnSize);
            lastBlockSource += ColumnSize;
        }
        bufferDataPart[DataParts - 1] = TRopeHelpers::TRopeFastView(lastBlockSource);
        if (LastPartTailSize) {
            TRopeUtils::Memcpy(bufferDataPart[DataParts - 1].GetBegin(), BufferDataPart[DataParts - 1].GetBegin() + WholeBlocks * ColumnSize,
                LastPartTailSize);
        }
        TRopeUtils::Memset(bufferDataPart[DataParts - 1].GetBegin() + LastPartTailSize, 0, ColumnSize - LastPartTailSize);
    }

    void PrepareLastBlockPointers(TRopeHelpers::Iterator lastBlockSource, TBufferDataPart &bufferDataPart) {
        bufferDataPart.resize(DataParts);
        for (ui32 i = 0; i < DataParts; ++i) {
            bufferDataPart[i] = TRopeHelpers::TRopeFastView(lastBlockSource);
            lastBlockSource += ColumnSize;
        }
    }

    void PlaceLastBlock(TBufferDataPart& bufferDataPart) {
        for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
            TRopeUtils::Memcpy(BufferDataPart[i].GetBegin() + WholeBlocks * ColumnSize, bufferDataPart[i].GetBegin(), ColumnSize);
        }
        TRopeUtils::Memcpy(BufferDataPart[DataParts - 1].GetBegin() + WholeBlocks * ColumnSize,
                                bufferDataPart[DataParts - 1].GetBegin(), LastPartTailSize);
    }

    template <bool isFromDataParts>
    void StarSplit(TDataPartSet &outPartSet) {
        // Use all whole columns of all the parts
        StarSplitWhole<isFromDataParts>(BufferDataPart, outPartSet, 0ull, WholeBlocks);

        // Use the remaining parts to fill in the last block
        // Write the tail of the data
        if (TailSize) {
            TRope lastBlockSource = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            if (!isFromDataParts) {
                PrepareLastBlockData(lastBlockSource.Begin(), bufferDataPart);
            }

            StarSplitWhole<isFromDataParts>(bufferDataPart, outPartSet, WholeBlocks * ColumnSize, 1);
        }
    }

    void EoSplit(TDataPartSet &outPartSet) {
        Y_ABORT_UNLESS(outPartSet.Parts[0].Offset % ColumnSize == 0);
        ui64 readPosition = outPartSet.Parts[0].Offset;
        ui64 wholeBlocks = Min(WholeBlocks - readPosition / ColumnSize, outPartSet.Parts[0].Size / ColumnSize);
        bool hasTail = TailSize && (outPartSet.Parts[0].Size + readPosition > WholeBlocks * ColumnSize);

        TRACE(__LINE__ << " EoSplit readPosition# " << readPosition
                << " wholeBlocks# " << wholeBlocks
                << " hasTail# " << hasTail
                << " fullDataSize# " << outPartSet.FullDataSize
                << Endl);
        // Use all columns of all the parts
        EoSplitWhole(outPartSet, readPosition, wholeBlocks + hasTail);
    }

    template <bool restoreParts>
    void GlueOutBuffer(TRope& dst, const TDataPartSet& partSet, ui32 missingPartIdxA, ui32 missingPartIdxB) {
        Y_ABORT_UNLESS(dst.GetSize() == 0);
        for (ui32 i = 0; i < DataParts; ++i) {
            const TPartFragment& part = partSet.Parts[i];
            Y_ABORT_UNLESS(part.Offset == 0 && part.Size == part.PartSize);
            ui64 partSize = i < FirstSmallPartIdx ? LargePartSize : SmallPartSize;
            partSize = i == DataParts - 1 ? SmallPartSize + LastPartTailSize : partSize;
            if (!restoreParts && (i == missingPartIdxA || i == missingPartIdxB)) {
                dst.Insert(dst.End(), TRopeHelpers::RopeUninitialized(partSize));
            } else {
                dst.Insert(dst.End(), TRope(part.OwnedRope.Begin(), part.OwnedRope.Begin() + partSize));
            }
        }
    }

    void GlueBlockPartsMemcpy(TRopeHelpers::Iterator dst, const TDataPartSet& partSet) const {
        if (LargePartSize) {
            for (ui32 i = 0; i < FirstSmallPartIdx; ++i) {
                TRopeUtils::Memcpy(dst, partSet.Parts[i].OwnedRope.Begin(), LargePartSize);
                dst += LargePartSize;
            }
            if (SmallPartSize) {
                for (ui32 i = FirstSmallPartIdx; i < DataParts - 1; ++i) {
                    TRopeUtils::Memcpy(dst, partSet.Parts[i].OwnedRope.Begin(), SmallPartSize);
                    dst += SmallPartSize;
                }
            }
        }
        if (SmallPartSize + LastPartTailSize) {
            TRopeUtils::Memcpy(dst, partSet.Parts[DataParts - 1].OwnedRope.Begin(), SmallPartSize + LastPartTailSize);
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
    template <bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
    void EoDiagonalRestorePartWhole(TBufferDataPart &bufferDataPart, TDataPartSet &partSet, ui64 readPosition,
            ui32 beginBlockIdx, ui32 endBlockIdx, ui32 missingDataPartIdx) {
        ui32 lastColumn = reversed ? DataParts + 2 : DataParts + 1;
        const ui32 m = Prime;
        // Use all whole columns of all the parts
        ui64 fullDataBuffer[MAX_TOTAL_PARTS][MAX_TOTAL_PARTS - 2];
        char partsBuffer[MAX_TOTAL_PARTS][(MAX_TOTAL_PARTS - 2) * sizeof(ui64)];

        for (ui64 blockIdx = beginBlockIdx; blockIdx < endBlockIdx; ++blockIdx) {
#define RIGHT_ROW(row) (reversed ? LineCount - 1 - (row) : (row))
#define OUT_EL_BLOCK(row, column) fullDataBuffer[column][RIGHT_ROW(row)]
#define IN_EL(row, column) *((ui64*)(partsBuffer[column] + RIGHT_ROW(row) * sizeof(ui64)))
#define IN_M(row) *((ui64*)(partSet.Parts[DataParts].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define IN_M12(row) *((ui64*)(partsBuffer[lastColumn] + RIGHT_ROW(row) * sizeof(ui64)))

            ui64 readContiguousSize = DataSize;
            ui64 writeContiguousSize = DataSize;
            if (restoreFullData) {
                for (ui32 i = 0; i < DataParts; ++i) {
                    writeContiguousSize = std::min(writeContiguousSize,
                            bufferDataPart[i].GetContiguousSize(blockIdx * LineCount * sizeof(ui64)));
                }
            }
            for (ui32 i = 0; i < DataParts; ++i) {
                if (restoreParts || i != missingDataPartIdx) {
                    readContiguousSize = std::min(readContiguousSize,
                            partSet.Parts[i].GetContiguousSize(readPosition));
                }
            }
            if (restoreParityParts) {
                readContiguousSize = std::min(readContiguousSize,
                        partSet.Parts[DataParts].GetContiguousSize(readPosition));
            }
            readContiguousSize = std::min(readContiguousSize,
                    partSet.Parts[lastColumn].GetContiguousSize(readPosition));

            readContiguousSize /= ColumnSize;
            writeContiguousSize /= LineCount * sizeof(ui64);
            ui64 contiguousSize = std::min(readContiguousSize, writeContiguousSize);

            for (ui32 i = 0; i < contiguousSize && blockIdx < endBlockIdx; ++i, ++blockIdx) {

#define FAST_OUT_EL_BLOCK(row, column) bufferDataPart[column].FastAt64(blockIdx * LineCount + RIGHT_ROW(row))
#define FAST_IN_EL(row, column) *((ui64*)(partSet.Parts[column].FastDataPtrAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define FAST_IN_M(row) *((ui64*)(partSet.Parts[DataParts].FastDataPtrAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define FAST_IN_M12(row) *((ui64*)(partSet.Parts[lastColumn].FastDataPtrAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))

                VERBOSE_COUT_BLOCK(true, FAST_IN_EL, FAST_IN_EL, FAST_IN_M, FAST_IN_M12);
                ui64 s = 0;
                ui32 colLimit = DataParts;
                ui32 rowLimit = LineCount;
                {
                    ui32 idx = (m + missingDataPartIdx - 1) % m;
                    if (idx < rowLimit) {
                        s = FAST_IN_M12(idx);
                        VERBOSE_COUT("s(" << idx << ", m1): " << DebugFormatBits(s) << Endl);
                    }
                }
                for (ui32 l = 0; l < colLimit; ++l) {
                    ui32 idx = (m + missingDataPartIdx - l - 1) % m;
                    if (idx < LineCount) {
                        ui64 value = FAST_IN_EL(idx, l);
                        s ^= value;
                        if (restoreFullData) {
                            VERBOSE_COUT("a [" << idx << ", " << l << "] = " << DebugFormatBits(value) << Endl);
                            FAST_OUT_EL_BLOCK(idx, l) = value;
                        }
                    }
                }
                VERBOSE_COUT("s: " << DebugFormatBits(s) << Endl);
                for (ui32 k = 0; k < LineCount; ++k) {
                    ui64 res = s;
                    for (ui32 l = 0; l < missingDataPartIdx; ++l) {
                        ui32 idx = (m + k + missingDataPartIdx - l) % m;
                        if (idx < LineCount) {
                            ui64 value = FAST_IN_EL(idx, l);
                            res ^= value;
                            if (restoreFullData) {
                                VERBOSE_COUT("b [" << idx << ", " << l << "] = " << DebugFormatBits(value) << Endl);
                                FAST_OUT_EL_BLOCK(idx, l) = value;
                            }
                        }
                    }
                    for (ui32 l = missingDataPartIdx + 1; l < colLimit; ++l) {
                        ui32 idx = (m + k + missingDataPartIdx - l) % m;
                        if (idx < LineCount) {
                            ui64 value = FAST_IN_EL(idx, l);
                            res ^= value;
                            if (restoreFullData) {
                                VERBOSE_COUT("c [" << idx << ", " << l << "] = " << DebugFormatBits(value) << Endl);
                                FAST_OUT_EL_BLOCK(idx, l) = value;
                            }
                        }
                    }
                    ui32 idx = (m + k + missingDataPartIdx) % m;
                    if (idx < LineCount) {
                        VERBOSE_COUT("idx = " << idx);
                        res ^= FAST_IN_M12(idx); // This is missing in the article!
                    }
                    if (restoreFullData) {
                        VERBOSE_COUT("out [" << k << ", " << missingDataPartIdx << "] = " << DebugFormatBits(res) << Endl);
                        FAST_OUT_EL_BLOCK(k, missingDataPartIdx) = res;
                    }
                    if (restoreParts) {
                        FAST_IN_EL(k, missingDataPartIdx) = res;
                        if (restoreParityParts) {
                            ui64 tmp = 0;
                            for (ui32 l = 0; l < DataParts; ++l) {
                                tmp ^= FAST_IN_EL(k, l);
                            }
                            FAST_IN_M(k) = tmp;
                        }
                    }
                }

                VERBOSE_COUT_BLOCK(restoreFullData, FAST_OUT_EL_BLOCK, FAST_IN_EL, FAST_IN_M, FAST_IN_M12);
                readPosition += ColumnSize;
#undef FAST_OUT_EL_BLOCK
#undef FAST_IN_EL
#undef FAST_IN_M
#undef FAST_IN_M12
            }
            if (blockIdx == endBlockIdx) {
                break;
            }

            for (ui32 i = 0; i < DataParts; ++i) {
                if (i != missingDataPartIdx) {
                    TRopeUtils::Memcpy(partsBuffer[i], partSet.Parts[i].FastViewer.GetCurrent(readPosition), ColumnSize);
                }
            }

            if (restoreParts) {
                TRopeUtils::Memcpy(partsBuffer[missingDataPartIdx],
                        partSet.Parts[missingDataPartIdx].FastViewer.GetCurrent(readPosition), ColumnSize);
            }

            TRopeUtils::Memcpy(partsBuffer[lastColumn],
                    partSet.Parts[lastColumn].FastViewer.GetCurrent(readPosition), ColumnSize);

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
                        OUT_EL_BLOCK(idx, l) = value;
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
                            OUT_EL_BLOCK(idx, l) = value;
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
                            OUT_EL_BLOCK(idx, l) = value;
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
                    OUT_EL_BLOCK(k, missingDataPartIdx) = res;
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

            VERBOSE_COUT_BLOCK(restoreFullData, OUT_EL_BLOCK, IN_EL, IN_M, IN_M12);

            if (restoreParts) {
                TRopeUtils::Memcpy(partSet.Parts[missingDataPartIdx].FastViewer.GetCurrent(readPosition),
                                  partsBuffer[missingDataPartIdx], ColumnSize);
            }

            if (restoreFullData) {
                for (ui32 i = 0; i < DataParts; ++i) {
                    TRopeUtils::Memcpy(bufferDataPart[i].GetCurrent(blockIdx * LineCount * sizeof(ui64)),
                                      (const char*)fullDataBuffer[i], LineCount * sizeof(ui64));
                }
            }

#undef IN_M12
#undef IN_M
#undef IN_EL
#undef OUT_EL_BLOCK
#undef RIGHT_ROW

            readPosition += ColumnSize;
        }
    }

    template <bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
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

        EoDiagonalRestorePartWhole<restoreParts, restoreFullData, reversed, restoreParityParts>(
                BufferDataPart, partSet, readPosition, beginBlock, endBlock, missingDataPartIdx);

        // Read the tail of the data
        if (TailSize && (partSet.Parts[presentPartIdx].Size + readPosition > WholeBlocks * ColumnSize)) {
            TRACE("EoDiagonalRestorePart tail" << Endl);
            TRope lastBlock = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers(lastBlock.Begin(), bufferDataPart);

            EoDiagonalRestorePartWhole<restoreParts, restoreFullData, reversed, restoreParityParts>(bufferDataPart,
                            partSet, WholeBlocks * ColumnSize, 0, 1, missingDataPartIdx);

            if (restoreFullData) {
                PlaceLastBlock(bufferDataPart);
            }
        }
        if (restoreParts && missingDataPartIdx < partSet.Parts.size()) {
            PadAndCrcPart(partSet, missingDataPartIdx);
        }
    }

    template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarMainRestorePartsWholeSymmetric(TBufferDataPart &bufferDataPart, TDataPartSet& partSet,
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
#define OUT_EL(row, column) bufferDataPart[column].At64(blockIdx * LineCount + (row))
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
        EoMainRestorePartsWhole<restoreParts, restoreFullData, false, restoreParityParts>(bufferDataPart,
                                        partSet, readPositionStart, endBlockIdx, Min(r, t), Max(r,t));
#undef IN_M2
#undef IN_M1
#undef IN_M
#undef IN_EL
#undef OUT_EL
    }

    template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarRestoreHorizontalPartWhole(TBufferDataPart &bufferDataPart, TDataPartSet& partSet,
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
#define OUT_EL(row, column) bufferDataPart[column].At64(blockIdx * LineCount + (row))
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
        EoMainRestorePartsWhole<restoreParts, restoreFullData, false, restoreParityParts>(bufferDataPart,
                                        partSet, readPositionStart, endBlockIdx, r, s);
#undef IN_M2
#undef IN_M1
#undef IN_M
#undef IN_EL
#undef OUT_EL
    }


    template <bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
    void EoMainRestorePartsWhole(TBufferDataPart &bufferDataPart, TDataPartSet& partSet, ui64 readPosition,
            ui32 endBlockIdx, ui32 missingDataPartIdxA, ui32 missingDataPartIdxB) {
        VERBOSE_COUT("Start of EoMainRestorePartsWhole" << Endl);
        ui32 lastColumn = reversed ? DataParts + 2 : DataParts + 1;
        const ui32 m = Prime;
        ui64 fullDataBuffer[MAX_TOTAL_PARTS][MAX_TOTAL_PARTS - 2];
        char partsBuffer[MAX_TOTAL_PARTS][(MAX_TOTAL_PARTS - 2) * sizeof(ui64)];
        // Use all whole columns of all the parts
#define RIGHT_ROW(row) (reversed ? LineCount - 1 - (row) : (row))
#define OUT_EL(row, column) fullDataBuffer[column][RIGHT_ROW(row)]
#define IN_EL(row, column) *((ui64*)(partsBuffer[column] + RIGHT_ROW(row) * sizeof(ui64)))
#define IN_EL_P(row, column) *((ui64*)(partSet.Parts[column].GetDataAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define IN_M(row) *((ui64*)(partsBuffer[DataParts] + RIGHT_ROW(row) * sizeof(ui64)))
#define IN_M12(row) *((ui64*)(partsBuffer[lastColumn] + RIGHT_ROW(row) * sizeof(ui64)))
        for (ui64 blockIdx = 0; blockIdx < endBlockIdx; ++blockIdx) {

            ui64 readContiguousSize = DataSize;
            ui64 writeContiguousSize = DataSize;
            if (restoreFullData) {
                for (ui32 i = 0; i < DataParts; ++i) {
                    writeContiguousSize = std::min(writeContiguousSize,
                            bufferDataPart[i].GetContiguousSize(blockIdx * LineCount * sizeof(ui64)));
                }
            }
            for (ui32 i = 0; i <= DataParts; ++i) {
                if (restoreParts || (i != missingDataPartIdxA && i != missingDataPartIdxB)) {
                    readContiguousSize = std::min(readContiguousSize, partSet.Parts[i].GetContiguousSize(readPosition));
                }
            }

            readContiguousSize = std::min(readContiguousSize, partSet.Parts[lastColumn].GetContiguousSize(readPosition));

            readContiguousSize /= ColumnSize;
            writeContiguousSize /= LineCount * sizeof(ui64);
            ui64 contiguousSize = std::min(readContiguousSize, writeContiguousSize);

            for (ui32 i = 0; i < contiguousSize && blockIdx < endBlockIdx; ++i, ++blockIdx) {

#define FAST_OUT_EL(row, column) bufferDataPart[column].FastAt64(blockIdx * LineCount + RIGHT_ROW(row))
#define FAST_IN_EL(row, column) *((ui64*)(partSet.Parts[column].FastDataPtrAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define FAST_IN_M(row) *((ui64*)(partSet.Parts[DataParts].FastDataPtrAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))
#define FAST_IN_M12(row) *((ui64*)(partSet.Parts[lastColumn].FastDataPtrAt(readPosition + RIGHT_ROW(row) * sizeof(ui64))))

                VERBOSE_COUT_BLOCK(true, IN_EL, IN_EL, IN_M, IN_M12);
                // compute diagonal partiy s
                ui64 s = 0;
                ui64 s0[MAX_LINES_IN_BLOCK];
                for (ui32 l = 0; l < LineCount; ++l) {
                    ui64 tmp = FAST_IN_M(l);
                    s0[l] = tmp;
                    s ^= tmp;
                    s ^= FAST_IN_M12(l);
                    VERBOSE_COUT("Diag [l,m] s:" << DebugFormatBits(s) << Endl);
                }

                // compute horizontal syndromes s0
                for (ui32 t = 0; t < DataParts; ++t) {
                    if (t == missingDataPartIdxA || t == missingDataPartIdxB) {
                        continue;
                    }
                    for (ui32 l = 0; l < LineCount; ++l) {
                        ui64 val = FAST_IN_EL(l, t);
                        s0[l] ^= val;
                        if (restoreFullData) {
                            FAST_OUT_EL(l, t) = val;
                        }
                    }
                }

                // compute diagonal syndromes s1
                ui64 s1[MAX_LINES_IN_BLOCK];
                for (ui32 u = 0; u < m; ++u) {
                    s1[u] = s;
                    VERBOSE_COUT("S1 = s = " << DebugFormatBits(s1[u]) << Endl);
                    if (u < LineCount) {
                        s1[u] ^= FAST_IN_M12(u);
                        VERBOSE_COUT("S1 ^= a[" << u << ", m+1] = " << DebugFormatBits(s1[u]) << Endl);
                    }
                    for (ui32 l = 0; l < missingDataPartIdxA; ++l) {
                        ui32 idx = (m + u - l) % m;
                        if (idx < LineCount) {
                            ui64 val = FAST_IN_EL(idx, l);
                            s1[u] ^= val;
                        }
                        VERBOSE_COUT("S1 ^= a[" << idx << ", " << l << "] = " << DebugFormatBits(s1[u]) << Endl);
                    }
                    for (ui32 l = missingDataPartIdxA + 1; l < missingDataPartIdxB; ++l) {
                        ui32 idx = (m + u - l) % m;
                        if (idx < LineCount) {
                            ui64 val = FAST_IN_EL(idx, l);
                            s1[u] ^= val;
                        }
                        VERBOSE_COUT("S1 ^= a[" << idx << ", " << l << "] = " << DebugFormatBits(s1[u]) << Endl);
                    }
                    for (ui32 l = missingDataPartIdxB + 1; l < DataParts; ++l) {
                        ui32 idx = (m + u - l) % m;
                        if (idx < LineCount) {
                            ui64 val = FAST_IN_EL(idx, l);
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
                            FAST_IN_EL(s, missingDataPartIdxB) = bVal;
                            VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxB << "] = " << DebugFormatBits(bVal)
                                                   << Endl);
                        }
                        if (restoreFullData) {
                            FAST_OUT_EL(s, missingDataPartIdxB) = bVal;
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
                            FAST_IN_EL(s, missingDataPartIdxA) = aVal;
                            VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxA << "] = " << DebugFormatBits(bVal)
                                                   << Endl);
                        }
                        if (restoreFullData) {
                            FAST_OUT_EL(s, missingDataPartIdxA) = aVal;
                            VERBOSE_COUT("write [" << s << ", " << missingDataPartIdxA << "] = " << DebugFormatBits(bVal)
                                                   << Endl);
                        }
                    }

                    s = (m + s - (missingDataPartIdxB - missingDataPartIdxA)) % m;
                } while (s != m - 1);
                VERBOSE_COUT_BLOCK(restoreFullData, OUT_EL, IN_EL, IN_M, IN_M12);
                readPosition += ColumnSize;

#undef FAST_OUT_EL
#undef FAST_IN_EL
#undef FAST_IN_M
#undef FAST_IN_M12

            }

            if (blockIdx == endBlockIdx) {
                break;
            }

            for (ui32 i = 0; i <= DataParts; ++i) {
                if (i != missingDataPartIdxA && i != missingDataPartIdxB) {
                    TRopeUtils::Memcpy(partsBuffer[i],
                            partSet.Parts[i].FastViewer.GetCurrent(readPosition), ColumnSize);
                }
            }

            TRopeUtils::Memcpy(partsBuffer[lastColumn],
                              partSet.Parts[lastColumn].FastViewer.GetCurrent(readPosition), ColumnSize);

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
                        IN_EL_P(s, missingDataPartIdxB) = bVal;
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
                        IN_EL_P(s, missingDataPartIdxA) = aVal;
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
#undef IN_EL_P

            if (restoreFullData) {
                for (ui32 i = 0; i < DataParts; ++i) {
                    TRopeUtils::Memcpy(bufferDataPart[i].GetCurrent(blockIdx * LineCount * sizeof(ui64)),
                                      (const char *) fullDataBuffer[i], LineCount * sizeof(ui64));
                }
            }

            readPosition += ColumnSize;
        }
    }

    template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarRestoreHorizontalPart(TDataPartSet& partSet, ui32 missingDataPartIdxA,
                                        ui32 missingDataPartIdxB) {
        // Read data and parity
        VERBOSE_COUT("StarRestoreHorizontalPart for " << missingDataPartIdxA << " " << missingDataPartIdxB << Endl);
        StarRestoreHorizontalPartWhole<restoreParts, restoreFullData, restoreParityParts>(BufferDataPart,
                    partSet, 0ull, WholeBlocks, missingDataPartIdxA, missingDataPartIdxB);

        if (TailSize) {
            TRope lastBlockSource = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers(lastBlockSource.Begin(), bufferDataPart);

            StarRestoreHorizontalPartWhole<restoreParts, restoreFullData, restoreParityParts>(
                        bufferDataPart, partSet, WholeBlocks * ColumnSize, 1, missingDataPartIdxA,
                        missingDataPartIdxB);

            if (restoreFullData) {
                PlaceLastBlock(bufferDataPart);
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


    template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void StarMainRestorePartsSymmetric(TDataPartSet& partSet, ui32 missingDataPartIdxA,
                                        ui32 missingDataPartIdxB, ui32 missingDataPartIdxC) {
        // Read data and parity
        VERBOSE_COUT("StarMainRestorePartsSymmetric" << Endl);
        StarMainRestorePartsWholeSymmetric<restoreParts, restoreFullData, restoreParityParts>(BufferDataPart,
                    partSet, 0ull, WholeBlocks, missingDataPartIdxA, missingDataPartIdxB, missingDataPartIdxC);

        if (TailSize) {
            TRope lastBlockSource = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers(lastBlockSource.Begin(), bufferDataPart);

            StarMainRestorePartsWholeSymmetric<restoreParts, restoreFullData, restoreParityParts>(
                        bufferDataPart, partSet, WholeBlocks * ColumnSize, 1, missingDataPartIdxA,
                        missingDataPartIdxB, missingDataPartIdxC);

            if (restoreFullData) {
                PlaceLastBlock(bufferDataPart);
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

    template <bool restoreParts, bool restoreFullData, bool reversed, bool restoreParityParts>
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
        EoMainRestorePartsWhole<restoreParts, restoreFullData, reversed, restoreParityParts>(BufferDataPart,
                partSet, readPosition, wholeBlocks, missingDataPartIdxA, missingDataPartIdxB);

        if (TailSize && (partSet.Parts[presentPartIdx].Size + readPosition > WholeBlocks * ColumnSize)) {
            TRACE("EoMainRestoreParts restore tail" << Endl);
            TRope lastBlockSource = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers(lastBlockSource.Begin(), bufferDataPart);

            EoMainRestorePartsWhole<restoreParts, restoreFullData, reversed, restoreParityParts>(
                    bufferDataPart, partSet, WholeBlocks * ColumnSize, 1, missingDataPartIdxA, missingDataPartIdxB);

            if (restoreFullData) {
                PlaceLastBlock(bufferDataPart);
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

    template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
    void XorRestorePartWhole(TBufferDataPart &bufferDataPart, TDataPartSet& partSet,
            ui64 readPosition, ui32 beginBlockIdx, ui32 endBlockIdx, ui32 missingDataPartIdx) {
        VERBOSE_COUT("XorRestorePartWhole: read:" << readPosition << " LineCount: " << LineCount << Endl);
        ui64 writePosition = 0;
        ui64 partsBuffer[MAX_TOTAL_PARTS][MAX_TOTAL_PARTS - 2];
        ui64 fullDataBuffer[MAX_TOTAL_PARTS][MAX_TOTAL_PARTS - 2];
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

            ui64 contiguousSize = DataSize;

            if (restoreFullData) {
                for (ui32 i = 0; i < DataParts; ++i) {
                    contiguousSize = std::min(contiguousSize,
                            bufferDataPart[i].GetContiguousSize(sizeof(ui64) * writePosition));
                }
            }

            for (ui32 i = 0; i <= DataParts; ++i) {
                if (restoreParts || i != missingDataPartIdx) {
                    contiguousSize = std::min(contiguousSize, partSet.Parts[i].GetContiguousSize(readPosition));
                }
            }

            if (restoreParts) {
                contiguousSize = std::min(contiguousSize, partSet.Parts[missingDataPartIdx].GetContiguousSize(readPosition));
            }

            contiguousSize /= sizeof(ui64) * LineCount;

            for (ui64 i = 0; i < contiguousSize && blockIdx < endBlockIdx; ++i, ++blockIdx) {

                for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                    ui64 restoredData = 0;
                    for (ui32 part = 0; part < DataParts; ++part) {
                        if (part != missingDataPartIdx) {
                            ui64 partData = *reinterpret_cast<const ui64*>(partSet.Parts[part].FastDataPtrAt(readPosition));
                            restoredData ^= partData;
                            if (restoreFullData) {
                                bufferDataPart[part].FastAt64(writePosition) = partData;
                            }
                        }
                    }
                    if (missingDataPartIdx < DataParts) {
                        ui64 partData = *reinterpret_cast<const ui64*>(partSet.Parts[DataParts].FastDataPtrAt(readPosition));
                        restoredData ^= partData;
                        if (restoreFullData) {
                            bufferDataPart[missingDataPartIdx].FastAt64(writePosition) = restoredData;
                        }
                        if (restoreParts) {
                            *reinterpret_cast<ui64*>(partSet.Parts[missingDataPartIdx].FastDataPtrAt(readPosition)) =
                                    restoredData;
                        }
                    } else if (restoreParts && missingDataPartIdx == DataParts) {
                        *reinterpret_cast<ui64*>(partSet.Parts[DataParts].FastDataPtrAt(readPosition)) = restoredData;
                    }
                    readPosition += sizeof(ui64);
                    if (restoreFullData) {
                        ++writePosition;
                    }
                }

            }

            if (blockIdx == endBlockIdx) {
                break;
            }

            for (ui32 i = 0; i <= DataParts; ++i) {
                if (i != missingDataPartIdx) {
                    TRopeUtils::Memcpy((char*)partsBuffer[i],
                               partSet.Parts[i].FastViewer.GetCurrent(readPosition), LineCount * sizeof(ui64));
                }
            }

            for (ui64 lineIdx = 0; lineIdx < LineCount; ++lineIdx) {
                ui64 restoredData = 0;
                for (ui32 part = 0; part < DataParts; ++part) {
                    if (part != missingDataPartIdx) {
                        ui64 partData = partsBuffer[part][lineIdx];
                        restoredData ^= partData;
                        if (restoreFullData) {
                            fullDataBuffer[part][lineIdx] = partData;
                        }
                    }
                }
                if (missingDataPartIdx < DataParts) {
                    ui64 partData = partsBuffer[DataParts][lineIdx];
                    restoredData ^= partData;
                    if (restoreFullData) {
                        fullDataBuffer[missingDataPartIdx][lineIdx] = restoredData;
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
                    ++writePosition;
                }
            }

            if (restoreFullData) {
                for (ui32 i = 0; i < DataParts; ++i) {
                    TRopeUtils::Memcpy(bufferDataPart[i].GetCurrent((writePosition - LineCount) * sizeof(ui64)),
                                      (const char*)fullDataBuffer[i], LineCount * sizeof(ui64));
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

    template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
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
        XorRestorePartWhole<restoreParts, restoreFullData, restoreParityParts>(BufferDataPart, partSet, readPosition,
            beginBlockIdx, beginBlockIdx + wholeBlocks, missingDataPartIdx);

        if (TailSize && (partSet.Parts[presentPartIdx].Size + readPosition > WholeBlocks * ColumnSize)) {
            TRACE("Restore tail, restoreFullData# " << restoreFullData << " resotreParts# " << restoreParts << Endl);
            TRope lastBlockSource = TRopeHelpers::CreateRope(MAX_TOTAL_PARTS * (MAX_TOTAL_PARTS - 2) * sizeof(ui64));
            TBufferDataPart bufferDataPart;
            PrepareLastBlockPointers(lastBlockSource.Begin(), bufferDataPart);

            XorRestorePartWhole<restoreParts, restoreFullData, restoreParityParts>(bufferDataPart,
                partSet, WholeBlocks * ColumnSize, WholeBlocks, WholeBlocks + 1, missingDataPartIdx);

            if (restoreFullData) {
                PlaceLastBlock(bufferDataPart);
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
        case TRopeErasureType::CrcModeNone:
            return;
        case TRopeErasureType::CrcModeWholePart:
            if (DataSize) {
                PadAndCrcAtTheEnd(inOutPartSet.Parts[partIdx].OwnedRope.Begin(), PartUserSize, PartContainerSize);
            } else {
                TRopeUtils::Memset(inOutPartSet.Parts[partIdx].OwnedRope.Begin(), 0, PartContainerSize);
            }
            return;
        }
        ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)CrcMode;
    }
};

void PadAndCrcParts(TRopeErasureType::ECrcMode crcMode, const TBlockParams &p, TDataPartSet &inOutPartSet) {
    if (inOutPartSet.IsFragment) {
        return;
    }
    switch (crcMode) {
    case TRopeErasureType::CrcModeNone:
        return;
    case TRopeErasureType::CrcModeWholePart:
        if (p.DataSize) {
            for (ui32 i = 0; i < p.TotalParts; ++i) {
                PadAndCrcAtTheEnd(inOutPartSet.Parts[i].OwnedRope.Begin(), p.PartUserSize, p.PartContainerSize);
            }
        } else {
            for (ui32 i = 0; i < p.TotalParts; ++i) {
                TRopeUtils::Memset(inOutPartSet.Parts[i].OwnedRope.Begin(), 0, p.PartContainerSize);
            }
        }
        return;
    }
    ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;
}

inline void StarBlockSplit(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, const TRope &buffer,
        TDataPartSet &outPartSet) {
    Y_ABORT_UNLESS(TRopeHelpers::Is8Aligned(buffer));
    TBlockParams p(crcMode, type, buffer.GetSize());

    // Prepare input data pointers
    p.PrepareInputDataPointers(buffer.Begin());

    outPartSet.FullDataSize = buffer.GetSize();
    outPartSet.PartsMask = ~((~(ui32)0) << p.TotalParts);
    outPartSet.Parts.resize(p.TotalParts);
    for (ui32 i = 0; i < p.TotalParts; ++i) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(outPartSet.Parts[i], p.PartContainerSize);
    }
    outPartSet.MemoryConsumed = p.TotalParts * outPartSet.Parts[0].MemoryConsumed();

    p.StarSplit<false>(outPartSet);
    PadAndCrcParts(crcMode, p, outPartSet);
}

inline void EoBlockSplit(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, const TRope &buffer,
        TDataPartSet &outPartSet) {
    TBlockParams p(crcMode, type, buffer.GetSize());

    outPartSet.FullDataSize = buffer.GetSize();
    outPartSet.PartsMask = ~((~(ui32)0) << p.TotalParts);
    outPartSet.Parts.resize(p.TotalParts);

    TRope::TConstIterator iterator = buffer.Begin();
    for (ui32 i = 0; i < p.DataParts; ++i) {
        size_t size = p.SmallPartSize + (i < p.FirstSmallPartIdx) * p.ColumnSize;
        outPartSet.Parts[i].OwnedRope = TRope();
        TRope &rope = outPartSet.Parts[i].OwnedRope;

        if (size) {
            rope = TRope(iterator, iterator + size);
            iterator += size;
        }

        if (i == p.DataParts - 1 && iterator != buffer.End()) {
            rope.Insert(rope.End(), TRope(iterator, buffer.End()));
        }

        TRopeHelpers::Resize(rope, p.PartContainerSize);
        outPartSet.Parts[i].ReferenceTo(rope);
    }

    for (ui32 i = p.DataParts; i < p.TotalParts; ++i) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(outPartSet.Parts[i], p.PartContainerSize);
    }

    outPartSet.MemoryConsumed = p.TotalParts * outPartSet.Parts[0].MemoryConsumed();

    p.EoSplit(outPartSet);

    PadAndCrcParts(crcMode, p, outPartSet);
}

inline void XorBlockSplit(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, const TRope& buffer,
        TDataPartSet& outPartSet) {
    Y_ABORT_UNLESS(TRopeHelpers::Is8Aligned(buffer));
    TBlockParams p(crcMode, type, buffer.GetSize());

    // Prepare input data pointers
    p.PrepareInputDataPointers(buffer.Begin());

    outPartSet.FullDataSize = buffer.GetSize();
    outPartSet.PartsMask = ~((~(ui32)0) << p.TotalParts);
    outPartSet.Parts.resize(p.TotalParts);
    for (ui32 i = 0; i < p.TotalParts; ++i) {
        TRACE("Line# " << __LINE__ << Endl);
        Refurbish(outPartSet.Parts[i], p.PartContainerSize);
    }
    outPartSet.MemoryConsumed = p.TotalParts * outPartSet.Parts[0].MemoryConsumed();

    p.XorSplit(outPartSet);
    PadAndCrcParts(crcMode, p, outPartSet);
}

template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
void EoBlockRestore(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, TDataPartSet& partSet) {
    TRope &outBuffer = partSet.FullDataFragment.OwnedRope;
    ui32 totalParts = type.TotalPartCount();
    Y_ABORT_UNLESS(partSet.Parts.size() >= totalParts);

    if (outBuffer.GetSize()) {
        outBuffer = TRope();
    }

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

    TBlockParams p(crcMode, type, dataSize);

    if (restoreFullData) {
        p.GlueOutBuffer<restoreParts>(outBuffer, partSet, missingDataPartIdxA, missingDataPartIdxB);
    } else if (missingDataPartCount == 0) {
        return;
    }

    if (missingDataPartCount == 2) {
        VERBOSE_COUT("missing parts " << missingDataPartIdxA << " and " << missingDataPartIdxB << Endl);
    } else if (missingDataPartCount == 1) {
        VERBOSE_COUT("missing part " << missingDataPartIdxA << Endl);
    }

    // Restore the fast way if all data parts are present
    if (missingDataPartCount == 0 ||
                (!restoreParts && missingDataPartIdxA >= p.TotalParts - 2)) {
        VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
        return;
    }

    // Prepare output data pointers
    if (restoreFullData) {
        p.PrepareInputDataPointers(outBuffer.Begin());
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
        if (!restoreFullData && restoreParts && missingDataPartIdxB == p.TotalParts - 1) {
            // The (f1) case, but no full data needed, only parts
            TRACE("case# f1" << Endl);
            VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);

            p.XorRestorePart<true, false, false>(partSet, missingDataPartIdxA);
            TRACE("case# f1 split" << Endl);
            p.EoSplit(partSet);
            p.PadAndCrcPart(partSet, missingDataPartIdxA);
            p.PadAndCrcPart(partSet, missingDataPartIdxB);
        } else {
            // Cases (a), (b) and (d2), case (f2) with full data and maybe parts needed
            TRACE("case# a b d2 f2" << Endl);
            VERBOSE_COUT(__LINE__ << " of " << __FILE__ << " missing " << missingDataPartIdxA << Endl);
            p.XorRestorePart<restoreParts, restoreFullData, restoreParityParts>(partSet, missingDataPartIdxA);
            if (restoreParts && missingDataPartIdxB == p.TotalParts - 1 && restoreParityParts) {
                // The (d2a) or (f2a) case with full data and parts needed
                TRACE("case# d2a f2a" << Endl);
                VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
                p.EoSplit(partSet);
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
        if (!restoreFullData) {
            TRACE(__LINE__ << Endl);
            if (!restoreParityParts) {
                TRACE(__LINE__ << Endl);
                return;
            }
            TRACE(__LINE__ << Endl);
        }
        if (restoreParts) {
            TRACE(__LINE__ << Endl);
            VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
            p.EoSplit(partSet);
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
        p.EoDiagonalRestorePart<restoreParts, restoreFullData, false, restoreParityParts>(partSet, missingDataPartIdxA);
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
    p.EoMainRestoreParts<restoreParts, restoreFullData, false, restoreParityParts>(partSet, missingDataPartIdxA,
            missingDataPartIdxB);
    if (restoreParts) {
        p.PadAndCrcPart(partSet, missingDataPartIdxA);
        p.PadAndCrcPart(partSet, missingDataPartIdxB);
    }
}

// restorePartiyParts may be set only togehter with restore parts
template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
void StarBlockRestore(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, TDataPartSet& partSet) {
    Y_ABORT_UNLESS(partSet.Is8Aligned());
    TRope &outBuffer = partSet.FullDataFragment.OwnedRope;

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
        p.PrepareInputDataPointers(outBuffer.Begin());
    } else if (missingDataPartCount == 0) {
        return;
    }

    // Restore the fast way if all data parts are present
    if (missingDataPartCount == 0 ||
            (!restoreParts && missingDataPartIdxA >= p.DataParts)) {
        VERBOSE_COUT(__LINE__ << " of " << __FILE__ << Endl);
        p.GlueBlockPartsMemcpy(outBuffer.Begin(), partSet);
        return;
    }


    // All possible failures of 2 disks which EVENODD capable to handle
    if (missingDataPartCount <= 2 && missingDataPartIdxA != p.TotalParts - 1
            && missingDataPartIdxB != p.TotalParts - 1) {
        if (p.DataParts == 4) {
            TRopeErasureType typeEO(TRopeErasureType::EErasureSpecies::Erasure4Plus2Block);
            EoBlockRestore<restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 3) {
            TRopeErasureType typeEO(TRopeErasureType::EErasureSpecies::Erasure3Plus2Block);
            EoBlockRestore<restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 2) {
            TRopeErasureType typeEO(TRopeErasureType::EErasureSpecies::Erasure2Plus2Block);
            EoBlockRestore<restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
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
            TRopeErasureType typeEO(TRopeErasureType::EErasureSpecies::Erasure4Plus2Block);
            EoBlockRestore<restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 3) {
            TRopeErasureType typeEO(TRopeErasureType::EErasureSpecies::Erasure3Plus2Block);
            EoBlockRestore<restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        } else if (p.DataParts == 2) {
            TRopeErasureType typeEO(TRopeErasureType::EErasureSpecies::Erasure2Plus2Block);
            EoBlockRestore<restoreParts, restoreFullData, restoreParityParts>(crcMode, typeEO, partSet);
        }
        if (restoreParts) {
            if (restoreParityParts) {
                p.StarSplit<true>(partSet);
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
            p.EoMainRestoreParts<restoreParts, restoreFullData, true, restoreParityParts>(partSet, missingDataPartIdxA,
                    missingDataPartIdxB);
        } else {
            // 1 1 1 1   - - +
            p.EoDiagonalRestorePart<restoreParts, restoreFullData, true, restoreParityParts>(partSet, missingDataPartIdxA);
        }
        if (restoreParts) {
            if (restoreParityParts) {
                p.StarSplit<!restoreFullData>(partSet);
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
        p.StarRestoreHorizontalPart<restoreParts, restoreFullData, restoreParityParts>(partSet,
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
    ui32 m = ErasureSpeciesParameters[TRopeErasureType::EErasureSpecies::Erasure4Plus3Block].Prime;
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
    p.StarMainRestorePartsSymmetric<restoreParts, restoreFullData, restoreParityParts>(partSet,
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

template <bool restoreParts, bool restoreFullData, bool restoreParityParts>
void XorBlockRestore(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, TDataPartSet &partSet) {
    TRope &outBuffer = partSet.FullDataFragment.OwnedRope;
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
        p.GlueBlockPartsMemcpy(outBuffer.Begin(), partSet);
        return;
    }
    // Prepare output data pointers
    if (restoreFullData) {
        p.PrepareInputDataPointers(outBuffer.Begin());
    }

    p.XorRestorePart<restoreParts, restoreFullData, restoreParityParts>(partSet, missingDataPartIdx);
}

const std::array<TString, TRopeErasureType::ErasureSpeciesCount> TRopeErasureType::ErasureName{{
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

ui32 TRopeErasureType::ParityParts() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.ParityParts;
}

ui32 TRopeErasureType::DataParts() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.DataParts;
}

ui32 TRopeErasureType::TotalPartCount() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.DataParts + erasure.ParityParts;
}

ui32 TRopeErasureType::MinimalRestorablePartCount() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.DataParts;
}

/*
ui32 TRopeErasureType::PartialRestoreStep() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
        case TRopeErasureType::ErasureMirror:
            return 1;
        case TRopeErasureType::ErasureParityStripe:
            if (erasure.ParityParts == 1) {
                return erasure.DataParts * sizeof(ui64);
            }
            return erasure.DataParts * (erasure.Prime - 1) * sizeof(ui64);
        case TRopeErasureType::ErasureParityBlock:
            if (erasure.ParityParts == 1) {
                return sizeof(ui64);
            }
            return (erasure.Prime - 1) * sizeof(ui64);
    }
    ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
}*/

ui32 TRopeErasureType::MinimalBlockSize() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TRopeErasureType::ErasureMirror:
        return 1;
    case TRopeErasureType::ErasureParityBlock:
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
    default:
        ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
}

ui64 TRopeErasureType::PartUserSize(ui64 dataSize) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TRopeErasureType::ErasureMirror:
        return dataSize;
    case TRopeErasureType::ErasureParityBlock:
        {
            ui32 blockSize = MinimalBlockSize();
            ui64 dataSizeBlocks = (dataSize + blockSize - 1) / blockSize;
            ui64 partSize = dataSizeBlocks * sizeof(ui64) * (erasure.ParityParts == 1 ? 1 : (erasure.Prime - 1));
            return partSize;
        }
    default:
        ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
}

ui64 TRopeErasureType::PartSize(ECrcMode crcMode, ui64 dataSize) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    switch (erasure.ErasureFamily) {
    case TRopeErasureType::ErasureMirror:
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
    case TRopeErasureType::ErasureParityBlock:
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
    default:
        ythrow TWithBackTrace<yexception>() << "Unknown ErasureFamily = " << (i32)erasure.ErasureFamily;
    }
}

ui32 TRopeErasureType::Prime() const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    return erasure.Prime;
}

// Block consists of columns.
// block = [column1, column2, ... ,columnN], where N == erasure.DataParts
//
// Input partitioning:
// | large, ... | small, ... | small + tail |

void TRopeErasureType::BlockSplitRange(ECrcMode crcMode, ui64 blobSize, ui64 wholeBegin, ui64 wholeEnd,
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

void MirrorSplit(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, const TRope& buffer,
        TDataPartSet& outPartSet) {
    outPartSet.FullDataSize = buffer.GetSize();
    outPartSet.Parts.resize(type.TotalPartCount());
    TString partBuffer;
    ui32 parityParts = type.ParityParts();
    switch (crcMode) {
    case TRopeErasureType::CrcModeNone:
        for (ui32 partIdx = 0; partIdx <= parityParts; ++partIdx) {
            outPartSet.Parts[partIdx].ReferenceTo(buffer);
            outPartSet.PartsMask |= (1 << partIdx);
        }
        outPartSet.MemoryConsumed = buffer.GetSize();
        return;
    case TRopeErasureType::CrcModeWholePart:
        {
            ui64 partSize = type.PartSize(crcMode, buffer.GetSize());
            TRope& part = outPartSet.FullDataFragment.OwnedRope;
            part = buffer;
            TRopeHelpers::Resize(part, partSize);
            if (buffer.GetSize() || part.GetSize()) {
                Y_ABORT_UNLESS(part.GetSize() >= buffer.GetSize() + sizeof(ui32), "Part size too small, buffer size# %" PRIu64
                        " partSize# %" PRIu64, (ui64)buffer.GetSize(), (ui64)partSize);
                PadAndCrcAtTheEnd(part.Begin(), buffer.GetSize(), part.GetSize());
            }
            for (ui32 partIdx = 0; partIdx <= parityParts; ++partIdx) {
                outPartSet.Parts[partIdx].ReferenceTo(part);
                outPartSet.PartsMask |= (1 << partIdx);
            }
            outPartSet.MemoryConsumed = part.GetSize();
        }
        return;
    }
    ythrow TWithBackTrace<yexception>() << "Unknown crcMode = " << (i32)crcMode;

}

template <bool restoreParts, bool restoreFullData>
void MirrorRestore(TRopeErasureType::ECrcMode crcMode, const TRopeErasureType &type, TDataPartSet& partSet) {
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
                    case TRopeErasureType::CrcModeNone:
                        partSet.FullDataFragment.ReferenceTo(partSet.Parts[partIdx].OwnedRope);
                        return;
                    case TRopeErasureType::CrcModeWholePart:
                        TRope outBuffer = partSet.Parts[partIdx].OwnedRope;
                        Y_ABORT_UNLESS(outBuffer.GetSize() >= partSet.FullDataSize, "Unexpected outBuffer.size# %" PRIu64
                                " fullDataSize# %" PRIu64, (ui64)outBuffer.GetSize(), (ui64)partSet.FullDataSize);
                        TRopeHelpers::Resize(outBuffer, partSet.FullDataSize); // To pad with zeroes!
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
            CHECK_ROPE_IS_DEFINED(partSet.Parts[idx].FastViewer.GetCurrent(partSet.Parts[idx].Offset),
                    partSet.Parts[idx].Size);
        }
    }
}

void TRopeErasureType::SplitData(ECrcMode crcMode, const TRope& buffer, TDataPartSet& outPartSet) const {
    const TErasureParameters& erasure = ErasureSpeciesParameters[ErasureSpecies];
    for (size_t i = 0; i < outPartSet.Parts.size(); ++i) {
        outPartSet.Parts[i].OwnedRope = TRope();
    }
    switch (erasure.ErasureFamily) {
        case TRopeErasureType::ErasureMirror:
            MirrorSplit(crcMode, *this, buffer, outPartSet);
            VerifyPartSizes(outPartSet, Max<size_t>());
            break;
        case TRopeErasureType::ErasureParityBlock:
            switch (erasure.ParityParts) {
                case 1:
                    XorBlockSplit(crcMode, *this, buffer, outPartSet);
                    VerifyPartSizes(outPartSet, Max<size_t>());
                    break;
                case 2:
                    EoBlockSplit(crcMode, *this, buffer, outPartSet);
                    VerifyPartSizes(outPartSet, Max<size_t>());
                    break;
                case 3:
                    StarBlockSplit(crcMode, *this, buffer, outPartSet);
                    VerifyPartSizes(outPartSet, Max<size_t>());
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
}

void TRopeErasureType::RestoreData(ECrcMode crcMode, TDataPartSet& partSet, TRope& outBuffer, bool restoreParts,
        bool restoreFullData, bool restoreParityParts) const {
    partSet.FullDataFragment.ReferenceTo(outBuffer);
    RestoreData(crcMode, partSet, restoreParts, restoreFullData, restoreParityParts);
    outBuffer = partSet.FullDataFragment.OwnedRope;
}

void TRopeErasureType::RestoreData(ECrcMode crcMode, TDataPartSet& partSet, bool restoreParts, bool restoreFullData,
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
        case TRopeErasureType::ErasureMirror:
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
        case TRopeErasureType::ErasureParityBlock:
            switch (erasure.ParityParts) {
                case 1:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                XorBlockRestore<true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                XorBlockRestore<true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                XorBlockRestore<true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                XorBlockRestore<true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        XorBlockRestore<false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                case 2:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                EoBlockRestore<true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                EoBlockRestore<true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                EoBlockRestore<true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                EoBlockRestore<true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        EoBlockRestore<false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                case 3:
                    if (restoreParts) {
                        if (restoreFullData) {
                            if (restoreParityParts) {
                                // restoreParts, restoreFullData, restoreParityParts
                                StarBlockRestore<true, true, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                StarBlockRestore<true, true, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        } else {
                            if (restoreParityParts) {
                                StarBlockRestore<true, false, true>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, Max<size_t>());
                            } else {
                                StarBlockRestore<true, false, false>(crcMode, *this, partSet);
                                VerifyPartSizes(partSet, erasure.DataParts);
                            }
                        }
                        partSet.MemoryConsumed = partSet.Parts[0].MemoryConsumed() * partSet.Parts.size();
                    } else if (restoreFullData) {
                        StarBlockRestore<false, true, false>(crcMode, *this, partSet);
                    }
                    break;
                default:
                    ythrow TWithBackTrace<yexception>() << "Unsupported number of parity parts: "
                        << erasure.ParityParts;
            }
            break;
        default:
            ythrow TWithBackTrace<yexception>() << "Unsupported erasure family";
    }
}

} // NKikimr
} // NErasureRope

Y_DECLARE_OUT_SPEC(, NKikimr::NErasureRope::TRopeErasureType::EErasureSpecies, stream, value) {
    stream << NKikimr::NErasureRope::TRopeErasureType::ErasureSpeciesToStr(value);
}
