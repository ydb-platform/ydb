// Copyright 2019 Joe Drago. All rights reserved.
// SPDX-License-Identifier: BSD-2-Clause

#include "avif/internal.h"

#include <assert.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#define AUXTYPE_SIZE 64
#define CONTENTTYPE_SIZE 64

// class VisualSampleEntry(codingname) extends SampleEntry(codingname) {
//     unsigned int(16) pre_defined = 0;
//     const unsigned int(16) reserved = 0;
//     unsigned int(32)[3] pre_defined = 0;
//     unsigned int(16) width;
//     unsigned int(16) height;
//     template unsigned int(32) horizresolution = 0x00480000; // 72 dpi
//     template unsigned int(32) vertresolution = 0x00480000;  // 72 dpi
//     const unsigned int(32) reserved = 0;
//     template unsigned int(16) frame_count = 1;
//     string[32] compressorname;
//     template unsigned int(16) depth = 0x0018;
//     int(16) pre_defined = -1;
//     // other boxes from derived specifications
//     CleanApertureBox clap;    // optional
//     PixelAspectRatioBox pasp; // optional
// }
static const size_t VISUALSAMPLEENTRY_SIZE = 78;

// The only supported ipma box values for both version and flags are [0,1], so there technically
// can't be more than 4 unique tuples right now.
#define MAX_IPMA_VERSION_AND_FLAGS_SEEN 4

// ---------------------------------------------------------------------------
// AVIF codec type (AV1 or AV2)

static avifCodecType avifGetCodecType(const uint8_t * fourcc)
{
    if (!memcmp(fourcc, "av01", 4)) {
        return AVIF_CODEC_TYPE_AV1;
    }
#if defined(AVIF_CODEC_AVM)
    if (!memcmp(fourcc, "av02", 4)) {
        return AVIF_CODEC_TYPE_AV2;
    }
#endif
    return AVIF_CODEC_TYPE_UNKNOWN;
}

static const char * avifGetConfigurationPropertyName(avifCodecType codecType)
{
    switch (codecType) {
        case AVIF_CODEC_TYPE_AV1:
            return "av1C";
#if defined(AVIF_CODEC_AVM)
        case AVIF_CODEC_TYPE_AV2:
            return "av2C";
#endif
        default:
            assert(AVIF_FALSE);
            return NULL;
    }
}

// ---------------------------------------------------------------------------
// Box data structures

typedef uint8_t avifBrand[4];
AVIF_ARRAY_DECLARE(avifBrandArray, avifBrand, brand);

// ftyp
typedef struct avifFileType
{
    uint8_t majorBrand[4];
    uint8_t minorVersion[4];
    // If not null, points to a memory block of 4 * compatibleBrandsCount bytes.
    const uint8_t * compatibleBrands;
    int compatibleBrandsCount;
} avifFileType;

// ispe
typedef struct avifImageSpatialExtents
{
    uint32_t width;
    uint32_t height;
} avifImageSpatialExtents;

// auxC
typedef struct avifAuxiliaryType
{
    char auxType[AUXTYPE_SIZE];
} avifAuxiliaryType;

// infe mime content_type
typedef struct avifContentType
{
    char contentType[CONTENTTYPE_SIZE];
} avifContentType;

// colr
typedef struct avifColourInformationBox
{
    avifBool hasICC;
    uint64_t iccOffset;
    size_t iccSize;

    avifBool hasNCLX;
    avifColorPrimaries colorPrimaries;
    avifTransferCharacteristics transferCharacteristics;
    avifMatrixCoefficients matrixCoefficients;
    avifRange range;
} avifColourInformationBox;

#define MAX_PIXI_PLANE_DEPTHS 4
typedef struct avifPixelInformationProperty
{
    uint8_t planeDepths[MAX_PIXI_PLANE_DEPTHS];
    uint8_t planeCount;
#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
    avifBool hasExtendedFields;                     // The fields below were signaled if this is true.
    uint8_t subsamplingFlag[MAX_PIXI_PLANE_DEPTHS]; // The fields below were signaled if this is true for a given channel.
    uint8_t subsamplingType[MAX_PIXI_PLANE_DEPTHS];
    uint8_t subsamplingLocation[MAX_PIXI_PLANE_DEPTHS];
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI
} avifPixelInformationProperty;

typedef struct avifOperatingPointSelectorProperty
{
    uint8_t opIndex;
} avifOperatingPointSelectorProperty;

typedef struct avifLayerSelectorProperty
{
    uint16_t layerID;
} avifLayerSelectorProperty;

typedef struct avifAV1LayeredImageIndexingProperty
{
    uint32_t layerSize[3];
} avifAV1LayeredImageIndexingProperty;

typedef struct avifOpaqueProperty
{
    uint8_t usertype[16];  // Same as in avifImageItemProperty.
    avifRWData boxPayload; // Same as in avifImageItemProperty.
} avifOpaqueProperty;

// Array of item or track ids.
AVIF_ARRAY_DECLARE(avifCodecEntityIDs, uint32_t, ids);

// Content of a box inside a 'grpl' box, representing a group of entities.
typedef struct avifEntityToGroup
{
    uint8_t groupingType[4];
    uint32_t groupID;
    avifCodecEntityIDs entityIDs;
} avifEntityToGroup;
AVIF_ARRAY_DECLARE(avifEntityToGroups, avifEntityToGroup, groups);

// ---------------------------------------------------------------------------
// Top-level structures

struct avifMeta;

// Temporary storage for ipco/stsd contents until they can be associated and memcpy'd to an avifDecoderItem
typedef struct avifProperty
{
    uint8_t type[4];
    avifBool isOpaque;
    union
    {
        avifImageSpatialExtents ispe;
        avifAuxiliaryType auxC; // Contents of 'auxC' for items, or 'auxi' for tracks
        avifColourInformationBox colr;
        avifCodecConfigurationBox av1C; // TODO(yguyon): Rename or add av2C
        avifPixelAspectRatioBox pasp;
        avifCleanApertureBox clap;
        avifImageRotation irot;
        avifImageMirror imir;
        avifPixelInformationProperty pixi;
        avifOperatingPointSelectorProperty a1op;
        avifLayerSelectorProperty lsel;
        avifAV1LayeredImageIndexingProperty a1lx;
        avifContentLightLevelInformationBox clli;
        avifOpaqueProperty opaque;
    } u;
} avifProperty;
AVIF_ARRAY_DECLARE(avifPropertyArray, avifProperty, prop);

// Finds the first property of a given type.
static const avifProperty * avifPropertyArrayFind(const avifPropertyArray * properties, const char * type)
{
    for (uint32_t propertyIndex = 0; propertyIndex < properties->count; ++propertyIndex) {
        const avifProperty * prop = &properties->prop[propertyIndex];
        if (!memcmp(prop->type, type, 4)) {
            return prop;
        }
    }
    return NULL;
}

AVIF_ARRAY_DECLARE(avifExtentArray, avifExtent, extent);

// one "item" worth for decoding (all iref, iloc, iprp, etc refer to one of these)
typedef struct avifDecoderItem
{
    uint32_t id;
    struct avifMeta * meta; // Unowned; A back-pointer for convenience
    uint8_t type[4];
    size_t size;
    avifBool idatStored; // If true, offset is relative to the associated meta box's idat box (iloc construction_method==1)
    uint32_t width;      // Set from this item's ispe property, if present
    uint32_t height;     // Set from this item's ispe property, if present
    avifContentType contentType;
    avifPropertyArray properties;
    avifExtentArray extents;       // All extent offsets/sizes
    avifRWData mergedExtents;      // if set, is a single contiguous block of this item's extents (unused when extents.count == 1)
    avifBool ownsMergedExtents;    // if true, mergedExtents must be freed when this item is destroyed
    avifBool partialMergedExtents; // If true, mergedExtents doesn't have all of the item data yet
    uint32_t thumbnailForID;       // if non-zero, this item is a thumbnail for Item #{thumbnailForID}
    uint32_t auxForID;             // if non-zero, this item is an auxC plane for Item #{auxForID}
    uint32_t descForID;            // if non-zero, this item is a content description for Item #{descForID}
    uint32_t dimgForID;            // if non-zero, this item is an input of derived Item #{dimgForID}
    uint32_t dimgIdx; // If dimgForId is non-zero, this is the zero-based index of this item in the list of Item #{dimgForID}'s dimg.
    avifBool hasDimgFrom; // whether there is a 'dimg' box with this item's id as 'fromID'
    uint32_t premByID;    // if non-zero, this item is premultiplied by Item #{premByID}
    avifBool hasUnsupportedEssentialProperty; // If true, this item cites a property flagged as 'essential' that libavif doesn't support (yet). Ignore the item, if so.
    avifBool ipmaSeen;    // if true, this item already received a property association
    avifBool progressive; // if true, this item has progressive layers (a1lx), but does not select a specific layer (the layer_id value in lsel is set to 0xFFFF)
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
    avifPixelFormat miniBoxPixelFormat; // Set from the MinimizedImageBox, if present (AVIF_PIXEL_FORMAT_NONE otherwise)
    avifChromaSamplePosition miniBoxChromaSamplePosition; // Set from the MinimizedImageBox, if present (AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN otherwise)
#endif
} avifDecoderItem;
AVIF_ARRAY_DECLARE(avifDecoderItemArray, avifDecoderItem *, item);

// grid storage
typedef struct avifImageGrid
{
    uint32_t rows;    // Legal range: [1-256]
    uint32_t columns; // Legal range: [1-256]
    uint32_t outputWidth;
    uint32_t outputHeight;
} avifImageGrid;

// ---------------------------------------------------------------------------
// avifTrack

typedef struct avifSampleTableChunk
{
    uint64_t offset;
} avifSampleTableChunk;
AVIF_ARRAY_DECLARE(avifSampleTableChunkArray, avifSampleTableChunk, chunk);

typedef struct avifSampleTableSampleToChunk
{
    uint32_t firstChunk;
    uint32_t samplesPerChunk;
    uint32_t sampleDescriptionIndex;
} avifSampleTableSampleToChunk;
AVIF_ARRAY_DECLARE(avifSampleTableSampleToChunkArray, avifSampleTableSampleToChunk, sampleToChunk);

typedef struct avifSampleTableSampleSize
{
    uint32_t size;
} avifSampleTableSampleSize;
AVIF_ARRAY_DECLARE(avifSampleTableSampleSizeArray, avifSampleTableSampleSize, sampleSize);

typedef struct avifSampleTableTimeToSample
{
    uint32_t sampleCount;
    uint32_t sampleDelta;
} avifSampleTableTimeToSample;
AVIF_ARRAY_DECLARE(avifSampleTableTimeToSampleArray, avifSampleTableTimeToSample, timeToSample);

typedef struct avifSyncSample
{
    uint32_t sampleNumber;
} avifSyncSample;
AVIF_ARRAY_DECLARE(avifSyncSampleArray, avifSyncSample, syncSample);

typedef struct avifSampleDescription
{
    uint8_t format[4];
    avifPropertyArray properties;
} avifSampleDescription;
AVIF_ARRAY_DECLARE(avifSampleDescriptionArray, avifSampleDescription, description);

typedef struct avifSampleTable
{
    avifSampleTableChunkArray chunks;
    avifSampleDescriptionArray sampleDescriptions;
    avifSampleTableSampleToChunkArray sampleToChunks;
    avifSampleTableSampleSizeArray sampleSizes;
    avifSampleTableTimeToSampleArray timeToSamples;
    avifSyncSampleArray syncSamples;
    uint32_t allSamplesSize; // If this is non-zero, sampleSizes will be empty and all samples will be this size
} avifSampleTable;

static void avifSampleTableDestroy(avifSampleTable * sampleTable);

static avifSampleTable * avifSampleTableCreate(void)
{
    avifSampleTable * sampleTable = (avifSampleTable *)avifAlloc(sizeof(avifSampleTable));
    if (sampleTable == NULL) {
        return NULL;
    }
    memset(sampleTable, 0, sizeof(avifSampleTable));
    if (!avifArrayCreate(&sampleTable->chunks, sizeof(avifSampleTableChunk), 16) ||
        !avifArrayCreate(&sampleTable->sampleDescriptions, sizeof(avifSampleDescription), 2) ||
        !avifArrayCreate(&sampleTable->sampleToChunks, sizeof(avifSampleTableSampleToChunk), 16) ||
        !avifArrayCreate(&sampleTable->sampleSizes, sizeof(avifSampleTableSampleSize), 16) ||
        !avifArrayCreate(&sampleTable->timeToSamples, sizeof(avifSampleTableTimeToSample), 16) ||
        !avifArrayCreate(&sampleTable->syncSamples, sizeof(avifSyncSample), 16)) {
        avifSampleTableDestroy(sampleTable);
        return NULL;
    }
    return sampleTable;
}

static void avifPropertyArrayDestroy(avifPropertyArray * array)
{
    for (size_t i = 0; i < array->count; ++i) {
        if (array->prop[i].isOpaque) {
            avifRWDataFree(&array->prop[i].u.opaque.boxPayload);
        }
    }
    avifArrayDestroy(array);
}

static void avifSampleTableDestroy(avifSampleTable * sampleTable)
{
    avifArrayDestroy(&sampleTable->chunks);
    for (uint32_t i = 0; i < sampleTable->sampleDescriptions.count; ++i) {
        avifSampleDescription * description = &sampleTable->sampleDescriptions.description[i];
        avifPropertyArrayDestroy(&description->properties);
    }
    avifArrayDestroy(&sampleTable->sampleDescriptions);
    avifArrayDestroy(&sampleTable->sampleToChunks);
    avifArrayDestroy(&sampleTable->sampleSizes);
    avifArrayDestroy(&sampleTable->timeToSamples);
    avifArrayDestroy(&sampleTable->syncSamples);
    avifFree(sampleTable);
}

static uint32_t avifSampleTableGetImageDelta(const avifSampleTable * sampleTable, uint32_t imageIndex)
{
    uint32_t maxSampleIndex = 0;
    for (uint32_t i = 0; i < sampleTable->timeToSamples.count; ++i) {
        const avifSampleTableTimeToSample * timeToSample = &sampleTable->timeToSamples.timeToSample[i];
        maxSampleIndex += timeToSample->sampleCount;
        if ((imageIndex < maxSampleIndex) || (i == (sampleTable->timeToSamples.count - 1))) {
            return timeToSample->sampleDelta;
        }
    }

    // TODO: fail here?
    return 1;
}

static avifCodecType avifSampleTableGetCodecType(const avifSampleTable * sampleTable)
{
    for (uint32_t i = 0; i < sampleTable->sampleDescriptions.count; ++i) {
        const avifCodecType codecType = avifGetCodecType(sampleTable->sampleDescriptions.description[i].format);
        if (codecType != AVIF_CODEC_TYPE_UNKNOWN) {
            return codecType;
        }
    }
    return AVIF_CODEC_TYPE_UNKNOWN;
}

static uint32_t avifCodecConfigurationBoxGetDepth(const avifCodecConfigurationBox * av1C)
{
    if (av1C->twelveBit) {
        return 12;
    } else if (av1C->highBitdepth) {
        return 10;
    }
    return 8;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
uint8_t avifCodecConfigurationBoxGetSubsamplingType(const avifCodecConfigurationBox * av1C, uint8_t channelIndex)
{
    if (channelIndex == 0) {
        return AVIF_PIXI_444;
    }
    if (av1C->chromaSubsamplingX == 0) {
        if (av1C->chromaSubsamplingY == 0) {
            return AVIF_PIXI_444;
        }
        return AVIF_PIXI_440;
    }
    if (av1C->chromaSubsamplingY == 0) {
        return AVIF_PIXI_422;
    }
    return AVIF_PIXI_420;
}

// Mapping from PixelInformationBox subsampling_type and subsampling_location as defined in ISO/IEC 23008-12:2024/CDAM 2:2025 section 6.5.6.3
// to chroma_sample_position as defined in AV1 specification Section 6.4.2.
static uint8_t avifSubsamplingLocationToChromaSamplePosition(uint8_t subsamplingType, uint8_t subsamplingLocation)
{
    if (subsamplingType == AVIF_PIXI_444) {
        return AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
    }
    if (subsamplingType == AVIF_PIXI_422) {
        if (subsamplingLocation == 0 || subsamplingLocation == 2 || subsamplingLocation == 4) {
            return AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
        }
    }
    if (subsamplingType == AVIF_PIXI_420) {
        if (subsamplingLocation == 0) {
            return AVIF_CHROMA_SAMPLE_POSITION_VERTICAL;
        }
        if (subsamplingLocation == 2) {
            return AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
        }
    }
    if (subsamplingType == AVIF_PIXI_411) {
        if (subsamplingLocation == 0 || subsamplingLocation == 2 || subsamplingLocation == 4) {
            return AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
        }
    }
    if (subsamplingType == AVIF_PIXI_440) {
        if (subsamplingLocation == 0 || subsamplingLocation == 1) {
            return AVIF_CHROMA_SAMPLE_POSITION_VERTICAL;
        }
        if (subsamplingLocation == 2 || subsamplingLocation == 3) {
            return AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
        }
    }
    return AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI

static const avifPropertyArray * avifSampleTableGetProperties(const avifSampleTable * sampleTable, avifCodecType codecType)
{
    for (uint32_t i = 0; i < sampleTable->sampleDescriptions.count; ++i) {
        const avifSampleDescription * description = &sampleTable->sampleDescriptions.description[i];
        if (avifGetCodecType(description->format) == codecType) {
            return &description->properties;
        }
    }
    return NULL;
}

// one video track ("trak" contents)
typedef struct avifTrack
{
    uint32_t id;
    uint8_t handlerType[4];
    uint32_t auxForID; // if non-zero, this track is an auxC plane for Track #{auxForID}
    uint32_t premByID; // if non-zero, this track is premultiplied by Track #{premByID}
    uint32_t mediaTimescale;
    uint64_t mediaDuration;
    uint64_t trackDuration;
    uint64_t segmentDuration;
    avifBool isRepeating;
    int repetitionCount;
    uint32_t width;
    uint32_t height;
    avifSampleTable * sampleTable;
    struct avifMeta * meta;
} avifTrack;
AVIF_ARRAY_DECLARE(avifTrackArray, avifTrack, track);

// ---------------------------------------------------------------------------
// avifCodecDecodeInput

avifCodecDecodeInput * avifCodecDecodeInputCreate(void)
{
    avifCodecDecodeInput * decodeInput = (avifCodecDecodeInput *)avifAlloc(sizeof(avifCodecDecodeInput));
    if (decodeInput == NULL) {
        return NULL;
    }
    memset(decodeInput, 0, sizeof(avifCodecDecodeInput));
    if (!avifArrayCreate(&decodeInput->samples, sizeof(avifDecodeSample), 1)) {
        avifFree(decodeInput);
        return NULL;
    }
    return decodeInput;
}

void avifCodecDecodeInputDestroy(avifCodecDecodeInput * decodeInput)
{
    for (uint32_t sampleIndex = 0; sampleIndex < decodeInput->samples.count; ++sampleIndex) {
        avifDecodeSample * sample = &decodeInput->samples.sample[sampleIndex];
        if (sample->ownsData) {
            avifRWDataFree((avifRWData *)&sample->data);
        }
    }
    avifArrayDestroy(&decodeInput->samples);
    avifFree(decodeInput);
}

// Returns how many samples are in the chunk.
static uint32_t avifGetSampleCountOfChunk(const avifSampleTableSampleToChunkArray * sampleToChunks, uint32_t chunkIndex)
{
    uint32_t sampleCount = 0;
    for (int sampleToChunkIndex = sampleToChunks->count - 1; sampleToChunkIndex >= 0; --sampleToChunkIndex) {
        const avifSampleTableSampleToChunk * sampleToChunk = &sampleToChunks->sampleToChunk[sampleToChunkIndex];
        if (sampleToChunk->firstChunk <= (chunkIndex + 1)) {
            sampleCount = sampleToChunk->samplesPerChunk;
            break;
        }
    }
    return sampleCount;
}

static avifResult avifCodecDecodeInputFillFromSampleTable(avifCodecDecodeInput * decodeInput,
                                                          avifSampleTable * sampleTable,
                                                          const uint32_t imageCountLimit,
                                                          const uint64_t sizeHint,
                                                          avifDiagnostics * diag)
{
    if (imageCountLimit) {
        // Verify that the we're not about to exceed the frame count limit.

        uint32_t imageCountLeft = imageCountLimit;
        for (uint32_t chunkIndex = 0; chunkIndex < sampleTable->chunks.count; ++chunkIndex) {
            // First, figure out how many samples are in this chunk
            uint32_t sampleCount = avifGetSampleCountOfChunk(&sampleTable->sampleToChunks, chunkIndex);
            if (sampleCount == 0) {
                // chunks with 0 samples are invalid
                avifDiagnosticsPrintf(diag, "Sample table contains a chunk with 0 samples");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            if (sampleCount > imageCountLeft) {
                // This file exceeds the imageCountLimit, bail out
                avifDiagnosticsPrintf(diag, "Exceeded avifDecoder's imageCountLimit");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            imageCountLeft -= sampleCount;
        }
    }

    uint32_t sampleSizeIndex = 0;
    for (uint32_t chunkIndex = 0; chunkIndex < sampleTable->chunks.count; ++chunkIndex) {
        avifSampleTableChunk * chunk = &sampleTable->chunks.chunk[chunkIndex];

        // First, figure out how many samples are in this chunk
        uint32_t sampleCount = avifGetSampleCountOfChunk(&sampleTable->sampleToChunks, chunkIndex);
        if (sampleCount == 0) {
            // chunks with 0 samples are invalid
            avifDiagnosticsPrintf(diag, "Sample table contains a chunk with 0 samples");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }

        uint64_t sampleOffset = chunk->offset;
        for (uint32_t sampleIndex = 0; sampleIndex < sampleCount; ++sampleIndex) {
            uint32_t sampleSize = sampleTable->allSamplesSize;
            if (sampleSize == 0) {
                if (sampleSizeIndex >= sampleTable->sampleSizes.count) {
                    // We've run out of samples to sum
                    avifDiagnosticsPrintf(diag, "Truncated sample table");
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                avifSampleTableSampleSize * sampleSizePtr = &sampleTable->sampleSizes.sampleSize[sampleSizeIndex];
                sampleSize = sampleSizePtr->size;
            }

            avifDecodeSample * sample = (avifDecodeSample *)avifArrayPush(&decodeInput->samples);
            AVIF_CHECKERR(sample != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            sample->offset = sampleOffset;
            sample->size = sampleSize;
            sample->spatialID = AVIF_SPATIAL_ID_UNSET; // Not filtering by spatial_id
            sample->sync = AVIF_FALSE;                 // to potentially be set to true following the outer loop

            if (sampleSize > UINT64_MAX - sampleOffset) {
                avifDiagnosticsPrintf(diag,
                                      "Sample table contains an offset/size pair which overflows: [%" PRIu64 " / %u]",
                                      sampleOffset,
                                      sampleSize);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            if (sizeHint && ((sampleOffset + sampleSize) > sizeHint)) {
                avifDiagnosticsPrintf(diag, "Exceeded avifIO's sizeHint, possibly truncated data");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            sampleOffset += sampleSize;
            ++sampleSizeIndex;
        }
    }

    // Mark appropriate samples as sync
    for (uint32_t syncSampleIndex = 0; syncSampleIndex < sampleTable->syncSamples.count; ++syncSampleIndex) {
        uint32_t frameIndex = sampleTable->syncSamples.syncSample[syncSampleIndex].sampleNumber - 1; // sampleNumber is 1-based
        if (frameIndex < decodeInput->samples.count) {
            decodeInput->samples.sample[frameIndex].sync = AVIF_TRUE;
        }
    }

    // Assume frame 0 is sync, just in case the stss box is absent in the BMFF. (Unnecessary?)
    if (decodeInput->samples.count > 0) {
        decodeInput->samples.sample[0].sync = AVIF_TRUE;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifCodecDecodeInputFillFromDecoderItem(avifCodecDecodeInput * decodeInput,
                                                          avifDecoderItem * item,
                                                          avifBool allowProgressive,
                                                          const uint32_t imageCountLimit,
                                                          const uint64_t sizeHint,
                                                          avifDiagnostics * diag)
{
    if (sizeHint && (item->size > sizeHint)) {
        avifDiagnosticsPrintf(diag, "Exceeded avifIO's sizeHint, possibly truncated data");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    uint8_t layerCount = 0;
    size_t layerSizes[4] = { 0 };
    const avifProperty * a1lxProp = avifPropertyArrayFind(&item->properties, "a1lx");
    if (a1lxProp) {
        // Calculate layer count and all layer sizes from the a1lx box, and then validate

        size_t remainingSize = item->size;
        for (int i = 0; i < 3; ++i) {
            ++layerCount;

            const size_t layerSize = (size_t)a1lxProp->u.a1lx.layerSize[i];
            if (layerSize) {
                if (layerSize >= remainingSize) { // >= instead of > because there must be room for the last layer
                    avifDiagnosticsPrintf(diag, "a1lx layer index [%d] does not fit in item size", i);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                layerSizes[i] = layerSize;
                remainingSize -= layerSize;
            } else {
                layerSizes[i] = remainingSize;
                remainingSize = 0;
                break;
            }
        }
        if (remainingSize > 0) {
            AVIF_ASSERT_OR_RETURN(layerCount == 3);
            ++layerCount;
            layerSizes[3] = remainingSize;
        }
    }

    const avifProperty * lselProp = avifPropertyArrayFind(&item->properties, "lsel");
    // Progressive images offer layers via the a1lxProp, but don't specify a layer selection with lsel.
    //
    // For backward compatibility with earlier drafts of AVIF spec v1.1.0, treat an absent lsel as
    // equivalent to layer_id == 0xFFFF during the transitional period. Remove !lselProp when the test
    // images have been updated to the v1.1.0 spec.
    item->progressive = (a1lxProp && (!lselProp || (lselProp->u.lsel.layerID == 0xFFFF)));
    if (lselProp && (lselProp->u.lsel.layerID != 0xFFFF)) {
        // Layer selection. This requires that the underlying AV1 codec decodes all layers,
        // and then only returns the requested layer as a single frame. To the user of libavif,
        // this appears to be a single frame.

        decodeInput->allLayers = AVIF_TRUE;

        size_t sampleSize = 0;
        if (layerCount > 0) {
            // Optimization: If we're selecting a layer that doesn't require the entire image's payload (hinted via the a1lx box)

            if (lselProp->u.lsel.layerID >= layerCount) {
                avifDiagnosticsPrintf(diag,
                                      "lsel property requests layer index [%u] which isn't present in a1lx property ([%u] layers)",
                                      lselProp->u.lsel.layerID,
                                      layerCount);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            for (uint8_t i = 0; i <= lselProp->u.lsel.layerID; ++i) {
                sampleSize += layerSizes[i];
            }
        } else {
            // This layer's payload subsection is unknown, just use the whole payload
            sampleSize = item->size;
        }

        avifDecodeSample * sample = (avifDecodeSample *)avifArrayPush(&decodeInput->samples);
        AVIF_CHECKERR(sample != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        sample->itemID = item->id;
        sample->offset = 0;
        sample->size = sampleSize;
        AVIF_ASSERT_OR_RETURN(lselProp->u.lsel.layerID < AVIF_MAX_AV1_LAYER_COUNT);
        sample->spatialID = (uint8_t)lselProp->u.lsel.layerID;
        sample->sync = AVIF_TRUE;
    } else if (allowProgressive && item->progressive) {
        // Progressive image. Decode all layers and expose them all to the user.

        if (imageCountLimit && (layerCount > imageCountLimit)) {
            avifDiagnosticsPrintf(diag, "Exceeded avifDecoder's imageCountLimit (progressive)");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }

        decodeInput->allLayers = AVIF_TRUE;

        size_t offset = 0;
        for (int i = 0; i < layerCount; ++i) {
            avifDecodeSample * sample = (avifDecodeSample *)avifArrayPush(&decodeInput->samples);
            AVIF_CHECKERR(sample != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            sample->itemID = item->id;
            sample->offset = offset;
            sample->size = layerSizes[i];
            sample->spatialID = AVIF_SPATIAL_ID_UNSET;
            sample->sync = (i == 0); // Assume all layers depend on the first layer

            offset += layerSizes[i];
        }
    } else {
        // Typical case: Use the entire item's payload for a single frame output

        avifDecodeSample * sample = (avifDecodeSample *)avifArrayPush(&decodeInput->samples);
        AVIF_CHECKERR(sample != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        sample->itemID = item->id;
        sample->offset = 0;
        sample->size = item->size;
        sample->spatialID = AVIF_SPATIAL_ID_UNSET;
        sample->sync = AVIF_TRUE;
    }
    return AVIF_RESULT_OK;
}

// ---------------------------------------------------------------------------
// Helper macros / functions

#define BEGIN_STREAM(VARNAME, PTR, SIZE, DIAG, CONTEXT) \
    avifROStream VARNAME;                               \
    avifROData VARNAME##_roData;                        \
    VARNAME##_roData.data = PTR;                        \
    VARNAME##_roData.size = SIZE;                       \
    avifROStreamStart(&VARNAME, &VARNAME##_roData, DIAG, CONTEXT)

typedef enum avifUniqueBoxFlag
{
    AVIF_UNIQUE_ILOC = 0,
    AVIF_UNIQUE_PITM,
    AVIF_UNIQUE_IDAT,
    AVIF_UNIQUE_IPRP,
    AVIF_UNIQUE_IINF,
    AVIF_UNIQUE_IREF,
    AVIF_UNIQUE_GRPL,
} avifUniqueBoxFlag;
// Use this to keep track of whether or not a child box that must be unique (0 or 1 present) has
// been seen yet, when parsing a parent box. If the "seen" bit is already set for a given box when
// it is encountered during parse, an error is thrown. Which bit corresponds to which box is
// dictated entirely by the calling function.
static avifBool uniqueBoxSeen(uint32_t * uniqueBoxFlags,
                              avifUniqueBoxFlag whichFlag,
                              const char * parentBoxType,
                              const char * boxType,
                              avifDiagnostics * diagnostics)
{
    const uint32_t flag = 1 << whichFlag;
    if (*uniqueBoxFlags & flag) {
        // This box has already been seen. Error!
        avifDiagnosticsPrintf(diagnostics, "Box[%s] contains a duplicate unique box of type '%s'", parentBoxType, boxType);
        return AVIF_FALSE;
    }

    // Mark this box as seen.
    *uniqueBoxFlags |= flag;
    return AVIF_TRUE;
}

// ---------------------------------------------------------------------------
// avifDecoderData

typedef struct avifTile
{
    avifCodecDecodeInput * input;
    avifCodecType codecType;
    // This may point to a codec that it owns or point to a shared codec that it does not own. In the shared case, this will
    // point to one of the avifCodec instances in avifDecoderData.
    struct avifCodec * codec;
    avifImage * image;
    uint32_t width;  // Either avifTrack.width or avifDecoderItem.width
    uint32_t height; // Either avifTrack.height or avifDecoderItem.height
    uint8_t operatingPoint;
} avifTile;
AVIF_ARRAY_DECLARE(avifTileArray, avifTile, tile);

// This holds one "meta" box (from the BMFF and HEIF standards) worth of relevant-to-AVIF information.
// * If a meta box is parsed from the root level of the BMFF, it can contain the information about
//   "items" which might be color planes, alpha planes, or EXIF or XMP metadata.
// * If a meta box is parsed from inside of a track ("trak") box, any metadata (EXIF/XMP) items inside
//   of that box are implicitly associated with that track.
typedef struct avifMeta
{
    // Items (from HEIF) are the generic storage for any data that does not require timed processing
    // (single image color planes, alpha planes, EXIF, XMP, etc). Each item has a unique integer ID >1,
    // and is defined by a series of child boxes in a meta box:
    //  * iloc - location:     byte offset to item data, item size in bytes
    //  * iinf - information:  type of item (color planes, alpha plane, EXIF, XMP)
    //  * ipco - properties:   dimensions, aspect ratio, image transformations, references to other items
    //  * ipma - associations: Attaches an item in the properties list to a given item
    //
    // Items are lazily created in this array when any of the above boxes refer to one by a new (unseen) ID,
    // and are then further modified/updated as new information for an item's ID is parsed.
    avifDecoderItemArray items;

    // Any ipco boxes explained above are populated into this array as a staging area, which are
    // then duplicated into the appropriate items upon encountering an item property association
    // (ipma) box.
    avifPropertyArray properties;

    // Filled with the contents of this meta box's "idat" box, which is raw data that an item can
    // directly refer to in its item location box (iloc) instead of just giving an offset into the
    // overall file. If all items' iloc boxes simply point at an offset/length in the file itself,
    // this buffer will likely be empty.
    avifRWData idat;

    // Ever-incrementing ID for uniquely identifying which 'meta' box contains an idat (when
    // multiple meta boxes exist as BMFF siblings). Each time avifParseMetaBox() is called on an
    // avifMeta struct, this value is incremented. Any time an additional meta box is detected at
    // the same "level" (root level, trak level, etc), this ID helps distinguish which meta box's
    // "idat" is which, as items implicitly reference idat boxes that exist in the same meta
    // box.
    uint32_t idatID;

    // Contents of a pitm box, which signal which of the items in this file is the main image. For
    // AVIF, this should point at an image item containing color planes, and all other items
    // are ignored unless they refer to this item in some way (alpha plane, EXIF/XMP metadata).
    uint32_t primaryItemID;

    // Contents of grpl box, which signal groups of entities (items or tracks).
    avifEntityToGroups entityToGroups;

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
    // If true, the fields above were extracted from a MinimizedImageBox.
    avifBool fromMiniBox;
#endif

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    // Parsed from Sample Transform metadata if present, otherwise empty.
    avifSampleTransformExpression sampleTransformExpression;
    // Bit depth extracted from the pixi property of the Sample Transform derived image item, if any.
    uint32_t sampleTransformDepth;
#endif
} avifMeta;

static void avifMetaDestroy(avifMeta * meta);

static avifMeta * avifMetaCreate(void)
{
    avifMeta * meta = (avifMeta *)avifAlloc(sizeof(avifMeta));
    if (meta == NULL) {
        return NULL;
    }
    memset(meta, 0, sizeof(avifMeta));
    if (!avifArrayCreate(&meta->items, sizeof(avifDecoderItem *), 8) || !avifArrayCreate(&meta->properties, sizeof(avifProperty), 16) ||
        !avifArrayCreate(&meta->entityToGroups, sizeof(avifEntityToGroup), 1)) {
        avifMetaDestroy(meta);
        return NULL;
    }
    return meta;
}

static void avifMetaDestroy(avifMeta * meta)
{
    for (uint32_t i = 0; i < meta->items.count; ++i) {
        avifDecoderItem * item = meta->items.item[i];
        avifPropertyArrayDestroy(&item->properties);
        avifArrayDestroy(&item->extents);
        if (item->ownsMergedExtents) {
            avifRWDataFree(&item->mergedExtents);
        }
        avifFree(item);
    }
    avifArrayDestroy(&meta->items);
    avifPropertyArrayDestroy(&meta->properties);
    avifRWDataFree(&meta->idat);
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    avifArrayDestroy(&meta->sampleTransformExpression);
#endif
    for (uint32_t i = 0; i < meta->entityToGroups.count; ++i) {
        avifArrayDestroy(&meta->entityToGroups.groups[i].entityIDs);
    }
    avifArrayDestroy(&meta->entityToGroups);
    avifFree(meta);
}

static avifResult avifCheckItemID(const char * boxFourcc, uint32_t itemID, avifDiagnostics * diag)
{
    // Section 8.11.1.1 of ISO/IEC 14496-12 about MetaBox definition:
    //   The item_ID value of 0 should not be used
    // Section 8.11.6 of ISO/IEC 14496-12 about ItemInfoEntry syntax and semantics:
    //   item_ID contains either 0 for the primary resource (e.g. the XML contained in an XMLBox)
    //   or the ID of the item for which the following information is defined.
    // Assuming 'infe' is the only way to properly define an item in AVIF, a compliant item cannot have an ID of zero.
    // One way to bypass that rule would be to have 'infe' with item_ID being 0, referring to "the primary resource",
    // and 'pitm' defining "the primary resource" as the item with an item_ID of 0. libavif considers that as invalid.
    if (itemID == 0) {
        avifDiagnosticsPrintf(diag, "Box[%.4s] has an invalid item ID [%u]", boxFourcc, itemID);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifMetaFindOrCreateItem(avifMeta * meta, uint32_t itemID, avifDecoderItem ** item)
{
    *item = NULL;
    AVIF_ASSERT_OR_RETURN(itemID != 0);

    for (uint32_t i = 0; i < meta->items.count; ++i) {
        if (meta->items.item[i]->id == itemID) {
            *item = meta->items.item[i];
            return AVIF_RESULT_OK;
        }
    }

    avifDecoderItem ** itemPtr = (avifDecoderItem **)avifArrayPush(&meta->items);
    AVIF_CHECKERR(itemPtr != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    *item = (avifDecoderItem *)avifAlloc(sizeof(avifDecoderItem));
    if (*item == NULL) {
        avifArrayPop(&meta->items);
        return AVIF_RESULT_OUT_OF_MEMORY;
    }
    memset(*item, 0, sizeof(avifDecoderItem));

    *itemPtr = *item;
    if (!avifArrayCreate(&(*item)->properties, sizeof(avifProperty), 16)) {
        avifFree(*item);
        *item = NULL;
        avifArrayPop(&meta->items);
        return AVIF_RESULT_OUT_OF_MEMORY;
    }
    if (!avifArrayCreate(&(*item)->extents, sizeof(avifExtent), 1)) {
        avifPropertyArrayDestroy(&(*item)->properties);
        avifFree(*item);
        *item = NULL;
        avifArrayPop(&meta->items);
        return AVIF_RESULT_OUT_OF_MEMORY;
    }
    (*item)->id = itemID;
    (*item)->meta = meta;
    return AVIF_RESULT_OK;
}

// A group of AVIF tiles in an image item, such as a single tile or a grid of multiple tiles.
typedef struct avifTileInfo
{
    unsigned int tileCount;
    unsigned int decodedTileCount;
    unsigned int firstTileIndex; // Within avifDecoderData.tiles.
    avifImageGrid grid;
} avifTileInfo;

typedef struct avifDecoderData
{
    avifMeta * meta; // The root-level meta box
    avifTrackArray tracks;
    avifTileArray tiles;
    avifTileInfo tileInfos[AVIF_ITEM_CATEGORY_COUNT];
    avifDecoderSource source;
    // When decoding AVIF images with grid, use a single decoder instance for all the tiles instead of creating a decoder instance
    // for each tile. If that is the case, |codec| will be used by all the tiles.
    //
    // There are some edge cases where we will still need multiple decoder instances:
    // * For animated AVIF with alpha, we will need two instances (one for the color planes and one for the alpha plane since they are both
    //   encoded as separate video sequences). In this case, |codec| will be used for the color planes and |codecAlpha| will be
    //   used for the alpha plane.
    // * For grid images with multiple layers. In this case, each tile will need its own decoder instance since there would be
    //   multiple layers in each tile. In this case, |codec| and |codecAlpha| are not used and each tile will have its own
    //   decoder instance.
    // * For grid images where the operating points of all the tiles are not the same. In this case, each tile needs its own
    //   decoder instance (same as above).
    avifCodec * codec;
    avifCodec * codecAlpha;
    uint8_t majorBrand[4];                     // From the file's ftyp, used by AVIF_DECODER_SOURCE_AUTO
    avifBrandArray compatibleBrands;           // From the file's ftyp
    avifDiagnostics * diag;                    // Shallow copy; owned by avifDecoder
    const avifSampleTable * sourceSampleTable; // NULL unless (source == AVIF_DECODER_SOURCE_TRACKS), owned by an avifTrack
    avifBool cicpSet;                          // True if avifDecoder's image has had its CICP set correctly yet.
                                               // This allows nclx colr boxes to override AV1 CICP, as specified in the MIAF
                                               // standard (ISO/IEC 23000-22:2019), section 7.3.6.4:
                                               //   The colour information property takes precedence over any colour information
                                               //   in the image bitstream, i.e. if the property is present, colour information in
                                               //   the bitstream shall be ignored.

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    // Remember the dimg association order to the Sample Transform derived image item.
    // Colour items only. The alpha items are implicit.
    uint8_t sampleTransformNumInputImageItems; // At most AVIF_SAMPLE_TRANSFORM_MAX_NUM_INPUT_IMAGE_ITEMS.
    avifItemCategory sampleTransformInputImageItems[AVIF_SAMPLE_TRANSFORM_MAX_NUM_INPUT_IMAGE_ITEMS];
#endif
} avifDecoderData;

static void avifDecoderDataDestroy(avifDecoderData * data);

static avifDecoderData * avifDecoderDataCreate(void)
{
    avifDecoderData * data = (avifDecoderData *)avifAlloc(sizeof(avifDecoderData));
    if (data == NULL) {
        return NULL;
    }
    memset(data, 0, sizeof(avifDecoderData));
    data->meta = avifMetaCreate();
    if (data->meta == NULL || !avifArrayCreate(&data->tracks, sizeof(avifTrack), 2) ||
        !avifArrayCreate(&data->tiles, sizeof(avifTile), 8)) {
        avifDecoderDataDestroy(data);
        return NULL;
    }
    return data;
}

static void avifDecoderDataResetCodec(avifDecoderData * data)
{
    for (unsigned int i = 0; i < data->tiles.count; ++i) {
        avifTile * tile = &data->tiles.tile[i];
        if (tile->image) {
            avifImageFreePlanes(tile->image, AVIF_PLANES_ALL); // forget any pointers into codec image buffers
        }
        if (tile->codec) {
            // Check if tile->codec was created separately and destroy it in that case.
            if (tile->codec != data->codec && tile->codec != data->codecAlpha) {
                avifCodecDestroy(tile->codec);
            }
            tile->codec = NULL;
        }
    }
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        data->tileInfos[c].decodedTileCount = 0;
    }
    if (data->codec) {
        avifCodecDestroy(data->codec);
        data->codec = NULL;
    }
    if (data->codecAlpha) {
        avifCodecDestroy(data->codecAlpha);
        data->codecAlpha = NULL;
    }
}

static avifTile * avifDecoderDataCreateTile(avifDecoderData * data, avifCodecType codecType, uint32_t width, uint32_t height, uint8_t operatingPoint)
{
    avifTile * tile = (avifTile *)avifArrayPush(&data->tiles);
    if (tile == NULL) {
        return NULL;
    }
    tile->codecType = codecType;
    tile->image = avifImageCreateEmpty();
    if (!tile->image) {
        goto error;
    }
    tile->input = avifCodecDecodeInputCreate();
    if (!tile->input) {
        goto error;
    }
    tile->width = width;
    tile->height = height;
    tile->operatingPoint = operatingPoint;
    return tile;

error:
    if (tile->input) {
        avifCodecDecodeInputDestroy(tile->input);
    }
    if (tile->image) {
        avifImageDestroy(tile->image);
    }
    avifArrayPop(&data->tiles);
    return NULL;
}

static avifTrack * avifDecoderDataCreateTrack(avifDecoderData * data)
{
    avifTrack * track = (avifTrack *)avifArrayPush(&data->tracks);
    if (track == NULL) {
        return NULL;
    }
    track->meta = avifMetaCreate();
    if (track->meta == NULL) {
        avifArrayPop(&data->tracks);
        return NULL;
    }
    return track;
}

static void avifDecoderDataClearTiles(avifDecoderData * data)
{
    for (unsigned int i = 0; i < data->tiles.count; ++i) {
        avifTile * tile = &data->tiles.tile[i];
        if (tile->input) {
            avifCodecDecodeInputDestroy(tile->input);
            tile->input = NULL;
        }
        if (tile->codec) {
            // Check if tile->codec was created separately and destroy it in that case.
            if (tile->codec != data->codec && tile->codec != data->codecAlpha) {
                avifCodecDestroy(tile->codec);
            }
            tile->codec = NULL;
        }
        if (tile->image) {
            avifImageDestroy(tile->image);
            tile->image = NULL;
        }
    }
    data->tiles.count = 0;
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        data->tileInfos[c].tileCount = 0;
        data->tileInfos[c].decodedTileCount = 0;
    }
    if (data->codec) {
        avifCodecDestroy(data->codec);
        data->codec = NULL;
    }
    if (data->codecAlpha) {
        avifCodecDestroy(data->codecAlpha);
        data->codecAlpha = NULL;
    }
}

static void avifDecoderDataDestroy(avifDecoderData * data)
{
    if (data->meta) {
        avifMetaDestroy(data->meta);
    }
    for (uint32_t i = 0; i < data->tracks.count; ++i) {
        avifTrack * track = &data->tracks.track[i];
        if (track->sampleTable) {
            avifSampleTableDestroy(track->sampleTable);
        }
        if (track->meta) {
            avifMetaDestroy(track->meta);
        }
    }
    avifArrayDestroy(&data->tracks);
    avifDecoderDataClearTiles(data);
    avifArrayDestroy(&data->tiles);
    avifArrayDestroy(&data->compatibleBrands);
    avifFree(data);
}

// This returns the max extent that has to be read in order to decode this item. If
// the item is stored in an idat, the data has already been read during Parse() and
// this function will return AVIF_RESULT_OK with a 0-byte extent.
static avifResult avifDecoderItemMaxExtent(const avifDecoderItem * item, const avifDecodeSample * sample, avifExtent * outExtent)
{
    if (item->extents.count == 0) {
        return AVIF_RESULT_TRUNCATED_DATA;
    }

    if (item->idatStored) {
        // construction_method: idat(1)

        if (item->meta->idat.size > 0) {
            // Already read from a meta box during Parse()
            memset(outExtent, 0, sizeof(avifExtent));
            return AVIF_RESULT_OK;
        }

        // no associated idat box was found in the meta box, bail out
        return AVIF_RESULT_NO_CONTENT;
    }

    // construction_method: file(0)

    if (sample->size == 0) {
        return AVIF_RESULT_TRUNCATED_DATA;
    }
    uint64_t remainingOffset = sample->offset;
    size_t remainingBytes = sample->size; // This may be smaller than item->size if the item is progressive

    // Assert that the for loop below will execute at least one iteration.
    AVIF_ASSERT_OR_RETURN(item->extents.count != 0);
    uint64_t minOffset = UINT64_MAX;
    uint64_t maxOffset = 0;
    for (uint32_t extentIter = 0; extentIter < item->extents.count; ++extentIter) {
        avifExtent * extent = &item->extents.extent[extentIter];

        // Make local copies of extent->offset and extent->size as they might need to be adjusted
        // due to the sample's offset.
        uint64_t startOffset = extent->offset;
        size_t extentSize = extent->size;
        if (remainingOffset) {
            if (remainingOffset >= extentSize) {
                remainingOffset -= extentSize;
                continue;
            } else {
                if (remainingOffset > UINT64_MAX - startOffset) {
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                startOffset += remainingOffset;
                extentSize -= (size_t)remainingOffset;
                remainingOffset = 0;
            }
        }

        const size_t usedExtentSize = (extentSize < remainingBytes) ? extentSize : remainingBytes;

        if (usedExtentSize > UINT64_MAX - startOffset) {
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
        const uint64_t endOffset = startOffset + usedExtentSize;

        if (minOffset > startOffset) {
            minOffset = startOffset;
        }
        if (maxOffset < endOffset) {
            maxOffset = endOffset;
        }

        remainingBytes -= usedExtentSize;
        if (remainingBytes == 0) {
            // We've got enough bytes for this sample.
            break;
        }
    }

    if (remainingBytes != 0) {
        return AVIF_RESULT_TRUNCATED_DATA;
    }

    outExtent->offset = minOffset;
    const uint64_t extentLength = maxOffset - minOffset;
#if UINT64_MAX > SIZE_MAX
    if (extentLength > SIZE_MAX) {
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
#endif
    outExtent->size = (size_t)extentLength;
    return AVIF_RESULT_OK;
}

static uint8_t avifDecoderItemOperatingPoint(const avifDecoderItem * item)
{
    const avifProperty * a1opProp = avifPropertyArrayFind(&item->properties, "a1op");
    if (a1opProp) {
        return a1opProp->u.a1op.opIndex;
    }
    return 0; // default
}

static avifResult avifDecoderItemValidateProperties(const avifDecoderItem * item,
                                                    const char * configPropName,
                                                    avifDiagnostics * diag,
                                                    const avifStrictFlags strictFlags)
{
    const avifProperty * const configProp = avifPropertyArrayFind(&item->properties, configPropName);
    if (!configProp) {
        // An item configuration property box is mandatory in all valid AVIF configurations. Bail out.
        avifDiagnosticsPrintf(diag, "Item ID %u of type '%.4s' is missing mandatory %s property", item->id, (const char *)item->type, configPropName);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    if (!memcmp(item->type, "grid", 4)) {
        for (uint32_t i = 0; i < item->meta->items.count; ++i) {
            avifDecoderItem * tile = item->meta->items.item[i];
            if (tile->dimgForID != item->id) {
                continue;
            }
            // Tile item types were checked in avifDecoderGenerateImageTiles(), no need to do it here.

            // MIAF (ISO 23000-22:2019), Section 7.3.11.4.1:
            //   All input images of a grid image item shall use the same [...] chroma sampling format,
            //   and the same decoder configuration (see 7.3.6.2).

            // The chroma sampling format is part of the decoder configuration.
            const avifProperty * tileConfigProp = avifPropertyArrayFind(&tile->properties, configPropName);
            if (!tileConfigProp) {
                avifDiagnosticsPrintf(diag,
                                      "Tile item ID %u of type '%.4s' is missing mandatory %s property",
                                      tile->id,
                                      (const char *)tile->type,
                                      configPropName);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            // configProp was copied from a tile item to the grid item. Comparing tileConfigProp with it
            // is equivalent to comparing tileConfigProp with the configPropName from the first tile.
            if ((tileConfigProp->u.av1C.seqProfile != configProp->u.av1C.seqProfile) ||
                (tileConfigProp->u.av1C.seqLevelIdx0 != configProp->u.av1C.seqLevelIdx0) ||
                (tileConfigProp->u.av1C.seqTier0 != configProp->u.av1C.seqTier0) ||
                (tileConfigProp->u.av1C.highBitdepth != configProp->u.av1C.highBitdepth) ||
                (tileConfigProp->u.av1C.twelveBit != configProp->u.av1C.twelveBit) ||
                (tileConfigProp->u.av1C.monochrome != configProp->u.av1C.monochrome) ||
                (tileConfigProp->u.av1C.chromaSubsamplingX != configProp->u.av1C.chromaSubsamplingX) ||
                (tileConfigProp->u.av1C.chromaSubsamplingY != configProp->u.av1C.chromaSubsamplingY) ||
                (tileConfigProp->u.av1C.chromaSamplePosition != configProp->u.av1C.chromaSamplePosition)) {
                avifDiagnosticsPrintf(diag,
                                      "The fields of the %s property of tile item ID %u of type '%.4s' differs from other tiles",
                                      configPropName,
                                      tile->id,
                                      (const char *)tile->type);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        }
    }

    const avifProperty * pixiProp = avifPropertyArrayFind(&item->properties, "pixi");
    if (!pixiProp && (strictFlags & AVIF_STRICT_PIXI_REQUIRED)) {
        // A pixi box is mandatory in all valid AVIF configurations. Bail out.
        avifDiagnosticsPrintf(diag,
                              "[Strict] Item ID %u of type '%.4s' is missing mandatory pixi property",
                              item->id,
                              (const char *)item->type);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    if (pixiProp) {
        const uint32_t configDepth = avifCodecConfigurationBoxGetDepth(&configProp->u.av1C);
        for (uint8_t i = 0; i < pixiProp->u.pixi.planeCount; ++i) {
            if (pixiProp->u.pixi.planeDepths[i] != configDepth) {
                // pixi depth must match configuration property depth
                avifDiagnosticsPrintf(diag,
                                      "Item ID %u depth specified by pixi property [%u] does not match %s property depth [%u]",
                                      item->id,
                                      pixiProp->u.pixi.planeDepths[i],
                                      configPropName,
                                      configDepth);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
            if (pixiProp->u.pixi.subsamplingFlag[i]) {
                if (pixiProp->u.pixi.subsamplingType[i] != avifCodecConfigurationBoxGetSubsamplingType(&configProp->u.av1C, i)) {
                    avifDiagnosticsPrintf(diag,
                                          "Item ID %u subsampling type specified by pixi property [%u] for channel %u does not match %s property [%u,%u]",
                                          item->id,
                                          pixiProp->u.pixi.subsamplingType[i],
                                          i,
                                          configPropName,
                                          configProp->u.av1C.chromaSubsamplingX,
                                          configProp->u.av1C.chromaSubsamplingY);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                if (configProp->u.av1C.chromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN) {
                    const avifChromaSamplePosition expectedChromaSamplePosition =
                        i == AVIF_CHAN_Y ? AVIF_CHROMA_SAMPLE_POSITION_COLOCATED : configProp->u.av1C.chromaSamplePosition;
                    if (avifSubsamplingLocationToChromaSamplePosition(pixiProp->u.pixi.subsamplingType[i],
                                                                      pixiProp->u.pixi.subsamplingLocation[i]) !=
                        expectedChromaSamplePosition) {
                        avifDiagnosticsPrintf(diag,
                                              "Item ID %u subsampling type and location specified by pixi property [%u,%u] for channel %u does not match %s property chroma sample position [%u]",
                                              item->id,
                                              pixiProp->u.pixi.subsamplingType[i],
                                              pixiProp->u.pixi.subsamplingLocation[i],
                                              i,
                                              configPropName,
                                              configProp->u.av1C.chromaSamplePosition);
                        return AVIF_RESULT_BMFF_PARSE_FAILED;
                    }
                }
            }
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI
        }
    }

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
    if (item->miniBoxPixelFormat != AVIF_PIXEL_FORMAT_NONE) {
        // This is a MinimizedImageBox ('mini').

        avifPixelFormat av1CPixelFormat;
        if (configProp->u.av1C.monochrome) {
            av1CPixelFormat = AVIF_PIXEL_FORMAT_YUV400;
        } else if (configProp->u.av1C.chromaSubsamplingY == 1) {
            av1CPixelFormat = AVIF_PIXEL_FORMAT_YUV420;
        } else if (configProp->u.av1C.chromaSubsamplingX == 1) {
            av1CPixelFormat = AVIF_PIXEL_FORMAT_YUV422;
        } else {
            av1CPixelFormat = AVIF_PIXEL_FORMAT_YUV444;
        }
        if (item->miniBoxPixelFormat != av1CPixelFormat) {
            if (!memcmp(configPropName, "av2C", 4) && item->miniBoxPixelFormat == AVIF_PIXEL_FORMAT_YUV400 &&
                av1CPixelFormat == AVIF_PIXEL_FORMAT_YUV420) {
                // avm does not handle monochrome as of research-v8.0.0.
                // 4:2:0 is used instead.
            } else {
                avifDiagnosticsPrintf(diag,
                                      "Item ID %u format [%s] specified by MinimizedImageBox does not match %s property format [%s]",
                                      item->id,
                                      avifPixelFormatToString(item->miniBoxPixelFormat),
                                      configPropName,
                                      avifPixelFormatToString(av1CPixelFormat));
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        }

        if (configProp->u.av1C.chromaSamplePosition == /*CSP_UNKNOWN=*/0) {
            // Section 6.4.2. Color config semantics of AV1 specification says:
            //   CSP_UNKNOWN - the source video transfer function must be signaled outside the AV1 bitstream
            // See https://aomediacodec.github.io/av1-spec/#color-config-semantics

            // So item->miniBoxChromaSamplePosition can differ and will override the AV1 value.
        } else if ((uint8_t)item->miniBoxChromaSamplePosition != configProp->u.av1C.chromaSamplePosition) {
            avifDiagnosticsPrintf(diag,
                                  "Item ID %u chroma sample position [%u] specified by MinimizedImageBox does not match %s property chroma sample position [%u]",
                                  item->id,
                                  (uint32_t)item->miniBoxChromaSamplePosition,
                                  configPropName,
                                  configProp->u.av1C.chromaSamplePosition);
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
    }
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

    if (strictFlags & AVIF_STRICT_CLAP_VALID) {
        const avifProperty * clapProp = avifPropertyArrayFind(&item->properties, "clap");
        if (clapProp) {
            const avifProperty * ispeProp = avifPropertyArrayFind(&item->properties, "ispe");
            if (!ispeProp) {
                avifDiagnosticsPrintf(diag,
                                      "[Strict] Item ID %u is missing an ispe property, so its clap property cannot be validated",
                                      item->id);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            avifCropRect cropRect;
            const uint32_t imageW = ispeProp->u.ispe.width;
            const uint32_t imageH = ispeProp->u.ispe.height;
            const avifBool validClap = avifCropRectFromCleanApertureBox(&cropRect, &clapProp->u.clap, imageW, imageH, diag);
            if (!validClap) {
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifDecoderItemRead(avifDecoderItem * item,
                                      avifIO * io,
                                      avifROData * outData,
                                      size_t offset,
                                      size_t partialByteCount,
                                      avifDiagnostics * diag)
{
    if (item->mergedExtents.data && !item->partialMergedExtents) {
        // Multiple extents have already been concatenated for this item, just return it
        if (offset >= item->mergedExtents.size) {
            avifDiagnosticsPrintf(diag, "Item ID %u read has overflowing offset", item->id);
            return AVIF_RESULT_TRUNCATED_DATA;
        }
        outData->data = item->mergedExtents.data + offset;
        outData->size = item->mergedExtents.size - offset;
        return AVIF_RESULT_OK;
    }

    if (item->extents.count == 0) {
        avifDiagnosticsPrintf(diag, "Item ID %u has zero extents", item->id);
        return AVIF_RESULT_TRUNCATED_DATA;
    }

    // Find this item's source of all extents' data, based on the construction method
    const avifRWData * idatBuffer = NULL;
    if (item->idatStored) {
        // construction_method: idat(1)

        if (item->meta->idat.size > 0) {
            idatBuffer = &item->meta->idat;
        } else {
            // no associated idat box was found in the meta box, bail out
            avifDiagnosticsPrintf(diag, "Item ID %u is stored in an idat, but no associated idat box was found", item->id);
            return AVIF_RESULT_NO_CONTENT;
        }
    }

    // Merge extents into a single contiguous buffer
    if ((io->sizeHint > 0) && (item->size > io->sizeHint)) {
        // Sanity check: somehow the sum of extents exceeds the entire file or idat size!
        avifDiagnosticsPrintf(diag, "Item ID %u reported size failed size hint sanity check. Truncated data?", item->id);
        return AVIF_RESULT_TRUNCATED_DATA;
    }

    if (offset >= item->size) {
        avifDiagnosticsPrintf(diag, "Item ID %u read has overflowing offset", item->id);
        return AVIF_RESULT_TRUNCATED_DATA;
    }
    const size_t maxOutputSize = item->size - offset;
    const size_t readOutputSize = (partialByteCount && (partialByteCount < maxOutputSize)) ? partialByteCount : maxOutputSize;
    const size_t totalBytesToRead = offset + readOutputSize;

    // If there is a single extent for this item and the source of the read buffer is going to be
    // persistent for the lifetime of the avifDecoder (whether it comes from its own internal
    // idatBuffer or from a known-persistent IO), we can avoid buffer duplication and just use the
    // preexisting buffer.
    avifBool singlePersistentBuffer = ((item->extents.count == 1) && (idatBuffer || io->persistent));
    if (!singlePersistentBuffer) {
        // Always allocate the item's full size here, as progressive image decodes will do partial
        // reads into this buffer and begin feeding the buffer to the underlying AV1 decoder, but
        // will then write more into this buffer without flushing the AV1 decoder (which is still
        // holding the address of the previous allocation of this buffer). This strategy avoids
        // use-after-free issues in the AV1 decoder and unnecessary reallocs as a typical
        // progressive decode use case will eventually decode the final layer anyway.
        AVIF_CHECKRES(avifRWDataRealloc(&item->mergedExtents, item->size));
        item->ownsMergedExtents = AVIF_TRUE;
    }

    // Set this until we manage to fill the entire mergedExtents buffer
    item->partialMergedExtents = AVIF_TRUE;

    uint8_t * front = item->mergedExtents.data;
    size_t remainingBytes = totalBytesToRead;
    for (uint32_t extentIter = 0; extentIter < item->extents.count; ++extentIter) {
        avifExtent * extent = &item->extents.extent[extentIter];

        size_t bytesToRead = extent->size;
        if (bytesToRead > remainingBytes) {
            bytesToRead = remainingBytes;
        }

        avifROData offsetBuffer;
        if (idatBuffer) {
            if (extent->offset > idatBuffer->size) {
                avifDiagnosticsPrintf(diag, "Item ID %u has impossible extent offset in idat buffer", item->id);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            // Since extent->offset (a uint64_t) is not bigger than idatBuffer->size (a size_t),
            // it is safe to cast extent->offset to size_t.
            const size_t extentOffset = (size_t)extent->offset;
            if (extent->size > idatBuffer->size - extentOffset) {
                avifDiagnosticsPrintf(diag, "Item ID %u has impossible extent size in idat buffer", item->id);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            offsetBuffer.data = idatBuffer->data + extentOffset;
            offsetBuffer.size = idatBuffer->size - extentOffset;
        } else {
            // construction_method: file(0)

            if ((io->sizeHint > 0) && (extent->offset > io->sizeHint)) {
                avifDiagnosticsPrintf(diag, "Item ID %u extent offset failed size hint sanity check. Truncated data?", item->id);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            avifResult readResult = io->read(io, 0, extent->offset, bytesToRead, &offsetBuffer);
            if (readResult != AVIF_RESULT_OK) {
                return readResult;
            }
            if (bytesToRead != offsetBuffer.size) {
                avifDiagnosticsPrintf(diag,
                                      "Item ID %u tried to read %zu bytes, but only received %zu bytes",
                                      item->id,
                                      bytesToRead,
                                      offsetBuffer.size);
                return AVIF_RESULT_TRUNCATED_DATA;
            }
        }

        if (singlePersistentBuffer) {
            memcpy(&item->mergedExtents, &offsetBuffer, sizeof(avifRWData));
            item->mergedExtents.size = bytesToRead;
        } else {
            AVIF_ASSERT_OR_RETURN(item->ownsMergedExtents);
            AVIF_ASSERT_OR_RETURN(front);
            memcpy(front, offsetBuffer.data, bytesToRead);
            front += bytesToRead;
        }

        remainingBytes -= bytesToRead;
        if (remainingBytes == 0) {
            // This happens when partialByteCount is set
            break;
        }
    }
    if (remainingBytes != 0) {
        // This should be impossible?
        avifDiagnosticsPrintf(diag, "Item ID %u has %zu unexpected trailing bytes", item->id, remainingBytes);
        return AVIF_RESULT_TRUNCATED_DATA;
    }

    outData->data = item->mergedExtents.data + offset;
    outData->size = readOutputSize;
    item->partialMergedExtents = (item->size != totalBytesToRead);
    return AVIF_RESULT_OK;
}

// Returns the avifCodecType of the first tile of the gridItem.
static avifCodecType avifDecoderItemGetGridCodecType(const avifDecoderItem * gridItem)
{
    for (uint32_t i = 0; i < gridItem->meta->items.count; ++i) {
        avifDecoderItem * item = gridItem->meta->items.item[i];
        const avifCodecType tileCodecType = avifGetCodecType(item->type);
        if ((item->dimgForID == gridItem->id) && (tileCodecType != AVIF_CODEC_TYPE_UNKNOWN)) {
            return tileCodecType;
        }
    }
    return AVIF_CODEC_TYPE_UNKNOWN;
}

// Fills the dimgIdxToItemIdx array with a mapping from each 0-based tile index in the 'dimg' reference
// to its corresponding 0-based index in the avifMeta::items array.
static avifResult avifFillDimgIdxToItemIdxArray(uint32_t * dimgIdxToItemIdx, uint32_t numExpectedTiles, const avifDecoderItem * gridItem)
{
    const uint32_t itemIndexNotSet = UINT32_MAX;
    for (uint32_t dimgIdx = 0; dimgIdx < numExpectedTiles; ++dimgIdx) {
        dimgIdxToItemIdx[dimgIdx] = itemIndexNotSet;
    }
    uint32_t numTiles = 0;
    for (uint32_t i = 0; i < gridItem->meta->items.count; ++i) {
        if (gridItem->meta->items.item[i]->dimgForID == gridItem->id) {
            const uint32_t tileItemDimgIdx = gridItem->meta->items.item[i]->dimgIdx;
            AVIF_CHECKERR(tileItemDimgIdx < numExpectedTiles, AVIF_RESULT_INVALID_IMAGE_GRID);
            AVIF_CHECKERR(dimgIdxToItemIdx[tileItemDimgIdx] == itemIndexNotSet, AVIF_RESULT_INVALID_IMAGE_GRID);
            dimgIdxToItemIdx[tileItemDimgIdx] = i;
            ++numTiles;
        }
    }
    // The number of tiles has been verified in avifDecoderItemReadAndParse().
    AVIF_ASSERT_OR_RETURN(numTiles == numExpectedTiles);
    return AVIF_RESULT_OK;
}

// Copies the codec type property (av1C or av2C) from the first grid tile to the grid item.
// Also checks that all tiles have the same codec type and that it's valid.
static avifResult avifDecoderAdoptGridTileCodecType(avifDecoder * decoder,
                                                    avifDecoderItem * gridItem,
                                                    const uint32_t * dimgIdxToItemIdx,
                                                    uint32_t numTiles)
{
    avifDecoderItem * firstTileItem = NULL;
    for (uint32_t dimgIdx = 0; dimgIdx < numTiles; ++dimgIdx) {
        const uint32_t itemIdx = dimgIdxToItemIdx[dimgIdx];
        AVIF_ASSERT_OR_RETURN(itemIdx < gridItem->meta->items.count);
        avifDecoderItem * item = gridItem->meta->items.item[itemIdx];

        // According to HEIF (ISO 14496-12), Section 6.6.2.3.1, the SingleItemTypeReferenceBox of type 'dimg'
        // identifies the input images of the derived image item of type 'grid'. Since the reference_count
        // shall be equal to rows*columns, unknown tile item types cannot be skipped but must be considered
        // as errors.
        const avifCodecType tileCodecType = avifGetCodecType(item->type);
        if (tileCodecType == AVIF_CODEC_TYPE_UNKNOWN) {
            char type[4];
            for (int j = 0; j < 4; j++) {
                if (isprint((unsigned char)item->type[j])) {
                    type[j] = item->type[j];
                } else {
                    type[j] = '.';
                }
            }
            avifDiagnosticsPrintf(&decoder->diag,
                                  "Tile item ID %u has an unknown item type '%.4s' (%02x%02x%02x%02x)",
                                  item->id,
                                  type,
                                  item->type[0],
                                  item->type[1],
                                  item->type[2],
                                  item->type[3]);
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }

        if (item->hasUnsupportedEssentialProperty) {
            // An essential property isn't supported by libavif; can't
            // decode a grid image if any tile in the grid isn't supported.
            avifDiagnosticsPrintf(&decoder->diag, "Grid image contains tile with an unsupported property marked as essential");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }

        if (firstTileItem == NULL) {
            firstTileItem = item;
            // Adopt the configuration property of the first image item tile, so that it can be queried from
            // the top-level color/alpha item during avifDecoderReset().
            const avifCodecType codecType = avifGetCodecType(item->type);
            const char * configPropName = avifGetConfigurationPropertyName(codecType);
            const avifProperty * srcProp = avifPropertyArrayFind(&item->properties, configPropName);
            if (!srcProp) {
                avifDiagnosticsPrintf(&decoder->diag, "Grid image's first tile is missing an %s property", configPropName);
                return AVIF_RESULT_INVALID_IMAGE_GRID;
            }
            avifProperty * dstProp = (avifProperty *)avifArrayPush(&gridItem->properties);
            AVIF_CHECKERR(dstProp != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            *dstProp = *srcProp;

        } else if (memcmp(item->type, firstTileItem->type, 4)) {
            // MIAF (ISO 23000-22:2019), Section 7.3.11.4.1:
            //   All input images of a grid image item shall use the same coding format [...]
            // The coding format is defined by the item type.
            avifDiagnosticsPrintf(&decoder->diag,
                                  "Tile item ID %u of type '%.4s' differs from other tile type '%.4s'",
                                  item->id,
                                  (const char *)item->type,
                                  (const char *)firstTileItem->type);
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }
    }
    return AVIF_RESULT_OK;
}

// If the item is a grid, copies the codec type property (av1C or av2C) from the first grid tile to the grid item.
// Also checks that all tiles have the same codec type and that it's valid.
static avifResult avifDecoderAdoptGridTileCodecTypeIfNeeded(avifDecoder * decoder, avifDecoderItem * item, const avifTileInfo * info)
{
    if ((info->grid.rows > 0) && (info->grid.columns > 0)) {
        // The number of tiles was verified in avifDecoderItemReadAndParse().
        const uint32_t numTiles = info->grid.rows * info->grid.columns;
        uint32_t * dimgIdxToItemIdx = (uint32_t *)avifAlloc(numTiles * sizeof(uint32_t));
        AVIF_CHECKERR(dimgIdxToItemIdx != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        avifResult result = avifFillDimgIdxToItemIdxArray(dimgIdxToItemIdx, numTiles, item);
        if (result == AVIF_RESULT_OK) {
            result = avifDecoderAdoptGridTileCodecType(decoder, item, dimgIdxToItemIdx, numTiles);
        }
        avifFree(dimgIdxToItemIdx);
        AVIF_CHECKRES(result);
    }
    return AVIF_RESULT_OK;
}

// Creates the tiles and associate them to the items in the order of the 'dimg' association.
static avifResult avifDecoderGenerateImageGridTiles(avifDecoder * decoder,
                                                    avifDecoderItem * gridItem,
                                                    avifItemCategory itemCategory,
                                                    const uint32_t * dimgIdxToItemIdx,
                                                    uint32_t numTiles)
{
    avifBool progressive = AVIF_TRUE;
    for (uint32_t dimgIdx = 0; dimgIdx < numTiles; ++dimgIdx) {
        const uint32_t itemIdx = dimgIdxToItemIdx[dimgIdx];
        AVIF_ASSERT_OR_RETURN(itemIdx < gridItem->meta->items.count);
        avifDecoderItem * item = gridItem->meta->items.item[itemIdx];

        const avifCodecType tileCodecType = avifGetCodecType(item->type);
        AVIF_CHECKERR(tileCodecType != AVIF_CODEC_TYPE_UNKNOWN, AVIF_RESULT_INVALID_IMAGE_GRID);
        const avifTile * tile =
            avifDecoderDataCreateTile(decoder->data, tileCodecType, item->width, item->height, avifDecoderItemOperatingPoint(item));
        AVIF_CHECKERR(tile != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKRES(avifCodecDecodeInputFillFromDecoderItem(tile->input,
                                                              item,
                                                              decoder->allowProgressive,
                                                              decoder->imageCountLimit,
                                                              decoder->io->sizeHint,
                                                              &decoder->diag));
        tile->input->itemCategory = itemCategory;

        if (!item->progressive) {
            progressive = AVIF_FALSE;
        }
    }
    if (itemCategory == AVIF_ITEM_COLOR && progressive) {
        // If all the items that make up the grid are progressive, then propagate that status to the top-level grid item.
        gridItem->progressive = AVIF_TRUE;
    }
    return AVIF_RESULT_OK;
}

// Allocates the dstImage. Also verifies some spec compliance rules for grids, if relevant.
static avifResult avifDecoderDataAllocateImagePlanes(avifDecoderData * data, const avifTileInfo * info, avifImage * dstImage)
{
    const avifTile * tile = &data->tiles.tile[info->firstTileIndex];
    uint32_t dstWidth;
    uint32_t dstHeight;

    if (info->grid.rows > 0 && info->grid.columns > 0) {
        const avifImageGrid * grid = &info->grid;
        // Validate grid image size and tile size.
        //
        // HEIF (ISO/IEC 23008-12:2017), Section 6.6.2.3.1:
        //   The tiled input images shall completely "cover" the reconstructed image grid canvas, ...
        if (((tile->image->width * grid->columns) < grid->outputWidth) || ((tile->image->height * grid->rows) < grid->outputHeight)) {
            avifDiagnosticsPrintf(data->diag,
                                  "Grid image tiles do not completely cover the image (HEIF (ISO/IEC 23008-12:2017), Section 6.6.2.3.1)");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }
        // Tiles in the rightmost column and bottommost row must overlap the reconstructed image grid canvas. See MIAF (ISO/IEC 23000-22:2019), Section 7.3.11.4.2, Figure 2.
        if (((tile->image->width * (grid->columns - 1)) >= grid->outputWidth) ||
            ((tile->image->height * (grid->rows - 1)) >= grid->outputHeight)) {
            avifDiagnosticsPrintf(data->diag,
                                  "Grid image tiles in the rightmost column and bottommost row do not overlap the reconstructed image grid canvas. See MIAF (ISO/IEC 23000-22:2019), Section 7.3.11.4.2, Figure 2");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }
        if (!avifAreGridDimensionsValid(tile->image->yuvFormat,
                                        grid->outputWidth,
                                        grid->outputHeight,
                                        tile->image->width,
                                        tile->image->height,
                                        data->diag)) {
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }
        dstWidth = grid->outputWidth;
        dstHeight = grid->outputHeight;
    } else {
        // Only one tile. Width and height are inherited from the 'ispe' property of the corresponding avifDecoderItem.
        dstWidth = tile->width;
        dstHeight = tile->height;
    }

    const avifBool alpha = avifIsAlpha(tile->input->itemCategory);
    if (alpha) {
        // An alpha tile does not contain any YUV pixels.
        AVIF_ASSERT_OR_RETURN(tile->image->yuvFormat == AVIF_PIXEL_FORMAT_NONE);
    }

    const uint32_t dstDepth = tile->image->depth;

    // Lazily populate dstImage with the new frame's properties.
    const avifBool dimsOrDepthIsDifferent = (dstImage->width != dstWidth) || (dstImage->height != dstHeight) ||
                                            (dstImage->depth != dstDepth);
    const avifBool yuvFormatIsDifferent = !alpha && (dstImage->yuvFormat != tile->image->yuvFormat);
    if (dimsOrDepthIsDifferent || yuvFormatIsDifferent) {
        if (alpha) {
            // Alpha doesn't match size, just bail out
            avifDiagnosticsPrintf(data->diag, "Alpha plane dimensions do not match color plane dimensions");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }

        if (dimsOrDepthIsDifferent) {
            avifImageFreePlanes(dstImage, AVIF_PLANES_ALL);
            dstImage->width = dstWidth;
            dstImage->height = dstHeight;
            dstImage->depth = dstDepth;
        }
        if (yuvFormatIsDifferent) {
            avifImageFreePlanes(dstImage, AVIF_PLANES_YUV);
            dstImage->yuvFormat = tile->image->yuvFormat;
        }
        // Keep dstImage->yuvRange which is already set to its correct value
        // (extracted from the 'colr' box if parsed or from a Sequence Header OBU otherwise).

        if (!data->cicpSet) {
            data->cicpSet = AVIF_TRUE;
            dstImage->colorPrimaries = tile->image->colorPrimaries;
            dstImage->transferCharacteristics = tile->image->transferCharacteristics;
            dstImage->matrixCoefficients = tile->image->matrixCoefficients;
        }
    }

    if (avifImageAllocatePlanes(dstImage, alpha ? AVIF_PLANES_A : AVIF_PLANES_YUV) != AVIF_RESULT_OK) {
        avifDiagnosticsPrintf(data->diag, "Image allocation failure");
        return AVIF_RESULT_OUT_OF_MEMORY;
    }
    return AVIF_RESULT_OK;
}

// Copies over the pixels from the tile into dstImage.
// Verifies that the relevant properties of the tile match those of the first tile in case of a grid.
static avifResult avifDecoderDataCopyTileToImage(avifDecoderData * data,
                                                 const avifTileInfo * info,
                                                 avifImage * dstImage,
                                                 const avifTile * tile,
                                                 unsigned int tileIndex)
{
    const avifTile * firstTile = &data->tiles.tile[info->firstTileIndex];
    if (tile != firstTile) {
        // Check for tile consistency. All tiles in a grid image should match the first tile in the properties checked below.
        if ((tile->image->width != firstTile->image->width) || (tile->image->height != firstTile->image->height) ||
            (tile->image->depth != firstTile->image->depth) || (tile->image->yuvFormat != firstTile->image->yuvFormat) ||
            (tile->image->yuvRange != firstTile->image->yuvRange) || (tile->image->colorPrimaries != firstTile->image->colorPrimaries) ||
            (tile->image->transferCharacteristics != firstTile->image->transferCharacteristics) ||
            (tile->image->matrixCoefficients != firstTile->image->matrixCoefficients)) {
            avifDiagnosticsPrintf(data->diag, "Grid image contains mismatched tiles");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }
    }

    avifImage srcView;
    avifImageSetDefaults(&srcView);
    avifImage dstView;
    avifImageSetDefaults(&dstView);
    avifCropRect dstViewRect = { 0, 0, firstTile->image->width, firstTile->image->height };
    if (info->grid.rows > 0 && info->grid.columns > 0) {
        unsigned int rowIndex = tileIndex / info->grid.columns;
        unsigned int colIndex = tileIndex % info->grid.columns;
        dstViewRect.x = firstTile->image->width * colIndex;
        dstViewRect.y = firstTile->image->height * rowIndex;
        if (dstViewRect.x + dstViewRect.width > info->grid.outputWidth) {
            dstViewRect.width = info->grid.outputWidth - dstViewRect.x;
        }
        if (dstViewRect.y + dstViewRect.height > info->grid.outputHeight) {
            dstViewRect.height = info->grid.outputHeight - dstViewRect.y;
        }
    }
    const avifCropRect srcViewRect = { 0, 0, dstViewRect.width, dstViewRect.height };
    AVIF_ASSERT_OR_RETURN(avifImageSetViewRect(&dstView, dstImage, &dstViewRect) == AVIF_RESULT_OK &&
                          avifImageSetViewRect(&srcView, tile->image, &srcViewRect) == AVIF_RESULT_OK);
    avifImageCopySamples(&dstView, &srcView, avifIsAlpha(tile->input->itemCategory) ? AVIF_PLANES_A : AVIF_PLANES_YUV);
    return AVIF_RESULT_OK;
}

// If colorId == 0 (a sentinel value as item IDs must be nonzero), accept any found EXIF/XMP metadata. Passing in 0
// is used when finding metadata in a meta box embedded in a trak box, as any items inside of a meta box that is
// inside of a trak box are implicitly associated to the track.
static avifResult avifDecoderFindMetadata(avifDecoder * decoder, avifMeta * meta, avifImage * image, uint32_t colorId)
{
    if (decoder->ignoreExif && decoder->ignoreXMP) {
        // Nothing to do!
        return AVIF_RESULT_OK;
    }

    for (uint32_t itemIndex = 0; itemIndex < meta->items.count; ++itemIndex) {
        avifDecoderItem * item = meta->items.item[itemIndex];
        if (!item->size) {
            continue;
        }
        if (item->hasUnsupportedEssentialProperty) {
            // An essential property isn't supported by libavif; ignore the item.
            continue;
        }

        if ((colorId > 0) && (item->descForID != colorId)) {
            // Not a content description (metadata) for the colorOBU, skip it
            continue;
        }

        if (!decoder->ignoreExif && !memcmp(item->type, "Exif", 4)) {
            avifROData exifContents;
            avifResult readResult = avifDecoderItemRead(item, decoder->io, &exifContents, 0, 0, &decoder->diag);
            if (readResult != AVIF_RESULT_OK) {
                return readResult;
            }

            // Advance past Annex A.2.1's header
            BEGIN_STREAM(exifBoxStream, exifContents.data, exifContents.size, &decoder->diag, "Exif header");
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
            // The MinimizedImageBox does not signal the exifTiffHeaderOffset.
            if (!meta->fromMiniBox)
#endif
            {
                uint32_t exifTiffHeaderOffset;
                AVIF_CHECKERR(avifROStreamReadU32(&exifBoxStream, &exifTiffHeaderOffset),
                              AVIF_RESULT_INVALID_EXIF_PAYLOAD); // unsigned int(32) exif_tiff_header_offset;
                size_t expectedExifTiffHeaderOffset;
                AVIF_CHECKRES(avifGetExifTiffHeaderOffset(avifROStreamCurrent(&exifBoxStream),
                                                          avifROStreamRemainingBytes(&exifBoxStream),
                                                          &expectedExifTiffHeaderOffset));
                AVIF_CHECKERR(exifTiffHeaderOffset == expectedExifTiffHeaderOffset, AVIF_RESULT_INVALID_EXIF_PAYLOAD);
            }

            AVIF_CHECKRES(avifRWDataSet(&image->exif, avifROStreamCurrent(&exifBoxStream), avifROStreamRemainingBytes(&exifBoxStream)));
        } else if (!decoder->ignoreXMP && !memcmp(item->type, "mime", 4) &&
                   !strcmp(item->contentType.contentType, AVIF_CONTENT_TYPE_XMP)) {
            avifROData xmpContents;
            avifResult readResult = avifDecoderItemRead(item, decoder->io, &xmpContents, 0, 0, &decoder->diag);
            if (readResult != AVIF_RESULT_OK) {
                return readResult;
            }

            AVIF_CHECKRES(avifImageSetMetadataXMP(image, xmpContents.data, xmpContents.size));
        }
    }
    return AVIF_RESULT_OK;
}

// ---------------------------------------------------------------------------
// URN

static avifBool isAlphaURN(const char * urn)
{
    return !strcmp(urn, AVIF_URN_ALPHA0) || !strcmp(urn, AVIF_URN_ALPHA1);
}

// ---------------------------------------------------------------------------
// BMFF Parsing

static avifBool avifParseHandlerBox(const uint8_t * raw, size_t rawLen, uint8_t handlerType[4], avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[hdlr]");

    AVIF_CHECK(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL));

    uint32_t predefined;
    AVIF_CHECK(avifROStreamReadU32(&s, &predefined)); // unsigned int(32) pre_defined = 0;
    if (predefined != 0) {
        avifDiagnosticsPrintf(diag, "Box[hdlr] contains a pre_defined value that is nonzero");
        return AVIF_FALSE;
    }

    AVIF_CHECK(avifROStreamRead(&s, handlerType, 4)); // unsigned int(32) handler_type;

    for (int i = 0; i < 3; ++i) {
        uint32_t reserved;
        AVIF_CHECK(avifROStreamReadU32(&s, &reserved)); // const unsigned int(32)[3] reserved = 0;
    }

    // Verify that a valid string is here, but don't bother to store it
    AVIF_CHECK(avifROStreamReadString(&s, NULL, 0)); // string name;
    return AVIF_TRUE;
}

static avifResult avifParseItemLocationBox(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[iloc]");

    // Section 8.11.3.2 of ISO/IEC 14496-12.
    uint8_t version;
    AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, &version, NULL), AVIF_RESULT_BMFF_PARSE_FAILED);
    if (version > 2) {
        avifDiagnosticsPrintf(diag, "Box[iloc] has an unsupported version [%u]", version);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    uint8_t offsetSize, lengthSize, baseOffsetSize, indexSize = 0;
    uint32_t reserved;
    AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &offsetSize, /*bitCount=*/4), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) offset_size;
    AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &lengthSize, /*bitCount=*/4), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) length_size;
    AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &baseOffsetSize, /*bitCount=*/4), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) base_offset_size;
    if (version == 1 || version == 2) {
        AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &indexSize, /*bitCount=*/4), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) index_size;
    } else {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &reserved, /*bitCount=*/4), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) reserved;
    }

    // Section 8.11.3.3 of ISO/IEC 14496-12.
    if ((offsetSize != 0 && offsetSize != 4 && offsetSize != 8) || (lengthSize != 0 && lengthSize != 4 && lengthSize != 8) ||
        (baseOffsetSize != 0 && baseOffsetSize != 4 && baseOffsetSize != 8) || (indexSize != 0 && indexSize != 4 && indexSize != 8)) {
        avifDiagnosticsPrintf(diag, "Box[iloc] has an invalid size");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    uint16_t tmp16;
    uint32_t itemCount;
    if (version < 2) {
        AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) item_count;
        itemCount = tmp16;
    } else {
        AVIF_CHECKERR(avifROStreamReadU32(&s, &itemCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) item_count;
    }
    for (uint32_t i = 0; i < itemCount; ++i) {
        uint32_t itemID;
        if (version < 2) {
            AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) item_ID;
            itemID = tmp16;
        } else {
            AVIF_CHECKERR(avifROStreamReadU32(&s, &itemID), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) item_ID;
        }
        AVIF_CHECKRES(avifCheckItemID("iloc", itemID, diag));

        avifDecoderItem * item;
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, itemID, &item));
        if (item->extents.count > 0) {
            // This item has already been given extents via this iloc box. This is invalid.
            avifDiagnosticsPrintf(diag, "Item ID [%u] contains duplicate sets of extents", itemID);
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }

        if (version == 1 || version == 2) {
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &reserved, /*bitCount=*/12), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(12) reserved = 0;
            if (reserved) {
                avifDiagnosticsPrintf(diag, "Box[iloc] has a non null reserved field [%u]", reserved);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            uint8_t constructionMethod;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &constructionMethod, /*bitCount=*/4),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) construction_method;
            if (constructionMethod != 0 /* file offset */ && constructionMethod != 1 /* idat offset */) {
                // construction method 2 (item offset) unsupported
                avifDiagnosticsPrintf(diag, "Box[iloc] has an unsupported construction method [%u]", constructionMethod);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            if (constructionMethod == 1) {
                item->idatStored = AVIF_TRUE;
            }
        }

        uint16_t dataReferenceIndex;
        AVIF_CHECKERR(avifROStreamReadU16(&s, &dataReferenceIndex), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) data_reference_index;
        uint64_t baseOffset;
        AVIF_CHECKERR(avifROStreamReadUX8(&s, &baseOffset, baseOffsetSize), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(base_offset_size*8) base_offset;
        uint16_t extentCount;
        AVIF_CHECKERR(avifROStreamReadU16(&s, &extentCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) extent_count;
        for (int extentIter = 0; extentIter < extentCount; ++extentIter) {
            if ((version == 1 || version == 2) && indexSize > 0) {
                // Section 8.11.3.1 of ISO/IEC 14496-12:
                //   The item_reference_index is only used for the method item_offset; it indicates the 1-based index
                //   of the item reference with referenceType 'iloc' linked from this item. If index_size is 0, then
                //   the value 1 is implied; the value 0 is reserved.
                uint64_t itemReferenceIndex; // Ignored unless construction_method=2 which is unsupported, but still read it.
                AVIF_CHECKERR(avifROStreamReadUX8(&s, &itemReferenceIndex, indexSize),
                              AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(index_size*8) item_reference_index;
            }

            uint64_t extentOffset;
            AVIF_CHECKERR(avifROStreamReadUX8(&s, &extentOffset, offsetSize), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(offset_size*8) extent_offset;
            uint64_t extentLength;
            AVIF_CHECKERR(avifROStreamReadUX8(&s, &extentLength, lengthSize), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(length_size*8) extent_length;

            avifExtent * extent = (avifExtent *)avifArrayPush(&item->extents);
            AVIF_CHECKERR(extent != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            if (extentOffset > UINT64_MAX - baseOffset) {
                avifDiagnosticsPrintf(diag,
                                      "Item ID [%u] contains an extent offset which overflows: [base: %" PRIu64 " offset:%" PRIu64 "]",
                                      itemID,
                                      baseOffset,
                                      extentOffset);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            uint64_t offset = baseOffset + extentOffset;
            extent->offset = offset;
#if UINT64_MAX > SIZE_MAX
            if (extentLength > SIZE_MAX) {
                avifDiagnosticsPrintf(diag, "Item ID [%u] contains an extent length which overflows: [%" PRIu64 "]", itemID, extentLength);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
#endif
            extent->size = (size_t)extentLength;
            if (extent->size > SIZE_MAX - item->size) {
                avifDiagnosticsPrintf(diag,
                                      "Item ID [%u] contains an extent length which overflows the item size: [%zu, %zu]",
                                      itemID,
                                      extent->size,
                                      item->size);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            item->size += extent->size;
        }
    }
    return AVIF_RESULT_OK;
}

static avifBool avifParseImageGridBox(avifImageGrid * grid,
                                      const uint8_t * raw,
                                      size_t rawLen,
                                      uint32_t imageSizeLimit,
                                      uint32_t imageDimensionLimit,
                                      avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[grid]");

    uint8_t version, flags;
    AVIF_CHECK(avifROStreamRead(&s, &version, 1)); // unsigned int(8) version = 0;
    if (version != 0) {
        avifDiagnosticsPrintf(diag, "Box[grid] has unsupported version [%u]", version);
        return AVIF_FALSE;
    }
    uint8_t rowsMinusOne, columnsMinusOne;
    AVIF_CHECK(avifROStreamRead(&s, &flags, 1));           // unsigned int(8) flags;
    AVIF_CHECK(avifROStreamRead(&s, &rowsMinusOne, 1));    // unsigned int(8) rows_minus_one;
    AVIF_CHECK(avifROStreamRead(&s, &columnsMinusOne, 1)); // unsigned int(8) columns_minus_one;
    grid->rows = (uint32_t)rowsMinusOne + 1;
    grid->columns = (uint32_t)columnsMinusOne + 1;

    uint32_t fieldLength = ((flags & 1) + 1) * 16;
    if (fieldLength == 16) {
        uint16_t outputWidth16, outputHeight16;
        AVIF_CHECK(avifROStreamReadU16(&s, &outputWidth16));  // unsigned int(FieldLength) output_width;
        AVIF_CHECK(avifROStreamReadU16(&s, &outputHeight16)); // unsigned int(FieldLength) output_height;
        grid->outputWidth = outputWidth16;
        grid->outputHeight = outputHeight16;
    } else {
        if (fieldLength != 32) {
            // This should be impossible
            avifDiagnosticsPrintf(diag, "Grid box contains illegal field length: [%u]", fieldLength);
            return AVIF_FALSE;
        }
        AVIF_CHECK(avifROStreamReadU32(&s, &grid->outputWidth));  // unsigned int(FieldLength) output_width;
        AVIF_CHECK(avifROStreamReadU32(&s, &grid->outputHeight)); // unsigned int(FieldLength) output_height;
    }
    if ((grid->outputWidth == 0) || (grid->outputHeight == 0)) {
        avifDiagnosticsPrintf(diag, "Grid box contains illegal dimensions: [%u x %u]", grid->outputWidth, grid->outputHeight);
        return AVIF_FALSE;
    }
    if (avifDimensionsTooLarge(grid->outputWidth, grid->outputHeight, imageSizeLimit, imageDimensionLimit)) {
        avifDiagnosticsPrintf(diag, "Grid box dimensions are too large: [%u x %u]", grid->outputWidth, grid->outputHeight);
        return AVIF_FALSE;
    }
    return avifROStreamRemainingBytes(&s) == 0;
}

static avifBool avifParseGainMapMetadata(avifGainMap * gainMap, avifROStream * s)
{
    uint32_t isMultichannel;
    AVIF_CHECK(avifROStreamReadBitsU32(s, &isMultichannel, 1)); // unsigned int(1) is_multichannel;
    const uint8_t channelCount = isMultichannel ? 3 : 1;

    uint32_t useBaseColorSpace;
    AVIF_CHECK(avifROStreamReadBitsU32(s, &useBaseColorSpace, 1)); // unsigned int(1) use_base_colour_space;
    gainMap->useBaseColorSpace = useBaseColorSpace ? AVIF_TRUE : AVIF_FALSE;

    uint32_t reserved;
    AVIF_CHECK(avifROStreamReadBitsU32(s, &reserved, 6)); // unsigned int(6) reserved;

    AVIF_CHECK(avifROStreamReadU32(s, &gainMap->baseHdrHeadroom.n));      // unsigned int(32) base_hdr_headroom_numerator;
    AVIF_CHECK(avifROStreamReadU32(s, &gainMap->baseHdrHeadroom.d));      // unsigned int(32) base_hdr_headroom_denominator;
    AVIF_CHECK(avifROStreamReadU32(s, &gainMap->alternateHdrHeadroom.n)); // unsigned int(32) alternate_hdr_headroom_numerator;
    AVIF_CHECK(avifROStreamReadU32(s, &gainMap->alternateHdrHeadroom.d)); // unsigned int(32) alternate_hdr_headroom_denominator;

    for (int c = 0; c < channelCount; ++c) {
        AVIF_CHECK(avifROStreamReadU32(s, (uint32_t *)&gainMap->gainMapMin[c].n)); // int(32) gain_map_min_numerator;
        AVIF_CHECK(avifROStreamReadU32(s, &gainMap->gainMapMin[c].d));             // unsigned int(32) gain_map_min_denominator;
        AVIF_CHECK(avifROStreamReadU32(s, (uint32_t *)&gainMap->gainMapMax[c].n)); // int(32) gain_map_max_numerator;
        AVIF_CHECK(avifROStreamReadU32(s, &gainMap->gainMapMax[c].d));             // unsigned int(32) gain_map_max_denominator;
        AVIF_CHECK(avifROStreamReadU32(s, &gainMap->gainMapGamma[c].n));           // unsigned int(32) gamma_numerator;
        AVIF_CHECK(avifROStreamReadU32(s, &gainMap->gainMapGamma[c].d));           // unsigned int(32) gamma_denominator;
        AVIF_CHECK(avifROStreamReadU32(s, (uint32_t *)&gainMap->baseOffset[c].n)); // int(32) base_offset_numerator;
        AVIF_CHECK(avifROStreamReadU32(s, &gainMap->baseOffset[c].d));             // unsigned int(32) base_offset_denominator;
        AVIF_CHECK(avifROStreamReadU32(s, (uint32_t *)&gainMap->alternateOffset[c].n)); // int(32) alternate_offset_numerator;
        AVIF_CHECK(avifROStreamReadU32(s, &gainMap->alternateOffset[c].d)); // unsigned int(32) alternate_offset_denominator;
    }

    // Fill the remaining values by copying those from the first channel.
    for (int c = channelCount; c < 3; ++c) {
        gainMap->gainMapMin[c] = gainMap->gainMapMin[0];
        gainMap->gainMapMax[c] = gainMap->gainMapMax[0];
        gainMap->gainMapGamma[c] = gainMap->gainMapGamma[0];
        gainMap->baseOffset[c] = gainMap->baseOffset[0];
        gainMap->alternateOffset[c] = gainMap->alternateOffset[0];
    }
    return AVIF_TRUE;
}

// If the gain map's version or minimum_version tag is not supported, returns AVIF_RESULT_NOT_IMPLEMENTED.
static avifResult avifParseToneMappedImageBox(avifGainMap * gainMap, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[tmap]");

    uint8_t version;
    AVIF_CHECKERR(avifROStreamRead(&s, &version, 1), AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE); // unsigned int(8) version = 0;
    if (version != 0) {
        avifDiagnosticsPrintf(diag, "Box[tmap] has unsupported version [%u]", version);
        return AVIF_RESULT_NOT_IMPLEMENTED;
    }

    uint16_t minimumVersion;
    AVIF_CHECKERR(avifROStreamReadU16(&s, &minimumVersion), AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE); // unsigned int(16) minimum_version;
    const uint16_t supportedMetadataVersion = 0;
    if (minimumVersion > supportedMetadataVersion) {
        avifDiagnosticsPrintf(diag, "Box[tmap] has unsupported minimum version [%u]", minimumVersion);
        return AVIF_RESULT_NOT_IMPLEMENTED;
    }
    uint16_t writerVersion;
    AVIF_CHECKERR(avifROStreamReadU16(&s, &writerVersion), AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE); // unsigned int(16) writer_version;
    AVIF_CHECKERR(writerVersion >= minimumVersion, AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE);

    AVIF_CHECKERR(avifParseGainMapMetadata(gainMap, &s), AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE);

    if (writerVersion <= supportedMetadataVersion) {
        AVIF_CHECKERR(avifROStreamRemainingBytes(&s) == 0, AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE);
    }

    if (avifGainMapValidateMetadata(gainMap, diag) != AVIF_RESULT_OK) {
        return AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE;
    }

    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
// bit_depth is assumed to be 2 (32-bit).
static avifResult avifParseSampleTransformTokens(avifROStream * s, avifSampleTransformExpression * expression)
{
    uint8_t tokenCount;
    AVIF_CHECKERR(avifROStreamRead(s, &tokenCount, /*size=*/1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(8) token_count;
    AVIF_CHECKERR(tokenCount != 0, AVIF_RESULT_BMFF_PARSE_FAILED);
    AVIF_CHECKERR(avifArrayCreate(expression, sizeof(expression->tokens[0]), tokenCount), AVIF_RESULT_OUT_OF_MEMORY);

    for (uint32_t t = 0; t < tokenCount; ++t) {
        avifSampleTransformToken * token = (avifSampleTransformToken *)avifArrayPush(expression);
        AVIF_CHECKERR(token != NULL, AVIF_RESULT_OUT_OF_MEMORY);

        uint8_t tokenValue;
        AVIF_CHECKERR(avifROStreamRead(s, &tokenValue, /*size=*/1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(8) token;
        if (tokenValue == AVIF_SAMPLE_TRANSFORM_CONSTANT) {
            token->type = AVIF_SAMPLE_TRANSFORM_CONSTANT;
            // Two's complement representation is assumed here.
            uint32_t constant;
            AVIF_CHECKERR(avifROStreamReadU32(s, &constant), AVIF_RESULT_BMFF_PARSE_FAILED); // signed int(1<<(bit_depth+3)) constant;
            token->constant = (int32_t)constant;
        } else if (tokenValue <= AVIF_SAMPLE_TRANSFORM_LAST_INPUT_IMAGE_ITEM_INDEX) {
            AVIF_ASSERT_OR_RETURN(tokenValue >= AVIF_SAMPLE_TRANSFORM_FIRST_INPUT_IMAGE_ITEM_INDEX);
            token->type = AVIF_SAMPLE_TRANSFORM_INPUT_IMAGE_ITEM_INDEX;
            token->inputImageItemIndex = tokenValue;
        } else if (tokenValue >= AVIF_SAMPLE_TRANSFORM_FIRST_UNARY_OPERATOR && tokenValue <= AVIF_SAMPLE_TRANSFORM_LAST_UNARY_OPERATOR) {
            token->type = (avifSampleTransformTokenType)tokenValue; // unary operator
        } else if (tokenValue >= AVIF_SAMPLE_TRANSFORM_FIRST_BINARY_OPERATOR && tokenValue <= AVIF_SAMPLE_TRANSFORM_LAST_BINARY_OPERATOR) {
            token->type = (avifSampleTransformTokenType)tokenValue; // binary operator
        } else {
            token->type = AVIF_SAMPLE_TRANSFORM_RESERVED;
        }
    }
    AVIF_CHECKERR(avifROStreamRemainingBytes(s) == 0, AVIF_RESULT_BMFF_PARSE_FAILED);
    return AVIF_RESULT_OK;
}

// Parses the raw bitstream of the 'sato' Sample Transform derived image item and extracts the expression.
static avifResult avifParseSampleTransformImageBox(const uint8_t * raw,
                                                   size_t rawLen,
                                                   uint32_t numInputImageItems,
                                                   avifSampleTransformExpression * expression,
                                                   avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[sato]");

    uint8_t version, reserved, bitDepth;
    AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &version, /*bitCount=*/2), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(2) version = 0;
    AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &reserved, /*bitCount=*/4), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) reserved;
    AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &bitDepth, /*bitCount=*/2), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(2) bit_depth;
    AVIF_CHECKERR(version == 0, AVIF_RESULT_NOT_IMPLEMENTED);
    AVIF_CHECKERR(bitDepth == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_32, AVIF_RESULT_NOT_IMPLEMENTED);

    const avifResult result = avifParseSampleTransformTokens(&s, expression);
    if (result != AVIF_RESULT_OK) {
        avifArrayDestroy(expression);
        return result;
    }
    if (!avifSampleTransformExpressionIsValid(expression, numInputImageItems)) {
        avifArrayDestroy(expression);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifDecoderSampleTransformItemValidateProperties(const avifDecoderItem * item, avifDiagnostics * diag)
{
    const avifProperty * pixiProp = avifPropertyArrayFind(&item->properties, "pixi");
    if (!pixiProp) {
        avifDiagnosticsPrintf(diag, "Item ID %u of type '%.4s' is missing mandatory pixi property", item->id, (const char *)item->type);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    for (uint8_t i = 0; i < pixiProp->u.pixi.planeCount; ++i) {
        if (pixiProp->u.pixi.planeDepths[i] != pixiProp->u.pixi.planeDepths[0]) {
            avifDiagnosticsPrintf(diag,
                                  "Item ID %u of type '%.4s' has different depths specified by pixi property [%u, %u], this is not supported",
                                  item->id,
                                  (const char *)item->type,
                                  pixiProp->u.pixi.planeDepths[0],
                                  pixiProp->u.pixi.planeDepths[i]);
            return AVIF_RESULT_NOT_IMPLEMENTED;
        }
    }

    const avifProperty * ispeProp = avifPropertyArrayFind(&item->properties, "ispe");
    if (!ispeProp) {
        avifDiagnosticsPrintf(diag, "Item ID %u of type '%.4s' is missing mandatory ispe property", item->id, (const char *)item->type);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    for (uint32_t i = 0; i < item->meta->items.count; ++i) {
        avifDecoderItem * inputImageItem = item->meta->items.item[i];
        if (inputImageItem->dimgForID != item->id) {
            continue;
        }
        // Even if inputImageItem is a grid, the ispe property from its first tile should have been copied to the grid item.
        const avifProperty * inputImageItemIspeProp = avifPropertyArrayFind(&inputImageItem->properties, "ispe");
        AVIF_ASSERT_OR_RETURN(inputImageItemIspeProp != NULL);
        if (inputImageItemIspeProp->u.ispe.width != ispeProp->u.ispe.width ||
            inputImageItemIspeProp->u.ispe.height != ispeProp->u.ispe.height) {
            avifDiagnosticsPrintf(diag,
                                  "The fields of the ispe property of item ID %u of type '%.4s' differs from item ID %u",
                                  inputImageItem->id,
                                  (const char *)inputImageItem->type,
                                  item->id);
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
        // TODO(yguyon): Check that all input image items share the same codec config (except for the bit depth value).
    }

    AVIF_CHECKERR(avifPropertyArrayFind(&item->properties, "clap") == NULL, AVIF_RESULT_NOT_IMPLEMENTED);
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

// Extracts the codecType from the item type or from its children.
// Also parses and outputs grid information if the item is a grid.
// isItemInInput must be false if the item is a made-up structure
// (and thus not part of the parseable input bitstream).
static avifResult avifDecoderItemReadAndParse(const avifDecoder * decoder,
                                              avifDecoderItem * item,
                                              avifBool isItemInInput,
                                              avifImageGrid * grid,
                                              avifCodecType * codecType)
{
    if (!memcmp(item->type, "grid", 4)) {
        if (isItemInInput) {
            avifROData readData;
            AVIF_CHECKRES(avifDecoderItemRead(item, decoder->io, &readData, 0, 0, decoder->data->diag));
            AVIF_CHECKERR(avifParseImageGridBox(grid,
                                                readData.data,
                                                readData.size,
                                                decoder->imageSizeLimit,
                                                decoder->imageDimensionLimit,
                                                decoder->data->diag),
                          AVIF_RESULT_INVALID_IMAGE_GRID);
            // Validate that there are exactly the same number of dimg items to form the grid.
            uint32_t dimgItemCount = 0;
            for (uint32_t i = 0; i < item->meta->items.count; ++i) {
                if (item->meta->items.item[i]->dimgForID == item->id) {
                    ++dimgItemCount;
                }
            }
            AVIF_CHECKERR(dimgItemCount == grid->rows * grid->columns, AVIF_RESULT_INVALID_IMAGE_GRID);
        } else {
            // item was generated for convenience and is not part of the bitstream.
            // grid information should already be set.
            AVIF_ASSERT_OR_RETURN(grid->rows > 0 && grid->columns > 0);
        }
        *codecType = avifDecoderItemGetGridCodecType(item);
        AVIF_CHECKERR(*codecType != AVIF_CODEC_TYPE_UNKNOWN, AVIF_RESULT_INVALID_IMAGE_GRID);
    } else {
        *codecType = avifGetCodecType(item->type);
        AVIF_ASSERT_OR_RETURN(*codecType != AVIF_CODEC_TYPE_UNKNOWN);
    }
    // TODO(yguyon): If AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM is defined, backward-incompatible
    //               files with a primary 'sato' Sample Transform derived image item could be
    //               handled here (compared to backward-compatible files with a 'sato' item in the
    //               same 'altr' group as the primary regular color item which are handled in
    //               avifDecoderDataFindSampleTransformImageItem() below).
    return AVIF_RESULT_OK;
}

static avifBool avifParseImageSpatialExtentsProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[ispe]");
    AVIF_CHECK(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL));

    avifImageSpatialExtents * ispe = &prop->u.ispe;
    AVIF_CHECK(avifROStreamReadU32(&s, &ispe->width));
    AVIF_CHECK(avifROStreamReadU32(&s, &ispe->height));
    return AVIF_TRUE;
}

static avifBool avifParseAuxiliaryTypeProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[auxC]");
    AVIF_CHECK(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL));

    AVIF_CHECK(avifROStreamReadString(&s, prop->u.auxC.auxType, AUXTYPE_SIZE));
    return AVIF_TRUE;
}

static avifBool avifParseColourInformationBox(avifProperty * prop, uint64_t rawOffset, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[colr]");

    avifColourInformationBox * colr = &prop->u.colr;
    colr->hasICC = AVIF_FALSE;
    colr->hasNCLX = AVIF_FALSE;

    uint8_t colorType[4]; // unsigned int(32) colour_type;
    AVIF_CHECK(avifROStreamRead(&s, colorType, 4));
    if (!memcmp(colorType, "rICC", 4) || !memcmp(colorType, "prof", 4)) {
        colr->hasICC = AVIF_TRUE;
        // Remember the offset of the ICC payload relative to the beginning of the stream. A direct pointer cannot be stored
        // because decoder->io->persistent could have been AVIF_FALSE when obtaining raw through decoder->io->read().
        // The bytes could be copied now instead of remembering the offset, but it is as invasive as passing rawOffset everywhere.
        colr->iccOffset = rawOffset + avifROStreamOffset(&s);
        colr->iccSize = avifROStreamRemainingBytes(&s);
    } else if (!memcmp(colorType, "nclx", 4)) {
        AVIF_CHECK(avifROStreamReadU16(&s, &colr->colorPrimaries));          // unsigned int(16) colour_primaries;
        AVIF_CHECK(avifROStreamReadU16(&s, &colr->transferCharacteristics)); // unsigned int(16) transfer_characteristics;
        AVIF_CHECK(avifROStreamReadU16(&s, &colr->matrixCoefficients));      // unsigned int(16) matrix_coefficients;
        uint8_t full_range_flag;
        AVIF_CHECK(avifROStreamReadBitsU8(&s, &full_range_flag, /*bitCount=*/1)); // unsigned int(1) full_range_flag;
        colr->range = full_range_flag ? AVIF_RANGE_FULL : AVIF_RANGE_LIMITED;
        uint8_t reserved;
        AVIF_CHECK(avifROStreamReadBitsU8(&s, &reserved, /*bitCount=*/7)); // unsigned int(7) reserved = 0;
        if (reserved) {
            avifDiagnosticsPrintf(diag, "Box[colr] contains nonzero reserved bits [%u]", reserved);
            return AVIF_FALSE;
        }
        colr->hasNCLX = AVIF_TRUE;
    }
    return AVIF_TRUE;
}

static avifResult avifParseContentLightLevelInformation(avifROStream * s, avifContentLightLevelInformationBox * clli)
{
    AVIF_CHECKERR(avifROStreamReadBitsU16(s, &clli->maxCLL, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) max_content_light_level
    AVIF_CHECKERR(avifROStreamReadBitsU16(s, &clli->maxPALL, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) max_pic_average_light_level
    return AVIF_RESULT_OK;
}
static avifResult avifParseContentLightLevelInformationBox(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[clli]");
    AVIF_CHECKRES(avifParseContentLightLevelInformation(&s, &prop->u.clli));
    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
static avifResult avifSkipMasteringDisplayColourVolume(avifROStream * s)
{
    for (int c = 0; c < 3; c++) {
        AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) display_primaries_x;
        AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) display_primaries_y;
    }
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) white_point_x;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) white_point_y;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) max_display_mastering_luminance;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) min_display_mastering_luminance;
    return AVIF_RESULT_OK;
}

static avifResult avifSkipContentColourVolume(avifROStream * s)
{
    AVIF_CHECKERR(avifROStreamSkipBits(s, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) reserved = 0; // ccv_cancel_flag
    AVIF_CHECKERR(avifROStreamSkipBits(s, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) reserved = 0; // ccv_persistence_flag
    uint8_t ccvPrimariesPresent;
    AVIF_CHECKERR(avifROStreamReadBitsU8(s, &ccvPrimariesPresent, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) ccv_primaries_present_flag;
    uint8_t ccvMinLuminanceValuePresent, ccvMaxLuminanceValuePresent, ccvAvgLuminanceValuePresent;
    AVIF_CHECKERR(avifROStreamReadBitsU8(s, &ccvMinLuminanceValuePresent, 1),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) ccv_min_luminance_value_present_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU8(s, &ccvMaxLuminanceValuePresent, 1),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) ccv_max_luminance_value_present_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU8(s, &ccvAvgLuminanceValuePresent, 1),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) ccv_avg_luminance_value_present_flag;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 2), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(2) reserved = 0;

    if (ccvPrimariesPresent) {
        for (int c = 0; c < 3; c++) {
            AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // signed int(32) ccv_primaries_x[[c]];
            AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // signed int(32) ccv_primaries_y[[c]];
        }
    }
    if (ccvMinLuminanceValuePresent) {
        AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) ccv_min_luminance_value;
    }
    if (ccvMaxLuminanceValuePresent) {
        AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) ccv_max_luminance_value;
    }
    if (ccvAvgLuminanceValuePresent) {
        AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) ccv_avg_luminance_value;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifSkipAmbientViewingEnvironment(avifROStream * s)
{
    AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) ambient_illuminance;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) ambient_light_x;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) ambient_light_y;
    return AVIF_RESULT_OK;
}

static avifResult avifSkipReferenceViewingEnvironment(avifROStream * s)
{
    AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) surround_luminance;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) surround_light_x;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) surround_light_y;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) periphery_luminance;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) periphery_light_x;
    AVIF_CHECKERR(avifROStreamSkipBits(s, 16), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) periphery_light_y;
    return AVIF_RESULT_OK;
}

static avifResult avifSkipNominalDiffuseWhite(avifROStream * s)
{
    AVIF_CHECKERR(avifROStreamSkipBits(s, 32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) diffuse_white_luminance;
    return AVIF_RESULT_OK;
}

static avifResult avifParseMiniHDRProperties(avifROStream * s, uint32_t * hasClli, avifContentLightLevelInformationBox * clli)
{
    AVIF_CHECKERR(avifROStreamReadBitsU32(s, hasClli, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) clli_flag;
    uint32_t hasMdcv, hasCclv, hasAmve, hasReve, hasNdwt;
    AVIF_CHECKERR(avifROStreamReadBitsU32(s, &hasMdcv, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) mdcv_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(s, &hasCclv, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) cclv_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(s, &hasAmve, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) amve_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(s, &hasReve, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) reve_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(s, &hasNdwt, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) ndwt_flag;
    if (*hasClli) {
        AVIF_CHECKRES(avifParseContentLightLevelInformation(s, clli)); // ContentLightLevel clli;
    }
    if (hasMdcv) {
        AVIF_CHECKRES(avifSkipMasteringDisplayColourVolume(s)); // MasteringDisplayColourVolume mdcv;
    }
    if (hasCclv) {
        AVIF_CHECKRES(avifSkipContentColourVolume(s)); // ContentColourVolume cclv;
    }
    if (hasAmve) {
        AVIF_CHECKRES(avifSkipAmbientViewingEnvironment(s)); // AmbientViewingEnvironment amve;
    }
    if (hasReve) {
        AVIF_CHECKRES(avifSkipReferenceViewingEnvironment(s)); // ReferenceViewingEnvironment reve;
    }
    if (hasNdwt) {
        AVIF_CHECKRES(avifSkipNominalDiffuseWhite(s)); // NominalDiffuseWhite ndwt;
    }
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

// Implementation of section 2.3.3 of AV1 Codec ISO Media File Format Binding specification v1.2.0.
// See https://aomediacodec.github.io/av1-isobmff/v1.2.0.html#av1codecconfigurationbox-syntax.
static avifBool avifParseCodecConfiguration(avifROStream * s, avifCodecConfigurationBox * config, const char * configPropName, avifDiagnostics * diag)
{
    const size_t av1COffset = s->offset;

    uint32_t marker, version;
    AVIF_CHECK(avifROStreamReadBitsU32(s, &marker, /*bitCount=*/1)); // unsigned int (1) marker = 1;
    if (!marker) {
        avifDiagnosticsPrintf(diag, "%.4s contains illegal marker: [%u]", configPropName, marker);
        return AVIF_FALSE;
    }
    AVIF_CHECK(avifROStreamReadBitsU32(s, &version, /*bitCount=*/7)); // unsigned int (7) version = 1;
    if (version != 1) {
        avifDiagnosticsPrintf(diag, "%.4s contains illegal version: [%u]", configPropName, version);
        return AVIF_FALSE;
    }

    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->seqProfile, /*bitCount=*/3));         // unsigned int (3) seq_profile;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->seqLevelIdx0, /*bitCount=*/5));       // unsigned int (5) seq_level_idx_0;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->seqTier0, /*bitCount=*/1));           // unsigned int (1) seq_tier_0;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->highBitdepth, /*bitCount=*/1));       // unsigned int (1) high_bitdepth;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->twelveBit, /*bitCount=*/1));          // unsigned int (1) twelve_bit;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->monochrome, /*bitCount=*/1));         // unsigned int (1) monochrome;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->chromaSubsamplingX, /*bitCount=*/1)); // unsigned int (1) chroma_subsampling_x;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->chromaSubsamplingY, /*bitCount=*/1)); // unsigned int (1) chroma_subsampling_y;
    AVIF_CHECK(avifROStreamReadBitsU8(s, &config->chromaSamplePosition, /*bitCount=*/2)); // unsigned int (2) chroma_sample_position;

    // unsigned int (3) reserved = 0;
    // unsigned int (1) initial_presentation_delay_present;
    // if (initial_presentation_delay_present) {
    //   unsigned int (4) initial_presentation_delay_minus_one;
    // } else {
    //   unsigned int (4) reserved = 0;
    // }
    AVIF_CHECK(avifROStreamSkip(s, /*byteCount=*/1));

    // According to section 2.2.1 of AV1 Image File Format specification v1.1.0:
    //   - Sequence Header OBUs should not be present in the AV1CodecConfigurationBox.
    //   - If a Sequence Header OBU is present in the AV1CodecConfigurationBox,
    //     it shall match the Sequence Header OBU in the AV1 Image Item Data.
    //   - Metadata OBUs, if present, shall match the values given in other item properties,
    //     such as the PixelInformationProperty or ColourInformationBox.
    // See https://aomediacodec.github.io/av1-avif/v1.1.0.html#av1-configuration-item-property.
    // For simplicity, the constraints above are not enforced.
    // The following is skipped by avifParseItemPropertyContainerBox().
    // unsigned int (8) configOBUs[];

    AVIF_CHECK(s->offset - av1COffset == 4); // Make sure avifParseCodecConfiguration() reads exactly 4 bytes.
    return AVIF_TRUE;
}

static avifBool avifParseCodecConfigurationBoxProperty(avifProperty * prop,
                                                       const uint8_t * raw,
                                                       size_t rawLen,
                                                       const char * configPropName,
                                                       avifDiagnostics * diag)
{
    char diagContext[10];
    snprintf(diagContext, sizeof(diagContext), "Box[%.4s]", configPropName); // "Box[av1C]" or "Box[av2C]"
    BEGIN_STREAM(s, raw, rawLen, diag, diagContext);
    return avifParseCodecConfiguration(&s, &prop->u.av1C, configPropName, diag);
}

static avifBool avifParsePixelAspectRatioBoxProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[pasp]");

    avifPixelAspectRatioBox * pasp = &prop->u.pasp;
    AVIF_CHECK(avifROStreamReadU32(&s, &pasp->hSpacing)); // unsigned int(32) hSpacing;
    AVIF_CHECK(avifROStreamReadU32(&s, &pasp->vSpacing)); // unsigned int(32) vSpacing;
    return AVIF_TRUE;
}

static avifBool avifParseCleanApertureBoxProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[clap]");

    avifCleanApertureBox * clap = &prop->u.clap;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->widthN));    // unsigned int(32) cleanApertureWidthN;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->widthD));    // unsigned int(32) cleanApertureWidthD;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->heightN));   // unsigned int(32) cleanApertureHeightN;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->heightD));   // unsigned int(32) cleanApertureHeightD;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->horizOffN)); // unsigned int(32) horizOffN;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->horizOffD)); // unsigned int(32) horizOffD;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->vertOffN));  // unsigned int(32) vertOffN;
    AVIF_CHECK(avifROStreamReadU32(&s, &clap->vertOffD));  // unsigned int(32) vertOffD;
    return AVIF_TRUE;
}

static avifBool avifParseImageRotationProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[irot]");

    avifImageRotation * irot = &prop->u.irot;
    uint8_t reserved;
    AVIF_CHECK(avifROStreamReadBitsU8(&s, &reserved, /*bitCount=*/6)); // unsigned int (6) reserved = 0;
    if (reserved) {
        avifDiagnosticsPrintf(diag, "Box[irot] contains nonzero reserved bits [%u]", reserved);
        return AVIF_FALSE;
    }
    AVIF_CHECK(avifROStreamReadBitsU8(&s, &irot->angle, /*bitCount=*/2)); // unsigned int (2) angle;
    return AVIF_TRUE;
}

static avifBool avifParseImageMirrorProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[imir]");

    avifImageMirror * imir = &prop->u.imir;
    uint8_t reserved;
    AVIF_CHECK(avifROStreamReadBitsU8(&s, &reserved, /*bitCount=*/7)); // unsigned int(7) reserved = 0;
    if (reserved) {
        avifDiagnosticsPrintf(diag, "Box[imir] contains nonzero reserved bits [%u]", reserved);
        return AVIF_FALSE;
    }
    AVIF_CHECK(avifROStreamReadBitsU8(&s, &imir->axis, /*bitCount=*/1)); // unsigned int(1) axis;
    return AVIF_TRUE;
}

static avifResult avifParsePixelInformationProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[pixi]");
    uint32_t flags = 0; // px_flags
    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, &flags), AVIF_RESULT_BMFF_PARSE_FAILED);

    avifPixelInformationProperty * pixi = &prop->u.pixi;
    AVIF_CHECKERR(avifROStreamRead(&s, &pixi->planeCount, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int (8) num_channels;
    if (pixi->planeCount < 1 || pixi->planeCount > MAX_PIXI_PLANE_DEPTHS) {
        avifDiagnosticsPrintf(diag, "Box[pixi] contains unsupported plane count [%u]", pixi->planeCount);
        return AVIF_RESULT_NOT_IMPLEMENTED;
    }
    for (uint8_t i = 0; i < pixi->planeCount; ++i) {
        AVIF_CHECKERR(avifROStreamRead(&s, &pixi->planeDepths[i], 1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int (8) bits_per_channel;
#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
        if (pixi->planeDepths[i] == 0) {
            avifDiagnosticsPrintf(diag, "Box[pixi] plane depth shall not be 0 for channel %u", i);
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI
        if (pixi->planeDepths[i] != pixi->planeDepths[0]) {
            avifDiagnosticsPrintf(diag,
                                  "Box[pixi] contains unsupported mismatched plane depths [%u != %u]",
                                  pixi->planeDepths[i],
                                  pixi->planeDepths[0]);
            return AVIF_RESULT_NOT_IMPLEMENTED;
        }
    }
#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
    if (flags & 1) {
        for (uint8_t i = 0; i < pixi->planeCount; ++i) {
            uint8_t channelIdc, reserved, componentFormat, channelLabelFlag;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &channelIdc, /*bitCount=*/3), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(3) channel_idc;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &reserved, /*bitCount=*/1), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) reserved = 0;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &componentFormat, /*bitCount=*/2), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(2) component_format;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &pixi->subsamplingFlag[i], /*bitCount=*/1),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) subsampling_flag;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &channelLabelFlag, /*bitCount=*/1),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) channel_label_flag;
            if (pixi->subsamplingFlag[i]) {
                AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &pixi->subsamplingType[i], /*bitCount=*/4),
                              AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) subsampling_type;
                AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &pixi->subsamplingLocation[i], /*bitCount=*/4),
                              AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(4) subsampling_location;
            }

            // ISO/IEC 23008-12:2024/CDAM 2:2025 section 6.5.6.3:
            //   This field indicates the contents of the channel. A value of 0 indicates colour/grayscale. A value of
            //   1 indicates alpha. A value of 2 indicates depth. Values 3-7 are reserved for future use. At most one
            //   channel shall have a channel_idc of 1.
            if (channelIdc != 0) {
                avifDiagnosticsPrintf(diag, "Box[pixi] contains unsupported channel_idc %u for channel %u", channelIdc, i);
                return AVIF_RESULT_NOT_IMPLEMENTED;
            }
            if (reserved != 0) {
                avifDiagnosticsPrintf(diag, "Box[pixi] contains non-zero reserved field %u for channel %u", reserved, i);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            // ISO/IEC 23008-12:2024/CDAM 2:2025 section 6.5.6.3:
            //   component_format: This field indicates the data type of the channel as defined by the component_format
            //   values in ISO/IEC 23001-17 where component_bit_depth is considered to be equal to bits_per_channel.
            // ISO/IEC 23001-17 section 5.2.1.2:
            //   component_format: When equal to 0, component value is an unsigned integer coded on component_bit_depth bits.
            if (componentFormat != 0) {
                avifDiagnosticsPrintf(diag, "Box[pixi] contains unsupported component_format %u for channel %u", componentFormat, i);
                return AVIF_RESULT_NOT_IMPLEMENTED;
            }
            if (pixi->subsamplingFlag[i]) {
                if (pixi->subsamplingType[i] >= AVIF_PIXI_SUBSAMPLING_RESERVED) {
                    avifDiagnosticsPrintf(diag,
                                          "Box[pixi] contains reserved subsampling_type %u for channel %u",
                                          pixi->subsamplingType[i],
                                          i);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                if (pixi->subsamplingLocation[i] > 4) {
                    avifDiagnosticsPrintf(diag,
                                          "Box[pixi] contains reserved subsampling_location %u for channel %u",
                                          pixi->subsamplingLocation[i],
                                          i);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
            }
            if (channelLabelFlag) {
                AVIF_CHECKERR(avifROStreamReadString(&s, NULL, 0), AVIF_RESULT_BMFF_PARSE_FAILED); // utf8string channel_label; (skipped)
            }
        }
    }
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI
    return AVIF_RESULT_OK;
}

static avifBool avifParseOperatingPointSelectorProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[a1op]");

    avifOperatingPointSelectorProperty * a1op = &prop->u.a1op;
    AVIF_CHECK(avifROStreamRead(&s, &a1op->opIndex, 1));
    if (a1op->opIndex > 31) { // 31 is AV1's max operating point value
        avifDiagnosticsPrintf(diag, "Box[a1op] contains an unsupported operating point [%u]", a1op->opIndex);
        return AVIF_FALSE;
    }
    return AVIF_TRUE;
}

static avifBool avifParseLayerSelectorProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[lsel]");

    avifLayerSelectorProperty * lsel = &prop->u.lsel;
    AVIF_CHECK(avifROStreamReadU16(&s, &lsel->layerID));
    if ((lsel->layerID != 0xFFFF) && (lsel->layerID >= AVIF_MAX_AV1_LAYER_COUNT)) {
        avifDiagnosticsPrintf(diag, "Box[lsel] contains an unsupported layer [%u]", lsel->layerID);
        return AVIF_FALSE;
    }
    return AVIF_TRUE;
}

static avifBool avifParseAV1LayeredImageIndexingProperty(avifProperty * prop, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[a1lx]");

    avifAV1LayeredImageIndexingProperty * a1lx = &prop->u.a1lx;

    uint8_t largeSize = 0;
    AVIF_CHECK(avifROStreamRead(&s, &largeSize, 1));
    if (largeSize & 0xFE) {
        avifDiagnosticsPrintf(diag, "Box[a1lx] has bits set in the reserved section [%u]", largeSize);
        return AVIF_FALSE;
    }

    for (int i = 0; i < 3; ++i) {
        if (largeSize) {
            AVIF_CHECK(avifROStreamReadU32(&s, &a1lx->layerSize[i]));
        } else {
            uint16_t layerSize16;
            AVIF_CHECK(avifROStreamReadU16(&s, &layerSize16));
            a1lx->layerSize[i] = (uint32_t)layerSize16;
        }
    }

    // Layer sizes will be validated later (when the item's size is known)
    return AVIF_TRUE;
}

static avifResult avifParseItemPropertyContainerBox(avifPropertyArray * properties,
                                                    uint64_t rawOffset,
                                                    const uint8_t * raw,
                                                    size_t rawLen,
                                                    avifBool isTrack,
                                                    avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[ipco]");

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        avifProperty * prop = (avifProperty *)avifArrayPush(properties);
        AVIF_CHECKERR(prop != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        memcpy(prop->type, header.type, 4);
        prop->isOpaque = AVIF_FALSE;
        if (!memcmp(header.type, "ispe", 4)) {
            AVIF_CHECKERR(avifParseImageSpatialExtentsProperty(prop, avifROStreamCurrent(&s), header.size, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if ((!memcmp(header.type, "auxC", 4) && !isTrack) || (!memcmp(header.type, "auxi", 4) && isTrack)) {
            AVIF_CHECKERR(avifParseAuxiliaryTypeProperty(prop, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "colr", 4)) {
            AVIF_CHECKERR(avifParseColourInformationBox(prop, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "av1C", 4)) {
            AVIF_CHECKERR(avifParseCodecConfigurationBoxProperty(prop, avifROStreamCurrent(&s), header.size, "av1C", diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
#if defined(AVIF_CODEC_AVM)
        } else if (!memcmp(header.type, "av2C", 4)) {
            AVIF_CHECKERR(avifParseCodecConfigurationBoxProperty(prop, avifROStreamCurrent(&s), header.size, "av2C", diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
#endif
        } else if (!memcmp(header.type, "pasp", 4)) {
            AVIF_CHECKERR(avifParsePixelAspectRatioBoxProperty(prop, avifROStreamCurrent(&s), header.size, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "clap", 4)) {
            AVIF_CHECKERR(avifParseCleanApertureBoxProperty(prop, avifROStreamCurrent(&s), header.size, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "irot", 4)) {
            AVIF_CHECKERR(avifParseImageRotationProperty(prop, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "imir", 4)) {
            AVIF_CHECKERR(avifParseImageMirrorProperty(prop, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "pixi", 4)) {
            AVIF_CHECKRES(avifParsePixelInformationProperty(prop, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "a1op", 4)) {
            AVIF_CHECKERR(avifParseOperatingPointSelectorProperty(prop, avifROStreamCurrent(&s), header.size, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "lsel", 4)) {
            AVIF_CHECKERR(avifParseLayerSelectorProperty(prop, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "a1lx", 4)) {
            AVIF_CHECKERR(avifParseAV1LayeredImageIndexingProperty(prop, avifROStreamCurrent(&s), header.size, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "clli", 4)) {
            AVIF_CHECKRES(avifParseContentLightLevelInformationBox(prop, avifROStreamCurrent(&s), header.size, diag));
        } else {
            prop->isOpaque = AVIF_TRUE;
            memset(&prop->u.opaque, 0, sizeof(prop->u.opaque));
            memcpy(prop->u.opaque.usertype, header.usertype, sizeof(prop->u.opaque.usertype));
            AVIF_CHECKRES(avifRWDataSet(&prop->u.opaque.boxPayload, avifROStreamCurrent(&s), header.size));
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseItemPropertyAssociation(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag, uint32_t * outVersionAndFlags)
{
    // NOTE: If this function ever adds support for versions other than [0,1] or flags other than
    //       [0,1], please increase the value of MAX_IPMA_VERSION_AND_FLAGS_SEEN accordingly.

    BEGIN_STREAM(s, raw, rawLen, diag, "Box[ipma]");

    uint8_t version;
    uint32_t flags;
    AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, &version, &flags), AVIF_RESULT_BMFF_PARSE_FAILED);
    avifBool propertyIndexIsU15 = ((flags & 0x1) != 0);
    *outVersionAndFlags = ((uint32_t)version << 24) | flags;

    uint32_t entryCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED);
    unsigned int prevItemID = 0;
    for (uint32_t entryIndex = 0; entryIndex < entryCount; ++entryIndex) {
        // ISO/IEC 14496-12, Seventh edition, 2022-01, Section 8.11.14.1:
        //   Each ItemPropertyAssociationBox shall be ordered by increasing item_ID, and there shall
        //   be at most one occurrence of a given item_ID, in the set of ItemPropertyAssociationBox
        //   boxes.
        unsigned int itemID;
        if (version < 1) {
            uint16_t tmp;
            AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp), AVIF_RESULT_BMFF_PARSE_FAILED);
            itemID = tmp;
        } else {
            AVIF_CHECKERR(avifROStreamReadU32(&s, &itemID), AVIF_RESULT_BMFF_PARSE_FAILED);
        }
        AVIF_CHECKRES(avifCheckItemID("ipma", itemID, diag));
        if (itemID <= prevItemID) {
            avifDiagnosticsPrintf(diag, "Box[ipma] item IDs are not ordered by increasing ID");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
        prevItemID = itemID;

        avifDecoderItem * item;
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, itemID, &item));
        if (item->ipmaSeen) {
            avifDiagnosticsPrintf(diag, "Duplicate Box[ipma] for item ID [%u]", itemID);
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
        item->ipmaSeen = AVIF_TRUE;

        uint8_t associationCount;
        AVIF_CHECKERR(avifROStreamRead(&s, &associationCount, 1), AVIF_RESULT_BMFF_PARSE_FAILED);
        for (uint8_t associationIndex = 0; associationIndex < associationCount; ++associationIndex) {
            uint8_t essential;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &essential, /*bitCount=*/1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) essential;
            uint32_t propertyIndex;
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &propertyIndex, /*bitCount=*/propertyIndexIsU15 ? 15 : 7),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(7/15) property_index;

            // ISO/IEC 14496-12 Section 8.11.14.3:
            //   0 indicating that no property is associated (the essential indicator shall also be 0)
            if (propertyIndex == 0) {
                if (essential) {
                    avifDiagnosticsPrintf(diag, "Box[ipma] for item ID [%u] contains an illegal essential property index 0", itemID);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                continue;
            }
            --propertyIndex; // 1-indexed

            if (propertyIndex >= meta->properties.count) {
                avifDiagnosticsPrintf(diag,
                                      "Box[ipma] for item ID [%u] contains an illegal property index [%u] (out of [%u] properties)",
                                      itemID,
                                      propertyIndex,
                                      meta->properties.count);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            // Copy property to item
            const avifProperty * srcProp = &meta->properties.prop[propertyIndex];

            // Some properties are supported and parsed by libavif.
            // Other properties are forwarded to the user as opaque blobs.
            const avifBool supportedType = !srcProp->isOpaque;
            if (supportedType) {
                if (essential) {
                    // Verify that it is legal for this property to be flagged as essential. Any
                    // types in this list are *required* in the spec to not be flagged as essential
                    // when associated with an item.
                    static const char * const nonessentialTypes[] = {

                        // AVIF: Section 2.3.2.3.2: "If associated, it shall not be marked as essential."
                        "a1lx"

                    };
                    size_t nonessentialTypesCount = sizeof(nonessentialTypes) / sizeof(nonessentialTypes[0]);
                    for (size_t i = 0; i < nonessentialTypesCount; ++i) {
                        if (!memcmp(srcProp->type, nonessentialTypes[i], 4)) {
                            avifDiagnosticsPrintf(diag,
                                                  "Item ID [%u] has a %s property association which must not be marked essential, but is",
                                                  itemID,
                                                  nonessentialTypes[i]);
                            return AVIF_RESULT_BMFF_PARSE_FAILED;
                        }
                    }
                } else {
                    // Verify that it is legal for this property to not be flagged as essential. Any
                    // types in this list are *required* in the spec to be flagged as essential when
                    // associated with an item.
                    static const char * const essentialTypes[] = {

                        // AVIF: Section 2.3.2.1.1: "If associated, it shall be marked as essential."
                        "a1op",

                        // HEIF: Section 6.5.11.1: "essential shall be equal to 1 for an 'lsel' item property."
                        "lsel",

                        // MIAF 2019/Amd. 2:2021: Section 7.3.9:
                        //   All transformative properties associated with coded and derived images shall be
                        //   marked as essential
                        // It makes no sense to allow for non-essential crop/orientation associated with an item
                        // that is not a coded or derived image, so for simplicity 'item' is not checked here.
                        "clap",
                        "irot",
                        "imir"

                    };
                    size_t essentialTypesCount = sizeof(essentialTypes) / sizeof(essentialTypes[0]);
                    for (size_t i = 0; i < essentialTypesCount; ++i) {
                        if (!memcmp(srcProp->type, essentialTypes[i], 4)) {
                            avifDiagnosticsPrintf(diag,
                                                  "Item ID [%u] has a %s property association which must be marked essential, but is not",
                                                  itemID,
                                                  essentialTypes[i]);
                            return AVIF_RESULT_BMFF_PARSE_FAILED;
                        }
                    }
                }

                // Supported and valid; associate it with this item.
                avifProperty * dstProp = (avifProperty *)avifArrayPush(&item->properties);
                AVIF_CHECKERR(dstProp != NULL, AVIF_RESULT_OUT_OF_MEMORY);
                *dstProp = *srcProp;
            } else {
                if (essential) {
                    // ISO/IEC 23008-12 Section 10.2.1:
                    //   Under any brand, the primary item (or an alternative if alternative support is required)
                    //   shall be processable by a reader implementing only the required features of that brand.
                    //   Specifically, given that each brand has a set of properties that a reader is required to
                    //   support: the item shall not have properties that are marked as essential and are outside
                    //   this set.
                    // It is assumed that this rule also applies to items the primary item depends on (such as
                    // the cells of a grid).

                    // Discovered an essential item property that libavif doesn't support!
                    // Make a note to ignore this item later.
                    item->hasUnsupportedEssentialProperty = AVIF_TRUE;
                }

                // Will be forwarded to the user through avifImage::properties.
                avifProperty * dstProp = (avifProperty *)avifArrayPush(&item->properties);
                AVIF_CHECKERR(dstProp != NULL, AVIF_RESULT_OUT_OF_MEMORY);
                dstProp->isOpaque = AVIF_TRUE;
                memcpy(dstProp->type, srcProp->type, sizeof(dstProp->type));
                memcpy(dstProp->u.opaque.usertype, srcProp->u.opaque.usertype, sizeof(dstProp->u.opaque.usertype));
                AVIF_CHECKRES(
                    avifRWDataSet(&dstProp->u.opaque.boxPayload, srcProp->u.opaque.boxPayload.data, srcProp->u.opaque.boxPayload.size));
            }
        }
    }
    return AVIF_RESULT_OK;
}

static avifBool avifParsePrimaryItemBox(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    if (meta->primaryItemID > 0) {
        // Illegal to have multiple pitm boxes, bail out
        avifDiagnosticsPrintf(diag, "Multiple boxes of unique Box[pitm] found");
        return AVIF_FALSE;
    }

    BEGIN_STREAM(s, raw, rawLen, diag, "Box[pitm]");

    uint8_t version;
    AVIF_CHECK(avifROStreamReadVersionAndFlags(&s, &version, NULL));

    if (version == 0) {
        uint16_t tmp16;
        AVIF_CHECK(avifROStreamReadU16(&s, &tmp16)); // unsigned int(16) item_ID;
        meta->primaryItemID = tmp16;
    } else {
        AVIF_CHECK(avifROStreamReadU32(&s, &meta->primaryItemID)); // unsigned int(32) item_ID;
    }
    return AVIF_TRUE;
}

static avifBool avifParseItemDataBox(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    // Check to see if we've already seen an idat box for this meta box. If so, bail out
    if (meta->idat.size > 0) {
        avifDiagnosticsPrintf(diag, "Meta box contains multiple idat boxes");
        return AVIF_FALSE;
    }
    if (rawLen == 0) {
        avifDiagnosticsPrintf(diag, "idat box has a length of 0");
        return AVIF_FALSE;
    }

    if (avifRWDataSet(&meta->idat, raw, rawLen) != AVIF_RESULT_OK) {
        return AVIF_FALSE;
    }
    return AVIF_TRUE;
}

static avifResult avifParseItemPropertiesBox(avifMeta * meta, uint64_t rawOffset, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[iprp]");

    avifBoxHeader ipcoHeader;
    AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &ipcoHeader), AVIF_RESULT_BMFF_PARSE_FAILED);
    if (memcmp(ipcoHeader.type, "ipco", 4)) {
        avifDiagnosticsPrintf(diag, "Failed to find Box[ipco] as the first box in Box[iprp]");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    // Read all item properties inside of ItemPropertyContainerBox
    AVIF_CHECKRES(avifParseItemPropertyContainerBox(&meta->properties,
                                                    rawOffset + avifROStreamOffset(&s),
                                                    avifROStreamCurrent(&s),
                                                    ipcoHeader.size,
                                                    /*isTrack=*/AVIF_FALSE,
                                                    diag));
    AVIF_CHECKERR(avifROStreamSkip(&s, ipcoHeader.size), AVIF_RESULT_BMFF_PARSE_FAILED);

    uint32_t versionAndFlagsSeen[MAX_IPMA_VERSION_AND_FLAGS_SEEN];
    uint32_t versionAndFlagsSeenCount = 0;

    // Now read all ItemPropertyAssociation until the end of the box, and make associations
    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader ipmaHeader;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &ipmaHeader), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(ipmaHeader.type, "ipma", 4)) {
            uint32_t versionAndFlags;
            AVIF_CHECKRES(avifParseItemPropertyAssociation(meta, avifROStreamCurrent(&s), ipmaHeader.size, diag, &versionAndFlags));
            for (uint32_t i = 0; i < versionAndFlagsSeenCount; ++i) {
                if (versionAndFlagsSeen[i] == versionAndFlags) {
                    // BMFF (ISO/IEC 14496-12:2022) 8.11.14.1 - There shall be at most one
                    // ItemPropertyAssociationBox with a given pair of values of version and
                    // flags.
                    avifDiagnosticsPrintf(diag, "Multiple Box[ipma] with a given pair of values of version and flags. See BMFF (ISO/IEC 14496-12:2022) 8.11.14.1");
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
            }
            if (versionAndFlagsSeenCount == MAX_IPMA_VERSION_AND_FLAGS_SEEN) {
                avifDiagnosticsPrintf(diag, "Exceeded possible count of unique ipma version and flags tuples");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            versionAndFlagsSeen[versionAndFlagsSeenCount] = versionAndFlags;
            ++versionAndFlagsSeenCount;
        } else {
            // These must all be type ipma
            avifDiagnosticsPrintf(diag, "Box[iprp] contains a box that isn't type 'ipma'");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, ipmaHeader.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseItemInfoEntry(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    // Section 8.11.6.2 of ISO/IEC 14496-12.
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[infe]");

    uint8_t version;
    uint32_t flags;
    AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, &version, &flags), AVIF_RESULT_BMFF_PARSE_FAILED);
    // Version 2+ is required for item_type
    if (version != 2 && version != 3) {
        avifDiagnosticsPrintf(s.diag, "%s: Expecting box version 2 or 3, got version %u", s.diagContext, version);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    // TODO: check flags. ISO/IEC 23008-12:2017, Section 9.2 says:
    //   The flags field of ItemInfoEntry with version greater than or equal to 2 is specified as
    //   follows:
    //
    //   (flags & 1) equal to 1 indicates that the item is not intended to be a part of the
    //   presentation. For example, when (flags & 1) is equal to 1 for an image item, the image
    //   item should not be displayed.
    //   (flags & 1) equal to 0 indicates that the item is intended to be a part of the
    //   presentation.
    //
    // See also Section 6.4.2.

    uint32_t itemID;
    if (version == 2) {
        uint16_t tmp;
        AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) item_ID;
        itemID = tmp;
    } else {
        AVIF_ASSERT_OR_RETURN(version == 3);
        AVIF_CHECKERR(avifROStreamReadU32(&s, &itemID), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) item_ID;
    }
    AVIF_CHECKRES(avifCheckItemID("infe", itemID, diag));
    uint16_t itemProtectionIndex;
    AVIF_CHECKERR(avifROStreamReadU16(&s, &itemProtectionIndex), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) item_protection_index;
    uint8_t itemType[4];
    AVIF_CHECKERR(avifROStreamRead(&s, itemType, 4), AVIF_RESULT_BMFF_PARSE_FAILED);   // unsigned int(32) item_type;
    AVIF_CHECKERR(avifROStreamReadString(&s, NULL, 0), AVIF_RESULT_BMFF_PARSE_FAILED); // utf8string item_name; (skipped)
    avifContentType contentType;
    if (!memcmp(itemType, "mime", 4)) {
        AVIF_CHECKERR(avifROStreamReadString(&s, contentType.contentType, CONTENTTYPE_SIZE), AVIF_RESULT_BMFF_PARSE_FAILED); // utf8string content_type;
        // utf8string content_encoding; //optional
    } else {
        // if (item_type == 'uri ') {
        //  utf8string item_uri_type;
        // }
        memset(&contentType, 0, sizeof(contentType));
    }

    avifDecoderItem * item;
    AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, itemID, &item));

    memcpy(item->type, itemType, sizeof(itemType));
    item->contentType = contentType;
    return AVIF_RESULT_OK;
}

static avifResult avifParseItemInfoBox(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[iinf]");

    uint8_t version;
    AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, &version, NULL), AVIF_RESULT_BMFF_PARSE_FAILED);
    uint32_t entryCount;
    if (version == 0) {
        uint16_t tmp;
        AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) entry_count;
        entryCount = tmp;
    } else if (version == 1) {
        AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_count;
    } else {
        avifDiagnosticsPrintf(diag, "Box[iinf] has an unsupported version %u", version);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    for (uint32_t entryIndex = 0; entryIndex < entryCount; ++entryIndex) {
        avifBoxHeader infeHeader;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &infeHeader), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(infeHeader.type, "infe", 4)) {
            AVIF_CHECKRES(avifParseItemInfoEntry(meta, avifROStreamCurrent(&s), infeHeader.size, diag));
        } else {
            // These must all be type infe
            avifDiagnosticsPrintf(diag, "Box[iinf] contains a box that isn't type 'infe'");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, infeHeader.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }

    return AVIF_RESULT_OK;
}

static avifResult avifParseItemReferenceBox(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[iref]");

    uint8_t version;
    AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, &version, NULL), AVIF_RESULT_BMFF_PARSE_FAILED);
    if (version > 1) {
        // iref versions > 1 are not supported. Skip it.
        return AVIF_RESULT_OK;
    }

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader irefHeader;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &irefHeader), AVIF_RESULT_BMFF_PARSE_FAILED);

        uint32_t fromID = 0;
        if (version == 0) {
            uint16_t tmp;
            AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) from_item_ID;
            fromID = tmp;
        } else {
            // version == 1
            AVIF_CHECKERR(avifROStreamReadU32(&s, &fromID), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) from_item_ID;
        }
        // ISO 14496-12 section 8.11.12.1: "index values start at 1"
        AVIF_CHECKRES(avifCheckItemID("iref", fromID, diag));

        avifDecoderItem * item;
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, fromID, &item));
        if (!memcmp(irefHeader.type, "dimg", 4)) {
            if (item->hasDimgFrom) {
                // ISO/IEC 23008-12 (HEIF) 6.6.1: The number of SingleItemTypeReferenceBoxes with the box type 'dimg'
                // and with the same value of from_item_ID shall not be greater than 1.
                avifDiagnosticsPrintf(diag, "Box[iinf] contains duplicate boxes of type 'dimg' with the same from_item_ID value %u", fromID);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            item->hasDimgFrom = AVIF_TRUE;
        }

        uint16_t referenceCount = 0;
        AVIF_CHECKERR(avifROStreamReadU16(&s, &referenceCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) reference_count;

        for (uint16_t refIndex = 0; refIndex < referenceCount; ++refIndex) {
            uint32_t toID = 0;
            if (version == 0) {
                uint16_t tmp;
                AVIF_CHECKERR(avifROStreamReadU16(&s, &tmp), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(16) to_item_ID;
                toID = tmp;
            } else {
                // version == 1
                AVIF_CHECKERR(avifROStreamReadU32(&s, &toID), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) to_item_ID;
            }
            AVIF_CHECKRES(avifCheckItemID("iref", toID, diag));

            // Read this reference as "{fromID} is a {irefType} for {toID}"
            if (!memcmp(irefHeader.type, "thmb", 4)) {
                item->thumbnailForID = toID;
            } else if (!memcmp(irefHeader.type, "auxl", 4)) {
                item->auxForID = toID;
            } else if (!memcmp(irefHeader.type, "cdsc", 4)) {
                item->descForID = toID;
            } else if (!memcmp(irefHeader.type, "dimg", 4)) {
                // derived images refer in the opposite direction
                avifDecoderItem * dimg;
                AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, toID, &dimg));

                // Section 8.11.12.1 of ISO/IEC 14496-12:
                //   The items linked to are then represented by an array of to_item_IDs;
                //   within a given array, a given value shall occur at most once.
                AVIF_CHECKERR(dimg->dimgForID != fromID, AVIF_RESULT_INVALID_IMAGE_GRID);
                // A given value may occur within multiple arrays but this is not supported by libavif.
                AVIF_CHECKERR(dimg->dimgForID == 0, AVIF_RESULT_NOT_IMPLEMENTED);
                dimg->dimgForID = fromID;
                dimg->dimgIdx = refIndex;
            } else if (!memcmp(irefHeader.type, "prem", 4)) {
                item->premByID = toID;
            }
        }
    }

    return AVIF_RESULT_OK;
}

static avifResult avifParseGroupsListBox(avifMeta * meta, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[grpl]");

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader groupHeader;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &groupHeader), AVIF_RESULT_BMFF_PARSE_FAILED);
        // We don't check the flag or version as they depend on the grouping type (and for simplicity).
        // ISO/IEC 14496-12:2024 Section 8.15.3.2
        //   version shall be 0 unless defined otherwise for the grouping_type. Any values of flags such that
        //   (flags & 0x000FFF) is not equal to 0 are reserved. The values of flags shall be such that (flags
        //   & 0xFFF000) is equal to 0 unless defined otherwise for the grouping_type.
        AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, NULL, NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

        avifEntityToGroup * group = avifArrayPush(&meta->entityToGroups);
        AVIF_CHECKERR(group != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifArrayCreate(&group->entityIDs, sizeof(uint32_t), 2), AVIF_RESULT_OUT_OF_MEMORY);

        memcpy(group->groupingType, groupHeader.type, 4);
        AVIF_CHECKERR(avifROStreamReadU32(&s, &group->groupID), AVIF_RESULT_BMFF_PARSE_FAILED);
        uint32_t numEntitiesInGroup;
        AVIF_CHECKERR(avifROStreamReadU32(&s, &numEntitiesInGroup), AVIF_RESULT_BMFF_PARSE_FAILED);
        for (uint32_t i = 0; i < numEntitiesInGroup; ++i) {
            uint32_t * entityId = avifArrayPush(&group->entityIDs);
            AVIF_CHECKERR(entityId != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            AVIF_CHECKERR(avifROStreamReadU32(&s, entityId), AVIF_RESULT_BMFF_PARSE_FAILED);
        }
    }

    return AVIF_RESULT_OK;
}

static avifResult avifParseMetaBox(avifMeta * meta, uint64_t rawOffset, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[meta]");

    uint32_t flags;
    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, 0, &flags), AVIF_RESULT_BMFF_PARSE_FAILED);

    ++meta->idatID; // for tracking idat

    avifBool firstBox = AVIF_TRUE;
    uint32_t uniqueBoxFlags = 0;
    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (firstBox) {
            if (!memcmp(header.type, "hdlr", 4)) {
                uint8_t handlerType[4];
                AVIF_CHECKERR(avifParseHandlerBox(avifROStreamCurrent(&s), header.size, handlerType, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
                // HEIF (ISO/IEC 23008-12:2022), Section 6.2:
                //   The handler type for the MetaBox shall be 'pict'.
                if (memcmp(handlerType, "pict", 4) != 0) {
                    avifDiagnosticsPrintf(diag, "Box[hdlr] handler_type is not 'pict'");
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                firstBox = AVIF_FALSE;
            } else {
                // hdlr must be the first box!
                avifDiagnosticsPrintf(diag, "Box[meta] does not have a Box[hdlr] as its first child box");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        } else if (!memcmp(header.type, "hdlr", 4)) {
            avifDiagnosticsPrintf(diag, "Box[meta] contains a duplicate unique box of type 'hdlr'");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        } else if (!memcmp(header.type, "iloc", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_ILOC, "meta", "iloc", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKRES(avifParseItemLocationBox(meta, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "pitm", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_PITM, "meta", "pitm", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(avifParsePrimaryItemBox(meta, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "idat", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_IDAT, "meta", "idat", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(avifParseItemDataBox(meta, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "iprp", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_IPRP, "meta", "iprp", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKRES(avifParseItemPropertiesBox(meta, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "iinf", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_IINF, "meta", "iinf", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKRES(avifParseItemInfoBox(meta, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "iref", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_IREF, "meta", "iref", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKRES(avifParseItemReferenceBox(meta, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "grpl", 4)) {
            AVIF_CHECKERR(uniqueBoxSeen(&uniqueBoxFlags, AVIF_UNIQUE_GRPL, "meta", "grpl", diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKRES(avifParseGroupsListBox(meta, avifROStreamCurrent(&s), header.size, diag));
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    if (firstBox) {
        // The meta box must not be empty (it must contain at least a hdlr box)
        avifDiagnosticsPrintf(diag, "Box[meta] has no child boxes");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    return AVIF_RESULT_OK;
}

static avifBool avifParseTrackHeaderBox(avifTrack * track, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[tkhd]");

    uint8_t version;
    AVIF_CHECK(avifROStreamReadVersionAndFlags(&s, &version, NULL));

    uint32_t ignored32, trackID;
    uint64_t ignored64;
    if (version == 1) {
        AVIF_CHECK(avifROStreamReadU64(&s, &ignored64));            // unsigned int(64) creation_time;
        AVIF_CHECK(avifROStreamReadU64(&s, &ignored64));            // unsigned int(64) modification_time;
        AVIF_CHECK(avifROStreamReadU32(&s, &trackID));              // unsigned int(32) track_ID;
        AVIF_CHECK(avifROStreamReadU32(&s, &ignored32));            // const unsigned int(32) reserved = 0;
        AVIF_CHECK(avifROStreamReadU64(&s, &track->trackDuration)); // unsigned int(64) duration;
    } else if (version == 0) {
        uint32_t trackDuration;
        AVIF_CHECK(avifROStreamReadU32(&s, &ignored32));     // unsigned int(32) creation_time;
        AVIF_CHECK(avifROStreamReadU32(&s, &ignored32));     // unsigned int(32) modification_time;
        AVIF_CHECK(avifROStreamReadU32(&s, &trackID));       // unsigned int(32) track_ID;
        AVIF_CHECK(avifROStreamReadU32(&s, &ignored32));     // const unsigned int(32) reserved = 0;
        AVIF_CHECK(avifROStreamReadU32(&s, &trackDuration)); // unsigned int(32) duration;
        track->trackDuration = (trackDuration == AVIF_INDEFINITE_DURATION32) ? AVIF_INDEFINITE_DURATION64 : trackDuration;
    } else {
        // Unsupported version
        avifDiagnosticsPrintf(diag, "Box[tkhd] has an unsupported version [%u]", version);
        return AVIF_FALSE;
    }
    track->id = trackID;

    // Skipping the following 52 bytes here:
    // ------------------------------------
    // const unsigned int(32)[2] reserved = 0;
    // template int(16) layer = 0;
    // template int(16) alternate_group = 0;
    // template int(16) volume = {if track_is_audio 0x0100 else 0};
    // const unsigned int(16) reserved = 0;
    // template int(32)[9] matrix= { 0x00010000,0,0,0,0x00010000,0,0,0,0x40000000 }; // unity matrix
    AVIF_CHECK(avifROStreamSkip(&s, 52));

    uint32_t width, height;
    AVIF_CHECK(avifROStreamReadU32(&s, &width));  // unsigned int(32) width;
    AVIF_CHECK(avifROStreamReadU32(&s, &height)); // unsigned int(32) height;
    track->width = width >> 16;
    track->height = height >> 16;

    // TODO: support scaling based on width/height track header info?

    return AVIF_TRUE;
}

static avifBool avifParseMediaHeaderBox(avifTrack * track, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[mdhd]");

    uint8_t version;
    AVIF_CHECK(avifROStreamReadVersionAndFlags(&s, &version, NULL));

    uint32_t ignored32, mediaTimescale, mediaDuration32;
    uint64_t ignored64, mediaDuration64;
    if (version == 1) {
        AVIF_CHECK(avifROStreamReadU64(&s, &ignored64));       // unsigned int(64) creation_time;
        AVIF_CHECK(avifROStreamReadU64(&s, &ignored64));       // unsigned int(64) modification_time;
        AVIF_CHECK(avifROStreamReadU32(&s, &mediaTimescale));  // unsigned int(32) timescale;
        AVIF_CHECK(avifROStreamReadU64(&s, &mediaDuration64)); // unsigned int(64) duration;
        track->mediaDuration = mediaDuration64;
    } else if (version == 0) {
        AVIF_CHECK(avifROStreamReadU32(&s, &ignored32));       // unsigned int(32) creation_time;
        AVIF_CHECK(avifROStreamReadU32(&s, &ignored32));       // unsigned int(32) modification_time;
        AVIF_CHECK(avifROStreamReadU32(&s, &mediaTimescale));  // unsigned int(32) timescale;
        AVIF_CHECK(avifROStreamReadU32(&s, &mediaDuration32)); // unsigned int(32) duration;
        track->mediaDuration = (uint64_t)mediaDuration32;
    } else {
        // Unsupported version
        avifDiagnosticsPrintf(diag, "Box[mdhd] has an unsupported version [%u]", version);
        return AVIF_FALSE;
    }

    track->mediaTimescale = mediaTimescale;
    return AVIF_TRUE;
}

static avifResult avifParseChunkOffsetBox(avifSampleTable * sampleTable, avifBool largeOffsets, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, largeOffsets ? "Box[co64]" : "Box[stco]");

    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

    uint32_t entryCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_count;
    for (uint32_t i = 0; i < entryCount; ++i) {
        uint64_t offset;
        if (largeOffsets) {
            AVIF_CHECKERR(avifROStreamReadU64(&s, &offset), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(64) chunk_offset;
        } else {
            uint32_t offset32;
            AVIF_CHECKERR(avifROStreamReadU32(&s, &offset32), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) chunk_offset;
            offset = (uint64_t)offset32;
        }

        avifSampleTableChunk * chunk = (avifSampleTableChunk *)avifArrayPush(&sampleTable->chunks);
        AVIF_CHECKERR(chunk != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        chunk->offset = offset;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseSampleToChunkBox(avifSampleTable * sampleTable, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[stsc]");

    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

    uint32_t entryCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_count;
    uint32_t prevFirstChunk = 0;
    for (uint32_t i = 0; i < entryCount; ++i) {
        avifSampleTableSampleToChunk * sampleToChunk = (avifSampleTableSampleToChunk *)avifArrayPush(&sampleTable->sampleToChunks);
        AVIF_CHECKERR(sampleToChunk != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifROStreamReadU32(&s, &sampleToChunk->firstChunk), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) first_chunk;
        AVIF_CHECKERR(avifROStreamReadU32(&s, &sampleToChunk->samplesPerChunk), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) samples_per_chunk;
        AVIF_CHECKERR(avifROStreamReadU32(&s, &sampleToChunk->sampleDescriptionIndex),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) sample_description_index;
        // The first_chunk fields should start with 1 and be strictly increasing.
        if (i == 0) {
            if (sampleToChunk->firstChunk != 1) {
                avifDiagnosticsPrintf(diag, "Box[stsc] does not begin with chunk 1 [%u]", sampleToChunk->firstChunk);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        } else {
            if (sampleToChunk->firstChunk <= prevFirstChunk) {
                avifDiagnosticsPrintf(diag, "Box[stsc] chunks are not strictly increasing");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        }
        prevFirstChunk = sampleToChunk->firstChunk;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseSampleSizeBox(avifSampleTable * sampleTable, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[stsz]");

    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

    uint32_t allSamplesSize, sampleCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &allSamplesSize), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) sample_size;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &sampleCount), AVIF_RESULT_BMFF_PARSE_FAILED);    // unsigned int(32) sample_count;

    if (allSamplesSize > 0) {
        sampleTable->allSamplesSize = allSamplesSize;
    } else {
        for (uint32_t i = 0; i < sampleCount; ++i) {
            avifSampleTableSampleSize * sampleSize = (avifSampleTableSampleSize *)avifArrayPush(&sampleTable->sampleSizes);
            AVIF_CHECKERR(sampleSize != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            AVIF_CHECKERR(avifROStreamReadU32(&s, &sampleSize->size), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_size;
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseSyncSampleBox(avifSampleTable * sampleTable, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[stss]");

    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

    uint32_t entryCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_count;

    for (uint32_t i = 0; i < entryCount; ++i) {
        uint32_t sampleNumber = 0;
        AVIF_CHECKERR(avifROStreamReadU32(&s, &sampleNumber), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) sample_number;
        avifSyncSample * syncSample = (avifSyncSample *)avifArrayPush(&sampleTable->syncSamples);
        AVIF_CHECKERR(syncSample != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        syncSample->sampleNumber = sampleNumber;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseTimeToSampleBox(avifSampleTable * sampleTable, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[stts]");

    AVIF_CHECKERR(avifROStreamReadAndEnforceVersion(&s, /*enforcedVersion=*/0, /*flags=*/NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

    uint32_t entryCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_count;

    for (uint32_t i = 0; i < entryCount; ++i) {
        avifSampleTableTimeToSample * timeToSample = (avifSampleTableTimeToSample *)avifArrayPush(&sampleTable->timeToSamples);
        AVIF_CHECKERR(timeToSample != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifROStreamReadU32(&s, &timeToSample->sampleCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) sample_count;
        AVIF_CHECKERR(avifROStreamReadU32(&s, &timeToSample->sampleDelta), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) sample_delta;
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseSampleDescriptionBox(avifSampleTable * sampleTable,
                                                uint64_t rawOffset,
                                                const uint8_t * raw,
                                                size_t rawLen,
                                                avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[stsd]");

    uint8_t version;
    AVIF_CHECKERR(avifROStreamReadVersionAndFlags(&s, &version, NULL), AVIF_RESULT_BMFF_PARSE_FAILED);

    // Section 8.5.2.3 of ISO/IEC 14496-12:
    //   version is set to zero. A version number of 1 shall be treated as a version of 0.
    if (version != 0 && version != 1) {
        avifDiagnosticsPrintf(diag, "Box[stsd]: Expecting box version 0 or 1, got version %u", version);
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    uint32_t entryCount;
    AVIF_CHECKERR(avifROStreamReadU32(&s, &entryCount), AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(32) entry_count;

    for (uint32_t i = 0; i < entryCount; ++i) {
        avifBoxHeader sampleEntryHeader;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &sampleEntryHeader), AVIF_RESULT_BMFF_PARSE_FAILED);

        avifSampleDescription * description = (avifSampleDescription *)avifArrayPush(&sampleTable->sampleDescriptions);
        AVIF_CHECKERR(description != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        if (!avifArrayCreate(&description->properties, sizeof(avifProperty), 16)) {
            avifArrayPop(&sampleTable->sampleDescriptions);
            return AVIF_RESULT_OUT_OF_MEMORY;
        }
        memcpy(description->format, sampleEntryHeader.type, sizeof(description->format));
        const size_t sampleEntryBytes = sampleEntryHeader.size;
        if (avifGetCodecType(description->format) != AVIF_CODEC_TYPE_UNKNOWN) {
            if (sampleEntryBytes < VISUALSAMPLEENTRY_SIZE) {
                avifDiagnosticsPrintf(diag, "Not enough bytes to parse VisualSampleEntry");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            AVIF_CHECKRES(avifParseItemPropertyContainerBox(&description->properties,
                                                            rawOffset + avifROStreamOffset(&s) + VISUALSAMPLEENTRY_SIZE,
                                                            avifROStreamCurrent(&s) + VISUALSAMPLEENTRY_SIZE,
                                                            sampleEntryBytes - VISUALSAMPLEENTRY_SIZE,
                                                            /*isTrack=*/AVIF_TRUE,
                                                            diag));
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, sampleEntryBytes), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseSampleTableBox(avifTrack * track, uint64_t rawOffset, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    if (track->sampleTable) {
        // A TrackBox may only have one SampleTable
        avifDiagnosticsPrintf(diag, "Duplicate Box[stbl] for a single track detected");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    track->sampleTable = avifSampleTableCreate();
    AVIF_CHECKERR(track->sampleTable != NULL, AVIF_RESULT_OUT_OF_MEMORY);

    BEGIN_STREAM(s, raw, rawLen, diag, "Box[stbl]");

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(header.type, "stco", 4)) {
            AVIF_CHECKRES(avifParseChunkOffsetBox(track->sampleTable, AVIF_FALSE, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "co64", 4)) {
            AVIF_CHECKRES(avifParseChunkOffsetBox(track->sampleTable, AVIF_TRUE, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "stsc", 4)) {
            AVIF_CHECKRES(avifParseSampleToChunkBox(track->sampleTable, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "stsz", 4)) {
            AVIF_CHECKRES(avifParseSampleSizeBox(track->sampleTable, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "stss", 4)) {
            AVIF_CHECKRES(avifParseSyncSampleBox(track->sampleTable, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "stts", 4)) {
            AVIF_CHECKRES(avifParseTimeToSampleBox(track->sampleTable, avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "stsd", 4)) {
            AVIF_CHECKRES(avifParseSampleDescriptionBox(track->sampleTable,
                                                        rawOffset + avifROStreamOffset(&s),
                                                        avifROStreamCurrent(&s),
                                                        header.size,
                                                        diag));
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseMediaInformationBox(avifTrack * track, uint64_t rawOffset, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[minf]");

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(header.type, "stbl", 4)) {
            AVIF_CHECKRES(avifParseSampleTableBox(track, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size, diag));
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    return AVIF_RESULT_OK;
}

static avifResult avifParseMediaBox(avifTrack * track, uint64_t rawOffset, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[mdia]");

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(header.type, "mdhd", 4)) {
            AVIF_CHECKERR(avifParseMediaHeaderBox(track, avifROStreamCurrent(&s), header.size, diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "minf", 4)) {
            AVIF_CHECKRES(
                avifParseMediaInformationBox(track, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size, diag));
        } else if (!memcmp(header.type, "hdlr", 4)) {
            AVIF_CHECKERR(avifParseHandlerBox(avifROStreamCurrent(&s), header.size, track->handlerType, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED);
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    return AVIF_RESULT_OK;
}

static avifBool avifTrackReferenceBox(avifTrack * track, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[tref]");

    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECK(avifROStreamReadBoxHeader(&s, &header));

        if (!memcmp(header.type, "auxl", 4)) {
            uint32_t toID;
            AVIF_CHECK(avifROStreamReadU32(&s, &toID));                       // unsigned int(32) track_IDs[];
            AVIF_CHECK(avifROStreamSkip(&s, header.size - sizeof(uint32_t))); // just take the first one
            track->auxForID = toID;
        } else if (!memcmp(header.type, "prem", 4)) {
            uint32_t byID;
            AVIF_CHECK(avifROStreamReadU32(&s, &byID));                       // unsigned int(32) track_IDs[];
            AVIF_CHECK(avifROStreamSkip(&s, header.size - sizeof(uint32_t))); // just take the first one
            track->premByID = byID;
        } else {
            AVIF_CHECK(avifROStreamSkip(&s, header.size));
        }
    }
    return AVIF_TRUE;
}

static avifBool avifParseEditListBox(avifTrack * track, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[elst]");

    uint8_t version;
    uint32_t flags;
    AVIF_CHECK(avifROStreamReadVersionAndFlags(&s, &version, &flags));

    if ((flags & 1) == 0) {
        track->isRepeating = AVIF_FALSE;
        return AVIF_TRUE;
    }

    track->isRepeating = AVIF_TRUE;
    uint32_t entryCount;
    AVIF_CHECK(avifROStreamReadU32(&s, &entryCount)); // unsigned int(32) entry_count;
    if (entryCount != 1) {
        avifDiagnosticsPrintf(diag, "Box[elst] contains an entry_count != 1 [%u]", entryCount);
        return AVIF_FALSE;
    }

    if (version == 1) {
        AVIF_CHECK(avifROStreamReadU64(&s, &track->segmentDuration)); // unsigned int(64) segment_duration;
    } else if (version == 0) {
        uint32_t segmentDuration;
        AVIF_CHECK(avifROStreamReadU32(&s, &segmentDuration)); // unsigned int(32) segment_duration;
        track->segmentDuration = segmentDuration;
    } else {
        // Unsupported version
        avifDiagnosticsPrintf(diag, "Box[elst] has an unsupported version [%u]", version);
        return AVIF_FALSE;
    }
    if (track->segmentDuration == 0) {
        avifDiagnosticsPrintf(diag, "Box[elst] Invalid value for segment_duration (0).");
        return AVIF_FALSE;
    }
    return AVIF_TRUE;
}

static avifBool avifParseEditBox(avifTrack * track, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[edts]");

    avifBool elstBoxSeen = AVIF_FALSE;
    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECK(avifROStreamReadBoxHeader(&s, &header));

        if (!memcmp(header.type, "elst", 4)) {
            if (elstBoxSeen) {
                avifDiagnosticsPrintf(diag, "More than one [elst] Box was found.");
                return AVIF_FALSE;
            }
            AVIF_CHECK(avifParseEditListBox(track, avifROStreamCurrent(&s), header.size, diag));
            elstBoxSeen = AVIF_TRUE;
        }
        AVIF_CHECK(avifROStreamSkip(&s, header.size));
    }
    if (!elstBoxSeen) {
        avifDiagnosticsPrintf(diag, "Box[edts] contains no [elst] Box.");
        return AVIF_FALSE;
    }
    return AVIF_TRUE;
}

static avifResult avifParseTrackBox(avifDecoderData * data, uint64_t rawOffset, const uint8_t * raw, size_t rawLen)
{
    BEGIN_STREAM(s, raw, rawLen, data->diag, "Box[trak]");

    avifTrack * track = avifDecoderDataCreateTrack(data);
    AVIF_CHECKERR(track != NULL, AVIF_RESULT_OUT_OF_MEMORY);

    avifBool edtsBoxSeen = AVIF_FALSE;
    avifBool tkhdSeen = AVIF_FALSE;
    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(header.type, "tkhd", 4)) {
            if (tkhdSeen) {
                avifDiagnosticsPrintf(data->diag, "Box[trak] contains a duplicate unique box of type 'tkhd'");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            AVIF_CHECKERR(avifParseTrackHeaderBox(track, avifROStreamCurrent(&s), header.size, data->diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            tkhdSeen = AVIF_TRUE;
        } else if (!memcmp(header.type, "meta", 4)) {
            AVIF_CHECKRES(
                avifParseMetaBox(track->meta, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size, data->diag));
        } else if (!memcmp(header.type, "mdia", 4)) {
            AVIF_CHECKRES(avifParseMediaBox(track, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size, data->diag));
        } else if (!memcmp(header.type, "tref", 4)) {
            AVIF_CHECKERR(avifTrackReferenceBox(track, avifROStreamCurrent(&s), header.size, data->diag), AVIF_RESULT_BMFF_PARSE_FAILED);
        } else if (!memcmp(header.type, "edts", 4)) {
            if (edtsBoxSeen) {
                avifDiagnosticsPrintf(data->diag, "Box[trak] contains a duplicate unique box of type 'edts'");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            AVIF_CHECKERR(avifParseEditBox(track, avifROStreamCurrent(&s), header.size, data->diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            edtsBoxSeen = AVIF_TRUE;
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    if (!tkhdSeen) {
        avifDiagnosticsPrintf(data->diag, "Box[trak] does not contain a mandatory [tkhd] box");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    if (!edtsBoxSeen) {
        track->repetitionCount = AVIF_REPETITION_COUNT_UNKNOWN;
    } else if (track->isRepeating) {
        if (track->trackDuration == AVIF_INDEFINITE_DURATION64) {
            // If isRepeating is true and the track duration is unknown/indefinite, then set the repetition count to infinite
            // (Section 9.6.1 of ISO/IEC 23008-12 Part 12).
            track->repetitionCount = AVIF_REPETITION_COUNT_INFINITE;
        } else {
            // Section 9.6.1. of ISO/IEC 23008-12 Part 12: 1, the entire edit list is repeated a sufficient number of times to
            // equal the track duration.
            //
            // Since libavif uses repetitionCount (which is 0-based), we subtract the value by 1 to derive the number of
            // repetitions.
            AVIF_ASSERT_OR_RETURN(track->segmentDuration != 0);
            // We specifically check for trackDuration == 0 here and not when it is actually read in order to accept files which
            // inadvertently has a trackDuration of 0 without any edit lists.
            if (track->trackDuration == 0) {
                avifDiagnosticsPrintf(data->diag, "Invalid track duration 0.");
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            const uint64_t repetitionCount =
                (track->trackDuration / track->segmentDuration) + (track->trackDuration % track->segmentDuration != 0) - 1;
            if (repetitionCount > INT_MAX) {
                // repetitionCount does not fit in an integer and hence it is
                // likely to be a very large value. So, we just set it to
                // infinite.
                track->repetitionCount = AVIF_REPETITION_COUNT_INFINITE;
            } else {
                track->repetitionCount = (int)repetitionCount;
            }
        }
    } else {
        track->repetitionCount = 0;
    }

    return AVIF_RESULT_OK;
}

static avifResult avifParseMovieBox(avifDecoderData * data,
                                    uint64_t rawOffset,
                                    const uint8_t * raw,
                                    size_t rawLen,
                                    uint32_t imageSizeLimit,
                                    uint32_t imageDimensionLimit)
{
    BEGIN_STREAM(s, raw, rawLen, data->diag, "Box[moov]");

    avifBool hasTrak = AVIF_FALSE;
    while (avifROStreamHasBytesLeft(&s, 1)) {
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeader(&s, &header), AVIF_RESULT_BMFF_PARSE_FAILED);

        if (!memcmp(header.type, "trak", 4)) {
            AVIF_CHECKRES(avifParseTrackBox(data, rawOffset + avifROStreamOffset(&s), avifROStreamCurrent(&s), header.size));
            hasTrak = AVIF_TRUE;

            const avifTrack * track = &data->tracks.track[data->tracks.count - 1];
            if (!memcmp(track->handlerType, "pict", 4) || !memcmp(track->handlerType, "vide", 4) ||
                !memcmp(track->handlerType, "auxv", 4)) {
                if ((track->width == 0) || (track->height == 0)) {
                    avifDiagnosticsPrintf(data->diag, "Track ID [%u] has an invalid size [%ux%u]", track->id, track->width, track->height);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
                if (avifDimensionsTooLarge(track->width, track->height, imageSizeLimit, imageDimensionLimit)) {
                    avifDiagnosticsPrintf(data->diag,
                                          "Track ID [%u] dimensions are too large [%ux%u]",
                                          track->id,
                                          track->width,
                                          track->height);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
            }
        }

        AVIF_CHECKERR(avifROStreamSkip(&s, header.size), AVIF_RESULT_BMFF_PARSE_FAILED);
    }
    if (!hasTrak) {
        avifDiagnosticsPrintf(data->diag, "moov box does not contain any tracks");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
static avifProperty * avifMetaCreateProperty(avifMeta * meta, const char * propertyType)
{
    avifProperty * metaProperty = avifArrayPush(&meta->properties);
    AVIF_CHECK(metaProperty);
    memcpy(metaProperty->type, propertyType, 4);
    return metaProperty;
}

static avifProperty * avifDecoderItemAddProperty(avifDecoderItem * item, const avifProperty * metaProperty)
{
    avifProperty * itemProperty = avifArrayPush(&item->properties);
    AVIF_CHECK(itemProperty);
    *itemProperty = *metaProperty;
    return itemProperty;
}

static avifResult avifParseMinimizedImageBox(avifDecoderData * data,
                                             uint64_t rawOffset,
                                             const uint8_t * raw,
                                             size_t rawLen,
                                             avifBool isAvifAccordingToMinorVersion,
                                             avifDiagnostics * diag)
{
    avifMeta * meta = data->meta;
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[mini]");

    meta->fromMiniBox = AVIF_TRUE;

    uint32_t version;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &version, 2), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(2) version = 0;
    AVIF_CHECKERR(version == 0, AVIF_RESULT_BMFF_PARSE_FAILED);

    // flags
    uint32_t hasExplicitCodecTypes, floatFlag, fullRange, hasAlpha, hasExplicitCicp, hasHdr, hasIcc, hasExif, hasXmp;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasExplicitCodecTypes, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) explicit_codec_types_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &floatFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED);             // bit(1) float_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &fullRange, 1), AVIF_RESULT_BMFF_PARSE_FAILED);       // bit(1) full_range_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasAlpha, 1), AVIF_RESULT_BMFF_PARSE_FAILED);        // bit(1) alpha_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasExplicitCicp, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) explicit_cicp_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasHdr, 1), AVIF_RESULT_BMFF_PARSE_FAILED);          // bit(1) hdr_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasIcc, 1), AVIF_RESULT_BMFF_PARSE_FAILED);          // bit(1) icc_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasExif, 1), AVIF_RESULT_BMFF_PARSE_FAILED);         // bit(1) exif_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasXmp, 1), AVIF_RESULT_BMFF_PARSE_FAILED);          // bit(1) xmp_flag;

    uint32_t chromaSubsampling, orientation;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &chromaSubsampling, 2), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(2) chroma_subsampling;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &orientation, 3), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(3) orientation_minus1;
    ++orientation;

    // Spatial extents
    uint32_t largeDimensionsFlag, width, height;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &largeDimensionsFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) large_dimensions_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &width, largeDimensionsFlag ? 15 : 7),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_dimensions_flag ? 15 : 7) width_minus1;
    ++width;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &height, largeDimensionsFlag ? 15 : 7),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_dimensions_flag ? 15 : 7) height_minus1;
    ++height;

    // Pixel information
    uint32_t chromaIsHorizontallyCentered = 0, chromaIsVerticallyCentered = 0;
    if (chromaSubsampling == 1 || chromaSubsampling == 2) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &chromaIsHorizontallyCentered, 1),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) chroma_is_horizontally_centered;
    }
    if (chromaSubsampling == 1) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &chromaIsVerticallyCentered, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) chroma_is_vertically_centered;
    }

    uint32_t bitDepth;
    if (floatFlag) {
        // bit(2) bit_depth_log2_minus4;
        return AVIF_RESULT_BMFF_PARSE_FAILED; // Either invalid AVIF or unsupported non-AVIF.
    } else {
        uint32_t highBitDepthFlag;
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &highBitDepthFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) high_bit_depth_flag;
        if (highBitDepthFlag) {
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &bitDepth, 3), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(3) bit_depth_minus9;
            bitDepth += 9;
        } else {
            bitDepth = 8;
        }
    }

    uint32_t alphaIsPremultiplied = 0;
    if (hasAlpha) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &alphaIsPremultiplied, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) alpha_is_premultiplied;
    }

    // Colour properties
    uint8_t colorPrimaries;
    uint8_t transferCharacteristics;
    uint8_t matrixCoefficients;
    if (hasExplicitCicp) {
        AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &colorPrimaries, 8), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) colour_primaries;
        AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &transferCharacteristics, 8), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) transfer_characteristics;
        if (chromaSubsampling != 0) {
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &matrixCoefficients, 8), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) matrix_coefficients;
        } else {
            matrixCoefficients = AVIF_MATRIX_COEFFICIENTS_UNSPECIFIED; // 2
        }
    } else {
        colorPrimaries = hasIcc ? AVIF_COLOR_PRIMARIES_UNSPECIFIED                         // 2
                                : AVIF_COLOR_PRIMARIES_BT709;                              // 1
        transferCharacteristics = hasIcc ? AVIF_TRANSFER_CHARACTERISTICS_UNSPECIFIED       // 2
                                         : AVIF_TRANSFER_CHARACTERISTICS_SRGB;             // 13
        matrixCoefficients = chromaSubsampling == 0 ? AVIF_MATRIX_COEFFICIENTS_UNSPECIFIED // 2
                                                    : AVIF_MATRIX_COEFFICIENTS_BT601;      // 6
    }

    uint8_t infeType[4];
    uint8_t codecConfigType[4];
    if (hasExplicitCodecTypes) {
        // bit(32) infe_type;
        for (int i = 0; i < 4; ++i) {
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &infeType[i], 8), AVIF_RESULT_BMFF_PARSE_FAILED);
        }
        // bit(32) codec_config_type;
        for (int i = 0; i < 4; ++i) {
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &codecConfigType[i], 8), AVIF_RESULT_BMFF_PARSE_FAILED);
        }
#if defined(AVIF_CODEC_AVM)
        AVIF_CHECKERR((!memcmp(infeType, "av01", 4) && !memcmp(codecConfigType, "av1C", 4)) ||
                          (!memcmp(infeType, "av02", 4) && !memcmp(codecConfigType, "av2C", 4)),
                      AVIF_RESULT_BMFF_PARSE_FAILED);
#else
        AVIF_CHECKERR(!memcmp(infeType, "av01", 4) && !memcmp(codecConfigType, "av1C", 4), AVIF_RESULT_BMFF_PARSE_FAILED);
#endif
    } else {
        AVIF_CHECKERR(isAvifAccordingToMinorVersion, AVIF_RESULT_BMFF_PARSE_FAILED);
        memcpy(infeType, "av01", 4);
        memcpy(codecConfigType, "av1C", 4);
    }

    // High Dynamic Range properties
    uint32_t hasGainmap = AVIF_FALSE;
    uint32_t tmapHasIcc = AVIF_FALSE;
    uint32_t gainmapWidth = 0, gainmapHeight = 0;
    uint8_t gainmapMatrixCoefficients = 0;
    uint32_t gainmapFullRange = 0;
    uint32_t gainmapChromaSubsampling = 0;
    uint32_t gainmapBitDepth = 0;
    uint32_t tmapHasExplicitCicp = AVIF_FALSE;
    uint8_t tmapColorPrimaries = AVIF_COLOR_PRIMARIES_UNKNOWN;
    uint8_t tmapTransferCharacteristics = AVIF_TRANSFER_CHARACTERISTICS_UNKNOWN;
    uint8_t tmapMatrixCoefficients = AVIF_MATRIX_COEFFICIENTS_IDENTITY;
    uint32_t tmapFullRange = AVIF_FALSE;
    uint32_t hasClli = AVIF_FALSE, tmapHasClli = AVIF_FALSE;
    avifContentLightLevelInformationBox clli = { 0 }, tmapClli = { 0 };
    if (hasHdr) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &hasGainmap, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) gainmap_flag;
        if (hasGainmap) {
            // avifDecoderReset() requires the 'tmap' brand to be registered for the tone mapping derived image item to be parsed.
            if (data->compatibleBrands.capacity == 0) {
                AVIF_CHECKERR(avifArrayCreate(&data->compatibleBrands, sizeof(avifBrand), 1), AVIF_RESULT_OUT_OF_MEMORY);
            }
            avifBrand * brand = avifArrayPush(&data->compatibleBrands);
            AVIF_CHECKERR(brand != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            memcpy(brand, "tmap", sizeof(avifBrand));

            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapWidth, largeDimensionsFlag ? 15 : 7),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_dimensions_flag ? 15 : 7) gainmap_width_minus1;
            ++gainmapWidth;
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapHeight, largeDimensionsFlag ? 15 : 7),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_dimensions_flag ? 15 : 7) gainmap_height_minus1;
            ++gainmapHeight;
            AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &gainmapMatrixCoefficients, 8), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) gainmap_matrix_coefficients;
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapFullRange, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) gainmap_full_range_flag;

            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapChromaSubsampling, 2), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(2) gainmap_chroma_subsampling;
            uint32_t gainmapChromaIsHorizontallyCentered = 0, gainmapChromaIsVerticallyCentered = 0;
            if (gainmapChromaSubsampling == 1 || gainmapChromaSubsampling == 2) {
                AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapChromaIsHorizontallyCentered, 1),
                              AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) gainmap_chroma_is_horizontally_centered;
            }
            if (gainmapChromaSubsampling == 1) {
                AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapChromaIsVerticallyCentered, 1),
                              AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) gainmap_chroma_is_vertically_centered;
            }

            uint32_t gainmapFloatFlag;
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapFloatFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) gainmap_float_flag;
            if (gainmapFloatFlag) {
                // bit(2) gainmap_bit_depth_log2_minus4;
                return AVIF_RESULT_BMFF_PARSE_FAILED; // Either invalid AVIF or unsupported non-AVIF.
            } else {
                uint32_t gainmapHighBitDepthFlag;
                AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapHighBitDepthFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) gainmap_high_bit_depth_flag;
                if (gainmapHighBitDepthFlag) {
                    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapBitDepth, 3), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(3) gainmap_bit_depth_minus9;
                    gainmapBitDepth += 9;
                } else {
                    gainmapBitDepth = 8;
                }
            }

            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &tmapHasIcc, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) tmap_icc_flag;
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &tmapHasExplicitCicp, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) tmap_explicit_cicp_flag;
            if (tmapHasExplicitCicp) {
                AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &tmapColorPrimaries, 8), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) tmap_colour_primaries;
                AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &tmapTransferCharacteristics, 8),
                              AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) tmap_transfer_characteristics;
                AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &tmapMatrixCoefficients, 8), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(8) tmap_matrix_coefficients;
                AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &tmapFullRange, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) tmap_full_range_flag;
            } else {
                tmapColorPrimaries = AVIF_COLOR_PRIMARIES_BT709;                  // 1
                tmapTransferCharacteristics = AVIF_TRANSFER_CHARACTERISTICS_SRGB; // 13
                tmapMatrixCoefficients = AVIF_MATRIX_COEFFICIENTS_BT601;          // 6
                tmapFullRange = 1;
            }
        }
        AVIF_CHECKRES(avifParseMiniHDRProperties(&s, &hasClli, &clli));
        if (hasGainmap) {
            AVIF_CHECKRES(avifParseMiniHDRProperties(&s, &tmapHasClli, &tmapClli));
        }
    }

    // Chunk sizes
    uint32_t largeMetadataFlag = 0, largeCodecConfigFlag = 0, largeItemDataFlag = 0;
    if (hasIcc || hasExif || hasXmp || (hasHdr && hasGainmap)) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &largeMetadataFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) large_metadata_flag;
    }
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &largeCodecConfigFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) large_codec_config_flag;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &largeItemDataFlag, 1), AVIF_RESULT_BMFF_PARSE_FAILED); // bit(1) large_item_data_flag;

    uint32_t iccDataSize = 0;
    if (hasIcc) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &iccDataSize, largeMetadataFlag ? 20 : 10),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_metadata_flag ? 20 : 10) icc_data_size_minus1;
        ++iccDataSize;
    }
    uint32_t tmapIccDataSize = 0;
    if (hasHdr && hasGainmap && tmapHasIcc) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &tmapIccDataSize, largeMetadataFlag ? 20 : 10),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_metadata_flag ? 20 : 10) tmap_icc_data_size_minus1;
        ++tmapIccDataSize;
    }

    uint32_t gainmapMetadataSize = 0, gainmapItemDataSize = 0, gainmapItemCodecConfigSize = 0;
    if (hasHdr && hasGainmap) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapMetadataSize, largeMetadataFlag ? 20 : 10),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_metadata_flag ? 20 : 10) gainmap_metadata_size;
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapItemDataSize, largeItemDataFlag ? 28 : 15),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_item_data_flag ? 28 : 15) gainmap_item_data_size;
        if (gainmapItemDataSize > 0) {
            AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &gainmapItemCodecConfigSize, largeCodecConfigFlag ? 12 : 3),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_codec_config_flag ? 12 : 3) gainmap_item_codec_config_size;
        }
    }

    uint32_t mainItemCodecConfigSize, mainItemDataSize;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &mainItemCodecConfigSize, largeCodecConfigFlag ? 12 : 3),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_codec_config_flag ? 12 : 3) main_item_codec_config_size;
    AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &mainItemDataSize, largeItemDataFlag ? 28 : 15),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_item_data_flag ? 28 : 15) main_item_data_size_minus1;
    ++mainItemDataSize;

    uint32_t alphaItemCodecConfigSize = 0, alphaItemDataSize = 0;
    if (hasAlpha) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &alphaItemDataSize, largeItemDataFlag ? 28 : 15),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_item_data_flag ? 28 : 15) alpha_item_data_size;
    }
    if (hasAlpha && alphaItemDataSize != 0) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &alphaItemCodecConfigSize, largeCodecConfigFlag ? 12 : 3),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_codec_config_flag ? 12 : 3) alpha_item_codec_config_size;
    }

    if (hasExif || hasXmp) {
        uint8_t exifXmpCompressedFlag;
        AVIF_CHECKERR(avifROStreamReadBitsU8(&s, &exifXmpCompressedFlag, 1),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(1) exif_xmp_compressed_flag;
        AVIF_CHECKERR(!exifXmpCompressedFlag, AVIF_RESULT_NOT_IMPLEMENTED);
    }
    uint32_t exifDataSize = 0;
    if (hasExif) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &exifDataSize, largeMetadataFlag ? 20 : 10),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_metadata_flag ? 20 : 10) exif_data_size_minus_one;
        ++exifDataSize;
    }
    uint32_t xmpDataSize = 0;
    if (hasXmp) {
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &xmpDataSize, largeMetadataFlag ? 20 : 10),
                      AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(large_metadata_flag ? 20 : 10) xmp_data_size_minus_one;
        ++xmpDataSize;
    }

    // trailing_bits(); // bit padding till byte alignment
    if (s.numUsedBitsInPartialByte) {
        uint32_t padding;
        AVIF_CHECKERR(avifROStreamReadBitsU32(&s, &padding, 8 - s.numUsedBitsInPartialByte), AVIF_RESULT_BMFF_PARSE_FAILED);
        AVIF_CHECKERR(padding == 0, AVIF_RESULT_BMFF_PARSE_FAILED); // Only accept zeros as padding.
    }

    // Codec configuration ('av1C' always uses 4 bytes)
    avifCodecConfigurationBox mainItemCodecConfig;
    AVIF_CHECKERR(mainItemCodecConfigSize == 4, AVIF_RESULT_BMFF_PARSE_FAILED);
    AVIF_CHECKERR(avifParseCodecConfiguration(&s, &mainItemCodecConfig, (const char *)codecConfigType, diag),
                  AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(8) main_item_codec_config[main_item_codec_config_size];
    avifCodecConfigurationBox alphaItemCodecConfig = { 0 };
    if (hasAlpha && alphaItemDataSize != 0) {
        if (alphaItemCodecConfigSize == 0) {
            alphaItemCodecConfigSize = mainItemCodecConfigSize;
            alphaItemCodecConfig = mainItemCodecConfig;
        } else {
            AVIF_CHECKERR(alphaItemCodecConfigSize == 4, AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(avifParseCodecConfiguration(&s, &alphaItemCodecConfig, (const char *)codecConfigType, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(8) alpha_item_codec_config[alpha_item_codec_config_size];
        }
    }
    avifCodecConfigurationBox gainmapItemCodecConfig = { 0 };
    if (hasHdr && hasGainmap) {
        if (gainmapItemCodecConfigSize == 0) {
            gainmapItemCodecConfigSize = mainItemCodecConfigSize;
            gainmapItemCodecConfig = mainItemCodecConfig;
        } else {
            AVIF_CHECKERR(gainmapItemCodecConfigSize == 4, AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(avifParseCodecConfiguration(&s, &gainmapItemCodecConfig, (const char *)codecConfigType, diag),
                          AVIF_RESULT_BMFF_PARSE_FAILED); // unsigned int(8) gainmap_item_codec_config[gainmap_item_codec_config_size];
        }
    }

    // Make sure all metadata and coded chunks fit into the 'meta' box whose size is rawLen.
    // There should be no missing nor unused byte.

    AVIF_CHECKERR(avifROStreamRemainingBytes(&s) == (uint64_t)iccDataSize + tmapIccDataSize + gainmapMetadataSize + alphaItemDataSize +
                                                        gainmapItemDataSize + mainItemDataSize + exifDataSize + xmpDataSize,
                  AVIF_RESULT_BMFF_PARSE_FAILED);

    // Create the items and properties generated by the MinimizedImageBox.
    // The MinimizedImageBox always creates 8 properties for specification easiness.
    // Use FreeSpaceBoxes as no-op placeholder properties when necessary.
    // There is no need to use placeholder items because item IDs do not have to
    // be contiguous, whereas property indices shall be 1, 2, 3, 4, 5 etc.

    meta->primaryItemID = 1;
    avifDecoderItem * colorItem;
    AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, meta->primaryItemID, &colorItem));
    memcpy(colorItem->type, infeType, 4);
    colorItem->width = width;
    colorItem->height = height;
    colorItem->miniBoxPixelFormat = chromaSubsampling == 0   ? AVIF_PIXEL_FORMAT_YUV400
                                    : chromaSubsampling == 1 ? AVIF_PIXEL_FORMAT_YUV420
                                    : chromaSubsampling == 2 ? AVIF_PIXEL_FORMAT_YUV422
                                                             : AVIF_PIXEL_FORMAT_YUV444;
    if (colorItem->miniBoxPixelFormat == AVIF_PIXEL_FORMAT_YUV422) {
        // In AV1, the chroma_sample_position syntax element is not present for the YUV 4:2:2 format.
        // Assume that AV1 uses the same 4:2:2 chroma sample location as HEVC and VVC (colocated).
        AVIF_CHECKERR(!chromaIsHorizontallyCentered, AVIF_RESULT_BMFF_PARSE_FAILED);
        // chromaIsVerticallyCentered: Ignored unless chroma_subsampling is 1.
        colorItem->miniBoxChromaSamplePosition = AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN;
    } else if (colorItem->miniBoxPixelFormat == AVIF_PIXEL_FORMAT_YUV420) {
        if (chromaIsHorizontallyCentered) {
            // There is no way to describe this with AV1's chroma_sample_position enum besides CSP_UNKNOWN.
            // There is a proposal to assign the reserved value 3 (CSP_RESERVED) to the center chroma sample position.
            colorItem->miniBoxChromaSamplePosition = AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN;
        } else {
            colorItem->miniBoxChromaSamplePosition = chromaIsVerticallyCentered ? AVIF_CHROMA_SAMPLE_POSITION_VERTICAL
                                                                                : AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
        }
    } else {
        // chromaIsHorizontallyCentered: Ignored unless chroma_subsampling is 1 or 2.
        // chromaIsVerticallyCentered: Ignored unless chroma_subsampling is 1.
        colorItem->miniBoxChromaSamplePosition = AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN;
    }

    avifDecoderItem * alphaItem = NULL;
    if (hasAlpha) {
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, /*itemID=*/2, &alphaItem));
        memcpy(alphaItem->type, infeType, 4);
        alphaItem->width = width;
        alphaItem->height = height;
        alphaItem->miniBoxPixelFormat = AVIF_PIXEL_FORMAT_YUV400;
        alphaItem->miniBoxChromaSamplePosition = AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN;
    }

    avifDecoderItem * tmapItem = NULL;
    if (hasGainmap) {
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, /*itemID=*/3, &tmapItem));
        memcpy(tmapItem->type, "tmap", 4);
        colorItem->dimgForID = tmapItem->id;
        colorItem->dimgIdx = 0;

        // avifDecoderReset() requires the 'tmap' item to be an alternative to the primary item.
        avifEntityToGroup * group = avifArrayPush(&data->meta->entityToGroups);
        AVIF_CHECKERR(group != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        memcpy(group->groupingType, "altr", 4);
        AVIF_CHECKERR(avifArrayCreate(&group->entityIDs, sizeof(uint32_t), 2), AVIF_RESULT_OUT_OF_MEMORY);
        uint32_t * groupEntityId = avifArrayPush(&group->entityIDs);
        AVIF_CHECKERR(groupEntityId != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        *groupEntityId = tmapItem->id;
        groupEntityId = avifArrayPush(&group->entityIDs);
        AVIF_CHECKERR(groupEntityId != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        *groupEntityId = colorItem->id;
    }
    avifDecoderItem * gainmapItem = NULL;
    if (gainmapItemDataSize != 0) {
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, /*itemID=*/4, &gainmapItem));
        memcpy(gainmapItem->type, infeType, 4);
        gainmapItem->width = gainmapWidth;
        gainmapItem->height = gainmapHeight;
        gainmapItem->dimgForID = tmapItem->id;
        gainmapItem->dimgIdx = 1;
    }

    // Property with fixed index 1.
    avifProperty * colorCodecConfigProp = avifMetaCreateProperty(meta, (const char *)codecConfigType);
    AVIF_CHECKERR(colorCodecConfigProp, AVIF_RESULT_OUT_OF_MEMORY);
    colorCodecConfigProp->u.av1C = mainItemCodecConfig;
    AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, colorCodecConfigProp), AVIF_RESULT_OUT_OF_MEMORY);

    // Property with fixed index 2.
    avifProperty * ispeProp = avifMetaCreateProperty(meta, "ispe");
    AVIF_CHECKERR(ispeProp, AVIF_RESULT_OUT_OF_MEMORY);
    ispeProp->u.ispe.width = width;
    ispeProp->u.ispe.height = height;
    AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, ispeProp), AVIF_RESULT_OUT_OF_MEMORY);

    // Property with fixed index 3.
    avifProperty * pixiProp = avifMetaCreateProperty(meta, "pixi");
    AVIF_CHECKERR(pixiProp, AVIF_RESULT_OUT_OF_MEMORY);
    pixiProp->u.pixi.planeCount = chromaSubsampling == 0 ? 1 : 3;
    for (uint8_t plane = 0; plane < pixiProp->u.pixi.planeCount; ++plane) {
        pixiProp->u.pixi.planeDepths[plane] = (uint8_t)bitDepth;
    }
    AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, pixiProp), AVIF_RESULT_OUT_OF_MEMORY);

    // Property with fixed index 4.
    avifProperty * colrPropNCLX = avifMetaCreateProperty(meta, "colr");
    AVIF_CHECKERR(colrPropNCLX, AVIF_RESULT_OUT_OF_MEMORY);
    colrPropNCLX->u.colr.hasNCLX = AVIF_TRUE; // colour_type "nclx"
    colrPropNCLX->u.colr.colorPrimaries = (avifColorPrimaries)colorPrimaries;
    colrPropNCLX->u.colr.transferCharacteristics = (avifTransferCharacteristics)transferCharacteristics;
    colrPropNCLX->u.colr.matrixCoefficients = (avifMatrixCoefficients)matrixCoefficients;
    colrPropNCLX->u.colr.range = fullRange ? AVIF_RANGE_FULL : AVIF_RANGE_LIMITED;
    AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, colrPropNCLX), AVIF_RESULT_OUT_OF_MEMORY);

    // Property with fixed index 5.
    if (iccDataSize != 0) {
        avifProperty * colrPropICC = avifMetaCreateProperty(meta, "colr");
        AVIF_CHECKERR(colrPropICC, AVIF_RESULT_OUT_OF_MEMORY);
        colrPropICC->u.colr.hasICC = AVIF_TRUE; // colour_type "rICC" or "prof"
        colrPropICC->u.colr.iccOffset = rawOffset + avifROStreamOffset(&s);
        colrPropICC->u.colr.iccSize = (size_t)iccDataSize;
        AVIF_CHECKERR(avifROStreamSkip(&s, colrPropICC->u.colr.iccSize), AVIF_RESULT_BMFF_PARSE_FAILED);
        AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, colrPropICC), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (hasAlpha) {
        // Property with fixed index 6.
        avifProperty * alphaCodecConfigProp = avifMetaCreateProperty(meta, (const char *)codecConfigType);
        AVIF_CHECKERR(alphaCodecConfigProp, AVIF_RESULT_OUT_OF_MEMORY);
        alphaCodecConfigProp->u.av1C = alphaItemCodecConfig;
        AVIF_CHECKERR(avifDecoderItemAddProperty(alphaItem, alphaCodecConfigProp), AVIF_RESULT_OUT_OF_MEMORY);

        // Property with fixed index 7.
        alphaItem->auxForID = colorItem->id;
        colorItem->premByID = alphaIsPremultiplied;
        avifProperty * alphaAuxProp = avifMetaCreateProperty(meta, "auxC");
        AVIF_CHECKERR(alphaAuxProp, AVIF_RESULT_OUT_OF_MEMORY);
        strcpy(alphaAuxProp->u.auxC.auxType, AVIF_URN_ALPHA0);
        AVIF_CHECKERR(avifDecoderItemAddProperty(alphaItem, alphaAuxProp), AVIF_RESULT_OUT_OF_MEMORY);

        // Property with fixed index 2 (reused).
        AVIF_CHECKERR(avifDecoderItemAddProperty(alphaItem, ispeProp), AVIF_RESULT_OUT_OF_MEMORY);

        // Property with fixed index 8.
        avifProperty * alphaPixiProp = avifMetaCreateProperty(meta, "pixi");
        AVIF_CHECKERR(alphaPixiProp, AVIF_RESULT_OUT_OF_MEMORY);
        memcpy(alphaPixiProp->type, "pixi", 4);
        alphaPixiProp->u.pixi.planeCount = 1;
        alphaPixiProp->u.pixi.planeDepths[0] = (uint8_t)bitDepth;
        AVIF_CHECKERR(avifDecoderItemAddProperty(alphaItem, alphaPixiProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        // Placeholders 6, 7 and 8.
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY);
    }

    // Same behavior as avifImageExtractExifOrientationToIrotImir().
    if (orientation == 3 || orientation == 5 || orientation == 6 || orientation == 7 || orientation == 8) {
        // Property with fixed index 9.
        avifProperty * irotProp = avifMetaCreateProperty(meta, "irot");
        AVIF_CHECKERR(irotProp, AVIF_RESULT_OUT_OF_MEMORY);
        irotProp->u.irot.angle = orientation == 3 ? 2 : (orientation == 5 || orientation == 8) ? 1 : 3;
        AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, irotProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }
    if (orientation == 2 || orientation == 4 || orientation == 5 || orientation == 7) {
        // Property with fixed index 10.
        avifProperty * imirProp = avifMetaCreateProperty(meta, "imir");
        AVIF_CHECKERR(imirProp, AVIF_RESULT_OUT_OF_MEMORY);
        imirProp->u.imir.axis = orientation == 2 ? 1 : 0;
        AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, imirProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (hasClli) {
        // Property with fixed index 11.
        avifProperty * clliProp = avifMetaCreateProperty(meta, "clli");
        AVIF_CHECKERR(clliProp, AVIF_RESULT_OUT_OF_MEMORY);
        clliProp->u.clli = clli;
        AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, clliProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }
    // Properties with fixed indices 12 to 16 are ignored by libavif (mdcv, cclv, amve, reve and ndwt).
    for (int i = 12; i <= 16; ++i) {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (gainmapItemCodecConfigSize != 0) {
        // Property with fixed index 17.
        avifProperty * gainmapCodecConfigProp = avifMetaCreateProperty(meta, (const char *)codecConfigType);
        AVIF_CHECKERR(gainmapCodecConfigProp, AVIF_RESULT_OUT_OF_MEMORY);
        gainmapCodecConfigProp->u.av1C = gainmapItemCodecConfig;
        AVIF_CHECKERR(avifDecoderItemAddProperty(gainmapItem, gainmapCodecConfigProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (gainmapItemDataSize != 0) {
        // Property with fixed index 18.
        avifProperty * gainmapIspeProp = avifMetaCreateProperty(meta, "ispe");
        AVIF_CHECKERR(gainmapIspeProp, AVIF_RESULT_OUT_OF_MEMORY);
        gainmapIspeProp->u.ispe.width = gainmapWidth;
        gainmapIspeProp->u.ispe.height = gainmapHeight;
        AVIF_CHECKERR(avifDecoderItemAddProperty(gainmapItem, gainmapIspeProp), AVIF_RESULT_OUT_OF_MEMORY);

        // Property with fixed index 19.
        avifProperty * gainmapPixiProp = avifMetaCreateProperty(meta, "pixi");
        AVIF_CHECKERR(gainmapPixiProp, AVIF_RESULT_OUT_OF_MEMORY);
        memcpy(gainmapPixiProp->type, "pixi", 4);
        gainmapPixiProp->u.pixi.planeCount = gainmapChromaSubsampling == 0 ? 1 : 3;
        for (uint8_t plane = 0; plane < gainmapPixiProp->u.pixi.planeCount; ++plane) {
            gainmapPixiProp->u.pixi.planeDepths[plane] = (uint8_t)gainmapBitDepth;
        }
        AVIF_CHECKERR(avifDecoderItemAddProperty(gainmapItem, gainmapPixiProp), AVIF_RESULT_OUT_OF_MEMORY);

        // Property with fixed index 20.
        avifProperty * gainmapColrPropNCLX = avifMetaCreateProperty(meta, "colr");
        AVIF_CHECKERR(gainmapColrPropNCLX, AVIF_RESULT_OUT_OF_MEMORY);
        gainmapColrPropNCLX->u.colr.hasNCLX = AVIF_TRUE;                                                 // colour_type "nclx"
        gainmapColrPropNCLX->u.colr.colorPrimaries = AVIF_COLOR_PRIMARIES_UNSPECIFIED;                   // 2
        gainmapColrPropNCLX->u.colr.transferCharacteristics = AVIF_TRANSFER_CHARACTERISTICS_UNSPECIFIED; // 2
        gainmapColrPropNCLX->u.colr.matrixCoefficients = (avifMatrixCoefficients)gainmapMatrixCoefficients;
        gainmapColrPropNCLX->u.colr.range = gainmapFullRange ? AVIF_RANGE_FULL : AVIF_RANGE_LIMITED;
        AVIF_CHECKERR(avifDecoderItemAddProperty(gainmapItem, gainmapColrPropNCLX), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        // Placeholders 18, 19 and 20.
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY);
    }

    if (hasGainmap) {
        // Property with fixed index 21.
        avifProperty * tmapIspeProp = avifMetaCreateProperty(meta, "ispe");
        AVIF_CHECKERR(tmapIspeProp, AVIF_RESULT_OUT_OF_MEMORY);
        tmapIspeProp->u.ispe.width = orientation <= 4 ? width : height;
        tmapIspeProp->u.ispe.height = orientation <= 4 ? height : width;
        AVIF_CHECKERR(avifDecoderItemAddProperty(tmapItem, tmapIspeProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (hasGainmap && (tmapHasExplicitCicp || !tmapHasIcc)) {
        // Property with fixed index 22.
        avifProperty * tmapColrPropNCLX = avifMetaCreateProperty(meta, "colr");
        AVIF_CHECKERR(tmapColrPropNCLX, AVIF_RESULT_OUT_OF_MEMORY);
        tmapColrPropNCLX->u.colr.hasNCLX = AVIF_TRUE; // colour_type "nclx"
        tmapColrPropNCLX->u.colr.colorPrimaries = (avifColorPrimaries)tmapColorPrimaries;
        tmapColrPropNCLX->u.colr.transferCharacteristics = (avifTransferCharacteristics)tmapTransferCharacteristics;
        tmapColrPropNCLX->u.colr.matrixCoefficients = (avifMatrixCoefficients)tmapMatrixCoefficients;
        tmapColrPropNCLX->u.colr.range = tmapFullRange ? AVIF_RANGE_FULL : AVIF_RANGE_LIMITED;
        AVIF_CHECKERR(avifDecoderItemAddProperty(tmapItem, tmapColrPropNCLX), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (tmapIccDataSize != 0) {
        // Property with fixed index 23.
        avifProperty * tmapColrPropICC = avifMetaCreateProperty(meta, "colr");
        AVIF_CHECKERR(tmapColrPropICC, AVIF_RESULT_OUT_OF_MEMORY);
        tmapColrPropICC->u.colr.hasICC = AVIF_TRUE; // colour_type "rICC" or "prof"
        tmapColrPropICC->u.colr.iccOffset = rawOffset + avifROStreamOffset(&s);
        tmapColrPropICC->u.colr.iccSize = tmapIccDataSize;
        AVIF_CHECKERR(avifROStreamSkip(&s, tmapColrPropICC->u.colr.iccSize), AVIF_RESULT_BMFF_PARSE_FAILED);
        AVIF_CHECKERR(avifDecoderItemAddProperty(colorItem, tmapColrPropICC), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }

    if (tmapHasClli) {
        // Property with fixed index 24.
        avifProperty * tmapClliProp = avifMetaCreateProperty(meta, "clli");
        AVIF_CHECKERR(tmapClliProp, AVIF_RESULT_OUT_OF_MEMORY);
        tmapClliProp->u.clli = tmapClli;
        AVIF_CHECKERR(avifDecoderItemAddProperty(tmapItem, tmapClliProp), AVIF_RESULT_OUT_OF_MEMORY);
    } else {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }
    // Properties with fixed indices 25 to 29 are ignored by libavif (mdcv, cclv, amve, reve and ndwt).
    for (int i = 25; i <= 29; ++i) {
        AVIF_CHECKERR(avifMetaCreateProperty(meta, "skip"), AVIF_RESULT_OUT_OF_MEMORY); // Placeholder.
    }
    AVIF_ASSERT_OR_RETURN(meta->properties.count == 29);

    // Extents.

    if (gainmapMetadataSize != 0) {
        // Prepend the version field to the GainMapMetadata to form the ToneMapImage syntax.
        tmapItem->size = gainmapMetadataSize + 1;
        AVIF_CHECKRES(avifRWDataRealloc(&tmapItem->mergedExtents, tmapItem->size));
        tmapItem->ownsMergedExtents = AVIF_TRUE;
        tmapItem->mergedExtents.data[0] = 0; // unsigned int(8) version = 0;
        AVIF_CHECKERR(avifROStreamRead(&s, tmapItem->mergedExtents.data + 1, gainmapMetadataSize), AVIF_RESULT_BMFF_PARSE_FAILED);
    }

    if (hasAlpha) {
        avifExtent * alphaExtent = (avifExtent *)avifArrayPush(&alphaItem->extents);
        AVIF_CHECKERR(alphaExtent, AVIF_RESULT_OUT_OF_MEMORY);
        alphaExtent->offset = rawOffset + avifROStreamOffset(&s);
        alphaExtent->size = alphaItemDataSize;
        AVIF_CHECKERR(avifROStreamSkip(&s, alphaExtent->size), AVIF_RESULT_BMFF_PARSE_FAILED);
        alphaItem->size = alphaExtent->size;
    }

    if (gainmapItemDataSize != 0) {
        avifExtent * gainmapExtent = (avifExtent *)avifArrayPush(&gainmapItem->extents);
        AVIF_CHECKERR(gainmapExtent, AVIF_RESULT_OUT_OF_MEMORY);
        gainmapExtent->offset = rawOffset + avifROStreamOffset(&s);
        gainmapExtent->size = gainmapItemDataSize;
        AVIF_CHECKERR(avifROStreamSkip(&s, gainmapExtent->size), AVIF_RESULT_BMFF_PARSE_FAILED);
        gainmapItem->size = gainmapExtent->size;
    }

    avifExtent * colorExtent = (avifExtent *)avifArrayPush(&colorItem->extents);
    AVIF_CHECKERR(colorExtent, AVIF_RESULT_OUT_OF_MEMORY);
    colorExtent->offset = rawOffset + avifROStreamOffset(&s);
    colorExtent->size = mainItemDataSize;
    AVIF_CHECKERR(avifROStreamSkip(&s, colorExtent->size), AVIF_RESULT_BMFF_PARSE_FAILED);
    colorItem->size = colorExtent->size;

    if (hasExif) {
        avifDecoderItem * exifItem;
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, /*itemID=*/6, &exifItem));
        memcpy(exifItem->type, "Exif", 4);
        exifItem->descForID = colorItem->id; // 'cdsc'

        avifExtent * exifExtent = (avifExtent *)avifArrayPush(&exifItem->extents);
        AVIF_CHECKERR(exifExtent, AVIF_RESULT_OUT_OF_MEMORY);
        exifExtent->offset = rawOffset + avifROStreamOffset(&s);
        exifExtent->size = exifDataSize; // Does not include unsigned int(32) exif_tiff_header_offset;
        AVIF_CHECKERR(avifROStreamSkip(&s, exifExtent->size), AVIF_RESULT_BMFF_PARSE_FAILED);
        exifItem->size = exifExtent->size;
    }

    if (hasXmp) {
        avifDecoderItem * xmpItem;
        AVIF_CHECKRES(avifMetaFindOrCreateItem(meta, /*itemID=*/7, &xmpItem));
        memcpy(xmpItem->type, "mime", 4);
        memcpy(xmpItem->contentType.contentType, AVIF_CONTENT_TYPE_XMP, sizeof(AVIF_CONTENT_TYPE_XMP));
        xmpItem->descForID = colorItem->id; // 'cdsc'

        avifExtent * xmpExtent = (avifExtent *)avifArrayPush(&xmpItem->extents);
        AVIF_CHECKERR(xmpExtent, AVIF_RESULT_OUT_OF_MEMORY);
        xmpExtent->offset = rawOffset + avifROStreamOffset(&s);
        xmpExtent->size = xmpDataSize;
        AVIF_CHECKERR(avifROStreamSkip(&s, xmpExtent->size), AVIF_RESULT_BMFF_PARSE_FAILED);
        xmpItem->size = xmpExtent->size;
    }
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

static avifBool avifParseFileTypeBox(avifFileType * ftyp, const uint8_t * raw, size_t rawLen, avifDiagnostics * diag)
{
    BEGIN_STREAM(s, raw, rawLen, diag, "Box[ftyp]");

    AVIF_CHECK(avifROStreamRead(&s, ftyp->majorBrand, 4));
    AVIF_CHECK(avifROStreamRead(&s, ftyp->minorVersion, 4));

    size_t compatibleBrandsBytes = avifROStreamRemainingBytes(&s);
    if ((compatibleBrandsBytes % 4) != 0) {
        avifDiagnosticsPrintf(diag, "Box[ftyp] contains a compatible brands section that isn't divisible by 4 [%zu]", compatibleBrandsBytes);
        return AVIF_FALSE;
    }
    ftyp->compatibleBrands = avifROStreamCurrent(&s);
    AVIF_CHECK(avifROStreamSkip(&s, compatibleBrandsBytes));
    ftyp->compatibleBrandsCount = (int)compatibleBrandsBytes / 4;

    return AVIF_TRUE;
}

static avifBool avifFileTypeHasBrand(avifFileType * ftyp, const char * brand);
static avifBool avifFileTypeIsCompatible(avifFileType * ftyp);

static avifResult avifParse(avifDecoder * decoder)
{
    // Note: this top-level function is the only avifParse*() function that returns avifResult instead of avifBool.
    // Be sure to use AVIF_CHECKERR() in this function with an explicit error result instead of simply using AVIF_CHECK().

    avifResult readResult;
    uint64_t parseOffset = 0;
    avifDecoderData * data = decoder->data;
    avifBool ftypSeen = AVIF_FALSE;
    avifBool metaSeen = AVIF_FALSE;
    avifBool metaIsSizeZero = AVIF_FALSE;
    avifBool moovSeen = AVIF_FALSE;
    avifBool needsMeta = AVIF_FALSE;
    avifBool needsMoov = AVIF_FALSE;
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
    avifBool miniSeen = AVIF_FALSE;
    avifBool needsMini = AVIF_FALSE;
#endif
    avifBool needsTmap = AVIF_FALSE;
    avifBool tmapSeen = AVIF_FALSE;
    avifFileType ftyp = { 0 };

    for (;;) {
        // Read just enough to get the next box header (a max of 32 bytes)
        avifROData headerContents;
        if ((decoder->io->sizeHint > 0) && (parseOffset > decoder->io->sizeHint)) {
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
        readResult = decoder->io->read(decoder->io, 0, parseOffset, 32, &headerContents);
        if (readResult != AVIF_RESULT_OK) {
            return readResult;
        }
        if (!headerContents.size) {
            // If we got AVIF_RESULT_OK from the reader but received 0 bytes,
            // we've reached the end of the file with no errors. Hooray!
            break;
        }

        // Parse the header, and find out how many bytes it actually was
        BEGIN_STREAM(headerStream, headerContents.data, headerContents.size, &decoder->diag, "File-level box header");
        avifBoxHeader header;
        AVIF_CHECKERR(avifROStreamReadBoxHeaderPartial(&headerStream, &header, /*topLevel=*/AVIF_TRUE), AVIF_RESULT_BMFF_PARSE_FAILED);
        parseOffset += headerStream.offset;
        AVIF_ASSERT_OR_RETURN(decoder->io->sizeHint == 0 || parseOffset <= decoder->io->sizeHint);

        // Try to get the remainder of the box, if necessary
        uint64_t boxOffset = 0;
        avifROData boxContents = AVIF_DATA_EMPTY;

        avifBool isFtyp = AVIF_FALSE, isMeta = AVIF_FALSE, isMoov = AVIF_FALSE;
        avifBool isNonSkippableVariableLengthBox = AVIF_FALSE;
        if (!memcmp(header.type, "ftyp", 4)) {
            isFtyp = AVIF_TRUE;
            isNonSkippableVariableLengthBox = AVIF_TRUE;
        } else if (!memcmp(header.type, "meta", 4)) {
            isMeta = AVIF_TRUE;
            isNonSkippableVariableLengthBox = AVIF_TRUE;
            metaIsSizeZero = header.isSizeZeroBox;
        } else if (!memcmp(header.type, "moov", 4)) {
            isMoov = AVIF_TRUE;
            isNonSkippableVariableLengthBox = AVIF_TRUE;
        }
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
        avifBool isMini = AVIF_FALSE;
        if (!isNonSkippableVariableLengthBox && !memcmp(header.type, "mini", 4)) {
            isMini = AVIF_TRUE;
            isNonSkippableVariableLengthBox = AVIF_TRUE;
        }
#endif

        if (!isFtyp && (isNonSkippableVariableLengthBox || !memcmp(header.type, "free", 4) || !memcmp(header.type, "skip", 4) ||
                        !memcmp(header.type, "mdat", 4))) {
            // Section 6.3.4 of ISO/IEC 14496-12:
            //   The FileTypeBox shall occur before any variable-length box (e.g. movie, free space, media data).
            AVIF_CHECKERR(ftypSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
        }

        if (isNonSkippableVariableLengthBox) {
            boxOffset = parseOffset;
            size_t sizeToRead;
            if (header.isSizeZeroBox) {
                // The box body goes till the end of the file.
                if (decoder->io->sizeHint != 0 && decoder->io->sizeHint - parseOffset < SIZE_MAX) {
                    sizeToRead = (size_t)(decoder->io->sizeHint - parseOffset);
                } else {
                    sizeToRead = SIZE_MAX; // This will get truncated. See the documentation of avifIOReadFunc.
                }
            } else {
                sizeToRead = header.size;
            }
            readResult = decoder->io->read(decoder->io, 0, parseOffset, sizeToRead, &boxContents);
            if (readResult != AVIF_RESULT_OK) {
                return readResult;
            }
            if (header.isSizeZeroBox) {
                header.size = boxContents.size;
            } else if (boxContents.size != header.size) {
                // A truncated box, bail out
                return AVIF_RESULT_TRUNCATED_DATA;
            }
        } else if (header.isSizeZeroBox) {
            // An unknown top level box with size 0 was found. If we reach here it means we haven't completed parsing successfully
            // since there are no further boxes left.
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        } else if (header.size > (UINT64_MAX - parseOffset)) {
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }
        parseOffset += header.size;

        if (isFtyp) {
            AVIF_CHECKERR(!ftypSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(avifParseFileTypeBox(&ftyp, boxContents.data, boxContents.size, data->diag), AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(avifFileTypeIsCompatible(&ftyp), AVIF_RESULT_INVALID_FTYP);
            ftypSeen = AVIF_TRUE;
            memcpy(data->majorBrand, ftyp.majorBrand, 4); // Remember the major brand for future AVIF_DECODER_SOURCE_AUTO decisions
            if (ftyp.compatibleBrandsCount > 0) {
                AVIF_CHECKERR(avifArrayCreate(&data->compatibleBrands, sizeof(avifBrand), ftyp.compatibleBrandsCount),
                              AVIF_RESULT_OUT_OF_MEMORY);
                memcpy(data->compatibleBrands.brand, ftyp.compatibleBrands, sizeof(avifBrand) * ftyp.compatibleBrandsCount);
                data->compatibleBrands.count = ftyp.compatibleBrandsCount;
            }
            needsMeta = avifFileTypeHasBrand(&ftyp, "avif");
            needsMoov = avifFileTypeHasBrand(&ftyp, "avis");
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
            needsMini = avifFileTypeHasBrand(&ftyp, "mif3");
            if (needsMini) {
                AVIF_CHECKERR(!needsMeta, AVIF_RESULT_INVALID_FTYP);
                // Section O.2.1.2 of ISO/IEC 23008-12:2014, CDAM 2:
                //   When the 'mif3' brand is present as the major_brand of the FileTypeBox,
                //   the minor_version of the FileTypeBox shall be 0 or a brand that is either
                //   structurally compatible with the 'mif3' brand, such as a codec brand
                //   complying with the 'mif3' structural brand, or a brand to which the file
                //   conforms after the equivalent MetaBox has been transformed from
                //   MinimizedImageBox as specified in Clause O.4.
                AVIF_CHECKERR(!memcmp(ftyp.minorVersion, "\0\0\0\0", 4) || !memcmp(ftyp.minorVersion, "avif", 4),
                              AVIF_RESULT_BMFF_PARSE_FAILED);
            }
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI
            needsTmap = avifFileTypeHasBrand(&ftyp, "tmap");
            if (needsTmap) {
                needsMeta = AVIF_TRUE;
            }
        } else if (isMeta) {
            AVIF_CHECKERR(!metaSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
            AVIF_CHECKERR(!miniSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
#endif
            AVIF_CHECKRES(avifParseMetaBox(data->meta, boxOffset, boxContents.data, boxContents.size, data->diag));
            metaSeen = AVIF_TRUE;

            for (uint32_t itemIndex = 0; itemIndex < data->meta->items.count; ++itemIndex) {
                if (!memcmp(data->meta->items.item[itemIndex]->type, "tmap", 4)) {
                    tmapSeen = AVIF_TRUE;
                    break;
                }
            }

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
        } else if (isMini) {
            AVIF_CHECKERR(!metaSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKERR(!miniSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
            const avifBool isAvifAccordingToMinorVersion = !memcmp(ftyp.minorVersion, "avif", 4);
            AVIF_CHECKRES(
                avifParseMinimizedImageBox(data, boxOffset, boxContents.data, boxContents.size, isAvifAccordingToMinorVersion, data->diag));
            miniSeen = AVIF_TRUE;
#endif
        } else if (isMoov) {
            AVIF_CHECKERR(!moovSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
            AVIF_CHECKRES(
                avifParseMovieBox(data, boxOffset, boxContents.data, boxContents.size, decoder->imageSizeLimit, decoder->imageDimensionLimit));
            moovSeen = AVIF_TRUE;
            decoder->imageSequenceTrackPresent = AVIF_TRUE;
        }

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
        if (ftypSeen && !needsMini) {
            // When MinimizedImageBox is present in a file, the 'mif3' brand or a derived brand that implies the 'mif3'
            // brand shall be the major brand or present among the compatible brands in the FileTypeBox.
            AVIF_CHECKERR(!miniSeen, AVIF_RESULT_BMFF_PARSE_FAILED);
        }
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

        // See if there is enough information to consider Parse() a success and early-out:
        // * If the brand 'avif' is present, require a meta box
        // * If the brand 'avis' is present, require a moov box
        // * If AVIF_ENABLE_EXPERIMENTAL_MINI is defined and the brand 'mif3' is present, require a mini box
        avifBool sawEverythingNeeded = ftypSeen && (!needsMeta || metaSeen) && (!needsMoov || moovSeen) && (!needsTmap || tmapSeen);
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
        sawEverythingNeeded = sawEverythingNeeded && (!needsMini || miniSeen);
#endif
        if (sawEverythingNeeded) {
            return AVIF_RESULT_OK;
        }
    }
    if (!ftypSeen) {
        return AVIF_RESULT_INVALID_FTYP;
    }
    if ((needsMeta && !metaSeen) || (needsMoov && !moovSeen)) {
        return AVIF_RESULT_TRUNCATED_DATA;
    }
    if (needsTmap && !tmapSeen) {
        return metaIsSizeZero ? AVIF_RESULT_TRUNCATED_DATA : AVIF_RESULT_BMFF_PARSE_FAILED;
    }
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
    if (needsMini && !miniSeen) {
        return AVIF_RESULT_TRUNCATED_DATA;
    }
#endif
    return AVIF_RESULT_OK;
}

// ---------------------------------------------------------------------------

static avifBool avifFileTypeHasBrand(avifFileType * ftyp, const char * brand)
{
    if (!memcmp(ftyp->majorBrand, brand, 4)) {
        return AVIF_TRUE;
    }

    for (int compatibleBrandIndex = 0; compatibleBrandIndex < ftyp->compatibleBrandsCount; ++compatibleBrandIndex) {
        const uint8_t * compatibleBrand = &ftyp->compatibleBrands[4 * compatibleBrandIndex];
        if (!memcmp(compatibleBrand, brand, 4)) {
            return AVIF_TRUE;
        }
    }
    return AVIF_FALSE;
}

static avifBool avifFileTypeIsCompatible(avifFileType * ftyp)
{
    return avifFileTypeHasBrand(ftyp, "avif") || avifFileTypeHasBrand(ftyp, "avis")
#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
           || avifFileTypeHasBrand(ftyp, "mif3")
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI
        ;
}

avifBool avifPeekCompatibleFileType(const avifROData * input)
{
    BEGIN_STREAM(s, input->data, input->size, NULL, NULL);

    avifBoxHeader header;
    if (!avifROStreamReadBoxHeaderPartial(&s, &header, /*topLevel=*/AVIF_TRUE) || memcmp(header.type, "ftyp", 4)) {
        return AVIF_FALSE;
    }
    if (header.isSizeZeroBox) {
        // The ftyp box goes on till the end of the file. Either there is no brand requiring anything in the file but a
        // FileTypebox (so not AVIF), or it is invalid.
        return AVIF_FALSE;
    }
    AVIF_CHECK(avifROStreamHasBytesLeft(&s, header.size));

    avifFileType ftyp;
    memset(&ftyp, 0, sizeof(avifFileType));
    avifBool parsed = avifParseFileTypeBox(&ftyp, avifROStreamCurrent(&s), header.size, NULL);
    if (!parsed) {
        return AVIF_FALSE;
    }
    return avifFileTypeIsCompatible(&ftyp);
}

static avifBool avifBrandArrayHasBrand(avifBrandArray * brands, const char * brand)
{
    for (uint32_t brandIndex = 0; brandIndex < brands->count; ++brandIndex) {
        if (!memcmp(brands->brand[brandIndex], brand, 4)) {
            return AVIF_TRUE;
        }
    }
    return AVIF_FALSE;
}

// ---------------------------------------------------------------------------

avifDecoder * avifDecoderCreate(void)
{
    avifDecoder * decoder = (avifDecoder *)avifAlloc(sizeof(avifDecoder));
    if (decoder == NULL) {
        return NULL;
    }
    memset(decoder, 0, sizeof(avifDecoder));
    decoder->maxThreads = 1;
    decoder->imageSizeLimit = AVIF_DEFAULT_IMAGE_SIZE_LIMIT;
    decoder->imageDimensionLimit = AVIF_DEFAULT_IMAGE_DIMENSION_LIMIT;
    decoder->imageCountLimit = AVIF_DEFAULT_IMAGE_COUNT_LIMIT;
    decoder->strictFlags = AVIF_STRICT_ENABLED;
    decoder->imageContentToDecode = AVIF_IMAGE_CONTENT_DECODE_DEFAULT;
    return decoder;
}

static void avifDecoderCleanup(avifDecoder * decoder)
{
    if (decoder->data) {
        avifDecoderDataDestroy(decoder->data);
        decoder->data = NULL;
    }

    if (decoder->image) {
        avifImageDestroy(decoder->image);
        decoder->image = NULL;
    }
    avifDiagnosticsClearError(&decoder->diag);
}

void avifDecoderDestroy(avifDecoder * decoder)
{
    avifDecoderCleanup(decoder);
    avifIODestroy(decoder->io);
    avifFree(decoder);
}

avifResult avifDecoderSetSource(avifDecoder * decoder, avifDecoderSource source)
{
    decoder->requestedSource = source;
    return avifDecoderReset(decoder);
}

void avifDecoderSetIO(avifDecoder * decoder, avifIO * io)
{
    avifIODestroy(decoder->io);
    decoder->io = io;
}

avifResult avifDecoderSetIOMemory(avifDecoder * decoder, const uint8_t * data, size_t size)
{
    avifIO * io = avifIOCreateMemoryReader(data, size);
    AVIF_CHECKERR(io != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    avifDecoderSetIO(decoder, io);
    return AVIF_RESULT_OK;
}

avifResult avifDecoderSetIOFile(avifDecoder * decoder, const char * filename)
{
    avifIO * io = avifIOCreateFileReader(filename);
    if (!io) {
        return AVIF_RESULT_IO_ERROR;
    }
    avifDecoderSetIO(decoder, io);
    return AVIF_RESULT_OK;
}

// 0-byte extents are ignored/overwritten during the merge, as they are the signal from helper
// functions that no extent was necessary for this given sample. If both provided extents are
// >0 bytes, this will set dst to be an extent that bounds both supplied extents.
static avifResult avifExtentMerge(avifExtent * dst, const avifExtent * src)
{
    if (!dst->size) {
        *dst = *src;
        return AVIF_RESULT_OK;
    }
    if (!src->size) {
        return AVIF_RESULT_OK;
    }

    const uint64_t minExtent1 = dst->offset;
    const uint64_t maxExtent1 = dst->offset + dst->size;
    const uint64_t minExtent2 = src->offset;
    const uint64_t maxExtent2 = src->offset + src->size;
    dst->offset = AVIF_MIN(minExtent1, minExtent2);
    const uint64_t extentLength = AVIF_MAX(maxExtent1, maxExtent2) - dst->offset;
#if UINT64_MAX > SIZE_MAX
    if (extentLength > SIZE_MAX) {
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
#endif
    dst->size = (size_t)extentLength;
    return AVIF_RESULT_OK;
}

avifResult avifDecoderNthImageMaxExtent(const avifDecoder * decoder, uint32_t frameIndex, avifExtent * outExtent)
{
    if (!decoder->data) {
        // Nothing has been parsed yet
        return AVIF_RESULT_NO_CONTENT;
    }

    memset(outExtent, 0, sizeof(avifExtent));

    uint32_t startFrameIndex = avifDecoderNearestKeyframe(decoder, frameIndex);
    uint32_t endFrameIndex = frameIndex;
    for (uint32_t currentFrameIndex = startFrameIndex; currentFrameIndex <= endFrameIndex; ++currentFrameIndex) {
        for (unsigned int tileIndex = 0; tileIndex < decoder->data->tiles.count; ++tileIndex) {
            avifTile * tile = &decoder->data->tiles.tile[tileIndex];
            if (currentFrameIndex >= tile->input->samples.count) {
                return AVIF_RESULT_NO_IMAGES_REMAINING;
            }

            avifDecodeSample * sample = &tile->input->samples.sample[currentFrameIndex];
            avifExtent sampleExtent;
            if (sample->itemID) {
                // The data comes from an item. Let avifDecoderItemMaxExtent() do the heavy lifting.

                avifDecoderItem * item;
                AVIF_CHECKRES(avifMetaFindOrCreateItem(decoder->data->meta, sample->itemID, &item));
                avifResult maxExtentResult = avifDecoderItemMaxExtent(item, sample, &sampleExtent);
                if (maxExtentResult != AVIF_RESULT_OK) {
                    return maxExtentResult;
                }
            } else {
                // The data likely comes from a sample table. Use the sample position directly.

                sampleExtent.offset = sample->offset;
                sampleExtent.size = sample->size;
            }

            if (sampleExtent.size > UINT64_MAX - sampleExtent.offset) {
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            avifResult extentMergeResult = avifExtentMerge(outExtent, &sampleExtent);
            if (extentMergeResult != AVIF_RESULT_OK) {
                return extentMergeResult;
            }
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifDecoderPrepareSample(avifDecoder * decoder, avifDecodeSample * sample, size_t partialByteCount)
{
    if (!sample->data.size || sample->partialData) {
        // This sample hasn't been read from IO or had its extents fully merged yet.

        size_t bytesToRead = sample->size;
        if (partialByteCount && (bytesToRead > partialByteCount)) {
            bytesToRead = partialByteCount;
        }

        if (sample->itemID) {
            // The data comes from an item. Let avifDecoderItemRead() do the heavy lifting.

            avifDecoderItem * item;
            AVIF_CHECKRES(avifMetaFindOrCreateItem(decoder->data->meta, sample->itemID, &item));
            avifROData itemContents;
#if UINT64_MAX > SIZE_MAX
            if (sample->offset > SIZE_MAX) {
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
#endif
            size_t offset = (size_t)sample->offset;
            avifResult readResult = avifDecoderItemRead(item, decoder->io, &itemContents, offset, bytesToRead, &decoder->diag);
            if (readResult != AVIF_RESULT_OK) {
                return readResult;
            }

            // avifDecoderItemRead is guaranteed to already be persisted by either the underlying IO
            // or by mergedExtents; just reuse the buffer here.
            sample->data = itemContents;
            sample->ownsData = AVIF_FALSE;
            sample->partialData = item->partialMergedExtents;
        } else {
            // The data likely comes from a sample table. Pull the sample and make a copy if necessary.

            avifROData sampleContents;
            if ((decoder->io->sizeHint > 0) && (sample->offset > decoder->io->sizeHint)) {
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            avifResult readResult = decoder->io->read(decoder->io, 0, sample->offset, bytesToRead, &sampleContents);
            if (readResult != AVIF_RESULT_OK) {
                return readResult;
            }
            if (sampleContents.size != bytesToRead) {
                return AVIF_RESULT_TRUNCATED_DATA;
            }

            sample->ownsData = !decoder->io->persistent;
            sample->partialData = (bytesToRead != sample->size);
            if (decoder->io->persistent) {
                sample->data = sampleContents;
            } else {
                AVIF_CHECKRES(avifRWDataSet((avifRWData *)&sample->data, sampleContents.data, sampleContents.size));
            }
        }
    }
    return AVIF_RESULT_OK;
}

// Returns AVIF_TRUE if the item should be skipped. Items should be skipped for one of the following reasons:
//  * Size is 0.
//  * Has an essential property that isn't supported by libavif.
//  * Item is not a single image or a grid.
//  * Item is a thumbnail.
static avifBool avifDecoderItemShouldBeSkipped(const avifDecoderItem * item)
{
    return !item->size || item->hasUnsupportedEssentialProperty ||
           (avifGetCodecType(item->type) == AVIF_CODEC_TYPE_UNKNOWN && memcmp(item->type, "grid", 4)) || item->thumbnailForID != 0;
}

avifResult avifDecoderParse(avifDecoder * decoder)
{
    avifDiagnosticsClearError(&decoder->diag);

    // An imageSizeLimit greater than AVIF_DEFAULT_IMAGE_SIZE_LIMIT and the special value of 0 to
    // disable the limit are not yet implemented.
    if ((decoder->imageSizeLimit > AVIF_DEFAULT_IMAGE_SIZE_LIMIT) || (decoder->imageSizeLimit == 0)) {
        return AVIF_RESULT_NOT_IMPLEMENTED;
    }
    if (!decoder->io || !decoder->io->read) {
        return AVIF_RESULT_IO_NOT_SET;
    }

    // Cleanup anything lingering in the decoder
    avifDecoderCleanup(decoder);

    // -----------------------------------------------------------------------
    // Parse BMFF boxes

    decoder->data = avifDecoderDataCreate();
    AVIF_CHECKERR(decoder->data != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    decoder->data->diag = &decoder->diag;

    AVIF_CHECKRES(avifParse(decoder));

    // Walk the decoded items (if any) and harvest ispe
    avifDecoderData * data = decoder->data;
    for (uint32_t itemIndex = 0; itemIndex < data->meta->items.count; ++itemIndex) {
        avifDecoderItem * item = data->meta->items.item[itemIndex];
        if (avifDecoderItemShouldBeSkipped(item)) {
            continue;
        }

        const avifProperty * ispeProp = avifPropertyArrayFind(&item->properties, "ispe");
        if (ispeProp) {
            item->width = ispeProp->u.ispe.width;
            item->height = ispeProp->u.ispe.height;

            if ((item->width == 0) || (item->height == 0)) {
                avifDiagnosticsPrintf(data->diag, "Item ID [%u] has an invalid size [%ux%u]", item->id, item->width, item->height);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            if (avifDimensionsTooLarge(item->width, item->height, decoder->imageSizeLimit, decoder->imageDimensionLimit)) {
                avifDiagnosticsPrintf(data->diag, "Item ID [%u] dimensions are too large [%ux%u]", item->id, item->width, item->height);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        } else {
            const avifProperty * auxCProp = avifPropertyArrayFind(&item->properties, "auxC");
            if (auxCProp && isAlphaURN(auxCProp->u.auxC.auxType)) {
                if (decoder->strictFlags & AVIF_STRICT_ALPHA_ISPE_REQUIRED) {
                    avifDiagnosticsPrintf(data->diag,
                                          "[Strict] Alpha auxiliary image item ID [%u] is missing a mandatory ispe property",
                                          item->id);
                    return AVIF_RESULT_BMFF_PARSE_FAILED;
                }
            } else {
                avifDiagnosticsPrintf(data->diag, "Item ID [%u] is missing a mandatory ispe property", item->id);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
        }
    }
    return avifDecoderReset(decoder);
}

static avifResult avifCodecCreateInternal(avifCodecChoice choice, const avifTile * tile, avifDiagnostics * diag, avifCodec ** codec)
{
#if defined(AVIF_CODEC_AVM)
    // AVIF_CODEC_CHOICE_AUTO leads to AVIF_CODEC_TYPE_AV1 by default. Reroute correctly.
    if (choice == AVIF_CODEC_CHOICE_AUTO && tile->codecType == AVIF_CODEC_TYPE_AV2) {
        choice = AVIF_CODEC_CHOICE_AVM;
    }
#endif

    const avifCodecType codecTypeFromChoice = avifCodecTypeFromChoice(choice, AVIF_CODEC_FLAG_CAN_DECODE);
    if (codecTypeFromChoice == AVIF_CODEC_TYPE_UNKNOWN) {
        avifDiagnosticsPrintf(diag,
                              "Tile type is %s but there is no compatible codec available to decode it",
                              avifGetConfigurationPropertyName(tile->codecType));
        return AVIF_RESULT_NO_CODEC_AVAILABLE;
    } else if (choice != AVIF_CODEC_CHOICE_AUTO && codecTypeFromChoice != tile->codecType) {
        avifDiagnosticsPrintf(diag,
                              "Tile type is %s but incompatible %s codec was explicitly set as decoding implementation",
                              avifGetConfigurationPropertyName(tile->codecType),
                              avifCodecName(choice, AVIF_CODEC_FLAG_CAN_DECODE));
        return AVIF_RESULT_DECODE_COLOR_FAILED;
    }

    AVIF_CHECKRES(avifCodecCreate(choice, AVIF_CODEC_FLAG_CAN_DECODE, codec));
    AVIF_CHECKERR(*codec, AVIF_RESULT_OUT_OF_MEMORY);
    (*codec)->diag = diag;
    (*codec)->operatingPoint = tile->operatingPoint;
    (*codec)->allLayers = tile->input->allLayers;
    return AVIF_RESULT_OK;
}

static avifBool avifTilesCanBeDecodedWithSameCodecInstance(const avifDecoderData * data)
{
    int32_t numImageBuffers = 0, numStolenImageBuffers = 0;
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        if (data->tileInfos[c].tileCount > 0) {
            ++numImageBuffers;
        }
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
        // The sample operations require multiple buffers for compositing so no plane is stolen
        // when there is a 'sato' Sample Transform derived image item.
        if (c >= AVIF_SAMPLE_TRANSFORM_MIN_CATEGORY && c <= AVIF_SAMPLE_TRANSFORM_MAX_CATEGORY && data->tileInfos[c].tileCount > 0) {
            continue;
        }
#endif
        if (data->tileInfos[c].tileCount == 1) {
            ++numStolenImageBuffers;
        }
    }
    if (numStolenImageBuffers > 0 && numImageBuffers > 1) {
        // Single tile image with single tile alpha plane or gain map. In this case each tile needs its own decoder since the planes will be
        // "stolen". Stealing either the color or the alpha plane (or gain map) will invalidate the other ones when decode is called the second
        // (or third) time.
        return AVIF_FALSE;
    }
    const uint8_t firstTileOperatingPoint = data->tiles.tile[0].operatingPoint;
    const avifBool firstTileAllLayers = data->tiles.tile[0].input->allLayers;
    for (unsigned int i = 1; i < data->tiles.count; ++i) {
        const avifTile * tile = &data->tiles.tile[i];
        if (tile->operatingPoint != firstTileOperatingPoint || tile->input->allLayers != firstTileAllLayers) {
            return AVIF_FALSE;
        }
        // avifDecoderItemValidateProperties() verified during avifDecoderParse() that all tiles
        // share the same coding format so no need to check for codecType equality here.
    }
    return AVIF_TRUE;
}

static avifResult avifDecoderCreateCodecs(avifDecoder * decoder)
{
    avifDecoderData * data = decoder->data;
    avifDecoderDataResetCodec(data);

    if (data->source == AVIF_DECODER_SOURCE_TRACKS) {
        // In this case, we will use at most two codec instances (one for the color planes and one for the alpha plane).
        // Gain maps are not supported.
        AVIF_CHECKRES(avifCodecCreateInternal(decoder->codecChoice, &decoder->data->tiles.tile[0], &decoder->diag, &data->codec));
        data->tiles.tile[0].codec = data->codec;
        if (data->tiles.count > 1) {
            AVIF_CHECKRES(avifCodecCreateInternal(decoder->codecChoice, &decoder->data->tiles.tile[1], &decoder->diag, &data->codecAlpha));
            data->tiles.tile[1].codec = data->codecAlpha;
        }
    } else {
        // In this case, we will use one codec instance when there is only one tile or when all of the following conditions are
        // met:
        //   - The image must have exactly one layer (i.e.) decoder->imageCount == 1.
        //   - All the tiles must have the same operating point (because the codecs take operating point once at initialization
        //     and do not allow it to be changed later).
        //   - All the tiles must have the same value for allLayers (because the codecs take allLayers once at initialization
        //     and do not allow it to be changed later).
        //   - If the image has a single tile, it must not have a single tile alpha plane (in this case we will steal the planes
        //     from the decoder, so we cannot use the same decoder for both the color and the alpha planes).
        //   - All tiles have the same type (AV1 or AV2).
        // Otherwise, we will use |tiles.count| decoder instances (one instance for each tile).
        avifBool canUseSingleCodecInstance = (data->tiles.count == 1) ||
                                             (decoder->imageCount == 1 && avifTilesCanBeDecodedWithSameCodecInstance(data));
        if (canUseSingleCodecInstance) {
            AVIF_CHECKRES(avifCodecCreateInternal(decoder->codecChoice, &decoder->data->tiles.tile[0], &decoder->diag, &data->codec));
            for (unsigned int i = 0; i < decoder->data->tiles.count; ++i) {
                decoder->data->tiles.tile[i].codec = data->codec;
            }
        } else {
            for (unsigned int i = 0; i < decoder->data->tiles.count; ++i) {
                avifTile * tile = &decoder->data->tiles.tile[i];
                AVIF_CHECKRES(avifCodecCreateInternal(decoder->codecChoice, tile, &decoder->diag, &tile->codec));
            }
        }
    }
    return AVIF_RESULT_OK;
}

// Returns the primary color item if found, or NULL.
static avifDecoderItem * avifMetaFindColorItem(avifMeta * meta)
{
    for (uint32_t itemIndex = 0; itemIndex < meta->items.count; ++itemIndex) {
        avifDecoderItem * item = meta->items.item[itemIndex];
        if (avifDecoderItemShouldBeSkipped(item)) {
            continue;
        }
        if (item->id == meta->primaryItemID) {
            return item;
        }
    }
    return NULL;
}

// Returns AVIF_TRUE if item is an alpha auxiliary item of the parent color
// item.
static avifBool avifDecoderItemIsAlphaAux(const avifDecoderItem * item, uint32_t colorItemId)
{
    if (item->auxForID != colorItemId)
        return AVIF_FALSE;
    const avifProperty * auxCProp = avifPropertyArrayFind(&item->properties, "auxC");
    return auxCProp && isAlphaURN(auxCProp->u.auxC.auxType);
}

// Finds the alpha item whose parent item is colorItem and sets it in the alphaItem output parameter. Returns AVIF_RESULT_OK on
// success. Note that *alphaItem can be NULL even if the return value is AVIF_RESULT_OK. If the colorItem is a grid and the alpha
// item is represented as a set of auxl items to each color tile, then a fake item will be created and *isAlphaItemInInput will be
// set to AVIF_FALSE. In this case, the alpha item merely exists to hold the locations of the alpha tile items. The data of this
// item need not be read and the pixi property cannot be validated. Otherwise, *isAlphaItemInInput will be set to AVIF_TRUE when
// *alphaItem is not NULL.
static avifResult avifMetaFindAlphaItem(avifMeta * meta,
                                        const avifDecoderItem * colorItem,
                                        const avifTileInfo * colorInfo,
                                        avifDecoderItem ** alphaItem,
                                        avifTileInfo * alphaInfo,
                                        avifBool * isAlphaItemInInput)
{
    for (uint32_t itemIndex = 0; itemIndex < meta->items.count; ++itemIndex) {
        avifDecoderItem * item = meta->items.item[itemIndex];
        if (avifDecoderItemShouldBeSkipped(item)) {
            continue;
        }
        if (avifDecoderItemIsAlphaAux(item, colorItem->id)) {
            *alphaItem = item;
            *isAlphaItemInInput = AVIF_TRUE;
            return AVIF_RESULT_OK;
        }
    }
    if (memcmp(colorItem->type, "grid", 4)) {
        *alphaItem = NULL;
        *isAlphaItemInInput = AVIF_FALSE;
        return AVIF_RESULT_OK;
    }
    // If color item is a grid, check if there is an alpha channel which is represented as an auxl item to each color tile item.
    const uint32_t tileCount = colorInfo->grid.rows * colorInfo->grid.columns;
    if (tileCount == 0) {
        *alphaItem = NULL;
        *isAlphaItemInInput = AVIF_FALSE;
        return AVIF_RESULT_OK;
    }
    // Keep the same 'dimg' order as it defines where each tile is located in the reconstructed image.
    uint32_t * dimgIdxToAlphaItemIdx = (uint32_t *)avifAlloc(tileCount * sizeof(uint32_t));
    AVIF_CHECKERR(dimgIdxToAlphaItemIdx != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    const uint32_t itemIndexNotSet = UINT32_MAX;
    for (uint32_t dimgIdx = 0; dimgIdx < tileCount; ++dimgIdx) {
        dimgIdxToAlphaItemIdx[dimgIdx] = itemIndexNotSet;
    }
    uint32_t alphaItemCount = 0;
    for (uint32_t i = 0; i < meta->items.count; ++i) {
        const avifDecoderItem * const item = meta->items.item[i];
        if (item->dimgForID == colorItem->id) {
            avifBool seenAlphaForCurrentItem = AVIF_FALSE;
            for (uint32_t j = 0; j < meta->items.count; ++j) {
                avifDecoderItem * auxlItem = meta->items.item[j];
                if (avifDecoderItemIsAlphaAux(auxlItem, item->id)) {
                    if (seenAlphaForCurrentItem || auxlItem->dimgForID != 0 || item->dimgIdx >= tileCount ||
                        dimgIdxToAlphaItemIdx[item->dimgIdx] != itemIndexNotSet) {
                        // One of the following invalid cases:
                        // * Multiple items are claiming to be the alpha auxiliary of the current item.
                        // * Alpha auxiliary is dimg for another item.
                        // * There are too many items in the dimg array (also checked later in avifFillDimgIdxToItemIdxArray()).
                        // * There is a repetition in the dimg array (also checked later in avifFillDimgIdxToItemIdxArray()).
                        avifFree(dimgIdxToAlphaItemIdx);
                        return AVIF_RESULT_INVALID_IMAGE_GRID;
                    }
                    dimgIdxToAlphaItemIdx[item->dimgIdx] = j;
                    ++alphaItemCount;
                    seenAlphaForCurrentItem = AVIF_TRUE;
                }
            }
            if (!seenAlphaForCurrentItem) {
                // No alpha auxiliary item was found for the current item. Treat this as an image without alpha.
                avifFree(dimgIdxToAlphaItemIdx);
                *alphaItem = NULL;
                *isAlphaItemInInput = AVIF_FALSE;
                return AVIF_RESULT_OK;
            }
        }
    }
    if (alphaItemCount != tileCount) {
        avifFree(dimgIdxToAlphaItemIdx);
        return AVIF_RESULT_INVALID_IMAGE_GRID;
    }
    // Find an unused ID.
    avifResult result;
    if (meta->items.count >= UINT32_MAX - 1) {
        // In the improbable case where all IDs are used.
        result = AVIF_RESULT_DECODE_ALPHA_FAILED;
    } else {
        uint32_t newItemID = 0;
        avifBool isUsed;
        do {
            ++newItemID;
            isUsed = AVIF_FALSE;
            for (uint32_t i = 0; i < meta->items.count; ++i) {
                if (meta->items.item[i]->id == newItemID) {
                    isUsed = AVIF_TRUE;
                    break;
                }
            }
        } while (isUsed && newItemID != 0);
        result = avifMetaFindOrCreateItem(meta, newItemID, alphaItem); // Create new empty item.
    }
    if (result != AVIF_RESULT_OK) {
        avifFree(dimgIdxToAlphaItemIdx);
        return result;
    }
    memcpy((*alphaItem)->type, "grid", 4); // Make it a grid and register alpha items as its tiles.
    (*alphaItem)->width = colorItem->width;
    (*alphaItem)->height = colorItem->height;
    for (uint32_t dimgIdx = 0; dimgIdx < tileCount; ++dimgIdx) {
        if (dimgIdxToAlphaItemIdx[dimgIdx] >= meta->items.count) {
            avifFree(dimgIdxToAlphaItemIdx);
            AVIF_ASSERT_OR_RETURN(AVIF_FALSE);
        }
        avifDecoderItem * alphaTileItem = meta->items.item[dimgIdxToAlphaItemIdx[dimgIdx]];
        alphaTileItem->dimgForID = (*alphaItem)->id;
        alphaTileItem->dimgIdx = dimgIdx;
    }
    avifFree(dimgIdxToAlphaItemIdx);
    *isAlphaItemInInput = AVIF_FALSE;
    alphaInfo->grid = colorInfo->grid;
    return AVIF_RESULT_OK;
}

// On success, this function returns AVIF_RESULT_OK and does the following:
// * If a nclx property was found in |properties|:
//   - Set |*colorPrimaries|, |*transferCharacteristics|, |*matrixCoefficients|
//     and |*yuvRange|.
//   - If cicpSet is not NULL, set |*cicpSet| to AVIF_TRUE.
// This function fails if more than one nclx property is found in |properties|.
// The output parameters may be populated even in case of failure and must be
// ignored.
static avifResult avifReadColorNclxProperty(const avifPropertyArray * properties,
                                            avifColorPrimaries * colorPrimaries,
                                            avifTransferCharacteristics * transferCharacteristics,
                                            avifMatrixCoefficients * matrixCoefficients,
                                            avifRange * yuvRange,
                                            avifBool * cicpSet)
{
    avifBool colrNCLXSeen = AVIF_FALSE;
    for (uint32_t propertyIndex = 0; propertyIndex < properties->count; ++propertyIndex) {
        avifProperty * prop = &properties->prop[propertyIndex];
        if (!memcmp(prop->type, "colr", 4) && prop->u.colr.hasNCLX) {
            if (colrNCLXSeen) {
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            colrNCLXSeen = AVIF_TRUE;
            if (cicpSet != NULL) {
                *cicpSet = AVIF_TRUE;
            }
            *colorPrimaries = prop->u.colr.colorPrimaries;
            *transferCharacteristics = prop->u.colr.transferCharacteristics;
            *matrixCoefficients = prop->u.colr.matrixCoefficients;
            *yuvRange = prop->u.colr.range;
        }
    }
    return AVIF_RESULT_OK;
}

// On success, this function returns AVIF_RESULT_OK and does the following:
// * If a colr property was found in |properties|:
//   - Read the icc data into |icc| from |io|.
//   - Sets the CICP values as documented in avifReadColorNclxProperty().
// This function fails if more than one icc or nclx property is found in
// |properties|. The output parameters may be populated even in case of failure
// and must be ignored (and the |icc| object may need to be freed).
static avifResult avifReadColorProperties(avifIO * io,
                                          const avifPropertyArray * properties,
                                          avifRWData * icc,
                                          avifColorPrimaries * colorPrimaries,
                                          avifTransferCharacteristics * transferCharacteristics,
                                          avifMatrixCoefficients * matrixCoefficients,
                                          avifRange * yuvRange,
                                          avifBool * cicpSet)
{
    // Find and adopt all colr boxes "at most one for a given value of colour type" (HEIF 6.5.5.1, from Amendment 3)
    // Accept one of each type, and bail out if more than one of a given type is provided.
    avifBool colrICCSeen = AVIF_FALSE;
    for (uint32_t propertyIndex = 0; propertyIndex < properties->count; ++propertyIndex) {
        avifProperty * prop = &properties->prop[propertyIndex];
        if (!memcmp(prop->type, "colr", 4) && prop->u.colr.hasICC) {
            if (colrICCSeen) {
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            avifROData iccRead;
            AVIF_CHECKRES(io->read(io, 0, prop->u.colr.iccOffset, prop->u.colr.iccSize, &iccRead));
            colrICCSeen = AVIF_TRUE;
            AVIF_CHECKRES(avifRWDataSet(icc, iccRead.data, iccRead.size));
        }
    }
    return avifReadColorNclxProperty(properties, colorPrimaries, transferCharacteristics, matrixCoefficients, yuvRange, cicpSet);
}

// Finds a 'tmap' (tone mapped image item) box associated with the given 'colorItem'.
// If found, fills 'toneMappedImageItem' and  sets 'gainMapItemID' to the id of the gain map
// item associated with the box. Otherwise, sets 'toneMappedImageItem' to NULL.
// Returns AVIF_RESULT_OK if no errors were encountered (whether or not a tmap box was found).
// Assumes that there is a single tmap item, and not, e.g., a grid of tmap items.
// TODO(maryla): add support for files with multiple tmap items if it gets allowed by the spec.
static avifResult avifDecoderDataFindToneMappedImageItem(const avifDecoderData * data,
                                                         const avifDecoderItem * colorItem,
                                                         avifDecoderItem ** toneMappedImageItem,
                                                         uint32_t * gainMapItemID)
{
    for (uint32_t itemIndex = 0; itemIndex < data->meta->items.count; ++itemIndex) {
        avifDecoderItem * item = data->meta->items.item[itemIndex];
        if (!item->size || item->hasUnsupportedEssentialProperty || item->thumbnailForID != 0) {
            continue;
        }
        if (!memcmp(item->type, "tmap", 4)) {
            // The tmap box should be associated (via 'iref'->'dimg') to two items:
            // the first one is the base image, the second one is the gain map.
            uint32_t dimgItemIDs[2] = { 0, 0 };
            uint32_t numDimgItemIDs = 0;
            for (uint32_t otherItemIndex = 0; otherItemIndex < data->meta->items.count; ++otherItemIndex) {
                avifDecoderItem * otherItem = data->meta->items.item[otherItemIndex];
                if (otherItem->dimgForID != item->id) {
                    continue;
                }
                if (otherItem->dimgIdx < 2) {
                    AVIF_ASSERT_OR_RETURN(dimgItemIDs[otherItem->dimgIdx] == 0);
                    dimgItemIDs[otherItem->dimgIdx] = otherItem->id;
                }
                numDimgItemIDs++;
            }
            // Even with numDimgItemIDs == 2, one of the ids could be 0 if there are duplicate entries in the 'dimg' box.
            if (numDimgItemIDs != 2 || dimgItemIDs[0] == 0 || dimgItemIDs[1] == 0) {
                avifDiagnosticsPrintf(data->diag, "box[dimg] for 'tmap' item %d must have exactly 2 entries with distinct ids", item->id);
                return AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE;
            }
            if (dimgItemIDs[0] != colorItem->id) {
                continue;
            }

            *toneMappedImageItem = item;
            *gainMapItemID = dimgItemIDs[1];
            return AVIF_RESULT_OK;
        }
    }
    *toneMappedImageItem = NULL;
    *gainMapItemID = 0;
    return AVIF_RESULT_OK;
}

// Returns AVIF_TRUE if the two entity ids (usually item ids) are part of an
// 'altr' group (representing entities that are alternatives of each other)
// with 'id1' appearing before 'id2' (meaning that 'id1' should be preferred).
static avifBool avifIsPreferredAlternativeTo(const avifDecoderData * data, uint32_t id1, uint32_t id2)
{
    for (uint32_t i = 0; i < data->meta->entityToGroups.count; ++i) {
        avifEntityToGroup * group = &data->meta->entityToGroups.groups[i];
        if (memcmp(group->groupingType, "altr", 4) != 0) {
            continue;
        }
        avifBool id1Found = AVIF_FALSE;
        for (uint32_t j = 0; j < group->entityIDs.count; ++j) {
            if (group->entityIDs.ids[j] == id1) {
                id1Found = AVIF_TRUE;
            } else if (group->entityIDs.ids[j] == id2) {
                // Assume id2 is only present in one altr group, as per ISO/IEC 14496-12:2022
                // Section 8.15.3.1:
                // Any entity_id value shall be mapped to only one grouping of type 'altr'.
                return id1Found;
            }
        }
    }
    return AVIF_FALSE;
}

// Finds a 'tmap' (tone mapped image item) box associated with the given 'colorItem',
// then finds the associated gain map image.
// If found, fills 'toneMappedImageItem', 'gainMapItem' and 'gainMapCodecType', and
// allocates and fills metadata in decoder->image->gainMap.
// Otherwise, sets 'toneMappedImageItem' and 'gainMapItem' to NULL.
// Returns AVIF_RESULT_OK if no errors were encountered (whether or not a gain map was found).
// Assumes that there is a single tmap item, and not, e.g., a grid of tmap items.
static avifResult avifDecoderFindGainMapItem(const avifDecoder * decoder,
                                             const avifDecoderItem * colorItem,
                                             avifDecoderItem ** toneMappedImageItem,
                                             avifDecoderItem ** gainMapItem,
                                             avifCodecType * gainMapCodecType)
{
    *toneMappedImageItem = NULL;
    *gainMapItem = NULL;
    *gainMapCodecType = AVIF_CODEC_TYPE_UNKNOWN;

    avifDecoderData * data = decoder->data;

    uint32_t gainMapItemID;
    avifDecoderItem * toneMappedImageItemTmp;
    AVIF_CHECKRES(avifDecoderDataFindToneMappedImageItem(data, colorItem, &toneMappedImageItemTmp, &gainMapItemID));
    if (!toneMappedImageItemTmp) {
        return AVIF_RESULT_OK;
    }

    if (!avifIsPreferredAlternativeTo(data, toneMappedImageItemTmp->id, colorItem->id)) {
        return AVIF_RESULT_OK;
    }

    AVIF_ASSERT_OR_RETURN(gainMapItemID != 0);
    avifDecoderItem * gainMapItemTmp;
    AVIF_CHECKRES(avifMetaFindOrCreateItem(data->meta, gainMapItemID, &gainMapItemTmp));
    if (avifDecoderItemShouldBeSkipped(gainMapItemTmp)) {
        avifDiagnosticsPrintf(data->diag, "Box[tmap] gain map item %d is not a supported image type", gainMapItemID);
        return AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE;
    }

    AVIF_CHECKRES(avifDecoderItemReadAndParse(decoder,
                                              gainMapItemTmp,
                                              /*isItemInInput=*/AVIF_TRUE,
                                              &data->tileInfos[AVIF_ITEM_GAIN_MAP].grid,
                                              gainMapCodecType));

    decoder->image->gainMap = avifGainMapCreate();
    AVIF_CHECKERR(decoder->image->gainMap, AVIF_RESULT_OUT_OF_MEMORY);

    avifGainMap * const gainMap = decoder->image->gainMap;
    AVIF_CHECKRES(avifReadColorProperties(decoder->io,
                                          &toneMappedImageItemTmp->properties,
                                          &gainMap->altICC,
                                          &gainMap->altColorPrimaries,
                                          &gainMap->altTransferCharacteristics,
                                          &gainMap->altMatrixCoefficients,
                                          &gainMap->altYUVRange,
                                          /*cicpSet=*/NULL));

    const avifProperty * clliProp = avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "clli");
    if (clliProp) {
        gainMap->altCLLI = clliProp->u.clli;
    }

    const avifProperty * pixiProp = avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "pixi");
    if (pixiProp) {
        gainMap->altPlaneCount = pixiProp->u.pixi.planeCount;
        gainMap->altDepth = pixiProp->u.pixi.planeDepths[0];
    }

    const avifProperty * ispeProp = avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "ispe");
    if (!ispeProp) {
        // HEIF (ISO/IEC 23008-12:2022), Section 6.5.3.1:
        // Every image item shall be associated with one property of this type, prior to the association
        // of all transformative properties.
        avifDiagnosticsPrintf(data->diag, "Box[tmap] missing mandatory ispe property");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    if (ispeProp->u.ispe.width != colorItem->width || ispeProp->u.ispe.height != colorItem->height) {
        avifDiagnosticsPrintf(data->diag, "Box[tmap] ispe property width/height does not match base image");
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }

    if (avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "pasp") ||
        avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "clap") ||
        avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "irot") ||
        avifPropertyArrayFind(&toneMappedImageItemTmp->properties, "imir")) {
        // libavif requires the bitstream contain the same pasp, clap, irot, imir
        // properties for both the base and gain map image items used as input to
        // the tone-mapped derived image item. libavif also requires the tone-mapped
        // derived image item itself not be associated with these properties. This is
        // enforced at encoding. Other patterns are rejected at decoding.
        avifDiagnosticsPrintf(data->diag,
                              "Box[tmap] 'pasp', 'clap', 'irot' and 'imir' properties must be associated with base and gain map items instead of 'tmap'");
        return AVIF_RESULT_INVALID_TONE_MAPPED_IMAGE;
    }

    if (decoder->imageContentToDecode & AVIF_IMAGE_CONTENT_GAIN_MAP) {
        gainMap->image = avifImageCreateEmpty();
        AVIF_CHECKERR(gainMap->image, AVIF_RESULT_OUT_OF_MEMORY);

        // Look for a colr nclx box. Other colr box types (e.g. ICC) are not supported.
        AVIF_CHECKRES(avifReadColorNclxProperty(&gainMapItemTmp->properties,
                                                &gainMap->image->colorPrimaries,
                                                &gainMap->image->transferCharacteristics,
                                                &gainMap->image->matrixCoefficients,
                                                &gainMap->image->yuvRange,
                                                /*cicpSet=*/NULL));
    }

    // Only set the output parameters after everything has been validated.
    *toneMappedImageItem = toneMappedImageItemTmp;
    *gainMapItem = gainMapItemTmp;
    return AVIF_RESULT_OK;
}

static avifResult aviDecoderCheckGainMapProperties(avifDecoder * decoder, const avifPropertyArray * gainMapProperties)
{
    const avifImage * image = decoder->image;
    // libavif requires the bitstream contain the same 'pasp', 'clap', 'irot', 'imir'
    // properties for both the base and gain map image items used as input to
    // the tone-mapped derived image item. libavif also requires the tone-mapped
    // derived image item itself not be associated with these properties. This is
    // enforced at encoding. Other patterns are rejected at decoding.
    const avifProperty * paspProp = avifPropertyArrayFind(gainMapProperties, "pasp");
    if (!paspProp != !(image->transformFlags & AVIF_TRANSFORM_PASP) ||
        (paspProp && (paspProp->u.pasp.hSpacing != image->pasp.hSpacing || paspProp->u.pasp.vSpacing != image->pasp.vSpacing))) {
        avifDiagnosticsPrintf(&decoder->diag,
                              "Pixel aspect ratio property mismatch between input items of tone-mapping derived image item");
        return AVIF_RESULT_DECODE_GAIN_MAP_FAILED;
    }
    const avifProperty * clapProp = avifPropertyArrayFind(gainMapProperties, "clap");
    if (!clapProp != !(image->transformFlags & AVIF_TRANSFORM_CLAP) ||
        (clapProp && (clapProp->u.clap.widthN != image->clap.widthN || clapProp->u.clap.widthD != image->clap.widthD ||
                      clapProp->u.clap.heightN != image->clap.heightN || clapProp->u.clap.heightD != image->clap.heightD ||
                      clapProp->u.clap.horizOffN != image->clap.horizOffN || clapProp->u.clap.horizOffD != image->clap.horizOffD ||
                      clapProp->u.clap.vertOffN != image->clap.vertOffN || clapProp->u.clap.vertOffD != image->clap.vertOffD))) {
        avifDiagnosticsPrintf(&decoder->diag, "Clean aperture property mismatch between input items of tone-mapping derived image item");
        return AVIF_RESULT_DECODE_GAIN_MAP_FAILED;
    }
    const avifProperty * irotProp = avifPropertyArrayFind(gainMapProperties, "irot");
    if (!irotProp != !(image->transformFlags & AVIF_TRANSFORM_IROT) || (irotProp && irotProp->u.irot.angle != image->irot.angle)) {
        avifDiagnosticsPrintf(&decoder->diag, "Rotation property mismatch between input items of tone-mapping derived image item");
        return AVIF_RESULT_DECODE_GAIN_MAP_FAILED;
    }
    const avifProperty * imirProp = avifPropertyArrayFind(gainMapProperties, "imir");
    if (!imirProp != !(image->transformFlags & AVIF_TRANSFORM_IMIR) || (imirProp && imirProp->u.imir.axis != image->imir.axis)) {
        avifDiagnosticsPrintf(&decoder->diag, "Mirroring property mismatch between input items of tone-mapping derived image item");
        return AVIF_RESULT_DECODE_GAIN_MAP_FAILED;
    }
    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
// Finds a 'sato' Sample Transform derived image item box.
// If found, fills 'sampleTransformItem'. Otherwise, sets 'sampleTransformItem' to NULL.
// Returns AVIF_RESULT_OK on success (whether or not a 'sato' box was found).
// Assumes that there is a single 'sato' item.
// Assumes that the 'sato' item is not the primary item and that both the primary item and 'sato'
// are in the same 'altr' group.
// TODO(yguyon): Check instead of assuming.
static avifResult avifDecoderDataFindSampleTransformImageItem(avifDecoderData * data, avifDecoderItem ** sampleTransformItem)
{
    for (uint32_t itemIndex = 0; itemIndex < data->meta->items.count; ++itemIndex) {
        avifDecoderItem * item = data->meta->items.item[itemIndex];
        if (!item->size || item->hasUnsupportedEssentialProperty || item->thumbnailForID != 0) {
            continue;
        }
        if (!memcmp(item->type, "sato", 4)) {
            *sampleTransformItem = item;
            return AVIF_RESULT_OK;
        }
    }
    *sampleTransformItem = NULL;
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

static avifResult avifDecoderGenerateImageTiles(avifDecoder * decoder, avifTileInfo * info, avifDecoderItem * item, avifItemCategory itemCategory)
{
    const uint32_t previousTileCount = decoder->data->tiles.count;
    if ((info->grid.rows > 0) && (info->grid.columns > 0)) {
        // The number of tiles was verified in avifDecoderItemReadAndParse().
        const uint32_t numTiles = info->grid.rows * info->grid.columns;
        uint32_t * dimgIdxToItemIdx = (uint32_t *)avifAlloc(numTiles * sizeof(uint32_t));
        AVIF_CHECKERR(dimgIdxToItemIdx != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        avifResult result = avifFillDimgIdxToItemIdxArray(dimgIdxToItemIdx, numTiles, item);
        if (result == AVIF_RESULT_OK) {
            result = avifDecoderGenerateImageGridTiles(decoder, item, itemCategory, dimgIdxToItemIdx, numTiles);
        }
        avifFree(dimgIdxToItemIdx);
        AVIF_CHECKRES(result);
    } else {
        AVIF_CHECKERR(item->size != 0, AVIF_RESULT_MISSING_IMAGE_ITEM);

        const avifCodecType codecType = avifGetCodecType(item->type);
        AVIF_ASSERT_OR_RETURN(codecType != AVIF_CODEC_TYPE_UNKNOWN);
        avifTile * tile =
            avifDecoderDataCreateTile(decoder->data, codecType, item->width, item->height, avifDecoderItemOperatingPoint(item));
        AVIF_CHECKERR(tile, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKRES(avifCodecDecodeInputFillFromDecoderItem(tile->input,
                                                              item,
                                                              decoder->allowProgressive,
                                                              decoder->imageCountLimit,
                                                              decoder->io->sizeHint,
                                                              &decoder->diag));
        tile->input->itemCategory = itemCategory;
    }
    info->tileCount = decoder->data->tiles.count - previousTileCount;
    return AVIF_RESULT_OK;
}

// Populates depth, yuvFormat and yuvChromaSamplePosition fields on 'image' based on data from the codec config property (e.g. "av1C").
static avifResult avifReadCodecConfigProperty(avifImage * image, const avifPropertyArray * properties, avifCodecType codecType)
{
    const avifProperty * configProp = avifPropertyArrayFind(properties, avifGetConfigurationPropertyName(codecType));
    if (configProp) {
        image->depth = avifCodecConfigurationBoxGetDepth(&configProp->u.av1C);
        if (configProp->u.av1C.monochrome) {
            image->yuvFormat = AVIF_PIXEL_FORMAT_YUV400;
        } else {
            if (configProp->u.av1C.chromaSubsamplingX && configProp->u.av1C.chromaSubsamplingY) {
                image->yuvFormat = AVIF_PIXEL_FORMAT_YUV420;
            } else if (configProp->u.av1C.chromaSubsamplingX) {
                image->yuvFormat = AVIF_PIXEL_FORMAT_YUV422;
            } else {
                image->yuvFormat = AVIF_PIXEL_FORMAT_YUV444;
            }
        }
        image->yuvChromaSamplePosition = (avifChromaSamplePosition)configProp->u.av1C.chromaSamplePosition;
    } else {
        // A configuration property box is mandatory in all valid AVIF configurations. Bail out.
        return AVIF_RESULT_BMFF_PARSE_FAILED;
    }
    return AVIF_RESULT_OK;
}

avifResult avifDecoderReset(avifDecoder * decoder)
{
    avifDiagnosticsClearError(&decoder->diag);

    avifDecoderData * data = decoder->data;
    if (!data) {
        // Nothing to reset.
        return AVIF_RESULT_OK;
    }

    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        memset(&data->tileInfos[c].grid, 0, sizeof(data->tileInfos[c].grid));
    }
    avifDecoderDataClearTiles(data);

    // Prepare / cleanup decoded image state
    if (decoder->image) {
        avifImageDestroy(decoder->image);
    }
    decoder->image = avifImageCreateEmpty();
    AVIF_CHECKERR(decoder->image, AVIF_RESULT_OUT_OF_MEMORY);
    decoder->progressiveState = AVIF_PROGRESSIVE_STATE_UNAVAILABLE;
    data->cicpSet = AVIF_FALSE;

    memset(&decoder->ioStats, 0, sizeof(decoder->ioStats));

    // -----------------------------------------------------------------------
    // Build decode input

    data->sourceSampleTable = NULL; // Reset
    if (decoder->requestedSource == AVIF_DECODER_SOURCE_AUTO) {
        // Honor the major brand (avif or avis) if present, otherwise prefer avis (tracks) if possible.
        if (!memcmp(data->majorBrand, "avis", 4)) {
            data->source = AVIF_DECODER_SOURCE_TRACKS;
        } else if (!memcmp(data->majorBrand, "avif", 4)) {
            data->source = AVIF_DECODER_SOURCE_PRIMARY_ITEM;
        } else if (data->tracks.count > 0) {
            data->source = AVIF_DECODER_SOURCE_TRACKS;
        } else {
            data->source = AVIF_DECODER_SOURCE_PRIMARY_ITEM;
        }
    } else {
        data->source = decoder->requestedSource;
    }

    avifCodecType colorCodecType = AVIF_CODEC_TYPE_UNKNOWN;
    const avifPropertyArray * colorProperties = NULL;
    const avifPropertyArray * gainMapProperties = NULL;
    if (data->source == AVIF_DECODER_SOURCE_TRACKS) {
        avifTrack * colorTrack = NULL;
        avifTrack * alphaTrack = NULL;

        // Find primary track - this probably needs some better detection
        uint32_t colorTrackIndex = 0;
        for (; colorTrackIndex < data->tracks.count; ++colorTrackIndex) {
            avifTrack * track = &data->tracks.track[colorTrackIndex];
            if (!track->sampleTable) {
                continue;
            }
            if (!track->id) { // trak box might be missing a tkhd box inside, skip it
                continue;
            }
            if (!track->sampleTable->chunks.count) {
                continue;
            }
            colorCodecType = avifSampleTableGetCodecType(track->sampleTable);
            if (colorCodecType == AVIF_CODEC_TYPE_UNKNOWN) {
                continue;
            }
            if (track->auxForID != 0) {
                continue;
            }
            // HEIF (ISO/IEC 23008-12:2022), Section 7.1:
            //   In order to distinguish image sequences from video, the handler type in the
            //   HandlerBox of the track is 'pict' to indicate an image sequence track.
            // But we do not check the handler type because it may break some existing files.

            // Found one!
            break;
        }
        if (colorTrackIndex == data->tracks.count) {
            avifDiagnosticsPrintf(&decoder->diag, "Failed to find AV1 color track");
            return AVIF_RESULT_NO_CONTENT;
        }
        colorTrack = &data->tracks.track[colorTrackIndex];

        colorProperties = avifSampleTableGetProperties(colorTrack->sampleTable, colorCodecType);
        if (!colorProperties) {
            avifDiagnosticsPrintf(&decoder->diag, "Failed to find AV1 color track's color properties");
            return AVIF_RESULT_BMFF_PARSE_FAILED;
        }

        // Find Exif and/or XMP metadata, if any
        if (colorTrack->meta) {
            // See the comment above avifDecoderFindMetadata() for the explanation of using 0 here
            avifResult findResult = avifDecoderFindMetadata(decoder, colorTrack->meta, decoder->image, 0);
            if (findResult != AVIF_RESULT_OK) {
                return findResult;
            }
        }

        uint32_t alphaTrackIndex = 0;
        avifCodecType alphaCodecType = AVIF_CODEC_TYPE_UNKNOWN;
        for (; alphaTrackIndex < data->tracks.count; ++alphaTrackIndex) {
            avifTrack * track = &data->tracks.track[alphaTrackIndex];
            if (!track->sampleTable) {
                continue;
            }
            if (!track->id) {
                continue;
            }
            if (!track->sampleTable->chunks.count) {
                continue;
            }
            alphaCodecType = avifSampleTableGetCodecType(track->sampleTable);
            if (alphaCodecType == AVIF_CODEC_TYPE_UNKNOWN) {
                continue;
            }
            const avifPropertyArray * alphaProperties = avifSampleTableGetProperties(track->sampleTable, alphaCodecType);
            const avifProperty * auxiProp = alphaProperties ? avifPropertyArrayFind(alphaProperties, "auxi") : NULL;
            // If auxi is present, check that it contains the alpha URN.
            // If auxi is not present, assume that the track is alpha. This is for backward compatibility with
            // old versions of libavif that did not write this property, see
            // https://github.com/AOMediaCodec/libavif/commit/98faa17
            if (auxiProp && !isAlphaURN(auxiProp->u.auxC.auxType)) {
                continue;
            }
            // Do not check the track's handlerType. It should be "auxv" according to
            // HEIF (ISO/IEC 23008-12:2022), Section 7.5.3.1, but old versions of libavif used to write
            // "pict" instead. See https://github.com/AOMediaCodec/libavif/commit/65d0af9

            if (track->auxForID == colorTrack->id) {
                // Found it!
                break;
            }
        }
        if (alphaTrackIndex != data->tracks.count) {
            alphaTrack = &data->tracks.track[alphaTrackIndex];
        }

        const uint8_t operatingPoint = 0; // No way to set operating point via tracks
        avifTile * colorTile = avifDecoderDataCreateTile(data, colorCodecType, colorTrack->width, colorTrack->height, operatingPoint);
        AVIF_CHECKERR(colorTile != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKRES(avifCodecDecodeInputFillFromSampleTable(colorTile->input,
                                                              colorTrack->sampleTable,
                                                              decoder->imageCountLimit,
                                                              decoder->io->sizeHint,
                                                              data->diag));
        data->tileInfos[AVIF_ITEM_COLOR].tileCount = 1;

        if (alphaTrack) {
            avifTile * alphaTile = avifDecoderDataCreateTile(data, alphaCodecType, alphaTrack->width, alphaTrack->height, operatingPoint);
            AVIF_CHECKERR(alphaTile != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            AVIF_CHECKRES(avifCodecDecodeInputFillFromSampleTable(alphaTile->input,
                                                                  alphaTrack->sampleTable,
                                                                  decoder->imageCountLimit,
                                                                  decoder->io->sizeHint,
                                                                  data->diag));
            alphaTile->input->itemCategory = AVIF_ITEM_ALPHA;
            data->tileInfos[AVIF_ITEM_ALPHA].tileCount = 1;
        }

        // Stash off sample table for future timing information
        data->sourceSampleTable = colorTrack->sampleTable;

        // Image sequence timing
        decoder->imageIndex = -1;
        decoder->imageCount = (int)colorTile->input->samples.count;
        decoder->timescale = colorTrack->mediaTimescale;
        decoder->durationInTimescales = colorTrack->mediaDuration;
        if (colorTrack->mediaTimescale) {
            decoder->duration = (double)decoder->durationInTimescales / (double)colorTrack->mediaTimescale;
        } else {
            decoder->duration = 0;
        }
        // If the alphaTrack->repetitionCount and colorTrack->repetitionCount are different, we will simply use the
        // colorTrack's repetitionCount.
        decoder->repetitionCount = colorTrack->repetitionCount;

        memset(&decoder->imageTiming, 0, sizeof(decoder->imageTiming)); // to be set in avifDecoderNextImage()

        decoder->image->width = colorTrack->width;
        decoder->image->height = colorTrack->height;
        decoder->alphaPresent = (alphaTrack != NULL);
        decoder->image->alphaPremultiplied = decoder->alphaPresent && (colorTrack->premByID == alphaTrack->id);
    } else {
        // Create from items

        if (data->meta->primaryItemID == 0) {
            // A primary item is required
            avifDiagnosticsPrintf(&decoder->diag, "Primary item not specified");
            return AVIF_RESULT_MISSING_IMAGE_ITEM;
        }

        // Main item of each group category (top-level item such as grid or single tile), if any.
        avifDecoderItem * mainItems[AVIF_ITEM_CATEGORY_COUNT];
        avifCodecType codecType[AVIF_ITEM_CATEGORY_COUNT];
        for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
            mainItems[c] = NULL;
            codecType[c] = AVIF_CODEC_TYPE_UNKNOWN;
        }

        // Mandatory primary color item
        mainItems[AVIF_ITEM_COLOR] = avifMetaFindColorItem(data->meta);
        if (!mainItems[AVIF_ITEM_COLOR]) {
            avifDiagnosticsPrintf(&decoder->diag, "Primary item not found");
            return AVIF_RESULT_MISSING_IMAGE_ITEM;
        }
        AVIF_CHECKRES(avifDecoderItemReadAndParse(decoder,
                                                  mainItems[AVIF_ITEM_COLOR],
                                                  /*isItemInInput=*/AVIF_TRUE,
                                                  &data->tileInfos[AVIF_ITEM_COLOR].grid,
                                                  &codecType[AVIF_ITEM_COLOR]));
        colorProperties = &mainItems[AVIF_ITEM_COLOR]->properties;
        colorCodecType = codecType[AVIF_ITEM_COLOR];

        // Optional alpha auxiliary item
        avifBool isAlphaItemInInput;
        AVIF_CHECKRES(avifMetaFindAlphaItem(data->meta,
                                            mainItems[AVIF_ITEM_COLOR],
                                            &data->tileInfos[AVIF_ITEM_COLOR],
                                            &mainItems[AVIF_ITEM_ALPHA],
                                            &data->tileInfos[AVIF_ITEM_ALPHA],
                                            &isAlphaItemInInput));
        if (mainItems[AVIF_ITEM_ALPHA]) {
            AVIF_CHECKRES(avifDecoderItemReadAndParse(decoder,
                                                      mainItems[AVIF_ITEM_ALPHA],
                                                      isAlphaItemInInput,
                                                      &data->tileInfos[AVIF_ITEM_ALPHA].grid,
                                                      &codecType[AVIF_ITEM_ALPHA]));
        }

        // Section 10.2.6 of 23008-12:2024/AMD 1:2024(E):
        //   'tmap' brand
        //   This brand enables file players to identify and decode HEIF files containing tone-map derived image
        //   items. When present, this brand shall be among the brands included in the compatible_brands
        //   array of the FileTypeBox.
        //
        // If the file contains a 'tmap' item but doesn't have the 'tmap' brand, it is technically invalid.
        // However, we don't report any error because in order to do detect this case consistently, we would
        // need to remove the early exit in avifParse() to check if a 'tmap' item might be present
        // further down the file. Instead, we simply ignore tmap items in files that lack the 'tmap' brand.
        if (avifBrandArrayHasBrand(&data->compatibleBrands, "tmap")) {
            avifDecoderItem * toneMappedImageItem;
            avifDecoderItem * gainMapItem;
            avifCodecType gainMapCodecType;
            AVIF_CHECKRES(
                avifDecoderFindGainMapItem(decoder, mainItems[AVIF_ITEM_COLOR], &toneMappedImageItem, &gainMapItem, &gainMapCodecType));
            if (toneMappedImageItem != NULL) {
                // Read the gain map's metadata.
                avifROData tmapData;
                AVIF_CHECKRES(avifDecoderItemRead(toneMappedImageItem, decoder->io, &tmapData, 0, 0, data->diag));
                AVIF_ASSERT_OR_RETURN(decoder->image->gainMap != NULL);
                const avifResult tmapParsingRes =
                    avifParseToneMappedImageBox(decoder->image->gainMap, tmapData.data, tmapData.size, data->diag);
                if (tmapParsingRes == AVIF_RESULT_NOT_IMPLEMENTED) {
                    // Unsupported gain map version. Simply ignore the gain map.
                    avifGainMapDestroy(decoder->image->gainMap);
                    decoder->image->gainMap = NULL;
                } else {
                    AVIF_CHECKRES(tmapParsingRes);
                    if (decoder->imageContentToDecode & AVIF_IMAGE_CONTENT_GAIN_MAP) {
                        mainItems[AVIF_ITEM_GAIN_MAP] = gainMapItem;
                        codecType[AVIF_ITEM_GAIN_MAP] = gainMapCodecType;
                    }
                }
            }
        }

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
        // AVIF_ITEM_SAMPLE_TRANSFORM (not used through mainItems because not a coded item (well grids are not coded items either but it's different)).
        avifDecoderItem * sampleTransformItem = NULL;
        AVIF_CHECKRES(avifDecoderDataFindSampleTransformImageItem(data, &sampleTransformItem));
        if (sampleTransformItem != NULL) {
            AVIF_ASSERT_OR_RETURN(data->sampleTransformNumInputImageItems == 0);

            for (uint32_t i = 0; i < data->meta->items.count; ++i) {
                avifDecoderItem * inputImageItem = data->meta->items.item[i];
                if (inputImageItem->dimgForID == sampleTransformItem->id) {
                    ++data->sampleTransformNumInputImageItems;
                }
            }
            // Check max number of input items allowed by the format.
            if (data->sampleTransformNumInputImageItems > 32) {
                avifDiagnosticsPrintf(data->diag,
                                      "Box[sato] too many input items, format allows up to 32, got %d",
                                      data->sampleTransformNumInputImageItems);
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }
            // Check max number of input items supported by this implementation.
            AVIF_CHECKERR(data->sampleTransformNumInputImageItems <= AVIF_SAMPLE_TRANSFORM_MAX_NUM_INPUT_IMAGE_ITEMS,
                          AVIF_RESULT_NOT_IMPLEMENTED);

            uint32_t numExtraInputImageItems = 0;
            for (uint32_t i = 0; i < data->meta->items.count; ++i) {
                avifDecoderItem * inputImageItem = data->meta->items.item[i];
                if (inputImageItem->dimgForID != sampleTransformItem->id) {
                    continue;
                }
                if (avifDecoderItemShouldBeSkipped(inputImageItem)) {
                    avifDiagnosticsPrintf(data->diag, "Box[sato] input item %u is not a supported image type", inputImageItem->id);
                    return AVIF_RESULT_DECODE_SAMPLE_TRANSFORM_FAILED;
                }

                AVIF_ASSERT_OR_RETURN(inputImageItem->dimgIdx < AVIF_SAMPLE_TRANSFORM_MAX_NUM_INPUT_IMAGE_ITEMS);
                avifItemCategory * category = &data->sampleTransformInputImageItems[inputImageItem->dimgIdx];
                avifBool foundItem = AVIF_FALSE;
                avifItemCategory alphaCategory = AVIF_ITEM_CATEGORY_COUNT;
                for (int c = AVIF_ITEM_COLOR; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
                    if (mainItems[c] && inputImageItem->id == mainItems[c]->id) {
                        *category = c;
                        AVIF_CHECKERR(*category == AVIF_ITEM_COLOR, AVIF_RESULT_NOT_IMPLEMENTED);
                        alphaCategory = AVIF_ITEM_ALPHA;
                        foundItem = AVIF_TRUE;
                        break;
                    }
                }
                if (!foundItem) {
                    AVIF_CHECKERR(numExtraInputImageItems < AVIF_SAMPLE_TRANSFORM_MAX_NUM_EXTRA_INPUT_IMAGE_ITEMS,
                                  AVIF_RESULT_NOT_IMPLEMENTED);
                    *category = (avifItemCategory)(AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_COLOR + numExtraInputImageItems);
                    alphaCategory = (avifItemCategory)(AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_ALPHA + numExtraInputImageItems);
                    mainItems[*category] = inputImageItem;
                    ++numExtraInputImageItems;

                    AVIF_CHECKRES(avifDecoderItemReadAndParse(decoder,
                                                              inputImageItem,
                                                              /*isItemInInput=*/AVIF_TRUE,
                                                              &data->tileInfos[*category].grid,
                                                              &codecType[*category]));

                    // Optional alpha auxiliary item
                    avifBool isAlphaInputImageItemInInput = AVIF_FALSE;
                    AVIF_CHECKRES(avifMetaFindAlphaItem(data->meta,
                                                        mainItems[*category],
                                                        &data->tileInfos[*category],
                                                        &mainItems[alphaCategory],
                                                        &data->tileInfos[alphaCategory],
                                                        &isAlphaInputImageItemInInput));

                    AVIF_CHECKERR(!mainItems[alphaCategory] == !mainItems[AVIF_ITEM_ALPHA], AVIF_RESULT_NOT_IMPLEMENTED);
                    if (mainItems[alphaCategory] != NULL) {
                        AVIF_CHECKERR(isAlphaInputImageItemInInput == isAlphaItemInInput, AVIF_RESULT_NOT_IMPLEMENTED);
                        AVIF_CHECKERR((mainItems[*category]->premByID == mainItems[alphaCategory]->id) ==
                                          (mainItems[AVIF_ITEM_COLOR]->premByID == mainItems[AVIF_ITEM_ALPHA]->id),
                                      AVIF_RESULT_NOT_IMPLEMENTED);
                        AVIF_CHECKRES(avifDecoderItemReadAndParse(decoder,
                                                                  mainItems[alphaCategory],
                                                                  isAlphaInputImageItemInInput,
                                                                  &data->tileInfos[alphaCategory].grid,
                                                                  &codecType[alphaCategory]));
                    }
                }
            }

            AVIF_ASSERT_OR_RETURN(data->meta->sampleTransformExpression.tokens == NULL);
            avifROData satoData;
            AVIF_CHECKRES(avifDecoderItemRead(sampleTransformItem, decoder->io, &satoData, 0, 0, data->diag));
            AVIF_CHECKRES(avifParseSampleTransformImageBox(satoData.data,
                                                           satoData.size,
                                                           data->sampleTransformNumInputImageItems,
                                                           &data->meta->sampleTransformExpression,
                                                           data->diag));
            AVIF_CHECKRES(avifDecoderSampleTransformItemValidateProperties(sampleTransformItem, data->diag));
            const avifProperty * pixiProp = avifPropertyArrayFind(&sampleTransformItem->properties, "pixi");
            AVIF_ASSERT_OR_RETURN(pixiProp != NULL);
            data->meta->sampleTransformDepth = pixiProp->u.pixi.planeDepths[0];
        }
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

        // Find Exif and/or XMP metadata, if any
        AVIF_CHECKRES(avifDecoderFindMetadata(decoder, data->meta, decoder->image, mainItems[AVIF_ITEM_COLOR]->id));

        // Set all counts and timing to safe-but-uninteresting values
        decoder->imageIndex = -1;
        decoder->imageCount = 1;
        decoder->imageTiming.timescale = 1;
        decoder->imageTiming.pts = 0;
        decoder->imageTiming.ptsInTimescales = 0;
        decoder->imageTiming.duration = 1;
        decoder->imageTiming.durationInTimescales = 1;
        decoder->timescale = 1;
        decoder->duration = 1;
        decoder->durationInTimescales = 1;

        for (int c = AVIF_ITEM_COLOR; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
            if (!mainItems[c]) {
                continue;
            }

            if (avifIsAlpha((avifItemCategory)c) && !mainItems[c]->width && !mainItems[c]->height) {
                // NON-STANDARD: Alpha subimage does not have an ispe property; adopt width/height from color item
                AVIF_ASSERT_OR_RETURN(!(decoder->strictFlags & AVIF_STRICT_ALPHA_ISPE_REQUIRED));
                mainItems[c]->width = mainItems[AVIF_ITEM_COLOR]->width;
                mainItems[c]->height = mainItems[AVIF_ITEM_COLOR]->height;
            }

            AVIF_CHECKRES(avifDecoderAdoptGridTileCodecTypeIfNeeded(decoder, mainItems[c], &data->tileInfos[c]));

            if (!(decoder->imageContentToDecode & AVIF_IMAGE_CONTENT_COLOR_AND_ALPHA) && (c == AVIF_ITEM_COLOR || c == AVIF_ITEM_ALPHA)) {
                continue;
            }

            AVIF_CHECKRES(avifDecoderGenerateImageTiles(decoder, &data->tileInfos[c], mainItems[c], (avifItemCategory)c));

            avifStrictFlags strictFlags = decoder->strictFlags;
            if (avifIsAlpha((avifItemCategory)c) && !isAlphaItemInInput) {
                // In this case, the made up grid item will not have an associated pixi property. So validate everything else
                // but the pixi property.
                strictFlags &= ~(avifStrictFlags)AVIF_STRICT_PIXI_REQUIRED;
            }
            AVIF_CHECKRES(
                avifDecoderItemValidateProperties(mainItems[c], avifGetConfigurationPropertyName(codecType[c]), &decoder->diag, strictFlags));
        }

        if (mainItems[AVIF_ITEM_COLOR]->progressive) {
            decoder->progressiveState = AVIF_PROGRESSIVE_STATE_AVAILABLE;
            // data->tileInfos[AVIF_ITEM_COLOR].firstTileIndex is not yet defined but will be set to 0 a few lines below.
            const avifTile * colorTile = &data->tiles.tile[0];
            if (colorTile->input->samples.count > 1) {
                decoder->progressiveState = AVIF_PROGRESSIVE_STATE_ACTIVE;
                decoder->imageCount = (int)colorTile->input->samples.count;
            }
        }

        decoder->image->width = mainItems[AVIF_ITEM_COLOR]->width;
        decoder->image->height = mainItems[AVIF_ITEM_COLOR]->height;
        decoder->alphaPresent = (mainItems[AVIF_ITEM_ALPHA] != NULL);
        decoder->image->alphaPremultiplied = decoder->alphaPresent &&
                                             (mainItems[AVIF_ITEM_COLOR]->premByID == mainItems[AVIF_ITEM_ALPHA]->id);

        if (mainItems[AVIF_ITEM_GAIN_MAP]) {
            AVIF_ASSERT_OR_RETURN(decoder->image->gainMap && decoder->image->gainMap->image);
            decoder->image->gainMap->image->width = mainItems[AVIF_ITEM_GAIN_MAP]->width;
            decoder->image->gainMap->image->height = mainItems[AVIF_ITEM_GAIN_MAP]->height;
            // Must be called after avifDecoderGenerateImageTiles() which among other things copies the
            // codec config property from the first tile of a grid to the grid item (when grids are used).
            AVIF_CHECKRES(avifReadCodecConfigProperty(decoder->image->gainMap->image,
                                                      &mainItems[AVIF_ITEM_GAIN_MAP]->properties,
                                                      codecType[AVIF_ITEM_GAIN_MAP]));
            gainMapProperties = &mainItems[AVIF_ITEM_GAIN_MAP]->properties;
        }
    }

    uint32_t firstTileIndex = 0;
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        data->tileInfos[c].firstTileIndex = firstTileIndex;
        firstTileIndex += data->tileInfos[c].tileCount;
    }

    // Sanity check tiles
    for (uint32_t tileIndex = 0; tileIndex < data->tiles.count; ++tileIndex) {
        avifTile * tile = &data->tiles.tile[tileIndex];
        for (uint32_t sampleIndex = 0; sampleIndex < tile->input->samples.count; ++sampleIndex) {
            avifDecodeSample * sample = &tile->input->samples.sample[sampleIndex];
            if (!sample->size) {
                // Every sample must have some data
                return AVIF_RESULT_BMFF_PARSE_FAILED;
            }

            if (tile->input->itemCategory == AVIF_ITEM_COLOR) {
                decoder->ioStats.colorOBUSize += sample->size;
            } else if (tile->input->itemCategory == AVIF_ITEM_ALPHA) {
                decoder->ioStats.alphaOBUSize += sample->size;
            }
        }
    }

    AVIF_CHECKRES(avifReadColorProperties(decoder->io,
                                          colorProperties,
                                          &decoder->image->icc,
                                          &decoder->image->colorPrimaries,
                                          &decoder->image->transferCharacteristics,
                                          &decoder->image->matrixCoefficients,
                                          &decoder->image->yuvRange,
                                          &data->cicpSet));

    const avifProperty * clliProp = avifPropertyArrayFind(colorProperties, "clli");
    if (clliProp) {
        decoder->image->clli = clliProp->u.clli;
    }

    // Transformations
    const avifProperty * paspProp = avifPropertyArrayFind(colorProperties, "pasp");
    if (paspProp) {
        decoder->image->transformFlags |= AVIF_TRANSFORM_PASP;
        decoder->image->pasp = paspProp->u.pasp;
    }
    const avifProperty * clapProp = avifPropertyArrayFind(colorProperties, "clap");
    if (clapProp) {
        decoder->image->transformFlags |= AVIF_TRANSFORM_CLAP;
        decoder->image->clap = clapProp->u.clap;
    }
    const avifProperty * irotProp = avifPropertyArrayFind(colorProperties, "irot");
    if (irotProp) {
        decoder->image->transformFlags |= AVIF_TRANSFORM_IROT;
        decoder->image->irot = irotProp->u.irot;
    }
    const avifProperty * imirProp = avifPropertyArrayFind(colorProperties, "imir");
    if (imirProp) {
        decoder->image->transformFlags |= AVIF_TRANSFORM_IMIR;
        decoder->image->imir = imirProp->u.imir;
    }
    if (gainMapProperties != NULL) {
        AVIF_CHECKRES(aviDecoderCheckGainMapProperties(decoder, gainMapProperties));
    }

    if (!data->cicpSet && (data->tiles.count > 0)) {
        avifTile * firstTile = &data->tiles.tile[0];
        if (firstTile->input->samples.count > 0) {
            avifDecodeSample * sample = &firstTile->input->samples.sample[0];

            // Harvest CICP from the AV1's sequence header, which should be very close to the front
            // of the first sample. Read in successively larger chunks until we successfully parse the sequence.
            static const size_t searchSampleChunkIncrement = 64;
            static const size_t searchSampleSizeMax = 4096;
            size_t searchSampleSize = 0;
            do {
                searchSampleSize += searchSampleChunkIncrement;
                if (searchSampleSize > sample->size) {
                    searchSampleSize = sample->size;
                }

                avifResult prepareResult = avifDecoderPrepareSample(decoder, sample, searchSampleSize);
                if (prepareResult != AVIF_RESULT_OK) {
                    return prepareResult;
                }

                avifSequenceHeader sequenceHeader;
                if (avifSequenceHeaderParse(&sequenceHeader, &sample->data, firstTile->codecType)) {
                    data->cicpSet = AVIF_TRUE;
                    decoder->image->colorPrimaries = sequenceHeader.colorPrimaries;
                    decoder->image->transferCharacteristics = sequenceHeader.transferCharacteristics;
                    decoder->image->matrixCoefficients = sequenceHeader.matrixCoefficients;
                    decoder->image->yuvRange = sequenceHeader.range;
                    break;
                }
            } while (searchSampleSize != sample->size && searchSampleSize < searchSampleSizeMax);
        }
    }

    AVIF_CHECKRES(avifReadCodecConfigProperty(decoder->image, colorProperties, colorCodecType));

    // Expose as raw bytes all other properties that libavif does not care about.
    for (size_t i = 0; i < colorProperties->count; ++i) {
        const avifProperty * property = &colorProperties->prop[i];
        if (property->isOpaque) {
            AVIF_CHECKRES(avifImagePushProperty(decoder->image,
                                                property->type,
                                                property->u.opaque.usertype,
                                                property->u.opaque.boxPayload.data,
                                                property->u.opaque.boxPayload.size));
        }
    }

    if (gainMapProperties) {
        for (size_t i = 0; i < gainMapProperties->count; ++i) {
            const avifProperty * property = &gainMapProperties->prop[i];
            if (property->isOpaque) {
                AVIF_CHECKRES(avifImagePushProperty(decoder->image->gainMap->image,
                                                    property->type,
                                                    property->u.opaque.usertype,
                                                    property->u.opaque.boxPayload.data,
                                                    property->u.opaque.boxPayload.size));
            }
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifDecoderPrepareTiles(avifDecoder * decoder, uint32_t nextImageIndex, const avifTileInfo * info)
{
    for (unsigned int tileIndex = info->decodedTileCount; tileIndex < info->tileCount; ++tileIndex) {
        avifTile * tile = &decoder->data->tiles.tile[info->firstTileIndex + tileIndex];

        if (nextImageIndex >= tile->input->samples.count) {
            return AVIF_RESULT_NO_IMAGES_REMAINING;
        }

        avifDecodeSample * sample = &tile->input->samples.sample[nextImageIndex];
        avifResult prepareResult = avifDecoderPrepareSample(decoder, sample, 0);
        if (prepareResult != AVIF_RESULT_OK) {
            return prepareResult;
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifImageLimitedToFullAlpha(avifImage * image)
{
    if (image->imageOwnsAlphaPlane) {
        return AVIF_RESULT_NOT_IMPLEMENTED;
    }

    const uint8_t * alphaPlane = image->alphaPlane;
    const uint32_t alphaRowBytes = image->alphaRowBytes;

    // We cannot do the range conversion in place since it will modify the
    // codec's internal frame buffers. Allocate memory for the conversion.
    image->alphaPlane = NULL;
    image->alphaRowBytes = 0;
    const avifResult allocationResult = avifImageAllocatePlanes(image, AVIF_PLANES_A);
    if (allocationResult != AVIF_RESULT_OK) {
        return allocationResult;
    }

    if (image->depth > 8) {
        for (uint32_t j = 0; j < image->height; ++j) {
            const uint8_t * srcRow = &alphaPlane[j * alphaRowBytes];
            uint8_t * dstRow = &image->alphaPlane[j * image->alphaRowBytes];
            for (uint32_t i = 0; i < image->width; ++i) {
                int srcAlpha = *((const uint16_t *)&srcRow[i * 2]);
                int dstAlpha = avifLimitedToFullY(image->depth, srcAlpha);
                *((uint16_t *)&dstRow[i * 2]) = (uint16_t)dstAlpha;
            }
        }
    } else {
        for (uint32_t j = 0; j < image->height; ++j) {
            const uint8_t * srcRow = &alphaPlane[j * alphaRowBytes];
            uint8_t * dstRow = &image->alphaPlane[j * image->alphaRowBytes];
            for (uint32_t i = 0; i < image->width; ++i) {
                int srcAlpha = srcRow[i];
                int dstAlpha = avifLimitedToFullY(image->depth, srcAlpha);
                dstRow[i] = (uint8_t)dstAlpha;
            }
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifGetErrorForItemCategory(avifItemCategory itemCategory)
{
    if (itemCategory == AVIF_ITEM_GAIN_MAP) {
        return AVIF_RESULT_DECODE_GAIN_MAP_FAILED;
    }
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (itemCategory >= AVIF_SAMPLE_TRANSFORM_MIN_CATEGORY && itemCategory <= AVIF_SAMPLE_TRANSFORM_MAX_CATEGORY) {
        return AVIF_RESULT_DECODE_SAMPLE_TRANSFORM_FAILED;
    }
#endif
    return avifIsAlpha(itemCategory) ? AVIF_RESULT_DECODE_ALPHA_FAILED : AVIF_RESULT_DECODE_COLOR_FAILED;
}

static avifResult avifDecoderDecodeTiles(avifDecoder * decoder, uint32_t nextImageIndex, avifTileInfo * info)
{
    const unsigned int oldDecodedTileCount = info->decodedTileCount;
    for (unsigned int tileIndex = oldDecodedTileCount; tileIndex < info->tileCount; ++tileIndex) {
        avifTile * tile = &decoder->data->tiles.tile[info->firstTileIndex + tileIndex];

        const avifDecodeSample * sample = &tile->input->samples.sample[nextImageIndex];
        if (sample->data.size < sample->size) {
            AVIF_ASSERT_OR_RETURN(decoder->allowIncremental);
            // Data is missing but there is no error yet. Output available pixel rows.
            return AVIF_RESULT_OK;
        }

        avifBool isLimitedRangeAlpha = AVIF_FALSE;
        tile->codec->maxThreads = decoder->maxThreads;
        tile->codec->imageSizeLimit = decoder->imageSizeLimit;
        if (!tile->codec->getNextImage(tile->codec, sample, avifIsAlpha(tile->input->itemCategory), &isLimitedRangeAlpha, tile->image)) {
            avifDiagnosticsPrintf(&decoder->diag, "tile->codec->getNextImage() failed");
            return avifGetErrorForItemCategory(tile->input->itemCategory);
        }

        // Section 2.3.4 of AV1 Codec ISO Media File Format Binding v1.2.0 says:
        //   the full_range_flag in the colr box shall match the color_range
        //   flag in the Sequence Header OBU.
        // See https://aomediacodec.github.io/av1-isobmff/v1.2.0.html#av1codecconfigurationbox-semantics.
        // If a 'colr' box of colour_type 'nclx' was parsed, a mismatch between
        // the 'colr' decoder->image->yuvRange and the AV1 OBU
        // tile->image->yuvRange should be treated as an error.
        // However codec_svt.c was not encoding the color_range field for
        // multiple years, so there probably are files in the wild that will
        // fail decoding if this is enforced. Thus this pattern is allowed.
        // Section 12.1.5.1 of ISO 14496-12 (ISOBMFF) says:
        //   If colour information is supplied in both this [colr] box, and also
        //   in the video bitstream, this box takes precedence, and over-rides
        //   the information in the bitstream.
        // So decoder->image->yuvRange is kept because it was either the 'colr'
        // value set when the 'colr' box was parsed, or it was the AV1 OBU value
        // extracted from the sequence header OBU of the first tile of the first
        // frame (if no 'colr' box of colour_type 'nclx' was found).

        // Alpha plane with limited range is not allowed by the latest revision
        // of the specification. However, it was allowed in version 1.0.0 of the
        // specification. To allow such files, simply convert the alpha plane to
        // full range.
        if (avifIsAlpha(tile->input->itemCategory) && isLimitedRangeAlpha) {
            avifResult result = avifImageLimitedToFullAlpha(tile->image);
            if (result != AVIF_RESULT_OK) {
                avifDiagnosticsPrintf(&decoder->diag, "avifImageLimitedToFullAlpha failed");
                return result;
            }
        }

        // Scale the decoded image so that it corresponds to this tile's output dimensions
        if ((tile->width != tile->image->width) || (tile->height != tile->image->height)) {
            if (avifImageScaleWithLimit(tile->image,
                                        tile->width,
                                        tile->height,
                                        decoder->imageSizeLimit,
                                        decoder->imageDimensionLimit,
                                        &decoder->diag) != AVIF_RESULT_OK) {
                return avifGetErrorForItemCategory(tile->input->itemCategory);
            }
        }

#if defined(AVIF_CODEC_AVM)
        avifDecoderItem * tileItem = NULL;
        for (uint32_t itemIndex = 0; itemIndex < decoder->data->meta->items.count; ++itemIndex) {
            avifDecoderItem * item = decoder->data->meta->items.item[itemIndex];
            if (avifDecoderItemShouldBeSkipped(item)) {
                continue;
            }
            if (item->id == sample->itemID) {
                tileItem = item;
                break;
            }
        }
        if (tileItem != NULL) {
            const avifProperty * prop = avifPropertyArrayFind(&tileItem->properties, "pixi");
            // Match the decoded image format with the number of planes specified in 'pixi'.
            if (prop != NULL && prop->u.pixi.planeCount == 1 && tile->image->yuvFormat == AVIF_PIXEL_FORMAT_YUV420) {
                // Codecs such as avm do not support monochrome so samples were encoded as 4:2:0.
                // Ignore the UV planes at decoding.
                tile->image->yuvFormat = AVIF_PIXEL_FORMAT_YUV400;
                if (tile->image->imageOwnsYUVPlanes) {
                    avifFree(tile->image->yuvPlanes[AVIF_CHAN_U]);
                    avifFree(tile->image->yuvPlanes[AVIF_CHAN_V]);
                }
                tile->image->yuvPlanes[AVIF_CHAN_U] = NULL;
                tile->image->yuvRowBytes[AVIF_CHAN_U] = 0;
                tile->image->yuvPlanes[AVIF_CHAN_V] = NULL;
                tile->image->yuvRowBytes[AVIF_CHAN_V] = 0;
            }
        }
#endif

        ++info->decodedTileCount;

        const avifBool isGrid = (info->grid.rows > 0) && (info->grid.columns > 0);
        avifBool stealPlanes = !isGrid;
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
        if (decoder->data->meta->sampleTransformExpression.count > 0) {
            // Keep everything as a copy for now.
            stealPlanes = AVIF_FALSE;
        }
        if (tile->input->itemCategory >= AVIF_SAMPLE_TRANSFORM_MIN_CATEGORY &&
            tile->input->itemCategory <= AVIF_SAMPLE_TRANSFORM_MAX_CATEGORY) {
            // Keep Sample Transform input image item samples in tiles.
            // The expression will be applied in avifDecoderNextImage() below instead, once all the tiles are available.
            continue;
        }
#endif

        if (!stealPlanes) {
            avifImage * dstImage = decoder->image;
            if (tile->input->itemCategory == AVIF_ITEM_GAIN_MAP) {
                AVIF_ASSERT_OR_RETURN(dstImage->gainMap && dstImage->gainMap->image);
                dstImage = dstImage->gainMap->image;
            }
            if (tileIndex == 0) {
                AVIF_CHECKRES(avifDecoderDataAllocateImagePlanes(decoder->data, info, dstImage));
            }
            AVIF_CHECKRES(avifDecoderDataCopyTileToImage(decoder->data, info, dstImage, tile, tileIndex));
        } else {
            AVIF_ASSERT_OR_RETURN(info->tileCount == 1);
            AVIF_ASSERT_OR_RETURN(tileIndex == 0);
            avifImage * src = tile->image;

            if (tile->input->itemCategory == AVIF_ITEM_GAIN_MAP) {
                AVIF_ASSERT_OR_RETURN(decoder->image->gainMap && decoder->image->gainMap->image);
                decoder->image->gainMap->image->width = src->width;
                decoder->image->gainMap->image->height = src->height;
                decoder->image->gainMap->image->depth = src->depth;
            } else {
                if ((decoder->image->width != src->width) || (decoder->image->height != src->height) ||
                    (decoder->image->depth != src->depth)) {
                    if (avifIsAlpha(tile->input->itemCategory)) {
                        avifDiagnosticsPrintf(&decoder->diag,
                                              "The color image item does not match the alpha image item in width, height, or bit depth");
                        return AVIF_RESULT_DECODE_ALPHA_FAILED;
                    }
                    avifImageFreePlanes(decoder->image, AVIF_PLANES_ALL);

                    decoder->image->width = src->width;
                    decoder->image->height = src->height;
                    decoder->image->depth = src->depth;
                }
            }

            if (avifIsAlpha(tile->input->itemCategory)) {
                avifImageStealPlanes(decoder->image, src, AVIF_PLANES_A);
            } else if (tile->input->itemCategory == AVIF_ITEM_GAIN_MAP) {
                AVIF_ASSERT_OR_RETURN(decoder->image->gainMap && decoder->image->gainMap->image);
                avifImageStealPlanes(decoder->image->gainMap->image, src, AVIF_PLANES_YUV);
            } else { // AVIF_ITEM_COLOR
                avifImageStealPlanes(decoder->image, src, AVIF_PLANES_YUV);
            }
        }
    }
    return AVIF_RESULT_OK;
}

// Returns AVIF_FALSE if there is currently a partially decoded frame.
static avifBool avifDecoderDataFrameFullyDecoded(const avifDecoderData * data)
{
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        if (data->tileInfos[c].decodedTileCount != data->tileInfos[c].tileCount) {
            return AVIF_FALSE;
        }
    }
    return AVIF_TRUE;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
static avifResult avifDecoderApplySampleTransform(const avifDecoder * decoder, avifImage * dstImage)
{
    if (dstImage->depth != decoder->data->meta->sampleTransformDepth) {
        AVIF_ASSERT_OR_RETURN(dstImage->yuvPlanes[0] != NULL);
        AVIF_ASSERT_OR_RETURN(dstImage->imageOwnsYUVPlanes);

        // Use a temporary buffer because dstImage may point to decoder->image, which could be an input image.
        avifImage * dstImageWithCorrectDepth =
            avifImageCreate(dstImage->width, dstImage->height, decoder->data->meta->sampleTransformDepth, dstImage->yuvFormat);
        AVIF_CHECKERR(dstImageWithCorrectDepth != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        avifResult result =
            avifImageAllocatePlanes(dstImageWithCorrectDepth, dstImage->alphaPlane != NULL ? AVIF_PLANES_ALL : AVIF_PLANES_YUV);
        if (result == AVIF_RESULT_OK) {
            result = avifDecoderApplySampleTransform(decoder, dstImageWithCorrectDepth);
            if (result == AVIF_RESULT_OK) {
                // Keep the same dstImage object rather than swapping decoder->image, in case the user already accessed it.
                avifImageFreePlanes(dstImage, AVIF_PLANES_ALL);
                dstImage->depth = dstImageWithCorrectDepth->depth;
                avifImageStealPlanes(dstImage, dstImageWithCorrectDepth, AVIF_PLANES_ALL);
            }
        }
        avifImageDestroy(dstImageWithCorrectDepth);
        return result;
    }

    for (int pass = 0; pass < (decoder->alphaPresent ? 2 : 1); ++pass) {
        avifBool alpha = (pass == 0) ? AVIF_FALSE : AVIF_TRUE;
        AVIF_ASSERT_OR_RETURN(decoder->data->sampleTransformNumInputImageItems <= AVIF_SAMPLE_TRANSFORM_MAX_NUM_INPUT_IMAGE_ITEMS);
        const avifImage * inputImages[AVIF_SAMPLE_TRANSFORM_MAX_NUM_INPUT_IMAGE_ITEMS];
        for (uint32_t i = 0; i < decoder->data->sampleTransformNumInputImageItems; ++i) {
            avifItemCategory category = decoder->data->sampleTransformInputImageItems[i];
            if (category == AVIF_ITEM_COLOR) {
                inputImages[i] = decoder->image;
            } else {
                AVIF_ASSERT_OR_RETURN(category >= AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_COLOR &&
                                      category < AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_COLOR +
                                                     AVIF_SAMPLE_TRANSFORM_MAX_NUM_EXTRA_INPUT_IMAGE_ITEMS);
                if (alpha) {
                    category += AVIF_SAMPLE_TRANSFORM_MAX_NUM_EXTRA_INPUT_IMAGE_ITEMS;
                }
                const avifTileInfo * tileInfo = &decoder->data->tileInfos[category];
                AVIF_CHECKERR(tileInfo->tileCount == 1, AVIF_RESULT_NOT_IMPLEMENTED); // TODO(yguyon): Implement Sample Transform grids
                inputImages[i] = decoder->data->tiles.tile[tileInfo->firstTileIndex].image;
                AVIF_ASSERT_OR_RETURN(inputImages[i] != NULL);
            }
        }
        AVIF_CHECKRES(avifImageApplyExpression(dstImage,
                                               AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_32,
                                               &decoder->data->meta->sampleTransformExpression,
                                               decoder->data->sampleTransformNumInputImageItems,
                                               inputImages,
                                               alpha ? AVIF_PLANES_A : AVIF_PLANES_YUV));
    }
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

avifResult avifDecoderNextImage(avifDecoder * decoder)
{
    avifDiagnosticsClearError(&decoder->diag);

    if (!decoder->data || decoder->data->tiles.count == 0) {
        // Nothing has been parsed yet
        return AVIF_RESULT_NO_CONTENT;
    }

    if (!decoder->io || !decoder->io->read) {
        return AVIF_RESULT_IO_NOT_SET;
    }

    if (avifDecoderDataFrameFullyDecoded(decoder->data)) {
        // A frame was decoded during the last avifDecoderNextImage() call.
        for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
            decoder->data->tileInfos[c].decodedTileCount = 0;
        }
    }

    AVIF_ASSERT_OR_RETURN(decoder->data->tiles.count == (decoder->data->tileInfos[AVIF_ITEM_CATEGORY_COUNT - 1].firstTileIndex +
                                                         decoder->data->tileInfos[AVIF_ITEM_CATEGORY_COUNT - 1].tileCount));

    const uint32_t nextImageIndex = (uint32_t)(decoder->imageIndex + 1);

    // Ensure that we have created the codecs before proceeding with the decoding.
    if (!decoder->data->tiles.tile[0].codec) {
        AVIF_CHECKRES(avifDecoderCreateCodecs(decoder));
    }

    // Acquire all sample data for the current image first, allowing for any read call to bail out
    // with AVIF_RESULT_WAITING_ON_IO harmlessly / idempotently, unless decoder->allowIncremental.
    avifResult prepareTileResult[AVIF_ITEM_CATEGORY_COUNT];
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        prepareTileResult[c] = avifDecoderPrepareTiles(decoder, nextImageIndex, &decoder->data->tileInfos[c]);
        if (!decoder->allowIncremental || (prepareTileResult[c] != AVIF_RESULT_WAITING_ON_IO)) {
            AVIF_CHECKRES(prepareTileResult[c]);
        }
    }

    // Decode all available color tiles now, then all available alpha tiles, then all available bit
    // depth extension tiles. The order of appearance of the tiles in the bitstream is left to the
    // encoder's choice, and decoding as many as possible of each category in parallel is beneficial
    // for incremental decoding, as pixel rows need all channels to be decoded before being
    // accessible to the user.
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        AVIF_CHECKRES(avifDecoderDecodeTiles(decoder, nextImageIndex, &decoder->data->tileInfos[c]));
    }

    if (!avifDecoderDataFrameFullyDecoded(decoder->data)) {
        AVIF_ASSERT_OR_RETURN(decoder->allowIncremental);
        // The image is not completely decoded. There should be no error unrelated to missing bytes,
        // and at least some missing bytes.
        avifResult firstNonOkResult = AVIF_RESULT_OK;
        for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
            AVIF_ASSERT_OR_RETURN(prepareTileResult[c] == AVIF_RESULT_OK || prepareTileResult[c] == AVIF_RESULT_WAITING_ON_IO);
            if (firstNonOkResult == AVIF_RESULT_OK) {
                firstNonOkResult = prepareTileResult[c];
            }
        }
        AVIF_ASSERT_OR_RETURN(firstNonOkResult != AVIF_RESULT_OK);
        // Return the "not enough bytes" status now instead of moving on to the next frame.
        return AVIF_RESULT_WAITING_ON_IO;
    }
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        AVIF_ASSERT_OR_RETURN(prepareTileResult[c] == AVIF_RESULT_OK);
    }

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (decoder->data->meta->sampleTransformExpression.count > 0) {
        // TODO(yguyon): Add a field in avifDecoder and only perform sample transformations upon request.
        AVIF_CHECKRES(avifDecoderApplySampleTransform(decoder, decoder->image));
    }
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

    // Only advance decoder->imageIndex once the image is completely decoded, so that
    // avifDecoderNthImage(decoder, decoder->imageIndex + 1) is equivalent to avifDecoderNextImage(decoder)
    // if the previous call to avifDecoderNextImage() returned AVIF_RESULT_WAITING_ON_IO.
    decoder->imageIndex = (int)nextImageIndex;
    // The decoded tile counts will be reset to 0 the next time avifDecoderNextImage() is called,
    // for avifDecoderDecodedRowCount() to work until then.
    if (decoder->data->sourceSampleTable) {
        // Decoding from a track! Provide timing information.

        avifResult timingResult = avifDecoderNthImageTiming(decoder, decoder->imageIndex, &decoder->imageTiming);
        if (timingResult != AVIF_RESULT_OK) {
            return timingResult;
        }
    }
    return AVIF_RESULT_OK;
}

avifResult avifDecoderNthImageTiming(const avifDecoder * decoder, uint32_t frameIndex, avifImageTiming * outTiming)
{
    if (!decoder->data) {
        // Nothing has been parsed yet
        return AVIF_RESULT_NO_CONTENT;
    }

    if ((frameIndex > INT_MAX) || ((int)frameIndex >= decoder->imageCount)) {
        // Impossible index
        return AVIF_RESULT_NO_IMAGES_REMAINING;
    }

    if (!decoder->data->sourceSampleTable) {
        // There isn't any real timing associated with this decode, so
        // just hand back the defaults chosen in avifDecoderReset().
        *outTiming = decoder->imageTiming;
        return AVIF_RESULT_OK;
    }

    outTiming->timescale = decoder->timescale;
    outTiming->ptsInTimescales = 0;
    for (uint32_t imageIndex = 0; imageIndex < frameIndex; ++imageIndex) {
        outTiming->ptsInTimescales += avifSampleTableGetImageDelta(decoder->data->sourceSampleTable, imageIndex);
    }
    outTiming->durationInTimescales = avifSampleTableGetImageDelta(decoder->data->sourceSampleTable, frameIndex);

    if (outTiming->timescale > 0) {
        outTiming->pts = (double)outTiming->ptsInTimescales / (double)outTiming->timescale;
        outTiming->duration = (double)outTiming->durationInTimescales / (double)outTiming->timescale;
    } else {
        outTiming->pts = 0.0;
        outTiming->duration = 0.0;
    }
    return AVIF_RESULT_OK;
}

avifResult avifDecoderNthImage(avifDecoder * decoder, uint32_t frameIndex)
{
    avifDiagnosticsClearError(&decoder->diag);

    if (!decoder->data) {
        // Nothing has been parsed yet
        return AVIF_RESULT_NO_CONTENT;
    }

    if ((frameIndex > INT_MAX) || ((int)frameIndex >= decoder->imageCount)) {
        // Impossible index
        return AVIF_RESULT_NO_IMAGES_REMAINING;
    }

    int requestedIndex = (int)frameIndex;
    if (requestedIndex == (decoder->imageIndex + 1)) {
        // It's just the next image (already partially decoded or not at all), nothing special here
        return avifDecoderNextImage(decoder);
    }

    if (requestedIndex == decoder->imageIndex) {
        if (avifDecoderDataFrameFullyDecoded(decoder->data)) {
            // The current fully decoded image (decoder->imageIndex) is requested, nothing to do
            return AVIF_RESULT_OK;
        }
        // The next image (decoder->imageIndex + 1) is partially decoded but
        // the previous image (decoder->imageIndex) is requested.
        // Fall through to resetting the decoder data and start decoding from
        // the nearest key frame.
    }

    int nearestKeyFrame = (int)avifDecoderNearestKeyframe(decoder, frameIndex);
    if ((nearestKeyFrame > (decoder->imageIndex + 1)) || (requestedIndex <= decoder->imageIndex)) {
        // If we get here, we need to start decoding from the nearest key frame.
        // So discard the unused decoder state and its previous frames. This
        // will force the setup of new AV1 decoder (avifCodec) instances in
        // avifDecoderNextImage().
        decoder->imageIndex = nearestKeyFrame - 1; // prepare to read nearest keyframe
        avifDecoderDataResetCodec(decoder->data);
    }
    for (;;) {
        avifResult result = avifDecoderNextImage(decoder);
        if (result != AVIF_RESULT_OK) {
            return result;
        }

        if (requestedIndex == decoder->imageIndex) {
            break;
        }
    }
    return AVIF_RESULT_OK;
}

avifBool avifDecoderIsKeyframe(const avifDecoder * decoder, uint32_t frameIndex)
{
    if (!decoder->data || (decoder->data->tiles.count == 0)) {
        // Nothing has been parsed yet
        return AVIF_FALSE;
    }

    // *All* tiles for the requested frameIndex must be keyframes in order for
    //  avifDecoderIsKeyframe() to return true, otherwise we may seek to a frame in which the color
    //  planes are a keyframe but the alpha plane isn't a keyframe, which will cause an alpha plane
    //  decode failure.
    for (unsigned int i = 0; i < decoder->data->tiles.count; ++i) {
        const avifTile * tile = &decoder->data->tiles.tile[i];
        if ((frameIndex >= tile->input->samples.count) || !tile->input->samples.sample[frameIndex].sync) {
            return AVIF_FALSE;
        }
    }
    return AVIF_TRUE;
}

uint32_t avifDecoderNearestKeyframe(const avifDecoder * decoder, uint32_t frameIndex)
{
    if (!decoder->data) {
        // Nothing has been parsed yet
        return 0;
    }

    for (; frameIndex != 0; --frameIndex) {
        if (avifDecoderIsKeyframe(decoder, frameIndex)) {
            break;
        }
    }
    return frameIndex;
}

// Returns the number of available rows in decoder->image given a color or alpha subimage.
static uint32_t avifGetDecodedRowCount(const avifDecoder * decoder, const avifTileInfo * info, const avifImage * image)
{
    if (info->decodedTileCount == info->tileCount) {
        return image->height;
    }
    if (info->decodedTileCount == 0) {
        return 0;
    }

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (decoder->data->meta->sampleTransformExpression.count > 0) {
        // TODO(yguyon): Support incremental Sample Transforms
        return 0;
    }
#endif

    if ((info->grid.rows > 0) && (info->grid.columns > 0)) {
        // Grid of AVIF tiles (not to be confused with AV1 tiles).
        const uint32_t tileHeight = decoder->data->tiles.tile[info->firstTileIndex].height;
        return AVIF_MIN((info->decodedTileCount / info->grid.columns) * tileHeight, image->height);
    } else {
        // Non-grid image.
        return image->height;
    }
}

uint32_t avifDecoderDecodedRowCount(const avifDecoder * decoder)
{
    uint32_t minRowCount = decoder->image->height;
    for (int c = 0; c < AVIF_ITEM_CATEGORY_COUNT; ++c) {
        if (c == AVIF_ITEM_GAIN_MAP) {
            const avifImage * const gainMap = decoder->image->gainMap ? decoder->image->gainMap->image : NULL;
            if ((decoder->imageContentToDecode & AVIF_IMAGE_CONTENT_GAIN_MAP) && gainMap != NULL && gainMap->height != 0) {
                uint32_t gainMapRowCount = avifGetDecodedRowCount(decoder, &decoder->data->tileInfos[AVIF_ITEM_GAIN_MAP], gainMap);
                if (gainMap->height != decoder->image->height) {
                    const uint32_t scaledGainMapRowCount =
                        (uint32_t)floorf((float)gainMapRowCount / gainMap->height * decoder->image->height);
                    // Make sure it matches the formula described in the comment of avifDecoderDecodedRowCount() in avif.h.
                    AVIF_CHECKERR((uint32_t)lround((double)scaledGainMapRowCount / decoder->image->height *
                                                   decoder->image->gainMap->image->height) <= gainMapRowCount,
                                  0);
                    gainMapRowCount = scaledGainMapRowCount;
                }
                minRowCount = AVIF_MIN(minRowCount, gainMapRowCount);
            }
            continue;
        }
        const uint32_t rowCount = avifGetDecodedRowCount(decoder, &decoder->data->tileInfos[c], decoder->image);
        minRowCount = AVIF_MIN(minRowCount, rowCount);
    }
    return minRowCount;
}

avifResult avifDecoderRead(avifDecoder * decoder, avifImage * image)
{
    avifResult result = avifDecoderParse(decoder);
    if (result != AVIF_RESULT_OK) {
        return result;
    }
    result = avifDecoderNextImage(decoder);
    if (result != AVIF_RESULT_OK) {
        return result;
    }
    // If decoder->image->imageOwnsYUVPlanes is true and decoder->image is not used after this call,
    // the ownership of the planes in decoder->image could be transferred here instead of copied.
    // However most codec_*.c implementations allocate the output buffer themselves and return a
    // view, unless some postprocessing is applied (container-level grid reconstruction for
    // example), so the first condition rarely holds.
    // The second condition does not hold either: it is not required by the documentation in avif.h.
    return avifImageCopy(image, decoder->image, AVIF_PLANES_ALL);
}

avifResult avifDecoderReadMemory(avifDecoder * decoder, avifImage * image, const uint8_t * data, size_t size)
{
    avifDiagnosticsClearError(&decoder->diag);
    avifResult result = avifDecoderSetIOMemory(decoder, data, size);
    if (result != AVIF_RESULT_OK) {
        return result;
    }
    return avifDecoderRead(decoder, image);
}

avifResult avifDecoderReadFile(avifDecoder * decoder, avifImage * image, const char * filename)
{
    avifDiagnosticsClearError(&decoder->diag);
    avifResult result = avifDecoderSetIOFile(decoder, filename);
    if (result != AVIF_RESULT_OK) {
        return result;
    }
    return avifDecoderRead(decoder, image);
}
