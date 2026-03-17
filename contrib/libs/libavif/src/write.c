// Copyright 2019 Joe Drago. All rights reserved.
// SPDX-License-Identifier: BSD-2-Clause

#include "avif/internal.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

// Section 8.11.14.2 of ISO/IEC 14496-12 (ItemPropertyAssociationBox 'ipma' syntax):
//   if (flags & 1)
//     unsigned int(15) property_index;
//   else
//     unsigned int(7) property_index;
//
// libavif writes 'ipma' with flags set to 0.
#define MAX_PROPERTY_INDEX ((1 << 7) - 1)

// The indices of the properties associated with an item.
typedef struct avifItemPropertyAssociation
{
    uint8_t property_index; // 1-indexed
    avifBool essential;
} avifItemPropertyAssociation;
AVIF_ARRAY_DECLARE(avifItemPropertyAssociationArray, avifItemPropertyAssociation, association);

// Used to store offsets in meta boxes which need to point at mdat offsets that
// aren't known yet. When an item's mdat payload is written, all registered fixups
// will have this now-known offset "fixed up".
typedef struct avifOffsetFixup
{
    size_t offset;
} avifOffsetFixup;
AVIF_ARRAY_DECLARE(avifOffsetFixupArray, avifOffsetFixup, fixup);

static const char alphaURN[] = AVIF_URN_ALPHA0;
static const size_t alphaURNSize = sizeof(alphaURN);

static const char xmpContentType[] = AVIF_CONTENT_TYPE_XMP;
static const size_t xmpContentTypeSize = sizeof(xmpContentType);

static avifResult writeCodecConfig(avifRWStream * s, const avifCodecConfigurationBox * cfg);
static avifResult writeConfigBox(avifRWStream * s, const avifCodecConfigurationBox * cfg, const char * configPropName);

// ---------------------------------------------------------------------------
// avifSetTileConfiguration

static int floorLog2(uint32_t n)
{
    assert(n > 0);
    int count = 0;
    while (n != 0) {
        ++count;
        n >>= 1;
    }
    return count - 1;
}

// Splits tilesLog2 into *tileDim1Log2 and *tileDim2Log2, considering the ratio of dim1 to dim2.
//
// Precondition:
//     dim1 >= dim2
// Postcondition:
//     tilesLog2 == *tileDim1Log2 + *tileDim2Log2
//     *tileDim1Log2 >= *tileDim2Log2
static void splitTilesLog2(uint32_t dim1, uint32_t dim2, int tilesLog2, int * tileDim1Log2, int * tileDim2Log2)
{
    assert(dim1 >= dim2);
    uint32_t ratio = dim1 / dim2;
    int diffLog2 = floorLog2(ratio);
    int subtract = tilesLog2 - diffLog2;
    if (subtract < 0) {
        subtract = 0;
    }
    *tileDim2Log2 = subtract / 2;
    *tileDim1Log2 = tilesLog2 - *tileDim2Log2;
    assert(*tileDim1Log2 >= *tileDim2Log2);
}

// Set the tile configuration: the number of tiles and the tile size.
//
// Tiles improve encoding and decoding speeds when multiple threads are available. However, for
// image coding, the total tile boundary length affects the compression efficiency because intra
// prediction can't go across tile boundaries. So the more tiles there are in an image, the worse
// the compression ratio is. For a given number of tiles, making the tile size close to a square
// tends to reduce the total tile boundary length inside the image. Use more tiles along the longer
// dimension of the image to make the tile size closer to a square.
void avifSetTileConfiguration(int threads, uint32_t width, uint32_t height, int * tileRowsLog2, int * tileColsLog2)
{
    *tileRowsLog2 = 0;
    *tileColsLog2 = 0;
    if (threads > 1) {
        // Avoid small tiles because they are particularly bad for image coding.
        //
        // Use no more tiles than the number of threads. Aim for one tile per thread. Using more
        // than one thread inside one tile could be less efficient. Using more tiles than the
        // number of threads would result in a compression penalty without much benefit.
        const uint32_t kMinTileArea = 512 * 512;
        const uint32_t kMaxTiles = 32;
        uint32_t imageArea = width * height;
        uint32_t tiles = (imageArea + kMinTileArea - 1) / kMinTileArea;
        if (tiles > kMaxTiles) {
            tiles = kMaxTiles;
        }
        if (tiles > (uint32_t)threads) {
            tiles = threads;
        }
        int tilesLog2 = floorLog2(tiles);
        // If the image's width is greater than the height, use more tile columns than tile rows.
        if (width >= height) {
            splitTilesLog2(width, height, tilesLog2, tileColsLog2, tileRowsLog2);
        } else {
            splitTilesLog2(height, width, tilesLog2, tileRowsLog2, tileColsLog2);
        }
    }
}

// ---------------------------------------------------------------------------
// avifCodecEncodeOutput

avifCodecEncodeOutput * avifCodecEncodeOutputCreate(void)
{
    avifCodecEncodeOutput * encodeOutput = (avifCodecEncodeOutput *)avifAlloc(sizeof(avifCodecEncodeOutput));
    if (encodeOutput == NULL) {
        return NULL;
    }
    memset(encodeOutput, 0, sizeof(avifCodecEncodeOutput));
    if (!avifArrayCreate(&encodeOutput->samples, sizeof(avifEncodeSample), 1)) {
        avifCodecEncodeOutputDestroy(encodeOutput);
        return NULL;
    }
    return encodeOutput;
}

avifResult avifCodecEncodeOutputAddSample(avifCodecEncodeOutput * encodeOutput, const uint8_t * data, size_t len, avifBool sync)
{
    avifEncodeSample * sample = (avifEncodeSample *)avifArrayPush(&encodeOutput->samples);
    AVIF_CHECKERR(sample, AVIF_RESULT_OUT_OF_MEMORY);
    const avifResult result = avifRWDataSet(&sample->data, data, len);
    if (result != AVIF_RESULT_OK) {
        avifArrayPop(&encodeOutput->samples);
        return result;
    }
    sample->sync = sync;
    return AVIF_RESULT_OK;
}

void avifCodecEncodeOutputDestroy(avifCodecEncodeOutput * encodeOutput)
{
    for (uint32_t sampleIndex = 0; sampleIndex < encodeOutput->samples.count; ++sampleIndex) {
        avifRWDataFree(&encodeOutput->samples.sample[sampleIndex].data);
    }
    avifArrayDestroy(&encodeOutput->samples);
    avifFree(encodeOutput);
}

// ---------------------------------------------------------------------------
// avifEncoderItem

// one "item" worth for encoder
typedef struct avifEncoderItem
{
    uint16_t id;
    uint8_t type[4];                      // 4-character 'item_type' field in the 'infe' (item info entry) box
    avifCodec * codec;                    // only present on image items
    avifCodecEncodeOutput * encodeOutput; // AV1 sample data
    avifRWData metadataPayload;           // Exif/XMP data
    avifCodecConfigurationBox av1C;       // Harvested in avifEncoderFinish(), if encodeOutput has samples
                                          // TODO(yguyon): Rename or add av2C
    uint32_t cellIndex;                   // Which row-major cell index corresponds to this item. only present on image items
    avifItemCategory itemCategory;        // Category of item being encoded
    avifBool hiddenImage;                 // A hidden image item has (flags & 1) equal to 1 in its ItemInfoEntry.

    const char * infeName;
    size_t infeNameSize;
    const char * infeContentType;
    size_t infeContentTypeSize;
    avifOffsetFixupArray mdatFixups;

    uint16_t irefToID; // if non-zero, make an iref from this id -> irefToID
    const char * irefType;

    uint32_t gridCols; // if non-zero (legal range [1-256]), this is a grid item
    uint32_t gridRows; // if non-zero (legal range [1-256]), this is a grid item

    // the reconstructed image of a grid item will be trimmed to these dimensions (only present on grid items)
    uint32_t gridWidth;
    uint32_t gridHeight;

    uint32_t extraLayerCount; // if non-zero (legal range [1-(AVIF_MAX_AV1_LAYER_COUNT-1)]), this is a layered AV1 image

    uint16_t dimgFromID; // if non-zero, make an iref from dimgFromID -> this id

    avifItemPropertyAssociationArray associations; // 'ipma'
} avifEncoderItem;
AVIF_ARRAY_DECLARE(avifEncoderItemArray, avifEncoderItem, item);

// ---------------------------------------------------------------------------
// avifEncoderItemReference

// pointer to one "item" interested in
typedef avifEncoderItem * avifEncoderItemReference;
AVIF_ARRAY_DECLARE(avifEncoderItemReferenceArray, avifEncoderItemReference, ref);

// ---------------------------------------------------------------------------
// avifEncoderFrame

typedef struct avifEncoderFrame
{
    uint64_t durationInTimescales;
} avifEncoderFrame;
AVIF_ARRAY_DECLARE(avifEncoderFrameArray, avifEncoderFrame, frame);

// ---------------------------------------------------------------------------
// avifEncoderData

AVIF_ARRAY_DECLARE(avifEncoderItemIdArray, uint16_t, itemID);

typedef struct avifEncoderData
{
    avifEncoderItemArray items;
    avifEncoderFrameArray frames;
    // Map the encoder settings quality and qualityAlpha to quantizer and quantizerAlpha
    int quantizer;
    int quantizerAlpha;
    int quantizerGainMap;
    // tileRowsLog2 and tileColsLog2 are the actual tiling values after automatic tiling is handled
    int tileRowsLog2;
    int tileColsLog2;
    avifEncoder lastEncoder;
    // lastQuantizer and lastQuantizerAlpha are the quantizer and quantizerAlpha values used last
    // time
    int lastQuantizer;
    int lastQuantizerAlpha;
    // lastTileRowsLog2 and lastTileColsLog2 are the actual tiling values used last time
    int lastTileRowsLog2;
    int lastTileColsLog2;
    avifImage * imageMetadata;
    // For convenience, holds metadata derived from the avifGainMap struct (when present) about the
    // altenate image
    avifImage * altImageMetadata;
    uint16_t lastItemID;
    uint16_t primaryItemID;
    avifEncoderItemIdArray alternativeItemIDs; // list of item ids for an 'altr' box (group of alternatives to each other)
    avifBool singleImage; // if true, the AVIF_ADD_IMAGE_FLAG_SINGLE flag was set on the first call to avifEncoderAddImage()
    avifBool alphaPresent;
    size_t gainMapSizeBytes;
    // Fields specific to AV1/AV2
    const char * imageItemType;  // "av01" for AV1 ("av02" for AV2 if AVIF_CODEC_AVM)
    const char * configPropName; // "av1C" for AV1 ("av2C" for AV2 if AVIF_CODEC_AVM)
} avifEncoderData;

static void avifEncoderDataDestroy(avifEncoderData * data);

// Returns NULL if a memory allocation failed.
static avifEncoderData * avifEncoderDataCreate(void)
{
    avifEncoderData * data = (avifEncoderData *)avifAlloc(sizeof(avifEncoderData));
    if (!data) {
        return NULL;
    }
    memset(data, 0, sizeof(avifEncoderData));
    data->imageMetadata = avifImageCreateEmpty();
    if (!data->imageMetadata) {
        goto error;
    }
    data->altImageMetadata = avifImageCreateEmpty();
    if (!data->altImageMetadata) {
        goto error;
    }
    if (!avifArrayCreate(&data->items, sizeof(avifEncoderItem), 8)) {
        goto error;
    }
    if (!avifArrayCreate(&data->frames, sizeof(avifEncoderFrame), 1)) {
        goto error;
    }
    if (!avifArrayCreate(&data->alternativeItemIDs, sizeof(uint16_t), 1)) {
        goto error;
    }
    return data;

error:
    avifEncoderDataDestroy(data);
    return NULL;
}

static avifEncoderItem * avifEncoderDataCreateItem(avifEncoderData * data, const char * type, const char * infeName, size_t infeNameSize, uint32_t cellIndex)
{
    avifEncoderItem * item = (avifEncoderItem *)avifArrayPush(&data->items);
    if (item == NULL) {
        return NULL;
    }
    ++data->lastItemID;
    item->id = data->lastItemID;
    memcpy(item->type, type, sizeof(item->type));
    item->infeName = infeName;
    item->infeNameSize = infeNameSize;
    item->encodeOutput = avifCodecEncodeOutputCreate();
    if (item->encodeOutput == NULL) {
        goto error;
    }
    item->cellIndex = cellIndex;
    if (!avifArrayCreate(&item->mdatFixups, sizeof(avifOffsetFixup), 4)) {
        goto error;
    }
    if (!avifArrayCreate(&item->associations, sizeof(avifItemPropertyAssociation), 4)) {
        goto error;
    }
    return item;

error:
    if (item->encodeOutput != NULL) {
        avifCodecEncodeOutputDestroy(item->encodeOutput);
    }
    avifArrayDestroy(&item->mdatFixups);
    --data->lastItemID;
    avifArrayPop(&data->items);
    return NULL;
}

static avifEncoderItem * avifEncoderDataFindItemByID(avifEncoderData * data, uint16_t id)
{
    for (uint32_t itemIndex = 0; itemIndex < data->items.count; ++itemIndex) {
        avifEncoderItem * item = &data->items.item[itemIndex];
        if (item->id == id) {
            return item;
        }
    }
    return NULL;
}

static void avifEncoderDataDestroy(avifEncoderData * data)
{
    for (uint32_t i = 0; i < data->items.count; ++i) {
        avifEncoderItem * item = &data->items.item[i];
        if (item->codec) {
            avifCodecDestroy(item->codec);
        }
        avifCodecEncodeOutputDestroy(item->encodeOutput);
        avifRWDataFree(&item->metadataPayload);
        avifArrayDestroy(&item->mdatFixups);
        avifArrayDestroy(&item->associations);
    }
    if (data->imageMetadata) {
        avifImageDestroy(data->imageMetadata);
    }
    if (data->altImageMetadata) {
        avifImageDestroy(data->altImageMetadata);
    }
    avifArrayDestroy(&data->items);
    avifArrayDestroy(&data->frames);
    avifArrayDestroy(&data->alternativeItemIDs);
    avifFree(data);
}

static avifResult avifEncoderItemAddMdatFixup(avifEncoderItem * item, const avifRWStream * s)
{
    avifOffsetFixup * fixup = (avifOffsetFixup *)avifArrayPush(&item->mdatFixups);
    AVIF_CHECKERR(fixup != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    fixup->offset = avifRWStreamOffset(s);
    return AVIF_RESULT_OK;
}

// ---------------------------------------------------------------------------
// avifItemPropertyDedup - Provides ipco deduplication

typedef struct avifItemProperty
{
    uint8_t index;
    size_t offset;
    size_t size;
} avifItemProperty;
AVIF_ARRAY_DECLARE(avifItemPropertyArray, avifItemProperty, property);

typedef struct avifItemPropertyDedup
{
    avifItemPropertyArray properties;
    avifRWStream s;    // Temporary stream for each new property, checked against already-written boxes for deduplications
    avifRWData buffer; // Temporary storage for 's'
    uint8_t nextIndex; // 1-indexed, incremented every time another unique property is finished
} avifItemPropertyDedup;

static avifItemPropertyDedup * avifItemPropertyDedupCreate(void)
{
    avifItemPropertyDedup * dedup = (avifItemPropertyDedup *)avifAlloc(sizeof(avifItemPropertyDedup));
    if (dedup == NULL) {
        return NULL;
    }
    memset(dedup, 0, sizeof(avifItemPropertyDedup));
    if (!avifArrayCreate(&dedup->properties, sizeof(avifItemProperty), 8)) {
        avifFree(dedup);
        return NULL;
    }
    if (avifRWDataRealloc(&dedup->buffer, 2048) != AVIF_RESULT_OK) {
        avifArrayDestroy(&dedup->properties);
        avifFree(dedup);
        return NULL;
    }
    return dedup;
}

static void avifItemPropertyDedupDestroy(avifItemPropertyDedup * dedup)
{
    avifArrayDestroy(&dedup->properties);
    avifRWDataFree(&dedup->buffer);
    avifFree(dedup);
}

// Resets the dedup's temporary write stream in preparation for a single item property's worth of writing
static void avifItemPropertyDedupStart(avifItemPropertyDedup * dedup)
{
    avifRWStreamStart(&dedup->s, &dedup->buffer);
}

// This compares the newly written item property (in the dedup's temporary storage buffer) to
// already-written properties (whose offsets/sizes in outputStream are recorded in the dedup). If a
// match is found, the previous property's index is used. If this new property is unique, it is
// assigned the next available property index, written to the output stream, and its offset/size in
// the output stream is recorded in the dedup for future comparisons.
//
// On success, this function adds to the given ipma box a property association linking the reused
// or newly created property with the item.
static avifResult avifItemPropertyDedupFinish(avifItemPropertyDedup * dedup,
                                              avifRWStream * outputStream,
                                              avifItemPropertyAssociationArray * associations,
                                              avifBool essential)
{
    uint8_t propertyIndex = 0;
    const size_t newPropertySize = avifRWStreamOffset(&dedup->s);

    for (size_t i = 0; i < dedup->properties.count; ++i) {
        avifItemProperty * property = &dedup->properties.property[i];
        if ((property->size == newPropertySize) &&
            !memcmp(&outputStream->raw->data[property->offset], dedup->buffer.data, newPropertySize)) {
            // We've already written this exact property, reuse it
            propertyIndex = property->index;
            AVIF_ASSERT_OR_RETURN(propertyIndex != 0);
            break;
        }
    }

    if (propertyIndex == 0) {
        // Write a new property, and remember its location in the output stream for future deduplication
        avifItemProperty * property = (avifItemProperty *)avifArrayPush(&dedup->properties);
        AVIF_CHECKERR(property != NULL, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKERR(dedup->nextIndex < MAX_PROPERTY_INDEX, AVIF_RESULT_INVALID_ARGUMENT);
        property->index = ++dedup->nextIndex; // preincrement so the first new index is 1 (as ipma is 1-indexed)
        property->size = newPropertySize;
        property->offset = avifRWStreamOffset(outputStream);
        AVIF_CHECKRES(avifRWStreamWrite(outputStream, dedup->buffer.data, newPropertySize));
        propertyIndex = property->index;
    }

    avifItemPropertyAssociation * association = (avifItemPropertyAssociation *)avifArrayPush(associations);
    AVIF_CHECKERR(association != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    association->property_index = propertyIndex;
    association->essential = essential;
    return AVIF_RESULT_OK;
}

// ---------------------------------------------------------------------------

static const avifScalingMode noScaling = { { 1, 1 }, { 1, 1 } };

avifEncoder * avifEncoderCreate(void)
{
    avifEncoder * encoder = (avifEncoder *)avifAlloc(sizeof(avifEncoder));
    if (!encoder) {
        return NULL;
    }
    memset(encoder, 0, sizeof(avifEncoder));
    encoder->codecChoice = AVIF_CODEC_CHOICE_AUTO;
    encoder->maxThreads = 1;
    encoder->speed = AVIF_SPEED_DEFAULT;
    encoder->keyframeInterval = 0;
    encoder->timescale = 1;
    encoder->repetitionCount = AVIF_REPETITION_COUNT_INFINITE;
    encoder->quality = AVIF_QUALITY_DEFAULT;
    encoder->qualityAlpha = AVIF_QUALITY_DEFAULT;
    encoder->qualityGainMap = AVIF_QUALITY_DEFAULT;
    encoder->minQuantizer = AVIF_QUANTIZER_BEST_QUALITY;
    encoder->maxQuantizer = AVIF_QUANTIZER_WORST_QUALITY;
    encoder->minQuantizerAlpha = AVIF_QUANTIZER_BEST_QUALITY;
    encoder->maxQuantizerAlpha = AVIF_QUANTIZER_WORST_QUALITY;
    encoder->tileRowsLog2 = 0;
    encoder->tileColsLog2 = 0;
    encoder->autoTiling = AVIF_FALSE;
    encoder->scalingMode = noScaling;
    encoder->data = avifEncoderDataCreate();
    encoder->csOptions = avifCodecSpecificOptionsCreate();
    if (!encoder->data || !encoder->csOptions) {
        avifEncoderDestroy(encoder);
        return NULL;
    }
    encoder->headerFormat = AVIF_HEADER_DEFAULT;
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    encoder->sampleTransformRecipe = AVIF_SAMPLE_TRANSFORM_NONE;
#endif
    return encoder;
}

void avifEncoderDestroy(avifEncoder * encoder)
{
    if (encoder->csOptions) {
        avifCodecSpecificOptionsDestroy(encoder->csOptions);
    }
    if (encoder->data) {
        avifEncoderDataDestroy(encoder->data);
    }
    avifFree(encoder);
}

avifResult avifEncoderSetCodecSpecificOption(avifEncoder * encoder, const char * key, const char * value)
{
    return avifCodecSpecificOptionsSet(encoder->csOptions, key, value);
}

static void avifEncoderBackupSettings(avifEncoder * encoder)
{
    avifEncoder * lastEncoder = &encoder->data->lastEncoder;

    // lastEncoder->data is only used to mark that lastEncoder is initialized. lastEncoder->data
    // must not be dereferenced.
    lastEncoder->data = encoder->data;
    lastEncoder->codecChoice = encoder->codecChoice;
    lastEncoder->maxThreads = encoder->maxThreads;
    lastEncoder->speed = encoder->speed;
    lastEncoder->keyframeInterval = encoder->keyframeInterval;
    lastEncoder->timescale = encoder->timescale;
    lastEncoder->repetitionCount = encoder->repetitionCount;
    lastEncoder->extraLayerCount = encoder->extraLayerCount;
    lastEncoder->minQuantizer = encoder->minQuantizer;
    lastEncoder->maxQuantizer = encoder->maxQuantizer;
    lastEncoder->minQuantizerAlpha = encoder->minQuantizerAlpha;
    lastEncoder->maxQuantizerAlpha = encoder->maxQuantizerAlpha;
    encoder->data->lastQuantizer = encoder->data->quantizer;
    encoder->data->lastQuantizerAlpha = encoder->data->quantizerAlpha;
    encoder->data->lastTileRowsLog2 = encoder->data->tileRowsLog2;
    encoder->data->lastTileColsLog2 = encoder->data->tileColsLog2;
    lastEncoder->scalingMode = encoder->scalingMode;
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    lastEncoder->sampleTransformRecipe = encoder->sampleTransformRecipe;
#endif
}

// This function detects changes made on avifEncoder. It returns true on success (i.e., if every
// change is valid), or false on failure (i.e., if any setting that can't change was changed). It
// reports a bitwise-OR of detected changes in encoderChanges.
static avifBool avifEncoderDetectChanges(const avifEncoder * encoder, avifEncoderChanges * encoderChanges)
{
    const avifEncoder * lastEncoder = &encoder->data->lastEncoder;
    *encoderChanges = 0;

    if (!lastEncoder->data) {
        // lastEncoder is not initialized.
        return AVIF_TRUE;
    }

    if ((lastEncoder->codecChoice != encoder->codecChoice) || (lastEncoder->maxThreads != encoder->maxThreads) ||
        (lastEncoder->speed != encoder->speed) || (lastEncoder->keyframeInterval != encoder->keyframeInterval) ||
        (lastEncoder->timescale != encoder->timescale) || (lastEncoder->repetitionCount != encoder->repetitionCount) ||
        (lastEncoder->extraLayerCount != encoder->extraLayerCount)) {
        return AVIF_FALSE;
    }

    if (encoder->data->lastQuantizer != encoder->data->quantizer) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_QUANTIZER;
    }
    if (encoder->data->lastQuantizerAlpha != encoder->data->quantizerAlpha) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_QUANTIZER_ALPHA;
    }
    if (lastEncoder->minQuantizer != encoder->minQuantizer) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_MIN_QUANTIZER;
    }
    if (lastEncoder->maxQuantizer != encoder->maxQuantizer) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_MAX_QUANTIZER;
    }
    if (lastEncoder->minQuantizerAlpha != encoder->minQuantizerAlpha) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_MIN_QUANTIZER_ALPHA;
    }
    if (lastEncoder->maxQuantizerAlpha != encoder->maxQuantizerAlpha) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_MAX_QUANTIZER_ALPHA;
    }
    if (encoder->data->lastTileRowsLog2 != encoder->data->tileRowsLog2) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_TILE_ROWS_LOG2;
    }
    if (encoder->data->lastTileColsLog2 != encoder->data->tileColsLog2) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_TILE_COLS_LOG2;
    }
    if (memcmp(&lastEncoder->scalingMode, &encoder->scalingMode, sizeof(avifScalingMode)) != 0) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_SCALING_MODE;
    }
    if (encoder->csOptions->count > 0) {
        *encoderChanges |= AVIF_ENCODER_CHANGE_CODEC_SPECIFIC;
    }

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (lastEncoder->sampleTransformRecipe != encoder->sampleTransformRecipe) {
        return AVIF_FALSE;
    }
#endif

    return AVIF_TRUE;
}

// Same as 'avifEncoderWriteColorProperties' but for the colr nclx box only.
static avifResult avifEncoderWriteNclxProperty(avifRWStream * dedupStream,
                                               avifRWStream * outputStream,
                                               const avifImage * imageMetadata,
                                               avifItemPropertyAssociationArray * associations,
                                               avifItemPropertyDedup * dedup)
{
    if (dedup) {
        avifItemPropertyDedupStart(dedup);
    }
    avifBoxMarker colr;
    AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "colr", AVIF_BOX_SIZE_TBD, &colr));
    AVIF_CHECKRES(avifRWStreamWriteChars(dedupStream, "nclx", 4));                   // unsigned int(32) colour_type;
    AVIF_CHECKRES(avifRWStreamWriteU16(dedupStream, imageMetadata->colorPrimaries)); // unsigned int(16) colour_primaries;
    AVIF_CHECKRES(avifRWStreamWriteU16(dedupStream, imageMetadata->transferCharacteristics)); // unsigned int(16) transfer_characteristics;
    AVIF_CHECKRES(avifRWStreamWriteU16(dedupStream, imageMetadata->matrixCoefficients)); // unsigned int(16) matrix_coefficients;
    AVIF_CHECKRES(avifRWStreamWriteBits(dedupStream, (imageMetadata->yuvRange == AVIF_RANGE_FULL) ? 1 : 0, /*bitCount=*/1)); // unsigned int(1) full_range_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(dedupStream, 0, /*bitCount=*/7)); // unsigned int(7) reserved = 0;
    avifRWStreamFinishBox(dedupStream, colr);
    if (dedup) {
        AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_FALSE));
    }
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderWritePaspProperty(avifRWStream * dedupStream,
                                               avifRWStream * outputStream,
                                               const avifImage * imageMetadata,
                                               avifItemPropertyAssociationArray * associations,
                                               avifItemPropertyDedup * dedup);
static avifResult avifEncoderWriteTransformativeProperties(avifRWStream * dedupStream,
                                                           avifRWStream * outputStream,
                                                           const avifImage * imageMetadata,
                                                           avifItemPropertyAssociationArray * associations,
                                                           avifItemPropertyDedup * dedup);

// This function is used in two codepaths:
// * writing color *item* properties
// * writing color *track* properties
//
// Item properties must have property associations with them and can be deduplicated (by reusing
// these associations), so this function leverages the ipma and dedup arguments to do this.
//
// Track properties, however, are implicitly associated by the track in which they are contained, so
// there is no need to build a property association box (ipma), and no way to deduplicate/reuse a
// property. In this case, the ipma and dedup properties should/will be set to NULL, and this
// function will avoid using them.
static avifResult avifEncoderWriteColorProperties(avifRWStream * outputStream,
                                                  const avifImage * imageMetadata,
                                                  avifItemPropertyAssociationArray * associations,
                                                  avifItemPropertyDedup * dedup)
{
    // outputStream is the final bitstream that will be output by the libavif encoder API.
    // dedupStream is either equal to outputStream or to &dedup->s which is a temporary stream used
    // to store parts of the final bitstream; these parts may be discarded if they are a duplicate
    // of an already stored property.
    avifRWStream * dedupStream = outputStream;
    if (dedup) {
        AVIF_ASSERT_OR_RETURN(associations);

        // Use the dedup's temporary stream for box writes.
        dedupStream = &dedup->s;
    }

    if (imageMetadata->icc.size > 0) {
        if (dedup) {
            avifItemPropertyDedupStart(dedup);
        }
        avifBoxMarker colr;
        AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "colr", AVIF_BOX_SIZE_TBD, &colr));
        AVIF_CHECKRES(avifRWStreamWriteChars(dedupStream, "prof", 4)); // unsigned int(32) colour_type;
        AVIF_CHECKRES(avifRWStreamWrite(dedupStream, imageMetadata->icc.data, imageMetadata->icc.size));
        avifRWStreamFinishBox(dedupStream, colr);
        if (dedup) {
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_FALSE));
        }
    }

    // HEIF 6.5.5.1, from Amendment 3 allows multiple colr boxes: "at most one for a given value of colour type"
    // Therefore, *always* writing an nclx box, even if a prof box was already written above.
    AVIF_CHECKRES(avifEncoderWriteNclxProperty(dedupStream, outputStream, imageMetadata, associations, dedup));

    AVIF_CHECKRES(avifEncoderWritePaspProperty(dedupStream, outputStream, imageMetadata, associations, dedup));

    return AVIF_RESULT_OK;
}

static avifResult avifEncoderWriteContentLightLevelInformation(avifRWStream * outputStream,
                                                               const avifContentLightLevelInformationBox * clli)
{
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, clli->maxCLL, 16));  // unsigned int(16) max_content_light_level;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, clli->maxPALL, 16)); // unsigned int(16) max_pic_average_light_level;
    return AVIF_RESULT_OK;
}

// Same as 'avifEncoderWriteColorProperties' but for properties related to High Dynamic Range only.
static avifResult avifEncoderWriteHDRProperties(avifRWStream * dedupStream,
                                                avifRWStream * outputStream,
                                                const avifImage * imageMetadata,
                                                avifItemPropertyAssociationArray * associations,
                                                avifItemPropertyDedup * dedup)
{
    // Write Content Light Level Information, if present
    if (imageMetadata->clli.maxCLL || imageMetadata->clli.maxPALL) {
        if (dedup) {
            avifItemPropertyDedupStart(dedup);
        }
        avifBoxMarker clli;
        AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "clli", AVIF_BOX_SIZE_TBD, &clli));
        AVIF_CHECKRES(avifEncoderWriteContentLightLevelInformation(dedupStream, &imageMetadata->clli));
        avifRWStreamFinishBox(dedupStream, clli);
        if (dedup) {
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_FALSE));
        }
    }

    // TODO(maryla): add other HDR boxes: mdcv, cclv, etc. (in avifEncoderWriteMiniHDRProperties() too)

    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
static avifResult avifEncoderWriteMiniHDRProperties(avifRWStream * outputStream, const avifImage * imageMetadata)
{
    const avifBool hasClli = imageMetadata->clli.maxCLL != 0 || imageMetadata->clli.maxPALL != 0;
    const avifBool hasMdcv = AVIF_FALSE;
    const avifBool hasCclv = AVIF_FALSE;
    const avifBool hasAmve = AVIF_FALSE;
    const avifBool hasReve = AVIF_FALSE;
    const avifBool hasNdwt = AVIF_FALSE;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, hasClli, 1)); // bit(1) clli_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, hasMdcv, 1)); // bit(1) mdcv_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, hasCclv, 1)); // bit(1) cclv_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, hasAmve, 1)); // bit(1) amve_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, hasReve, 1)); // bit(1) reve_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(outputStream, hasNdwt, 1)); // bit(1) ndwt_flag;

    if (hasClli) {
        // ContentLightLevel clli;
        AVIF_CHECKRES(avifEncoderWriteContentLightLevelInformation(outputStream, &imageMetadata->clli));
    }
    if (hasMdcv) {
        // MasteringDisplayColourVolume mdcv;
    }
    if (hasCclv) {
        // ContentColourVolume cclv;
    }
    if (hasAmve) {
        // AmbientViewingEnvironment amve;
    }
    if (hasReve) {
        // ReferenceViewingEnvironment reve;
    }
    if (hasNdwt) {
        // NominalDiffuseWhite ndwt;
    }
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

static avifResult avifEncoderWritePaspProperty(avifRWStream * dedupStream,
                                               avifRWStream * outputStream,
                                               const avifImage * imageMetadata,
                                               avifItemPropertyAssociationArray * associations,
                                               avifItemPropertyDedup * dedup)
{
    if (imageMetadata->transformFlags & AVIF_TRANSFORM_PASP) {
        if (dedup) {
            avifItemPropertyDedupStart(dedup);
        }
        avifBoxMarker pasp;
        AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "pasp", AVIF_BOX_SIZE_TBD, &pasp));
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->pasp.hSpacing)); // unsigned int(32) hSpacing;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->pasp.vSpacing)); // unsigned int(32) vSpacing;
        avifRWStreamFinishBox(dedupStream, pasp);
        if (dedup) {
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_FALSE));
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderWriteTransformativeProperties(avifRWStream * dedupStream,
                                                           avifRWStream * outputStream,
                                                           const avifImage * imageMetadata,
                                                           avifItemPropertyAssociationArray * associations,
                                                           avifItemPropertyDedup * dedup)
{
    if (imageMetadata->transformFlags & AVIF_TRANSFORM_CLAP) {
        if (dedup) {
            avifItemPropertyDedupStart(dedup);
        }
        avifBoxMarker clap;
        AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "clap", AVIF_BOX_SIZE_TBD, &clap));
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.widthN));    // unsigned int(32) cleanApertureWidthN;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.widthD));    // unsigned int(32) cleanApertureWidthD;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.heightN));   // unsigned int(32) cleanApertureHeightN;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.heightD));   // unsigned int(32) cleanApertureHeightD;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.horizOffN)); // unsigned int(32) horizOffN;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.horizOffD)); // unsigned int(32) horizOffD;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.vertOffN));  // unsigned int(32) vertOffN;
        AVIF_CHECKRES(avifRWStreamWriteU32(dedupStream, imageMetadata->clap.vertOffD));  // unsigned int(32) vertOffD;
        avifRWStreamFinishBox(dedupStream, clap);
        if (dedup) {
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_TRUE));
        }
    }
    if (imageMetadata->transformFlags & AVIF_TRANSFORM_IROT) {
        if (dedup) {
            avifItemPropertyDedupStart(dedup);
        }
        avifBoxMarker irot;
        AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "irot", AVIF_BOX_SIZE_TBD, &irot));
        AVIF_CHECKRES(avifRWStreamWriteBits(dedupStream, 0, /*bitCount=*/6)); // unsigned int (6) reserved = 0;
        AVIF_CHECKRES(avifRWStreamWriteBits(dedupStream, imageMetadata->irot.angle & 0x3, /*bitCount=*/2)); // unsigned int (2) angle;
        avifRWStreamFinishBox(dedupStream, irot);
        if (dedup) {
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_TRUE));
        }
    }
    if (imageMetadata->transformFlags & AVIF_TRANSFORM_IMIR) {
        if (dedup) {
            avifItemPropertyDedupStart(dedup);
        }
        avifBoxMarker imir;
        AVIF_CHECKRES(avifRWStreamWriteBox(dedupStream, "imir", AVIF_BOX_SIZE_TBD, &imir));
        AVIF_CHECKRES(avifRWStreamWriteBits(dedupStream, 0, /*bitCount=*/7)); // unsigned int(7) reserved = 0;
        AVIF_CHECKRES(avifRWStreamWriteBits(dedupStream, imageMetadata->imir.axis ? 1 : 0, /*bitCount=*/1)); // unsigned int(1) axis;
        avifRWStreamFinishBox(dedupStream, imir);
        if (dedup) {
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, outputStream, associations, /*essential=*/AVIF_TRUE));
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifRWStreamWriteHandlerBox(avifRWStream * s, const char handlerType[4])
{
    avifBoxMarker hdlr;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(s, "hdlr", AVIF_BOX_SIZE_TBD, 0, 0, &hdlr));
    AVIF_CHECKRES(avifRWStreamWriteU32(s, 0));                // unsigned int(32) pre_defined = 0;
    AVIF_CHECKRES(avifRWStreamWriteChars(s, handlerType, 4)); // unsigned int(32) handler_type;
    AVIF_CHECKRES(avifRWStreamWriteZeros(s, 12));             // const unsigned int(32)[3] reserved = 0;
    AVIF_CHECKRES(avifRWStreamWriteChars(s, "", 1));          // string name; (writing null terminator)
    avifRWStreamFinishBox(s, hdlr);
    return AVIF_RESULT_OK;
}

// Write unassociated metadata items (EXIF, XMP) to a small meta box inside of a trak box.
// These items are implicitly associated with the track they are contained within.
static avifResult avifEncoderWriteTrackMetaBox(avifEncoder * encoder, avifRWStream * s)
{
    // Count how many non-image items (such as EXIF/XMP) are being written
    uint32_t metadataItemCount = 0;
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        if (memcmp(item->type, encoder->data->imageItemType, 4) != 0) {
            ++metadataItemCount;
        }
    }
    if (metadataItemCount == 0) {
        // Don't even bother writing the trak meta box
        return AVIF_RESULT_OK;
    }

    avifBoxMarker meta;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(s, "meta", AVIF_BOX_SIZE_TBD, 0, 0, &meta));

    AVIF_CHECKRES(avifRWStreamWriteHandlerBox(s, "pict"));

    avifBoxMarker iloc;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(s, "iloc", AVIF_BOX_SIZE_TBD, 0, 0, &iloc));
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 4, /*bitCount=*/4));          // unsigned int(4) offset_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 4, /*bitCount=*/4));          // unsigned int(4) length_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, /*bitCount=*/4));          // unsigned int(4) base_offset_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, /*bitCount=*/4));          // unsigned int(4) reserved;
    AVIF_CHECKRES(avifRWStreamWriteU16(s, (uint16_t)metadataItemCount)); // unsigned int(16) item_count;
    for (uint32_t trakItemIndex = 0; trakItemIndex < encoder->data->items.count; ++trakItemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[trakItemIndex];
        if (memcmp(item->type, encoder->data->imageItemType, 4) == 0) {
            // Skip over all non-metadata items
            continue;
        }

        AVIF_CHECKRES(avifRWStreamWriteU16(s, item->id));          // unsigned int(16) item_ID;
        AVIF_CHECKRES(avifRWStreamWriteU16(s, 0));                 // unsigned int(16) data_reference_index;
        AVIF_CHECKRES(avifRWStreamWriteU16(s, 1));                 // unsigned int(16) extent_count;
        AVIF_CHECKRES(avifEncoderItemAddMdatFixup(item, s));       //
        AVIF_CHECKRES(avifRWStreamWriteU32(s, 0 /* set later */)); // unsigned int(offset_size*8) extent_offset;
        AVIF_CHECKRES(avifRWStreamWriteU32(s, (uint32_t)item->metadataPayload.size)); // unsigned int(length_size*8) extent_length;
    }
    avifRWStreamFinishBox(s, iloc);

    avifBoxMarker iinf;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(s, "iinf", AVIF_BOX_SIZE_TBD, 0, 0, &iinf));
    AVIF_CHECKRES(avifRWStreamWriteU16(s, (uint16_t)metadataItemCount)); //  unsigned int(16) entry_count;
    for (uint32_t trakItemIndex = 0; trakItemIndex < encoder->data->items.count; ++trakItemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[trakItemIndex];
        if (memcmp(item->type, encoder->data->imageItemType, 4) == 0) {
            continue;
        }

        AVIF_ASSERT_OR_RETURN(!item->hiddenImage);
        avifBoxMarker infe;
        AVIF_CHECKRES(avifRWStreamWriteFullBox(s, "infe", AVIF_BOX_SIZE_TBD, 2, 0, &infe));
        AVIF_CHECKRES(avifRWStreamWriteU16(s, item->id));                             // unsigned int(16) item_ID;
        AVIF_CHECKRES(avifRWStreamWriteU16(s, 0));                                    // unsigned int(16) item_protection_index;
        AVIF_CHECKRES(avifRWStreamWrite(s, item->type, 4));                           // unsigned int(32) item_type;
        AVIF_CHECKRES(avifRWStreamWriteChars(s, item->infeName, item->infeNameSize)); // string item_name; (writing null terminator)
        if (item->infeContentType && item->infeContentTypeSize) { // string content_type; (writing null terminator)
            AVIF_CHECKRES(avifRWStreamWriteChars(s, item->infeContentType, item->infeContentTypeSize));
        }
        avifRWStreamFinishBox(s, infe);
    }
    avifRWStreamFinishBox(s, iinf);

    avifRWStreamFinishBox(s, meta);
    return AVIF_RESULT_OK;
}

static avifResult avifWriteGridPayload(avifRWData * data, uint32_t gridCols, uint32_t gridRows, uint32_t gridWidth, uint32_t gridHeight)
{
    // ISO/IEC 23008-12 6.6.2.3.2
    // aligned(8) class ImageGrid {
    //     unsigned int(8) version = 0;
    //     unsigned int(8) flags;
    //     FieldLength = ((flags & 1) + 1) * 16;
    //     unsigned int(8) rows_minus_one;
    //     unsigned int(8) columns_minus_one;
    //     unsigned int(FieldLength) output_width;
    //     unsigned int(FieldLength) output_height;
    // }

    uint8_t gridFlags = ((gridWidth > 65535) || (gridHeight > 65535)) ? 1 : 0;

    avifRWStream s;
    avifRWStreamStart(&s, data);
    AVIF_CHECKRES(avifRWStreamWriteU8(&s, 0));                       // unsigned int(8) version = 0;
    AVIF_CHECKRES(avifRWStreamWriteU8(&s, gridFlags));               // unsigned int(8) flags;
    AVIF_CHECKRES(avifRWStreamWriteU8(&s, (uint8_t)(gridRows - 1))); // unsigned int(8) rows_minus_one;
    AVIF_CHECKRES(avifRWStreamWriteU8(&s, (uint8_t)(gridCols - 1))); // unsigned int(8) columns_minus_one;
    if (gridFlags & 1) {
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, gridWidth));  // unsigned int(FieldLength) output_width;
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, gridHeight)); // unsigned int(FieldLength) output_height;
    } else {
        uint16_t tmpWidth = (uint16_t)gridWidth;
        uint16_t tmpHeight = (uint16_t)gridHeight;
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, tmpWidth));  // unsigned int(FieldLength) output_width;
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, tmpHeight)); // unsigned int(FieldLength) output_height;
    }
    avifRWStreamFinishWrite(&s);
    return AVIF_RESULT_OK;
}

static avifBool avifGainMapIdenticalChannels(const avifGainMap * gainMap)
{
    return gainMap->gainMapMin[0].n == gainMap->gainMapMin[1].n && gainMap->gainMapMin[0].n == gainMap->gainMapMin[2].n &&
           gainMap->gainMapMin[0].d == gainMap->gainMapMin[1].d && gainMap->gainMapMin[0].d == gainMap->gainMapMin[2].d &&
           gainMap->gainMapMax[0].n == gainMap->gainMapMax[1].n && gainMap->gainMapMax[0].n == gainMap->gainMapMax[2].n &&
           gainMap->gainMapMax[0].d == gainMap->gainMapMax[1].d && gainMap->gainMapMax[0].d == gainMap->gainMapMax[2].d &&
           gainMap->gainMapGamma[0].n == gainMap->gainMapGamma[1].n && gainMap->gainMapGamma[0].n == gainMap->gainMapGamma[2].n &&
           gainMap->gainMapGamma[0].d == gainMap->gainMapGamma[1].d && gainMap->gainMapGamma[0].d == gainMap->gainMapGamma[2].d &&
           gainMap->baseOffset[0].n == gainMap->baseOffset[1].n && gainMap->baseOffset[0].n == gainMap->baseOffset[2].n &&
           gainMap->baseOffset[0].d == gainMap->baseOffset[1].d && gainMap->baseOffset[0].d == gainMap->baseOffset[2].d &&
           gainMap->alternateOffset[0].n == gainMap->alternateOffset[1].n &&
           gainMap->alternateOffset[0].n == gainMap->alternateOffset[2].n &&
           gainMap->alternateOffset[0].d == gainMap->alternateOffset[1].d &&
           gainMap->alternateOffset[0].d == gainMap->alternateOffset[2].d;
}

// Returns the number of bytes written by avifWriteGainmapMetadata().
static uint32_t avifGainMapMetadataSize(const avifGainMap * gainMap)
{
    const uint8_t channelCount = avifGainMapIdenticalChannels(gainMap) ? 1u : 3u;
    return (uint32_t)(sizeof(uint16_t) * 2 + sizeof(uint8_t) + sizeof(uint32_t) * 4 + channelCount * sizeof(uint32_t) * 10);
}

static avifResult avifWriteGainmapMetadata(avifRWStream * s, const avifGainMap * gainMap, avifDiagnostics * diag)
{
    AVIF_CHECKRES(avifGainMapValidateMetadata(gainMap, diag));
    const size_t offset = avifRWStreamOffset(s);

    // GainMapMetadata syntax as per clause C.2.2 of ISO 21496-1:

    // GainMapVersion syntax as per clause C.2.2 of ISO 21496-1:
    const uint16_t minimumVersion = 0;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, minimumVersion, 16)); // unsigned int(16) minimum_version;
    const uint16_t writerVersion = 0;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, writerVersion, 16)); // unsigned int(16) writer_version;

    if (minimumVersion == 0) {
        const uint8_t channelCount = avifGainMapIdenticalChannels(gainMap) ? 1u : 3u;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, channelCount == 3, 1));          // unsigned int(1) is_multichannel;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->useBaseColorSpace, 1)); // unsigned int(1) use_base_colour_space;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, 6));                          // unsigned int(6) reserved;

        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->baseHdrHeadroom.n, 32)); // unsigned int(32) base_hdr_headroom_numerator;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->baseHdrHeadroom.d, 32)); // unsigned int(32) base_hdr_headroom_denominator;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->alternateHdrHeadroom.n, 32)); // unsigned int(32) alternate_hdr_headroom_numerator;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->alternateHdrHeadroom.d, 32)); // unsigned int(32) alternate_hdr_headroom_denominator;

        // GainMapChannel channels[channel_count];
        for (int c = 0; c < channelCount; ++c) {
            // GainMapChannel syntax as per clause C.2.2 of ISO 21496-1:
            AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)gainMap->gainMapMin[c].n, 32)); // int(32) gain_map_min_numerator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->gainMapMin[c].d, 32)); // unsigned int(32) gain_map_min_denominator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)gainMap->gainMapMax[c].n, 32)); // int(32) gain_map_max_numerator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->gainMapMax[c].d, 32));   // unsigned int(32) gain_map_max_denominator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->gainMapGamma[c].n, 32)); // unsigned int(32) gamma_numerator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->gainMapGamma[c].d, 32)); // unsigned int(32) gamma_denominator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)gainMap->baseOffset[c].n, 32)); // int(32) base_offset_numerator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->baseOffset[c].d, 32)); // unsigned int(32) base_offset_denominator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)gainMap->alternateOffset[c].n, 32)); // int(32) alternate_offset_numerator;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainMap->alternateOffset[c].d, 32)); // unsigned int(32) alternate_offset_denominator;
        }
    }

    AVIF_ASSERT_OR_RETURN(avifRWStreamOffset(s) == offset + avifGainMapMetadataSize(gainMap));
    return AVIF_RESULT_OK;
}

static avifResult avifWriteToneMappedImagePayload(avifRWData * data, const avifGainMap * gainMap, avifDiagnostics * diag)
{
    avifRWStream s;
    avifRWStreamStart(&s, data);
    // ToneMapImage syntax as per section 6.6.2.4.2 of ISO/IEC 23008-12:2024
    // amendment "Support for tone map derived image items and other improvements":
    const uint8_t version = 0;
    AVIF_CHECKRES(avifRWStreamWriteU8(&s, version)); // unsigned int(8) version = 0;
    if (version == 0) {
        AVIF_CHECKRES(avifWriteGainmapMetadata(&s, gainMap, diag)); // GainMapMetadata;
    }
    avifRWStreamFinishWrite(&s);
    return AVIF_RESULT_OK;
}

size_t avifEncoderGetGainMapSizeBytes(avifEncoder * encoder)
{
    return encoder->data->gainMapSizeBytes;
}

// Sets altImageMetadata's metadata values to represent the "alternate" image as if applying the gain map to the base image.
// For grid images, imageWithGainMap is the metadata of the first cell. gridWidth and gridHeight are the dimensions of the
// full image.
static avifResult avifImageCopyAltImageMetadata(avifImage * altImageMetadata, const avifImage * imageWithGainMap, uint32_t gridWidth, uint32_t gridHeight)
{
    altImageMetadata->width = gridWidth;
    altImageMetadata->height = gridHeight;
    AVIF_CHECKRES(avifRWDataSet(&altImageMetadata->icc, imageWithGainMap->gainMap->altICC.data, imageWithGainMap->gainMap->altICC.size));
    altImageMetadata->colorPrimaries = imageWithGainMap->gainMap->altColorPrimaries;
    altImageMetadata->transferCharacteristics = imageWithGainMap->gainMap->altTransferCharacteristics;
    altImageMetadata->matrixCoefficients = imageWithGainMap->gainMap->altMatrixCoefficients;
    altImageMetadata->yuvRange = imageWithGainMap->gainMap->altYUVRange;
    altImageMetadata->depth = imageWithGainMap->gainMap->altDepth
                                  ? imageWithGainMap->gainMap->altDepth
                                  : AVIF_MAX(imageWithGainMap->depth, imageWithGainMap->gainMap->image->depth);
    altImageMetadata->yuvFormat = (imageWithGainMap->gainMap->altPlaneCount == 1) ? AVIF_PIXEL_FORMAT_YUV400 : AVIF_PIXEL_FORMAT_YUV444;
    altImageMetadata->clli = imageWithGainMap->gainMap->altCLLI;
    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
static avifResult avifEncoderWriteSampleTransformTokens(avifRWStream * s, const avifSampleTransformExpression * expression)
{
    AVIF_ASSERT_OR_RETURN(expression->count <= 255);
    AVIF_CHECKRES(avifRWStreamWriteU8(s, (uint8_t)expression->count)); // unsigned int(8) token_count;

    for (uint32_t t = 0; t < expression->count; ++t) {
        const avifSampleTransformToken * token = &expression->tokens[t];

        if (token->type == AVIF_SAMPLE_TRANSFORM_CONSTANT) {
            AVIF_CHECKRES(avifRWStreamWriteU8(s, token->type)); // unsigned int(8) token;
            const uint32_t constant = (uint32_t)token->constant;
            AVIF_CHECKRES(avifRWStreamWriteU32(s, constant)); // signed int(1<<(bit_depth+3)) constant;
        } else if (token->type == AVIF_SAMPLE_TRANSFORM_INPUT_IMAGE_ITEM_INDEX) {
            AVIF_CHECKRES(avifRWStreamWriteU8(s, token->inputImageItemIndex)); // unsigned int(8) token;
        } else {
            // Operator.
            AVIF_CHECKRES(avifRWStreamWriteU8(s, token->type)); // unsigned int(8) token;
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderWriteSampleTransformPayload(avifEncoder * encoder, avifRWData * data)
{
    avifRWStream s;
    avifRWStreamStart(&s, data);
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, 0, /*bitCount=*/2)); // unsigned int(2) version = 0;
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, 0, /*bitCount=*/4)); // unsigned int(4) reserved;
    // AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_32 is necessary because the two input images
    // once combined use 16-bit unsigned values, but intermediate results are stored in signed integers.
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_32, /*bitCount=*/2)); // unsigned int(2) bit_depth;

    avifSampleTransformExpression expression = { 0 };
    AVIF_CHECKRES(avifSampleTransformRecipeToExpression(encoder->sampleTransformRecipe, &expression));
    const avifResult result = avifEncoderWriteSampleTransformTokens(&s, &expression);
    avifArrayDestroy(&expression);
    if (result != AVIF_RESULT_OK) {
        avifDiagnosticsPrintf(&encoder->diag, "Failed to write sample transform metadata for recipe %d", (int)encoder->sampleTransformRecipe);
        return result;
    }

    avifRWStreamFinishWrite(&s);
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

static avifResult avifEncoderDataCreateExifItem(avifEncoderData * data, const avifRWData * exif)
{
    size_t exifTiffHeaderOffset;
    const avifResult result = avifGetExifTiffHeaderOffset(exif->data, exif->size, &exifTiffHeaderOffset);
    if (result != AVIF_RESULT_OK) {
        // Couldn't find the TIFF header
        return result;
    }

    avifEncoderItem * exifItem = avifEncoderDataCreateItem(data, "Exif", "Exif", 5, 0);
    if (!exifItem) {
        return AVIF_RESULT_OUT_OF_MEMORY;
    }
    exifItem->irefToID = data->primaryItemID;
    exifItem->irefType = "cdsc";

    const uint32_t offset32bit = avifHTONL((uint32_t)exifTiffHeaderOffset);
    AVIF_CHECKRES(avifRWDataRealloc(&exifItem->metadataPayload, sizeof(offset32bit) + exif->size));
    memcpy(exifItem->metadataPayload.data, &offset32bit, sizeof(offset32bit));
    memcpy(exifItem->metadataPayload.data + sizeof(offset32bit), exif->data, exif->size);
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderDataCreateXMPItem(avifEncoderData * data, const avifRWData * xmp)
{
    avifEncoderItem * xmpItem = avifEncoderDataCreateItem(data, "mime", "XMP", 4, 0);
    if (!xmpItem) {
        return AVIF_RESULT_OUT_OF_MEMORY;
    }
    xmpItem->irefToID = data->primaryItemID;
    xmpItem->irefType = "cdsc";

    xmpItem->infeContentType = xmpContentType;
    xmpItem->infeContentTypeSize = xmpContentTypeSize;
    AVIF_CHECKRES(avifRWDataSet(&xmpItem->metadataPayload, xmp->data, xmp->size));
    return AVIF_RESULT_OK;
}

// Same as avifImageCopy() but pads the dstImage with border pixel values to reach dstWidth and dstHeight.
static avifResult avifImageCopyAndPad(avifImage * const dstImage, const avifImage * srcImage, uint32_t dstWidth, uint32_t dstHeight)
{
    AVIF_ASSERT_OR_RETURN(dstImage);
    AVIF_ASSERT_OR_RETURN(!dstImage->width && !dstImage->height); // dstImage is not set yet.
    AVIF_ASSERT_OR_RETURN(dstWidth >= srcImage->width);
    AVIF_ASSERT_OR_RETURN(dstHeight >= srcImage->height);

    // Copy all fields but do not allocate the planes.
    AVIF_CHECKRES(avifImageCopy(dstImage, srcImage, (avifPlanesFlag)0));
    dstImage->width = dstWidth;
    dstImage->height = dstHeight;

    if (srcImage->yuvPlanes[AVIF_CHAN_Y]) {
        AVIF_CHECKRES(avifImageAllocatePlanes(dstImage, AVIF_PLANES_YUV));
    }
    if (srcImage->alphaPlane) {
        AVIF_CHECKRES(avifImageAllocatePlanes(dstImage, AVIF_PLANES_A));
    }
    const avifBool usesU16 = avifImageUsesU16(srcImage);
    for (int plane = AVIF_CHAN_Y; plane <= AVIF_CHAN_A; ++plane) {
        const uint8_t * srcRow = avifImagePlane(srcImage, plane);
        const uint32_t srcRowBytes = avifImagePlaneRowBytes(srcImage, plane);
        const uint32_t srcPlaneWidth = avifImagePlaneWidth(srcImage, plane);
        const uint32_t srcPlaneHeight = avifImagePlaneHeight(srcImage, plane); // 0 for A if no alpha and 0 for UV if 4:0:0.
        const size_t srcPlaneWidthBytes = (size_t)srcPlaneWidth << usesU16;

        uint8_t * dstRow = avifImagePlane(dstImage, plane);
        const uint32_t dstRowBytes = avifImagePlaneRowBytes(dstImage, plane);
        const uint32_t dstPlaneWidth = avifImagePlaneWidth(dstImage, plane);
        const uint32_t dstPlaneHeight = avifImagePlaneHeight(dstImage, plane); // 0 for A if no alpha and 0 for UV if 4:0:0.
        const size_t dstPlaneWidthBytes = (size_t)dstPlaneWidth << usesU16;

        for (uint32_t j = 0; j < srcPlaneHeight; ++j) {
            memcpy(dstRow, srcRow, srcPlaneWidthBytes);

            // Pad columns.
            if (dstPlaneWidth > srcPlaneWidth) {
                if (usesU16) {
                    uint16_t * dstRow16 = (uint16_t *)dstRow;
                    for (uint32_t x = srcPlaneWidth; x < dstPlaneWidth; ++x) {
                        dstRow16[x] = dstRow16[srcPlaneWidth - 1];
                    }
                } else {
                    memset(&dstRow[srcPlaneWidth], dstRow[srcPlaneWidth - 1], dstPlaneWidth - srcPlaneWidth);
                }
            }
            srcRow += srcRowBytes;
            dstRow += dstRowBytes;
        }

        // Pad rows.
        for (uint32_t j = srcPlaneHeight; j < dstPlaneHeight; ++j) {
            memcpy(dstRow, dstRow - dstRowBytes, dstPlaneWidthBytes);
            dstRow += dstRowBytes;
        }
    }
    return AVIF_RESULT_OK;
}

static int avifQualityToQuantizer(int quality, int minQuantizer, int maxQuantizer)
{
    int quantizer;
    if (quality == AVIF_QUALITY_DEFAULT) {
        // In older libavif releases, avifEncoder didn't have the quality and qualityAlpha fields.
        // Supply a default value for quantizer.
        quantizer = (minQuantizer + maxQuantizer) / 2;
        quantizer = AVIF_CLAMP(quantizer, 0, 63);
    } else {
        quality = AVIF_CLAMP(quality, 0, 100);
        quantizer = ((100 - quality) * 63 + 50) / 100;
    }
    return quantizer;
}

static const char infeNameColor[] = "Color";
static const char infeNameAlpha[] = "Alpha";
static const char infeNameGainMap[] = "GMap";
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
static const char infeNameSampleTransform[] = "SampleTransform";
#endif

static const char * getInfeName(avifItemCategory itemCategory)
{
    if (avifIsAlpha(itemCategory)) {
        return infeNameAlpha;
    }
    if (itemCategory == AVIF_ITEM_GAIN_MAP) {
        return infeNameGainMap;
    }
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (itemCategory >= AVIF_SAMPLE_TRANSFORM_MIN_CATEGORY && itemCategory <= AVIF_SAMPLE_TRANSFORM_MAX_CATEGORY) {
        return infeNameSampleTransform;
    }
#endif
    return infeNameColor;
}

// Adds the items for a single cell or a grid of cells. Outputs the topLevelItemID which is
// the only item if there is exactly one cell, or the grid item for multiple cells.
// Note: The topLevelItemID output argument has the type uint16_t* instead of avifEncoderItem** because
//       the avifEncoderItem pointer may be invalidated by a call to avifEncoderDataCreateItem().
static avifResult avifEncoderAddImageItems(avifEncoder * encoder,
                                           uint32_t gridCols,
                                           uint32_t gridRows,
                                           uint32_t gridWidth,
                                           uint32_t gridHeight,
                                           avifItemCategory itemCategory,
                                           uint16_t * topLevelItemID)
{
    const uint32_t cellCount = gridCols * gridRows;
    const char * infeName = getInfeName(itemCategory);
    const size_t infeNameSize = strlen(infeName) + 1;

    if (cellCount > 1) {
        avifEncoderItem * gridItem = avifEncoderDataCreateItem(encoder->data, "grid", infeName, infeNameSize, 0);
        AVIF_CHECKRES(avifWriteGridPayload(&gridItem->metadataPayload, gridCols, gridRows, gridWidth, gridHeight));
        gridItem->itemCategory = itemCategory;
        gridItem->gridCols = gridCols;
        gridItem->gridRows = gridRows;
        gridItem->gridWidth = gridWidth;
        gridItem->gridHeight = gridHeight;
        *topLevelItemID = gridItem->id;
    }

    for (uint32_t cellIndex = 0; cellIndex < cellCount; ++cellIndex) {
        avifEncoderItem * item =
            avifEncoderDataCreateItem(encoder->data, encoder->data->imageItemType, infeName, infeNameSize, cellIndex);
        AVIF_CHECKERR(item, AVIF_RESULT_OUT_OF_MEMORY);
        AVIF_CHECKRES(avifCodecCreate(encoder->codecChoice, AVIF_CODEC_FLAG_CAN_ENCODE, &item->codec));
        item->codec->csOptions = encoder->csOptions;
        item->codec->diag = &encoder->diag;
        item->itemCategory = itemCategory;
        item->extraLayerCount = encoder->extraLayerCount;

        if (cellCount > 1) {
            item->dimgFromID = *topLevelItemID;
            item->hiddenImage = AVIF_TRUE;
        } else {
            *topLevelItemID = item->id;
        }
    }
    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
static avifResult avifEncoderCreateBitDepthExtensionItems(avifEncoder * encoder,
                                                          uint32_t gridCols,
                                                          uint32_t gridRows,
                                                          uint32_t gridWidth,
                                                          uint32_t gridHeight,
                                                          uint16_t colorItemID)
{
    AVIF_ASSERT_OR_RETURN(encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_8B_8B ||
                          encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_4B ||
                          encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_8B_OVERLAP_4B);

    // There are multiple possible ISOBMFF box hierarchies for translucent images,
    // using 'sato' (Sample Transform) derived image items:
    //  - a primary 'sato' item uses a main color coded item and a hidden color coded item; each color coded
    //    item has an auxiliary alpha coded item; the main color coded item and the 'sato' item are in
    //    an 'altr' group (backward-compatible, implemented)
    //  - a primary 'sato' item uses a main color coded item and a hidden color coded item; the primary
    //    'sato' item has an auxiliary alpha 'sato' item using two alpha coded items (backward-incompatible)
    // Likewise, there are multiple possible ISOBMFF box hierarchies for bit-depth-extended grids,
    // using 'sato' (Sample Transform) derived image items:
    //  - a primary color 'grid', an auxiliary alpha 'grid', a hidden color 'grid', a hidden auxiliary alpha 'grid'
    //    and a 'sato' using the two color 'grid's as input items in this order; the primary color item
    //    and the 'sato' item being in an 'altr' group (backward-compatible, implemented)
    //  - a primary 'grid' of 'sato' cells and an auxiliary alpha 'grid' of 'sato' cells (backward-incompatible)
    avifEncoderItem * sampleTransformItem = avifEncoderDataCreateItem(encoder->data,
                                                                      "sato",
                                                                      infeNameSampleTransform,
                                                                      /*infeNameSize=*/strlen(infeNameSampleTransform) + 1,
                                                                      /*cellIndex=*/0);
    AVIF_CHECKRES(avifEncoderWriteSampleTransformPayload(encoder, &sampleTransformItem->metadataPayload));
    sampleTransformItem->itemCategory = AVIF_ITEM_SAMPLE_TRANSFORM;
    uint16_t sampleTransformItemID = sampleTransformItem->id;
    // 'altr' group
    AVIF_ASSERT_OR_RETURN(encoder->data->alternativeItemIDs.count == 0);
    uint16_t * alternativeItemID = (uint16_t *)avifArrayPush(&encoder->data->alternativeItemIDs);
    AVIF_CHECKERR(alternativeItemID != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    *alternativeItemID = sampleTransformItem->id;
    alternativeItemID = (uint16_t *)avifArrayPush(&encoder->data->alternativeItemIDs);
    AVIF_CHECKERR(alternativeItemID != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    *alternativeItemID = colorItemID;

    uint16_t bitDepthExtensionColorItemId;
    AVIF_CHECKRES(
        avifEncoderAddImageItems(encoder, gridCols, gridRows, gridWidth, gridHeight, AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_COLOR, &bitDepthExtensionColorItemId));
    avifEncoderItem * bitDepthExtensionColorItem = avifEncoderDataFindItemByID(encoder->data, bitDepthExtensionColorItemId);
    assert(bitDepthExtensionColorItem);
    bitDepthExtensionColorItem->hiddenImage = AVIF_TRUE;

    // Set the color and bit depth extension items' dimgFromID value to point to the sample transform item.
    // The color item shall be first, and the bit depth extension item second. avifEncoderFinish() writes the
    // dimg item references in item id order, so as long as colorItemID < bitDepthExtensionColorItemId, the order
    // will be correct.
    AVIF_ASSERT_OR_RETURN(colorItemID < bitDepthExtensionColorItemId);
    avifEncoderItem * colorItem = avifEncoderDataFindItemByID(encoder->data, colorItemID);
    AVIF_ASSERT_OR_RETURN(colorItem != NULL);
    AVIF_ASSERT_OR_RETURN(colorItem->dimgFromID == 0); // Our internal API only allows one dimg value per item.
    colorItem->dimgFromID = sampleTransformItemID;
    bitDepthExtensionColorItem->dimgFromID = sampleTransformItemID;

    if (encoder->data->alphaPresent) {
        uint16_t bitDepthExtensionAlphaItemId;
        AVIF_CHECKRES(
            avifEncoderAddImageItems(encoder, gridCols, gridRows, gridWidth, gridHeight, AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_ALPHA, &bitDepthExtensionAlphaItemId));
        avifEncoderItem * bitDepthExtensionAlphaItem = avifEncoderDataFindItemByID(encoder->data, bitDepthExtensionAlphaItemId);
        assert(bitDepthExtensionAlphaItem);
        bitDepthExtensionAlphaItem->irefType = "auxl";
        bitDepthExtensionAlphaItem->irefToID = bitDepthExtensionColorItemId;
        if (encoder->data->imageMetadata->alphaPremultiplied) {
            // The reference may have changed; fetch it again.
            bitDepthExtensionColorItem = avifEncoderDataFindItemByID(encoder->data, bitDepthExtensionColorItemId);
            assert(bitDepthExtensionColorItem);
            bitDepthExtensionColorItem->irefType = "prem";
            bitDepthExtensionColorItem->irefToID = bitDepthExtensionAlphaItemId;
        }
    }
    return AVIF_RESULT_OK;
}

// Same as avifImageApplyExpression() but for the expression (inputImageItem [op] constant).
// Convenience function.
static avifResult avifImageApplyImgOpConst(avifImage * result,
                                           const avifImage * inputImageItem,
                                           avifSampleTransformTokenType op,
                                           int32_t constant,
                                           avifPlanesFlags planes)
{
    // Postfix notation.
    const avifSampleTransformToken tokens[] = { { AVIF_SAMPLE_TRANSFORM_INPUT_IMAGE_ITEM_INDEX, 0, /*inputImageItemIndex=*/1 },
                                                { AVIF_SAMPLE_TRANSFORM_CONSTANT, constant, 0 },
                                                { (uint8_t)op, 0, 0 } };
    return avifImageApplyOperations(result, AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_32, /*numTokens=*/3, tokens, /*numInputImageItems=*/1, &inputImageItem, planes);
}

static avifResult avifImageCreateAllocate(avifImage ** sampleTransformedImage, const avifImage * reference, uint32_t numBits, avifPlanesFlag planes)
{
    *sampleTransformedImage = avifImageCreate(reference->width, reference->height, numBits, reference->yuvFormat);
    AVIF_CHECKERR(*sampleTransformedImage != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    return avifImageAllocatePlanes(*sampleTransformedImage, planes);
}

// Finds the encoded base image and decodes it. Callers of this function must free
// *codec and *decodedBaseImage if not null, whether the function succeeds or not.
static avifResult avifEncoderDecodeSatoBaseImage(avifEncoder * encoder,
                                                 const avifImage * original,
                                                 uint32_t numBits,
                                                 avifPlanesFlag planes,
                                                 avifCodec ** codec,
                                                 avifImage ** decodedBaseImage)
{
    avifDecodeSample sample;
    memset(&sample, 0, sizeof(sample));
    sample.spatialID = AVIF_SPATIAL_ID_UNSET;

    // Find the encoded bytes of the base image item.
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        if ((item->itemCategory != AVIF_ITEM_COLOR || planes != AVIF_PLANES_YUV) &&
            (item->itemCategory != AVIF_ITEM_ALPHA || planes != AVIF_PLANES_A)) {
            continue;
        }

        AVIF_ASSERT_OR_RETURN(item->encodeOutput != NULL); // TODO: Support grids?
        AVIF_ASSERT_OR_RETURN(item->encodeOutput->samples.count == 1);
        AVIF_ASSERT_OR_RETURN(item->encodeOutput->samples.sample[0].data.size != 0);
        AVIF_ASSERT_OR_RETURN(sample.data.size == 0); // There should be only one base item.
        sample.data.data = item->encodeOutput->samples.sample[0].data.data;
        sample.data.size = item->encodeOutput->samples.sample[0].data.size;
    }
    AVIF_ASSERT_OR_RETURN(sample.data.size != 0); // There should be at least one base item.

    AVIF_CHECKRES(avifCodecCreate(AVIF_CODEC_CHOICE_AUTO, AVIF_CODEC_FLAG_CAN_DECODE, codec));
    (*codec)->diag = &encoder->diag;
    (*codec)->maxThreads = encoder->maxThreads;
    (*codec)->imageSizeLimit = AVIF_DEFAULT_IMAGE_SIZE_LIMIT;
    AVIF_CHECKRES(avifImageCreateAllocate(decodedBaseImage, original, numBits, planes));
    avifBool isLimitedRangeAlpha = AVIF_FALSE; // Ignored.
    AVIF_CHECKERR((*codec)->getNextImage(*codec, &sample, planes == AVIF_PLANES_A, &isLimitedRangeAlpha, *decodedBaseImage),
                  AVIF_RESULT_ENCODE_SAMPLE_TRANSFORM_FAILED);
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderCreateSatoImage(avifEncoder * encoder,
                                             const avifEncoderItem * item,
                                             avifBool itemWillBeEncodedLosslessly,
                                             const avifImage * image,
                                             avifImage ** sampleTransformedImage)
{
    const avifPlanesFlag planes = avifIsAlpha(item->itemCategory) ? AVIF_PLANES_A : AVIF_PLANES_YUV;
    // The first image item used as input to the 'sato' Sample Transform derived image item.
    avifBool isBase = item->itemCategory == AVIF_ITEM_COLOR || item->itemCategory == AVIF_ITEM_ALPHA;
    if (!isBase) {
        // The second image item used as input to the 'sato' Sample Transform derived image item.
        AVIF_ASSERT_OR_RETURN(item->itemCategory >= AVIF_SAMPLE_TRANSFORM_MIN_CATEGORY &&
                              item->itemCategory <= AVIF_SAMPLE_TRANSFORM_MAX_CATEGORY);
    }

    if (encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_8B_8B) {
        if (isBase) {
            AVIF_CHECKRES(avifImageCreateAllocate(sampleTransformedImage, image, 8, planes));
            AVIF_CHECKRES(avifImageApplyImgOpConst(*sampleTransformedImage, image, AVIF_SAMPLE_TRANSFORM_QUOTIENT, 256, planes));
        } else {
            AVIF_CHECKRES(avifImageCreateAllocate(sampleTransformedImage, image, 8, planes));
            AVIF_CHECKRES(avifImageApplyImgOpConst(*sampleTransformedImage, image, AVIF_SAMPLE_TRANSFORM_AND, 255, planes));
        }
    } else if (encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_4B) {
        if (isBase) {
            AVIF_CHECKRES(avifImageCreateAllocate(sampleTransformedImage, image, 12, planes));
            AVIF_CHECKRES(avifImageApplyImgOpConst(*sampleTransformedImage, image, AVIF_SAMPLE_TRANSFORM_QUOTIENT, 16, planes));
        } else {
            AVIF_CHECKRES(avifImageCreateAllocate(sampleTransformedImage, image, 8, planes));
            AVIF_CHECKRES(avifImageApplyImgOpConst(*sampleTransformedImage, image, AVIF_SAMPLE_TRANSFORM_AND, 15, planes));
            // AVIF only supports 8, 10 or 12-bit image items. Scale the samples to fit the range.
            // Note: The samples could be encoded as is without being shifted left before encoding,
            //       but they would not be shifted right after decoding either. Right shifting after
            //       decoding provides a guarantee on the range of values and on the lack of integer
            //       overflow, so it is safer to do these extra steps.
            //       It also makes more sense from a compression point-of-view to use the full range.
            // Transform in-place.
            AVIF_CHECKRES(
                avifImageApplyImgOpConst(*sampleTransformedImage, *sampleTransformedImage, AVIF_SAMPLE_TRANSFORM_PRODUCT, 16, planes));
            if (!itemWillBeEncodedLosslessly) {
                // Small loss at encoding could be amplified by the truncation caused by the right
                // shift after decoding. Offset sample values now, before encoding, to round rather
                // than floor the samples shifted after decoding.
                // Note: Samples were just left shifted by numShiftedBits, so adding less than
                //       (1<<numShiftedBits) will not trigger any integer overflow.
                // Transform in-place.
                AVIF_CHECKRES(
                    avifImageApplyImgOpConst(*sampleTransformedImage, *sampleTransformedImage, AVIF_SAMPLE_TRANSFORM_SUM, 7, planes));
            }
        }
    } else {
        AVIF_CHECKERR(encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_8B_OVERLAP_4B,
                      AVIF_RESULT_NOT_IMPLEMENTED);
        if (isBase) {
            AVIF_CHECKRES(avifImageCreateAllocate(sampleTransformedImage, image, 12, planes));
            AVIF_CHECKRES(avifImageApplyImgOpConst(*sampleTransformedImage, image, AVIF_SAMPLE_TRANSFORM_QUOTIENT, 16, planes));
        } else {
            AVIF_CHECKRES(avifImageCreateAllocate(sampleTransformedImage, image, 8, planes));
            avifCodec * codec = NULL;
            avifImage * decodedBaseImage = NULL;
            avifResult result = avifEncoderDecodeSatoBaseImage(encoder, image, 12, planes, &codec, &decodedBaseImage);
            if (result == AVIF_RESULT_OK) {
                // decoded = main*16+hidden-128 so hidden = clamp_8b(original-main*16+128). Postfix notation.
                const avifSampleTransformToken tokens[] = { { AVIF_SAMPLE_TRANSFORM_INPUT_IMAGE_ITEM_INDEX, 0, /*inputImageItemIndex=*/1 },
                                                            { AVIF_SAMPLE_TRANSFORM_INPUT_IMAGE_ITEM_INDEX, 0, /*inputImageItemIndex=*/2 },
                                                            { AVIF_SAMPLE_TRANSFORM_CONSTANT, /*constant=*/16, 0 },
                                                            { AVIF_SAMPLE_TRANSFORM_PRODUCT, 0, 0 },
                                                            { AVIF_SAMPLE_TRANSFORM_DIFFERENCE, 0, 0 },
                                                            { AVIF_SAMPLE_TRANSFORM_CONSTANT, /*constant=*/128, 0 },
                                                            { AVIF_SAMPLE_TRANSFORM_SUM, 0, 0 } };
                // image is "original" (index 1) and decodedBaseImage is "main" (index 2) in the formula above.
                const avifImage * inputImageItems[] = { image, decodedBaseImage };
                result = avifImageApplyOperations(*sampleTransformedImage,
                                                  AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_32,
                                                  /*numTokens=*/7,
                                                  tokens,
                                                  /*numInputImageItems=*/2,
                                                  inputImageItems,
                                                  planes);
            }
            if (decodedBaseImage) {
                avifImageDestroy(decodedBaseImage);
            }
            if (codec) {
                avifCodecDestroy(codec);
            }
            AVIF_CHECKRES(result);
        }
    }
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderCreateBitDepthExtensionImage(avifEncoder * encoder,
                                                          const avifEncoderItem * item,
                                                          avifBool itemWillBeEncodedLosslessly,
                                                          const avifImage * image,
                                                          avifImage ** sampleTransformedImage)
{
    AVIF_ASSERT_OR_RETURN(image->depth == 16); // Other bit depths could be supported but for now it is 16-bit only.
    *sampleTransformedImage = NULL;
    const avifResult result = avifEncoderCreateSatoImage(encoder, item, itemWillBeEncodedLosslessly, image, sampleTransformedImage);
    if (result != AVIF_RESULT_OK && *sampleTransformedImage != NULL) {
        avifImageDestroy(*sampleTransformedImage);
    }
    return result;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

static avifCodecType avifEncoderGetCodecType(const avifEncoder * encoder)
{
    // TODO(yguyon): Rework when AVIF_CODEC_CHOICE_AUTO can be AVM
    assert((encoder->codecChoice != AVIF_CODEC_CHOICE_AUTO) ||
           (strcmp(avifCodecName(encoder->codecChoice, AVIF_CODEC_FLAG_CAN_ENCODE), "avm") != 0));
    return avifCodecTypeFromChoice(encoder->codecChoice, AVIF_CODEC_FLAG_CAN_ENCODE);
}

// This function is called after every color frame is encoded. It returns AVIF_TRUE if a keyframe needs to be forced for the next
// alpha frame to be encoded, AVIF_FALSE otherwise.
static avifBool avifEncoderDataShouldForceKeyframeForAlpha(const avifEncoderData * data,
                                                           const avifEncoderItem * colorItem,
                                                           avifAddImageFlags addImageFlags)
{
    if (!data->alphaPresent) {
        // There is no alpha plane.
        return AVIF_FALSE;
    }
    if (addImageFlags & AVIF_ADD_IMAGE_FLAG_SINGLE) {
        // Not an animated image.
        return AVIF_FALSE;
    }
    if (data->frames.count == 0) {
        // data->frames.count is the number of frames that have been encoded so far by previous calls to avifEncoderAddImage. If
        // this is the first frame, there is no need to force keyframe.
        return AVIF_FALSE;
    }
    const uint32_t colorFramesOutputSoFar = colorItem->encodeOutput->samples.count;
    const avifBool isLaggedOutput = (data->frames.count + 1) != colorFramesOutputSoFar;
    if (isLaggedOutput) {
        // If the encoder is operating with lag, then there is no way to determine if the last encoded frame was a keyframe until
        // the encoder outputs it (after the lag). So do not force keyframe for alpha channel in this case.
        return AVIF_FALSE;
    }
    return colorItem->encodeOutput->samples.sample[colorFramesOutputSoFar - 1].sync;
}

static avifResult avifGetErrorForItemCategory(avifItemCategory itemCategory)
{
    if (itemCategory == AVIF_ITEM_GAIN_MAP) {
        return AVIF_RESULT_ENCODE_GAIN_MAP_FAILED;
    }
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (itemCategory == AVIF_ITEM_SAMPLE_TRANSFORM ||
        (itemCategory >= AVIF_SAMPLE_TRANSFORM_MIN_CATEGORY && itemCategory <= AVIF_SAMPLE_TRANSFORM_MAX_CATEGORY)) {
        return AVIF_RESULT_ENCODE_SAMPLE_TRANSFORM_FAILED;
    }
#endif
    return avifIsAlpha(itemCategory) ? AVIF_RESULT_ENCODE_ALPHA_FAILED : AVIF_RESULT_ENCODE_COLOR_FAILED;
}

static uint32_t avifGridWidth(uint32_t gridCols, const avifImage * firstCell, const avifImage * bottomRightCell)
{
    return (gridCols - 1) * firstCell->width + bottomRightCell->width;
}

static uint32_t avifGridHeight(uint32_t gridRows, const avifImage * firstCell, const avifImage * bottomRightCell)
{
    return (gridRows - 1) * firstCell->height + bottomRightCell->height;
}

static avifResult avifValidateGrid(uint32_t gridCols,
                                   uint32_t gridRows,
                                   const avifImage * const * cellImages,
                                   avifBool validateGainMap,
                                   avifDiagnostics * diag)
{
    const uint32_t cellCount = gridCols * gridRows;
    const avifImage * firstCell = cellImages[0];
    const avifImage * bottomRightCell = cellImages[cellCount - 1];
    if (validateGainMap) {
        AVIF_ASSERT_OR_RETURN(firstCell->gainMap && firstCell->gainMap->image);
        firstCell = firstCell->gainMap->image;
        AVIF_ASSERT_OR_RETURN(bottomRightCell->gainMap && bottomRightCell->gainMap->image);
        bottomRightCell = bottomRightCell->gainMap->image;
    }
    const uint32_t tileWidth = firstCell->width;
    const uint32_t tileHeight = firstCell->height;
    const uint32_t gridWidth = avifGridWidth(gridCols, firstCell, bottomRightCell);
    const uint32_t gridHeight = avifGridHeight(gridRows, firstCell, bottomRightCell);
    for (uint32_t cellIndex = 0; cellIndex < cellCount; ++cellIndex) {
        const avifImage * cellImage = cellImages[cellIndex];
        if (validateGainMap) {
            AVIF_ASSERT_OR_RETURN(cellImage->gainMap && cellImage->gainMap->image);
            cellImage = cellImage->gainMap->image;
        }
        const uint32_t expectedCellWidth = ((cellIndex + 1) % gridCols) ? tileWidth : bottomRightCell->width;
        const uint32_t expectedCellHeight = (cellIndex < (cellCount - gridCols)) ? tileHeight : bottomRightCell->height;
        if ((cellImage->width != expectedCellWidth) || (cellImage->height != expectedCellHeight)) {
            avifDiagnosticsPrintf(diag,
                                  "%s cell %u has invalid dimensions: expected %ux%u found %ux%u",
                                  validateGainMap ? "gain map" : "image",
                                  cellIndex,
                                  expectedCellWidth,
                                  expectedCellHeight,
                                  cellImage->width,
                                  cellImage->height);
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }

        // MIAF (ISO 23000-22:2019), Section 7.3.11.4.1:
        //   All input images of a grid image item shall use the same coding format, chroma sampling format, and the
        //   same decoder configuration (see 7.3.6.2).
        if ((cellImage->depth != firstCell->depth) || (cellImage->yuvFormat != firstCell->yuvFormat) ||
            (cellImage->yuvRange != firstCell->yuvRange) || (cellImage->colorPrimaries != firstCell->colorPrimaries) ||
            (cellImage->transferCharacteristics != firstCell->transferCharacteristics) ||
            (cellImage->matrixCoefficients != firstCell->matrixCoefficients) || (!!cellImage->alphaPlane != !!firstCell->alphaPlane) ||
            (cellImage->alphaPremultiplied != firstCell->alphaPremultiplied)) {
            avifDiagnosticsPrintf(diag,
                                  "all grid cells should have the same value for: depth, yuvFormat, yuvRange, colorPrimaries, "
                                  "transferCharacteristics, matrixCoefficients, alphaPlane presence, alphaPremultiplied");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }

        // AV1 (Version 1.0.0 with Errata 1), Section 6.4.2. Color config semantics
        //   If matrix_coefficients is equal to MC_IDENTITY, it is a requirement of bitstream conformance that
        //   subsampling_x is equal to 0 and subsampling_y is equal to 0.
        // Although matrix_coefficients in the Sequence Header OBU is set to Undefined (2), the requirement
        // is still enforced here between what is written in the ColourInformationProperty of colour_type 'nclx'
        // and the subsampling information in the Sequence Header OBU.
        if (cellImage->matrixCoefficients == AVIF_MATRIX_COEFFICIENTS_IDENTITY && cellImage->yuvFormat != AVIF_PIXEL_FORMAT_YUV444) {
            avifDiagnosticsPrintf(diag, "subsampling must be 0 (4:4:4) with identity matrix coefficients");
            return AVIF_RESULT_INVALID_ARGUMENT;
        }

        if (!cellImage->yuvPlanes[AVIF_CHAN_Y]) {
            return AVIF_RESULT_NO_CONTENT;
        }
    }

    if ((bottomRightCell->width > tileWidth) || (bottomRightCell->height > tileHeight)) {
        avifDiagnosticsPrintf(diag,
                              "the last %s cell can be smaller but not larger than the other cells which are %ux%u, found %ux%u",
                              validateGainMap ? "gain map" : "image",
                              tileWidth,
                              tileHeight,
                              bottomRightCell->width,
                              bottomRightCell->height);
        return AVIF_RESULT_INVALID_IMAGE_GRID;
    }
    if ((cellCount > 1) && !avifAreGridDimensionsValid(firstCell->yuvFormat, gridWidth, gridHeight, tileWidth, tileHeight, diag)) {
        return AVIF_RESULT_INVALID_IMAGE_GRID;
    }

    return AVIF_RESULT_OK;
}

static avifResult avifEncoderAddImageInternal(avifEncoder * encoder,
                                              uint32_t gridCols,
                                              uint32_t gridRows,
                                              const avifImage * const * cellImages,
                                              uint64_t durationInTimescales,
                                              avifAddImageFlags addImageFlags)
{
    // -----------------------------------------------------------------------
    // Verify encoding is possible

    if (!avifCodecName(encoder->codecChoice, AVIF_CODEC_FLAG_CAN_ENCODE)) {
        return AVIF_RESULT_NO_CODEC_AVAILABLE;
    }

    if (encoder->extraLayerCount >= AVIF_MAX_AV1_LAYER_COUNT) {
        avifDiagnosticsPrintf(&encoder->diag, "extraLayerCount [%u] must be less than %d", encoder->extraLayerCount, AVIF_MAX_AV1_LAYER_COUNT);
        return AVIF_RESULT_INVALID_ARGUMENT;
    }

    // -----------------------------------------------------------------------
    // Validate images

    const uint32_t cellCount = gridCols * gridRows;
    if (cellCount == 0) {
        return AVIF_RESULT_INVALID_ARGUMENT;
    }

    const avifImage * firstCell = cellImages[0];
    const avifImage * bottomRightCell = cellImages[cellCount - 1];
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    AVIF_CHECKERR(firstCell->depth == 8 || firstCell->depth == 10 || firstCell->depth == 12 ||
                      (firstCell->depth == 16 && encoder->sampleTransformRecipe != AVIF_SAMPLE_TRANSFORM_NONE),
                  AVIF_RESULT_UNSUPPORTED_DEPTH);
#else
    AVIF_CHECKERR(firstCell->depth == 8 || firstCell->depth == 10 || firstCell->depth == 12, AVIF_RESULT_UNSUPPORTED_DEPTH);
#endif
    AVIF_CHECKERR(firstCell->yuvFormat != AVIF_PIXEL_FORMAT_NONE, AVIF_RESULT_NO_YUV_FORMAT_SELECTED);
    if (!firstCell->width || !firstCell->height || !bottomRightCell->width || !bottomRightCell->height) {
        return AVIF_RESULT_NO_CONTENT;
    }

    AVIF_CHECKRES(avifValidateGrid(gridCols, gridRows, cellImages, /*validateGainMap=*/AVIF_FALSE, &encoder->diag));

    const avifBool hasGainMap = (firstCell->gainMap && firstCell->gainMap->image != NULL);

    // Check that either all cells have a gain map, or none of them do.
    // If a gain map is present, check that they all have the same gain map metadata.
    for (uint32_t cellIndex = 0; cellIndex < cellCount; ++cellIndex) {
        const avifImage * cellImage = cellImages[cellIndex];
        const avifBool cellHasGainMap = (cellImage->gainMap && cellImage->gainMap->image);
        if (cellHasGainMap != hasGainMap) {
            avifDiagnosticsPrintf(&encoder->diag, "cells should either all have a gain map image, or none of them should, found a mix");
            return AVIF_RESULT_INVALID_IMAGE_GRID;
        }
        if (hasGainMap) {
            if (!avifSameGainMapAltMetadata(firstCell->gainMap, cellImage->gainMap)) {
                avifDiagnosticsPrintf(&encoder->diag, "all cells should have the same alternate image metadata in the gain map");
                return AVIF_RESULT_INVALID_IMAGE_GRID;
            }
            if (!avifSameGainMapMetadata(firstCell->gainMap, cellImage->gainMap)) {
                avifDiagnosticsPrintf(&encoder->diag, "all cells should have the same gain map metadata");
                return AVIF_RESULT_INVALID_IMAGE_GRID;
            }
        }
    }

    if (hasGainMap) {
        // AVIF supports 16-bit images through sample transforms used as bit depth extensions,
        // but this is not implemented for gain maps for now. Stick to at most 12 bits.
        // TODO(yguyon): Implement 16-bit gain maps.
        AVIF_CHECKERR(firstCell->gainMap->image->depth == 8 || firstCell->gainMap->image->depth == 10 ||
                          firstCell->gainMap->image->depth == 12,
                      AVIF_RESULT_UNSUPPORTED_DEPTH);
        AVIF_CHECKERR(firstCell->gainMap->image->yuvFormat != AVIF_PIXEL_FORMAT_NONE, AVIF_RESULT_NO_YUV_FORMAT_SELECTED);
        AVIF_CHECKRES(avifValidateGrid(gridCols, gridRows, cellImages, /*validateGainMap=*/AVIF_TRUE, &encoder->diag));
        if (firstCell->gainMap->image->colorPrimaries != AVIF_COLOR_PRIMARIES_UNSPECIFIED ||
            firstCell->gainMap->image->transferCharacteristics != AVIF_TRANSFER_CHARACTERISTICS_UNSPECIFIED) {
            avifDiagnosticsPrintf(&encoder->diag, "the gain map image must have colorPrimaries = 2 and transferCharacteristics = 2");
            return AVIF_RESULT_INVALID_ARGUMENT;
        }
    }

    // -----------------------------------------------------------------------
    // Validate flags

    if (encoder->data->singleImage) {
        // The previous call to avifEncoderAddImage() set AVIF_ADD_IMAGE_FLAG_SINGLE.
        // avifEncoderAddImage() cannot be called again for this encode.
        return AVIF_RESULT_ENCODE_COLOR_FAILED;
    }

    if (addImageFlags & AVIF_ADD_IMAGE_FLAG_SINGLE) {
        encoder->data->singleImage = AVIF_TRUE;

        if (encoder->extraLayerCount > 0) {
            // AVIF_ADD_IMAGE_FLAG_SINGLE may not be set for layered image.
            return AVIF_RESULT_INVALID_ARGUMENT;
        }

        if (encoder->data->items.count > 0) {
            // AVIF_ADD_IMAGE_FLAG_SINGLE may only be set on the first and only image.
            return AVIF_RESULT_INVALID_ARGUMENT;
        }
    }

    // -----------------------------------------------------------------------
    // Choose AV1 or AV2

    const avifCodecType codecType = avifEncoderGetCodecType(encoder);
    switch (codecType) {
        case AVIF_CODEC_TYPE_AV1:
            encoder->data->imageItemType = "av01";
            encoder->data->configPropName = "av1C";
            break;
#if defined(AVIF_CODEC_AVM)
        case AVIF_CODEC_TYPE_AV2:
            encoder->data->imageItemType = "av02";
            encoder->data->configPropName = "av2C";
            break;
#endif
        default:
            return AVIF_RESULT_NO_CODEC_AVAILABLE;
    }

    // -----------------------------------------------------------------------
    // Map quality settings to quantizer values.
    encoder->data->quantizer = avifQualityToQuantizer(encoder->quality, encoder->minQuantizer, encoder->maxQuantizer);
    // If alpha quality, and min and max alpha quantizer have their default values, default to the same quality as color.
    if (encoder->qualityAlpha == AVIF_QUALITY_DEFAULT && encoder->minQuantizerAlpha == AVIF_QUANTIZER_BEST_QUALITY &&
        encoder->maxQuantizerAlpha == AVIF_QUANTIZER_WORST_QUALITY) {
        encoder->data->quantizerAlpha = encoder->data->quantizer;
    } else {
        encoder->data->quantizerAlpha =
            avifQualityToQuantizer(encoder->qualityAlpha, encoder->minQuantizerAlpha, encoder->maxQuantizerAlpha);
    }
    if (encoder->qualityGainMap == AVIF_QUALITY_DEFAULT) {
        encoder->data->quantizerGainMap = encoder->data->quantizer; // Default to the same quality as color.
    } else {
        encoder->data->quantizerGainMap =
            avifQualityToQuantizer(encoder->qualityGainMap, AVIF_QUANTIZER_BEST_QUALITY, AVIF_QUANTIZER_WORST_QUALITY);
    }

    // -----------------------------------------------------------------------
    // Handle automatic tiling

    encoder->data->tileRowsLog2 = AVIF_CLAMP(encoder->tileRowsLog2, 0, 6);
    encoder->data->tileColsLog2 = AVIF_CLAMP(encoder->tileColsLog2, 0, 6);
    if (encoder->autoTiling) {
        // Use as many tiles as allowed by the minimum tile area requirement and impose a maximum
        // of 8 tiles.
        const int threads = 8;
        avifSetTileConfiguration(threads, firstCell->width, firstCell->height, &encoder->data->tileRowsLog2, &encoder->data->tileColsLog2);
    }

    // -----------------------------------------------------------------------
    // All encoder settings are known now. Detect changes.

    avifEncoderChanges encoderChanges;
    if (!avifEncoderDetectChanges(encoder, &encoderChanges)) {
        return AVIF_RESULT_CANNOT_CHANGE_SETTING;
    }
    avifEncoderBackupSettings(encoder);

    // -----------------------------------------------------------------------

    if (durationInTimescales == 0) {
        durationInTimescales = 1;
    }

    if (encoder->data->items.count == 0) {
        // Make a copy of the first image's metadata (sans pixels) for future writing/validation
        AVIF_CHECKRES(avifImageCopy(encoder->data->imageMetadata, firstCell, 0));

        const uint32_t gridWidth = avifGridWidth(gridCols, firstCell, bottomRightCell);
        const uint32_t gridHeight = avifGridHeight(gridRows, firstCell, bottomRightCell);

        if (hasGainMap) {
            AVIF_CHECKRES(
                avifImageCopyAltImageMetadata(encoder->data->altImageMetadata, encoder->data->imageMetadata, gridWidth, gridHeight));
        }

        // Prepare all AV1 items
        uint16_t colorItemID;
        AVIF_CHECKRES(avifEncoderAddImageItems(encoder, gridCols, gridRows, gridWidth, gridHeight, AVIF_ITEM_COLOR, &colorItemID));
        encoder->data->primaryItemID = colorItemID;

        encoder->data->alphaPresent = (firstCell->alphaPlane != NULL);
        if (encoder->data->alphaPresent && (addImageFlags & AVIF_ADD_IMAGE_FLAG_SINGLE)) {
            // If encoding a single image in which the alpha plane exists but is entirely opaque,
            // simply skip writing an alpha AV1 payload entirely, as it'll be interpreted as opaque
            // and is less bytes.
            //
            // However, if encoding an image sequence, the first frame's alpha plane being entirely
            // opaque could be a false positive for removing the alpha AV1 payload, as it might simply
            // be a fade out later in the sequence. This is why avifImageIsOpaque() is only called
            // when encoding a single image.

            encoder->data->alphaPresent = AVIF_FALSE;
            for (uint32_t cellIndex = 0; cellIndex < cellCount; ++cellIndex) {
                const avifImage * cellImage = cellImages[cellIndex];
                if (!avifImageIsOpaque(cellImage)) {
                    encoder->data->alphaPresent = AVIF_TRUE;
                    break;
                }
            }
        }

        if (encoder->data->alphaPresent) {
            uint16_t alphaItemID;
            AVIF_CHECKRES(avifEncoderAddImageItems(encoder, gridCols, gridRows, gridWidth, gridHeight, AVIF_ITEM_ALPHA, &alphaItemID));
            avifEncoderItem * alphaItem = avifEncoderDataFindItemByID(encoder->data, alphaItemID);
            AVIF_ASSERT_OR_RETURN(alphaItem);
            alphaItem->irefType = "auxl";
            alphaItem->irefToID = colorItemID;
            if (encoder->data->imageMetadata->alphaPremultiplied) {
                avifEncoderItem * colorItem = avifEncoderDataFindItemByID(encoder->data, colorItemID);
                AVIF_ASSERT_OR_RETURN(colorItem);
                colorItem->irefType = "prem";
                colorItem->irefToID = alphaItemID;
            }
        }

        if (firstCell->gainMap && firstCell->gainMap->image) {
            avifEncoderItem * toneMappedItem = avifEncoderDataCreateItem(encoder->data,
                                                                         "tmap",
                                                                         infeNameGainMap,
                                                                         /*infeNameSize=*/strlen(infeNameGainMap) + 1,
                                                                         /*cellIndex=*/0);
            AVIF_CHECKRES(avifWriteToneMappedImagePayload(&toneMappedItem->metadataPayload, firstCell->gainMap, &encoder->diag));
            // Even though the 'tmap' item is related to the gain map, it represents a color image and its metadata is more similar to the color item.
            toneMappedItem->itemCategory = AVIF_ITEM_COLOR;
            uint16_t toneMappedItemID = toneMappedItem->id;

            AVIF_ASSERT_OR_RETURN(encoder->data->alternativeItemIDs.count == 0);
            uint16_t * alternativeItemID = (uint16_t *)avifArrayPush(&encoder->data->alternativeItemIDs);
            AVIF_CHECKERR(alternativeItemID != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            *alternativeItemID = toneMappedItemID;

            alternativeItemID = (uint16_t *)avifArrayPush(&encoder->data->alternativeItemIDs);
            AVIF_CHECKERR(alternativeItemID != NULL, AVIF_RESULT_OUT_OF_MEMORY);
            *alternativeItemID = colorItemID;

            const uint32_t gainMapGridWidth =
                avifGridWidth(gridCols, cellImages[0]->gainMap->image, cellImages[gridCols * gridRows - 1]->gainMap->image);
            const uint32_t gainMapGridHeight =
                avifGridHeight(gridRows, cellImages[0]->gainMap->image, cellImages[gridCols * gridRows - 1]->gainMap->image);

            uint16_t gainMapItemID;
            AVIF_CHECKRES(
                avifEncoderAddImageItems(encoder, gridCols, gridRows, gainMapGridWidth, gainMapGridHeight, AVIF_ITEM_GAIN_MAP, &gainMapItemID));
            avifEncoderItem * gainMapItem = avifEncoderDataFindItemByID(encoder->data, gainMapItemID);
            AVIF_ASSERT_OR_RETURN(gainMapItem);
            gainMapItem->hiddenImage = AVIF_TRUE;

            // Set the color item and gain map item's dimgFromID value to point to the tone mapped item.
            // The color item shall be first, and the gain map second. avifEncoderFinish() writes the
            // dimg item references in item id order, so as long as colorItemID < gainMapItemID, the order
            // will be correct.
            AVIF_ASSERT_OR_RETURN(colorItemID < gainMapItemID);
            avifEncoderItem * colorItem = avifEncoderDataFindItemByID(encoder->data, colorItemID);
            AVIF_ASSERT_OR_RETURN(colorItem);
            AVIF_ASSERT_OR_RETURN(colorItem->dimgFromID == 0); // Our internal API only allows one dimg value per item.
            colorItem->dimgFromID = toneMappedItemID;
            gainMapItem->dimgFromID = toneMappedItemID;
        }

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
        if (encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_8B_8B ||
            encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_4B ||
            encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_8B_OVERLAP_4B) {
            // For now, only 16-bit depth is supported.
            AVIF_ASSERT_OR_RETURN(firstCell->depth == 16);
            AVIF_CHECKERR(!firstCell->gainMap, AVIF_RESULT_NOT_IMPLEMENTED); // TODO(yguyon): Implement 16-bit HDR
            AVIF_CHECKRES(avifEncoderCreateBitDepthExtensionItems(encoder, gridCols, gridRows, gridWidth, gridHeight, colorItemID));
        } else {
            AVIF_CHECKERR(encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_NONE, AVIF_RESULT_NOT_IMPLEMENTED);
        }
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

        // -----------------------------------------------------------------------
        // Create metadata items (Exif, XMP)

        if (firstCell->exif.size > 0) {
            const avifResult result = avifEncoderDataCreateExifItem(encoder->data, &firstCell->exif);
            if (result != AVIF_RESULT_OK) {
                return result;
            }
        }

        if (firstCell->xmp.size > 0) {
            const avifResult result = avifEncoderDataCreateXMPItem(encoder->data, &firstCell->xmp);
            if (result != AVIF_RESULT_OK) {
                return result;
            }
        }
    } else {
        // Another frame in an image sequence, or layer in a layered image

        if (hasGainMap) {
            avifDiagnosticsPrintf(&encoder->diag, "gain maps are not supported for image sequences or layered images");
            return AVIF_RESULT_NOT_IMPLEMENTED;
        }

        const avifImage * imageMetadata = encoder->data->imageMetadata;
        // Image metadata that are copied to the configuration property and nclx boxes are not allowed to change.
        // If the first image in the sequence had an alpha plane (even if fully opaque), all
        // subsequent images must have alpha as well.
        if ((imageMetadata->depth != firstCell->depth) || (imageMetadata->yuvFormat != firstCell->yuvFormat) ||
            (imageMetadata->yuvRange != firstCell->yuvRange) ||
            (imageMetadata->yuvChromaSamplePosition != firstCell->yuvChromaSamplePosition) ||
            (imageMetadata->colorPrimaries != firstCell->colorPrimaries) ||
            (imageMetadata->transferCharacteristics != firstCell->transferCharacteristics) ||
            (imageMetadata->matrixCoefficients != firstCell->matrixCoefficients) ||
            (imageMetadata->alphaPremultiplied != firstCell->alphaPremultiplied) ||
            (encoder->data->alphaPresent && !firstCell->alphaPlane)) {
            return AVIF_RESULT_INCOMPATIBLE_IMAGE;
        }
    }

    if (encoder->data->frames.count == 1) {
        // We will be writing an image sequence. When writing the AV1SampleEntry (derived from
        // VisualSampleEntry) in the stsd box, we need to cast imageMetadata->width and
        // imageMetadata->height to uint16_t:
        //     class VisualSampleEntry(codingname) extends SampleEntry (codingname){
        //        ...
        //        unsigned int(16) width;
        //        unsigned int(16) height;
        //        ...
        //     }
        // Check whether it is safe to cast width and height to uint16_t. The maximum width and
        // height of an AV1 frame are 65536, which just exceeds uint16_t.
        AVIF_ASSERT_OR_RETURN(encoder->data->items.count > 0);
        const avifImage * imageMetadata = encoder->data->imageMetadata;
        AVIF_CHECKERR(imageMetadata->width <= 65535 && imageMetadata->height <= 65535, AVIF_RESULT_INVALID_ARGUMENT);
    }

    // -----------------------------------------------------------------------
    // Encode AV1 OBUs

    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        if (item->codec) {
            const avifImage * cellImage = cellImages[item->cellIndex];
            avifImage * cellImagePlaceholder = NULL; // May be used as a temporary, modified cellImage. Left as NULL otherwise.
            const avifImage * firstCellImage = firstCell;

            if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
                AVIF_ASSERT_OR_RETURN(cellImage->gainMap && cellImage->gainMap->image);
                cellImage = cellImage->gainMap->image;
                AVIF_ASSERT_OR_RETURN(firstCell->gainMap && firstCell->gainMap->image);
                firstCellImage = firstCell->gainMap->image;
            }

            if ((cellImage->width != firstCellImage->width) || (cellImage->height != firstCellImage->height)) {
                // Pad the right-most and/or bottom-most tiles so that all tiles share the same dimensions.
                cellImagePlaceholder = avifImageCreateEmpty();
                AVIF_CHECKERR(cellImagePlaceholder, AVIF_RESULT_OUT_OF_MEMORY);
                const avifResult result =
                    avifImageCopyAndPad(cellImagePlaceholder, cellImage, firstCellImage->width, firstCellImage->height);
                if (result != AVIF_RESULT_OK) {
                    avifImageDestroy(cellImagePlaceholder);
                    return result;
                }
                cellImage = cellImagePlaceholder;
            }

            const avifBool isAlpha = avifIsAlpha(item->itemCategory);
            int quantizer = isAlpha                                      ? encoder->data->quantizerAlpha
                            : (item->itemCategory == AVIF_ITEM_GAIN_MAP) ? encoder->data->quantizerGainMap
                                                                         : encoder->data->quantizer;

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
            // Remember original quantizer values in case they change, to reset them afterwards.
            int * encoderMinQuantizer = isAlpha ? &encoder->minQuantizerAlpha : &encoder->minQuantizer;
            int * encoderMaxQuantizer = isAlpha ? &encoder->maxQuantizerAlpha : &encoder->maxQuantizer;
            const int originalMinQuantizer = *encoderMinQuantizer;
            const int originalMaxQuantizer = *encoderMaxQuantizer;

            if (encoder->sampleTransformRecipe != AVIF_SAMPLE_TRANSFORM_NONE) {
                if ((encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_8B_8B ||
                     encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_4B) &&
                    (item->itemCategory == AVIF_ITEM_COLOR || item->itemCategory == AVIF_ITEM_ALPHA)) {
                    // Encoding the least significant bits of a sample does not make any sense if the
                    // other bits are lossily compressed. Encode the most significant bits losslessly.
                    quantizer = AVIF_QUANTIZER_LOSSLESS;
                    *encoderMinQuantizer = AVIF_QUANTIZER_LOSSLESS;
                    *encoderMaxQuantizer = AVIF_QUANTIZER_LOSSLESS;
                    if (!avifEncoderDetectChanges(encoder, &encoderChanges)) {
                        assert(AVIF_FALSE);
                    }
                }

                // Replace cellImage by the first or second input to the AVIF_ITEM_SAMPLE_TRANSFORM derived image item.
                const avifBool itemWillBeEncodedLosslessly = (quantizer == AVIF_QUANTIZER_LOSSLESS);
                avifImage * sampleTransformedImage = NULL;
                if (cellImagePlaceholder) {
                    avifImageDestroy(cellImagePlaceholder); // Replaced by sampleTransformedImage.
                    cellImagePlaceholder = NULL;
                }
                AVIF_CHECKRES(
                    avifEncoderCreateBitDepthExtensionImage(encoder, item, itemWillBeEncodedLosslessly, cellImage, &sampleTransformedImage));
                cellImagePlaceholder = sampleTransformedImage; // Transfer ownership.
                cellImage = cellImagePlaceholder;
            }
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM

            // If alpha channel is present, set disableLaggedOutput to AVIF_TRUE. If the encoder supports it, this enables
            // avifEncoderDataShouldForceKeyframeForAlpha to force a keyframe in the alpha channel whenever a keyframe has been
            // encoded in the color channel for animated images.
            avifResult encodeResult = item->codec->encodeImage(item->codec,
                                                               encoder,
                                                               cellImage,
                                                               isAlpha,
                                                               encoder->data->tileRowsLog2,
                                                               encoder->data->tileColsLog2,
                                                               quantizer,
                                                               encoderChanges,
                                                               /*disableLaggedOutput=*/encoder->data->alphaPresent,
                                                               addImageFlags,
                                                               item->encodeOutput);
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
            // Revert quality settings if they changed.
            if (*encoderMinQuantizer != originalMinQuantizer || *encoderMaxQuantizer != originalMaxQuantizer) {
                avifEncoderBackupSettings(encoder); // Remember last encoding settings for next avifEncoderDetectChanges().
                *encoderMinQuantizer = originalMinQuantizer;
                *encoderMaxQuantizer = originalMaxQuantizer;
            }
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM
            if (cellImagePlaceholder) {
                avifImageDestroy(cellImagePlaceholder);
            }
            if (encodeResult == AVIF_RESULT_UNKNOWN_ERROR) {
                encodeResult = avifGetErrorForItemCategory(item->itemCategory);
            }
            AVIF_CHECKRES(encodeResult);
            if (itemIndex == 0 && avifEncoderDataShouldForceKeyframeForAlpha(encoder->data, item, addImageFlags)) {
                addImageFlags |= AVIF_ADD_IMAGE_FLAG_FORCE_KEYFRAME;
            }
        }
    }

    avifCodecSpecificOptionsClear(encoder->csOptions);
    avifEncoderFrame * frame = (avifEncoderFrame *)avifArrayPush(&encoder->data->frames);
    AVIF_CHECKERR(frame != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    frame->durationInTimescales = durationInTimescales;
    return AVIF_RESULT_OK;
}

avifResult avifEncoderAddImage(avifEncoder * encoder, const avifImage * image, uint64_t durationInTimescales, avifAddImageFlags addImageFlags)
{
    avifDiagnosticsClearError(&encoder->diag);
    return avifEncoderAddImageInternal(encoder, 1, 1, &image, durationInTimescales, addImageFlags);
}

avifResult avifEncoderAddImageGrid(avifEncoder * encoder,
                                   uint32_t gridCols,
                                   uint32_t gridRows,
                                   const avifImage * const * cellImages,
                                   avifAddImageFlags addImageFlags)
{
    avifDiagnosticsClearError(&encoder->diag);
    if ((gridCols == 0) || (gridCols > 256) || (gridRows == 0) || (gridRows > 256)) {
        return AVIF_RESULT_INVALID_IMAGE_GRID;
    }
    if (encoder->extraLayerCount == 0) {
        addImageFlags |= AVIF_ADD_IMAGE_FLAG_SINGLE; // image grids cannot be image sequences
    }
    return avifEncoderAddImageInternal(encoder, gridCols, gridRows, cellImages, 1, addImageFlags);
}

static size_t avifEncoderFindExistingChunk(avifRWStream * s, size_t mdatStartOffset, const uint8_t * data, size_t size)
{
    const size_t mdatCurrentOffset = avifRWStreamOffset(s);
    const size_t mdatSearchSize = mdatCurrentOffset - mdatStartOffset;
    if (mdatSearchSize < size) {
        return 0;
    }
    const size_t mdatEndSearchOffset = mdatCurrentOffset - size;
    for (size_t searchOffset = mdatStartOffset; searchOffset <= mdatEndSearchOffset; ++searchOffset) {
        if (!memcmp(data, &s->raw->data[searchOffset], size)) {
            return searchOffset;
        }
    }
    return 0;
}

static avifResult avifEncoderWriteMediaDataBox(avifEncoder * encoder,
                                               avifRWStream * s,
                                               avifEncoderItemReferenceArray * layeredColorItems,
                                               avifEncoderItemReferenceArray * layeredAlphaItems)
{
    encoder->ioStats.colorOBUSize = 0;
    encoder->ioStats.alphaOBUSize = 0;
    encoder->data->gainMapSizeBytes = 0;

    avifBoxMarker mdat;
    AVIF_CHECKRES(avifRWStreamWriteBox(s, "mdat", AVIF_BOX_SIZE_TBD, &mdat));
    const size_t mdatStartOffset = avifRWStreamOffset(s);
    for (uint32_t itemPasses = 0; itemPasses < 3; ++itemPasses) {
        // Use multiple passes to pack in the following order:
        //   * Pass 0: metadata (Exif/XMP/gain map metadata)
        //   * Pass 1: alpha, gain map image (AV1)
        //   * Pass 2: all other item data (AV1 color)
        //
        // See here for the discussion on alpha coming before color:
        // https://github.com/AOMediaCodec/libavif/issues/287
        //
        // Exif and XMP are packed first as they're required to be fully available
        // by avifDecoderParse() before it returns AVIF_RESULT_OK, unless ignoreXMP
        // and ignoreExif are enabled.
        //
        const avifBool metadataPass = (itemPasses == 0);
        const avifBool alphaAndGainMapPass = (itemPasses == 1);

        for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
            avifEncoderItem * item = &encoder->data->items.item[itemIndex];
            if ((item->metadataPayload.size == 0) && (item->encodeOutput->samples.count == 0)) {
                // this item has nothing for the mdat box
                continue;
            }
            const avifBool isMetadata = !memcmp(item->type, "mime", 4) || !memcmp(item->type, "Exif", 4) ||
                                        !memcmp(item->type, "tmap", 4);
            if (metadataPass != isMetadata) {
                // only process metadata (XMP/Exif) payloads when metadataPass is true
                continue;
            }
            const avifBool isAlpha = avifIsAlpha(item->itemCategory);
            const avifBool isAlphaOrGainMap = isAlpha || item->itemCategory == AVIF_ITEM_GAIN_MAP;
            if (alphaAndGainMapPass != isAlphaOrGainMap) {
                // only process alpha payloads when alphaPass is true
                continue;
            }

            if ((encoder->extraLayerCount > 0) && (item->encodeOutput->samples.count > 0)) {
                // Interleave - Pick out AV1 items and interleave them later.
                // We always interleave all AV1 items for layered images.
                AVIF_ASSERT_OR_RETURN(item->encodeOutput->samples.count == item->mdatFixups.count);

                avifEncoderItemReference * ref =
                    (avifEncoderItemReference *)avifArrayPush(isAlpha ? layeredAlphaItems : layeredColorItems);
                AVIF_CHECKERR(ref != NULL, AVIF_RESULT_OUT_OF_MEMORY);
                *ref = item;
                continue;
            }

            size_t chunkOffset = 0;

            // Deduplication - See if an identical chunk to this has already been written.
            // Doing it when item->encodeOutput->samples.count > 1 would require contiguous memory.
            if (item->encodeOutput->samples.count == 1) {
                avifEncodeSample * sample = &item->encodeOutput->samples.sample[0];
                chunkOffset = avifEncoderFindExistingChunk(s, mdatStartOffset, sample->data.data, sample->data.size);
            } else if (item->encodeOutput->samples.count == 0) {
                chunkOffset = avifEncoderFindExistingChunk(s, mdatStartOffset, item->metadataPayload.data, item->metadataPayload.size);
            }

            if (!chunkOffset) {
                // We've never seen this chunk before; write it out
                chunkOffset = avifRWStreamOffset(s);
                if (item->encodeOutput->samples.count > 0) {
                    for (uint32_t sampleIndex = 0; sampleIndex < item->encodeOutput->samples.count; ++sampleIndex) {
                        avifEncodeSample * sample = &item->encodeOutput->samples.sample[sampleIndex];
                        AVIF_CHECKRES(avifRWStreamWrite(s, sample->data.data, sample->data.size));

                        if (isAlpha) {
                            encoder->ioStats.alphaOBUSize += sample->data.size;
                        } else if (item->itemCategory == AVIF_ITEM_COLOR) {
                            encoder->ioStats.colorOBUSize += sample->data.size;
                        } else if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
                            encoder->data->gainMapSizeBytes += sample->data.size;
                        }
                    }
                } else {
                    AVIF_CHECKRES(avifRWStreamWrite(s, item->metadataPayload.data, item->metadataPayload.size));
                }
            }

            for (uint32_t fixupIndex = 0; fixupIndex < item->mdatFixups.count; ++fixupIndex) {
                avifOffsetFixup * fixup = &item->mdatFixups.fixup[fixupIndex];
                size_t prevOffset = avifRWStreamOffset(s);
                avifRWStreamSetOffset(s, fixup->offset);
                AVIF_CHECKRES(avifRWStreamWriteU32(s, (uint32_t)chunkOffset));
                avifRWStreamSetOffset(s, prevOffset);
            }
        }
    }

    uint32_t layeredItemCount = AVIF_MAX(layeredColorItems->count, layeredAlphaItems->count);
    if (layeredItemCount > 0) {
        // Interleave samples of all AV1 items.
        // We first write the first layer of all items,
        // in which we write first layer of each cell,
        // in which we write alpha first and then color.
        avifBool hasMoreSample;
        uint32_t layerIndex = 0;
        do {
            hasMoreSample = AVIF_FALSE;
            for (uint32_t itemIndex = 0; itemIndex < layeredItemCount; ++itemIndex) {
                for (int samplePass = 0; samplePass < 2; ++samplePass) {
                    // Alpha coming before color
                    avifEncoderItemReferenceArray * currentItems = (samplePass == 0) ? layeredAlphaItems : layeredColorItems;
                    if (itemIndex >= currentItems->count) {
                        continue;
                    }

                    // TODO: Offer the ability for a user to specify which grid cell should be written first.
                    avifEncoderItem * item = currentItems->ref[itemIndex];
                    if (item->encodeOutput->samples.count <= layerIndex) {
                        // We've already written all samples of this item
                        continue;
                    } else if (item->encodeOutput->samples.count > layerIndex + 1) {
                        hasMoreSample = AVIF_TRUE;
                    }
                    avifRWData * data = &item->encodeOutput->samples.sample[layerIndex].data;
                    size_t chunkOffset = avifEncoderFindExistingChunk(s, mdatStartOffset, data->data, data->size);
                    if (!chunkOffset) {
                        // We've never seen this chunk before; write it out
                        chunkOffset = avifRWStreamOffset(s);
                        AVIF_CHECKRES(avifRWStreamWrite(s, data->data, data->size));
                        if (samplePass == 0) {
                            encoder->ioStats.alphaOBUSize += data->size;
                        } else {
                            encoder->ioStats.colorOBUSize += data->size;
                        }
                    }

                    size_t prevOffset = avifRWStreamOffset(s);
                    avifRWStreamSetOffset(s, item->mdatFixups.fixup[layerIndex].offset);
                    AVIF_CHECKRES(avifRWStreamWriteU32(s, (uint32_t)chunkOffset));
                    avifRWStreamSetOffset(s, prevOffset);
                }
            }
            ++layerIndex;
        } while (hasMoreSample);

        AVIF_ASSERT_OR_RETURN(layerIndex <= AVIF_MAX_AV1_LAYER_COUNT);
    }
    avifRWStreamFinishBox(s, mdat);
    return AVIF_RESULT_OK;
}

static avifResult avifWriteAltrGroup(avifRWStream * s, uint32_t groupID, const avifEncoderItemIdArray * itemIDs)
{
    avifBoxMarker grpl;
    AVIF_CHECKRES(avifRWStreamWriteBox(s, "grpl", AVIF_BOX_SIZE_TBD, &grpl));

    avifBoxMarker altr;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(s, "altr", AVIF_BOX_SIZE_TBD, 0, 0, &altr));

    AVIF_CHECKRES(avifRWStreamWriteU32(s, groupID));                  // unsigned int(32) group_id;
    AVIF_CHECKRES(avifRWStreamWriteU32(s, (uint32_t)itemIDs->count)); // unsigned int(32) num_entities_in_group;
    for (uint32_t i = 0; i < itemIDs->count; ++i) {
        AVIF_CHECKRES(avifRWStreamWriteU32(s, (uint32_t)itemIDs->itemID[i])); // unsigned int(32) entity_id;
    }

    avifRWStreamFinishBox(s, altr);

    avifRWStreamFinishBox(s, grpl);

    return AVIF_RESULT_OK;
}

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
// Returns true if the image can be encoded with a MinimizedImageBox instead of a full regular MetaBox.
static avifBool avifEncoderIsMiniCompatible(const avifEncoder * encoder)
{
    // The MinimizedImageBox ("mif3" brand) only supports non-layered, still images.
    if (encoder->extraLayerCount || (encoder->data->frames.count != 1)) {
        return AVIF_FALSE;
    }

#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
    if (encoder->sampleTransformRecipe != AVIF_SAMPLE_TRANSFORM_NONE) {
        return AVIF_FALSE;
    }
#endif

    // Check for maximum field values and maximum chunk sizes.

    // width_minus1 and height_minus1
    if (encoder->data->imageMetadata->width > (1 << 15) || encoder->data->imageMetadata->height > (1 << 15)) {
        return AVIF_FALSE;
    }
    // icc_data_size_minus1, exif_data_size_minus1 and xmp_data_size_minus1
    if (encoder->data->imageMetadata->icc.size > (1 << 20) || encoder->data->imageMetadata->exif.size > (1 << 20) ||
        encoder->data->imageMetadata->xmp.size > (1 << 20)) {
        return AVIF_FALSE;
    }
    // gainmap_width_minus1 and gainmap_height_minus1
    if (encoder->data->imageMetadata->gainMap != NULL && encoder->data->imageMetadata->gainMap->image != NULL &&
        (encoder->data->imageMetadata->gainMap->image->width > (1 << 15) ||
         encoder->data->imageMetadata->gainMap->image->height > (1 << 15))) {
        return AVIF_FALSE;
    }
    // tmap_icc_data_size_minus1
    if (encoder->data->altImageMetadata->icc.size > (1 << 20)) {
        return AVIF_FALSE;
    }
    // gainmap_metadata_size
    if (encoder->data->imageMetadata->gainMap != NULL && avifGainMapMetadataSize(encoder->data->imageMetadata->gainMap) >= (1 << 20)) {
        return AVIF_FALSE;
    }

    // 4:4:4, 4:2:2, 4:2:0 and 4:0:0 are supported by a MinimizedImageBox.
    // chroma_subsampling
    if (encoder->data->imageMetadata->yuvFormat != AVIF_PIXEL_FORMAT_YUV444 &&
        encoder->data->imageMetadata->yuvFormat != AVIF_PIXEL_FORMAT_YUV422 &&
        encoder->data->imageMetadata->yuvFormat != AVIF_PIXEL_FORMAT_YUV420 &&
        encoder->data->imageMetadata->yuvFormat != AVIF_PIXEL_FORMAT_YUV400) {
        return AVIF_FALSE;
    }
    // gainmap_chroma_subsampling
    if (encoder->data->imageMetadata->gainMap != NULL && encoder->data->imageMetadata->gainMap->image != NULL &&
        (encoder->data->imageMetadata->gainMap->image->yuvFormat != AVIF_PIXEL_FORMAT_YUV444 &&
         encoder->data->imageMetadata->gainMap->image->yuvFormat != AVIF_PIXEL_FORMAT_YUV422 &&
         encoder->data->imageMetadata->gainMap->image->yuvFormat != AVIF_PIXEL_FORMAT_YUV420 &&
         encoder->data->imageMetadata->gainMap->image->yuvFormat != AVIF_PIXEL_FORMAT_YUV400)) {
        return AVIF_FALSE;
    }

    // colour_primaries, transfer_characteristics and matrix_coefficients
    if (encoder->data->imageMetadata->colorPrimaries > 255 || encoder->data->imageMetadata->transferCharacteristics > 255 ||
        encoder->data->imageMetadata->matrixCoefficients > 255) {
        return AVIF_FALSE;
    }
    // gainmap_colour_primaries, gainmap_transfer_characteristics and gainmap_matrix_coefficients
    if (encoder->data->imageMetadata->gainMap != NULL && encoder->data->imageMetadata->gainMap->image != NULL &&
        (encoder->data->imageMetadata->gainMap->image->colorPrimaries > 255 ||
         encoder->data->imageMetadata->gainMap->image->transferCharacteristics > 255 ||
         encoder->data->imageMetadata->gainMap->image->matrixCoefficients > 255)) {
        return AVIF_FALSE;
    }
    // tmap_colour_primaries, tmap_transfer_characteristics and tmap_matrix_coefficients
    if (encoder->data->altImageMetadata->colorPrimaries > 255 || encoder->data->altImageMetadata->transferCharacteristics > 255 ||
        encoder->data->altImageMetadata->matrixCoefficients > 255) {
        return AVIF_FALSE;
    }

    const avifEncoderItem * colorItem = NULL;
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];

        // Grids are not supported by a MinimizedImageBox.
        if (item->gridCols || item->gridRows) {
            return AVIF_FALSE;
        }

        if (item->id == encoder->data->primaryItemID) {
            assert(!colorItem);
            colorItem = item;
            // main_item_data_size_minus1
            if (item->encodeOutput->samples.count != 1 || item->encodeOutput->samples.sample[0].data.size > (1 << 28)) {
                return AVIF_FALSE;
            }
            continue; // The primary item can be stored in the MinimizedImageBox.
        }
        if (item->itemCategory == AVIF_ITEM_ALPHA && item->irefToID == encoder->data->primaryItemID) {
            // alpha_item_data_size
            if (item->encodeOutput->samples.count != 1 || item->encodeOutput->samples.sample[0].data.size >= (1 << 28)) {
                return AVIF_FALSE;
            }
            continue; // The alpha auxiliary item can be stored in the MinimizedImageBox.
        }
        if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
            // gainmap_item_data_size
            if (item->encodeOutput->samples.count != 1 || item->encodeOutput->samples.sample[0].data.size >= (1 << 28)) {
                return AVIF_FALSE;
            }
            continue; // The gainmap input image item can be stored in the MinimizedImageBox.
        }
        if (!memcmp(item->type, "tmap", 4)) {
            assert(item->itemCategory == AVIF_ITEM_COLOR); // Cannot be differentiated from the primary item by its itemCategory.
            continue; // The tone mapping derived image item can be represented in the MinimizedImageBox.
        }
        if (!memcmp(item->type, "mime", 4) && !memcmp(item->infeName, "XMP", item->infeNameSize)) {
            assert(item->metadataPayload.size == encoder->data->imageMetadata->xmp.size);
            continue; // XMP metadata can be stored in the MinimizedImageBox.
        }
        if (!memcmp(item->type, "Exif", 4) && !memcmp(item->infeName, "Exif", item->infeNameSize)) {
            assert(item->metadataPayload.size == encoder->data->imageMetadata->exif.size + 4);
            const uint32_t exif_tiff_header_offset = *(uint32_t *)item->metadataPayload.data;
            if (exif_tiff_header_offset != 0) {
                return AVIF_FALSE;
            }
            continue; // Exif metadata can be stored in the MinimizedImageBox if exif_tiff_header_offset is 0.
        }

        // Items besides the colorItem, the alphaItem, the gainmap item and Exif/XMP/ICC/HDR
        // metadata are not directly supported by the MinimizedImageBox.
        return AVIF_FALSE;
    }
    // A primary item is necessary.
    if (!colorItem) {
        return AVIF_FALSE;
    }
    return AVIF_TRUE;
}

static avifResult avifEncoderWriteMiniBox(avifEncoder * encoder, avifRWStream * s);

static avifResult avifEncoderWriteFileTypeBoxAndMiniBox(avifEncoder * encoder, avifRWData * output)
{
    avifRWStream s;
    avifRWStreamStart(&s, output);

    avifBoxMarker ftyp;
    AVIF_CHECKRES(avifRWStreamWriteBox(&s, "ftyp", AVIF_BOX_SIZE_TBD, &ftyp));
    AVIF_CHECKRES(avifRWStreamWriteChars(&s, "mif3", 4)); // unsigned int(32) major_brand;
    AVIF_CHECKRES(avifRWStreamWriteChars(&s, "avif", 4)); // unsigned int(32) minor_version;
                                                          // unsigned int(32) compatible_brands[];
    avifRWStreamFinishBox(&s, ftyp);

    AVIF_CHECKRES(avifEncoderWriteMiniBox(encoder, &s));

    avifRWStreamFinishWrite(&s);
    return AVIF_RESULT_OK;
}

static avifResult avifEncoderWriteMiniBox(avifEncoder * encoder, avifRWStream * s)
{
    const avifEncoderItem * colorItem = NULL;
    const avifEncoderItem * alphaItem = NULL;
    const avifEncoderItem * gainmapItem = NULL;
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        if (item->id == encoder->data->primaryItemID) {
            AVIF_ASSERT_OR_RETURN(!colorItem);
            colorItem = item;
        } else if (item->itemCategory == AVIF_ITEM_ALPHA && item->irefToID == encoder->data->primaryItemID) {
            AVIF_ASSERT_OR_RETURN(!alphaItem);
            alphaItem = item;
        }
        if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
            AVIF_ASSERT_OR_RETURN(!gainmapItem);
            gainmapItem = item;
        }
    }

    AVIF_ASSERT_OR_RETURN(colorItem);
    const avifRWData * colorData = &colorItem->encodeOutput->samples.sample[0].data;
    const avifRWData * alphaData = alphaItem ? &alphaItem->encodeOutput->samples.sample[0].data : NULL;
    const avifRWData * gainmapData = gainmapItem ? &gainmapItem->encodeOutput->samples.sample[0].data : NULL;

    const avifImage * const image = encoder->data->imageMetadata;

    const avifBool hasAlpha = alphaItem != NULL;
    const avifBool alphaIsPremultiplied = encoder->data->imageMetadata->alphaPremultiplied;
    const avifBool hasGainmap = gainmapItem != NULL;
    const avifBool hasHdr = hasGainmap; // libavif only supports gainmap-based HDR encoding for now.
    const avifBool hasIcc = image->icc.size != 0;
    const uint32_t chromaSubsampling = image->yuvFormat == AVIF_PIXEL_FORMAT_YUV400   ? 0
                                       : image->yuvFormat == AVIF_PIXEL_FORMAT_YUV420 ? 1
                                       : image->yuvFormat == AVIF_PIXEL_FORMAT_YUV422 ? 2
                                                                                      : 3;

    const avifColorPrimaries defaultColorPrimaries = hasIcc ? AVIF_COLOR_PRIMARIES_UNSPECIFIED : AVIF_COLOR_PRIMARIES_BT709;
    const avifTransferCharacteristics defaultTransferCharacteristics = hasIcc ? AVIF_TRANSFER_CHARACTERISTICS_UNSPECIFIED
                                                                              : AVIF_TRANSFER_CHARACTERISTICS_SRGB;
    const avifMatrixCoefficients defaultMatrixCoefficients = chromaSubsampling == 0 ? AVIF_MATRIX_COEFFICIENTS_UNSPECIFIED
                                                                                    : AVIF_MATRIX_COEFFICIENTS_BT601;
    const avifBool hasExplicitCicp = image->colorPrimaries != defaultColorPrimaries ||
                                     image->transferCharacteristics != defaultTransferCharacteristics ||
                                     image->matrixCoefficients != defaultMatrixCoefficients;

    const avifBool floatFlag = AVIF_FALSE;
    const avifBool fullRange = image->yuvRange == AVIF_RANGE_FULL;

    // In AV1, the chroma_sample_position syntax element is not present for the YUV 4:2:2 format.
    // Assume that AV1 uses the same 4:2:2 chroma sample location as HEVC and VVC (colocated).
    if (image->yuvFormat != AVIF_PIXEL_FORMAT_YUV420 && image->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_UNKNOWN) {
        avifDiagnosticsPrintf(&encoder->diag,
                              "YUV chroma sample position %d is only supported with 4:2:0 YUV format in AV1",
                              image->yuvChromaSamplePosition);
        return AVIF_RESULT_INVALID_ARGUMENT;
    }
    // For the YUV 4:2:0 format, assume centered sample position unless specified otherwise.
    // This is consistent with the behavior in read.c.
    const avifBool chromaIsHorizontallyCentered = image->yuvFormat == AVIF_PIXEL_FORMAT_YUV420 &&
                                                  image->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_VERTICAL &&
                                                  image->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
    const avifBool chromaIsVerticallyCentered = image->yuvFormat == AVIF_PIXEL_FORMAT_YUV420 &&
                                                image->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;

    const uint32_t orientationMinus1 = avifImageIrotImirToExifOrientation(image) - 1;

    uint8_t infeType[4];
    uint8_t codecConfigType[4];
    avifBool hasExplicitCodecTypes;
    if (encoder->codecChoice == AVIF_CODEC_CHOICE_AVM) {
        memcpy(infeType, "av02", 4);
        memcpy(codecConfigType, "av2C", 4); // Same syntax as 'av1C'.
        hasExplicitCodecTypes = AVIF_TRUE;
    } else {
        memcpy(infeType, "av01", 4);
        memcpy(codecConfigType, "av1C", 4);
        // 'av01' and 'av1C' are implied by 'avif' minor_version field of FileTypeBox. No need to write them.
        hasExplicitCodecTypes = AVIF_FALSE;
    }

    // _minus1 is encoded for these fields.
    AVIF_ASSERT_OR_RETURN(image->width != 0 && image->height != 0);
    AVIF_ASSERT_OR_RETURN(colorData->size != 0);

    uint32_t largeDimensionsFlag = image->width - 1 >= (1 << 7) || image->height - 1 >= (1 << 7);
    const uint32_t codecConfigSize = 4;  // 'av1C' always uses 4 bytes.
    uint32_t alphaCodecConfigSize = 0;   // 0 if same codec config as main. Equal to codecConfigSize otherwise.
    uint32_t gainmapCodecConfigSize = 0; // 0 if same codec config as main. Equal to codecConfigSize otherwise.
    uint32_t gainmapMetadataSize = 0;
    const uint32_t largeCodecConfigFlag = codecConfigSize >= (1 << 3);
    uint32_t largeItemDataFlag = colorData->size - 1 >= (1 << 15) || (alphaData && alphaData->size >= (1 << 15));
    uint32_t largeMetadataFlag = (hasIcc && image->icc.size - 1 >= (1 << 10)) ||
                                 (image->exif.size != 0 && image->exif.size - 1 >= (1 << 10)) ||
                                 (image->xmp.size != 0 && image->xmp.size - 1 >= (1 << 10));

    if (hasGainmap) {
        AVIF_ASSERT_OR_RETURN(image->gainMap != NULL && image->gainMap->image != NULL);
        gainmapMetadataSize = avifGainMapMetadataSize(image->gainMap);
        AVIF_ASSERT_OR_RETURN(gainmapData != NULL);

        // _minus1 is encoded for these fields.
        AVIF_ASSERT_OR_RETURN(image->gainMap->image->width != 0 && image->gainMap->image->height != 0);

        largeDimensionsFlag |= image->gainMap->image->width - 1 >= (1 << 7) || image->gainMap->image->height - 1 >= (1 << 7);
        largeItemDataFlag |= gainmapData->size >= (1 << 15);
        largeMetadataFlag |=
            (encoder->data->altImageMetadata->icc.size != 0 && encoder->data->altImageMetadata->icc.size - 1 >= (1 << 10)) ||
            gainmapMetadataSize >= (1 << 10);
        // image->gainMap->image->icc is ignored.
    }

    avifBoxMarker mini;
    AVIF_CHECKRES(avifRWStreamWriteBox(s, "mini", AVIF_BOX_SIZE_TBD, &mini));
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, 2)); // bit(2) version = 0;

    // flags
    AVIF_CHECKRES(avifRWStreamWriteBits(s, hasExplicitCodecTypes, 1)); // bit(1) explicit_codec_types_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, floatFlag, 1));             // bit(1) float_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, fullRange, 1));             // bit(1) full_range_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, alphaItem != 0, 1));        // bit(1) alpha_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, hasExplicitCicp, 1));       // bit(1) explicit_cicp_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, hasHdr, 1));                // bit(1) hdr_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, hasIcc, 1));                // bit(1) icc_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, image->exif.size != 0, 1)); // bit(1) exif_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, image->xmp.size != 0, 1));  // bit(1) xmp_flag;

    AVIF_CHECKRES(avifRWStreamWriteBits(s, chromaSubsampling, 2)); // bit(2) chroma_subsampling;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, orientationMinus1, 3)); // bit(3) orientation_minus1;

    // Spatial extents
    AVIF_CHECKRES(avifRWStreamWriteBits(s, largeDimensionsFlag, 1));                         // bit(1) large_dimensions_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, image->width - 1, largeDimensionsFlag ? 15 : 7)); // unsigned int(large_dimensions_flag ? 15 : 7) width_minus1;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, image->height - 1, largeDimensionsFlag ? 15 : 7)); // unsigned int(large_dimensions_flag ? 15 : 7) height_minus1;

    // Pixel information
    if (chromaSubsampling == 1 || chromaSubsampling == 2) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, chromaIsHorizontallyCentered, 1)); // bit(1) chroma_is_horizontally_centered;
    }
    if (chromaSubsampling == 1) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, chromaIsVerticallyCentered, 1)); // bit(1) chroma_is_vertically_centered;
    }

    if (floatFlag) {
        // bit(2) bit_depth_log2_minus4;
        AVIF_ASSERT_OR_RETURN(AVIF_FALSE);
    } else {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, image->depth > 8, 1)); // bit(1) high_bit_depth_flag;
        if (image->depth > 8) {
            AVIF_CHECKRES(avifRWStreamWriteBits(s, image->depth - 9, 3)); // bit(3) bit_depth_minus9;
        }
    }

    if (alphaItem) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, alphaIsPremultiplied, 1)); // bit(1) alpha_is_premultiplied;
    }

    // Colour properties
    if (hasExplicitCicp) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, image->colorPrimaries, 8));          // bit(8) colour_primaries;
        AVIF_CHECKRES(avifRWStreamWriteBits(s, image->transferCharacteristics, 8)); // bit(8) transfer_characteristics;
        if (chromaSubsampling != 0) {
            AVIF_CHECKRES(avifRWStreamWriteBits(s, image->matrixCoefficients, 8)); // bit(8) matrix_coefficients;
        } else {
            AVIF_CHECKERR(image->matrixCoefficients == AVIF_MATRIX_COEFFICIENTS_UNSPECIFIED, AVIF_RESULT_ENCODE_COLOR_FAILED);
        }
    }

    if (hasExplicitCodecTypes) {
        // bit(32) infe_type;
        for (int i = 0; i < 4; ++i) {
            AVIF_CHECKRES(avifRWStreamWriteBits(s, infeType[i], 8));
        }
        // bit(32) codec_config_type;
        for (int i = 0; i < 4; ++i) {
            AVIF_CHECKRES(avifRWStreamWriteBits(s, codecConfigType[i], 8));
        }
    }

    // High Dynamic Range properties
    size_t tmapIccSize = 0;
    if (hasHdr) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, hasGainmap, 1)); // bit(1) gainmap_flag;
        if (hasGainmap) {
            const avifImage * tmap = encoder->data->altImageMetadata;
            const avifImage * gainmap = image->gainMap->image;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmap->width - 1, largeDimensionsFlag ? 15 : 7)); // unsigned int(large_dimensions_flag ? 15 : 7) gainmap_width_minus1;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmap->height - 1, largeDimensionsFlag ? 15 : 7)); // unsigned int(large_dimensions_flag ? 15 : 7) gainmap_height_minus1;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmap->matrixCoefficients, 8)); // bit(8) gainmap_matrix_coefficients;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmap->yuvRange == AVIF_RANGE_FULL, 1)); // bit(1) gainmap_full_range_flag;
            const uint32_t gainmapChromaSubsampling = gainmap->yuvFormat == AVIF_PIXEL_FORMAT_YUV400   ? 0
                                                      : gainmap->yuvFormat == AVIF_PIXEL_FORMAT_YUV420 ? 1
                                                      : gainmap->yuvFormat == AVIF_PIXEL_FORMAT_YUV422 ? 2
                                                                                                       : 3;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmapChromaSubsampling, 2)); // bit(1) gainmap_chroma_subsampling;
            if (gainmapChromaSubsampling == 1 || gainmapChromaSubsampling == 2) {
                AVIF_CHECKRES(avifRWStreamWriteBits(s,
                                                    gainmap->yuvFormat == AVIF_PIXEL_FORMAT_YUV420 &&
                                                        gainmap->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_VERTICAL &&
                                                        gainmap->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_COLOCATED,
                                                    1)); // bit(1) gainmap_chroma_is_horizontally_centered;
            }
            if (gainmapChromaSubsampling == 1) {
                AVIF_CHECKRES(avifRWStreamWriteBits(s,
                                                    gainmap->yuvFormat == AVIF_PIXEL_FORMAT_YUV420 &&
                                                        gainmap->yuvChromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_COLOCATED,
                                                    1)); // bit(1) gainmap_chroma_is_vertically_centered;
            }

            const avifBool gainmapFloatFlag = AVIF_FALSE;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmapFloatFlag, 1)); // bit(1) gainmap_float_flag;
            if (gainmapFloatFlag) {
                // bit(2) gainmap_bit_depth_log2_minus4;
                AVIF_ASSERT_OR_RETURN(AVIF_FALSE);
            } else {
                AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmap->depth > 8, 1)); // bit(1) gainmap_high_bit_depth_flag;
                if (gainmap->depth > 8) {
                    AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmap->depth - 9, 3)); // bit(3) gainmap_bit_depth_minus9;
                }
            }

            tmapIccSize = encoder->data->altImageMetadata->icc.size;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, tmapIccSize != 0, 1)); // bit(1) tmap_icc_flag;
            const avifBool tmapHasExplicitCicp = tmap->colorPrimaries != AVIF_COLOR_PRIMARIES_BT709 ||
                                                 tmap->transferCharacteristics != AVIF_TRANSFER_CHARACTERISTICS_SRGB ||
                                                 tmap->matrixCoefficients != AVIF_MATRIX_COEFFICIENTS_BT601 ||
                                                 tmap->yuvRange != AVIF_RANGE_FULL;
            AVIF_CHECKRES(avifRWStreamWriteBits(s, tmapHasExplicitCicp, 1)); // bit(1) tmap_explicit_cicp_flag;
            if (tmapHasExplicitCicp) {
                AVIF_CHECKRES(avifRWStreamWriteBits(s, tmap->colorPrimaries, 8));          // bit(8) tmap_colour_primaries;
                AVIF_CHECKRES(avifRWStreamWriteBits(s, tmap->transferCharacteristics, 8)); // bit(8) tmap_transfer_characteristics;
                AVIF_CHECKRES(avifRWStreamWriteBits(s, tmap->matrixCoefficients, 8));      // bit(8) tmap_matrix_coefficients;
                AVIF_CHECKRES(avifRWStreamWriteBits(s, tmap->yuvRange == AVIF_RANGE_FULL, 1)); // bit(8) tmap_full_range_flag;
            }
            // gainmap->icc is ignored.
        }

        AVIF_CHECKRES(avifEncoderWriteMiniHDRProperties(s, image));
        if (hasGainmap) {
            AVIF_CHECKRES(avifEncoderWriteMiniHDRProperties(s, encoder->data->altImageMetadata));
        }
    }

    // Chunk sizes
    if (hasIcc || image->exif.size || image->xmp.size || (hasHdr && hasGainmap)) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, largeMetadataFlag, 1)); // bit(1) large_metadata_flag;
    }
    AVIF_CHECKRES(avifRWStreamWriteBits(s, largeCodecConfigFlag, 1)); // bit(1) large_codec_config_flag;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, largeItemDataFlag, 1));    // bit(1) large_item_data_flag;

    if (hasIcc) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)image->icc.size - 1, largeMetadataFlag ? 20 : 10)); // unsigned int(large_metadata_flag ? 20 : 10) icc_data_size_minus1;
    }
    if (hasHdr && hasGainmap && tmapIccSize != 0) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)tmapIccSize - 1, largeMetadataFlag ? 20 : 10)); // unsigned int(large_metadata_flag ? 20 : 10) tmap_icc_data_size_minus1;
    }

    if (hasHdr && hasGainmap) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmapMetadataSize, largeMetadataFlag ? 20 : 10)); // unsigned int(large_metadata_flag ? 20 : 10) gainmap_metadata_size;
    }
    if (hasHdr && hasGainmap) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)gainmapData->size, largeItemDataFlag ? 28 : 15)); // unsigned int(large_item_data_flag ? 28 : 15) gainmap_item_data_size;
    }
    if (hasHdr && hasGainmap && gainmapData->size != 0) {
        if (!memcmp(&colorItem->av1C, &gainmapItem->av1C, sizeof(colorItem->av1C))) {
            // The gainmap codec config is copied from the main codec config.
            // This is signaled by a size of 0.
            gainmapCodecConfigSize = 0;
        } else {
            gainmapCodecConfigSize = codecConfigSize;
        }
        AVIF_CHECKRES(avifRWStreamWriteBits(s, gainmapCodecConfigSize, largeCodecConfigFlag ? 12 : 3)); // unsigned int(large_codec_config_flag ? 12 : 3) gainmap_item_codec_config_size;
    }

    AVIF_CHECKRES(avifRWStreamWriteBits(s, codecConfigSize, largeCodecConfigFlag ? 12 : 3)); // unsigned int(large_codec_config_flag ? 12 : 3) main_item_codec_config_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)colorData->size - 1, largeItemDataFlag ? 28 : 15)); // unsigned int(large_item_data_flag ? 28 : 15) main_item_data_size_minus1;

    if (hasAlpha) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)alphaData->size, largeItemDataFlag ? 28 : 15)); // unsigned int(large_item_data_flag ? 28 : 15) alpha_item_data_size;
    }
    if (hasAlpha && alphaData->size != 0) {
        if (!memcmp(&colorItem->av1C, &alphaItem->av1C, sizeof(colorItem->av1C))) {
            // The alpha codec config is copied from the main codec config.
            // This is signaled by a size of 0.
            alphaCodecConfigSize = 0;
        } else {
            alphaCodecConfigSize = codecConfigSize;
        }
        AVIF_CHECKRES(avifRWStreamWriteBits(s, alphaCodecConfigSize, largeCodecConfigFlag ? 12 : 3)); // unsigned int(large_codec_config_flag ? 12 : 3) alpha_item_codec_config_size;
    }

    if (image->exif.size || image->xmp.size) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, 1)); // unsigned int(1) exif_xmp_compressed_flag
    }
    if (image->exif.size) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)image->exif.size - 1, largeMetadataFlag ? 20 : 10)); // unsigned int(large_metadata_flag ? 20 : 10) exif_data_size_minus_one;
    }
    if (image->xmp.size) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, (uint32_t)image->xmp.size - 1, largeMetadataFlag ? 20 : 10)); // unsigned int(large_metadata_flag ? 20 : 10) xmp_data_size_minus_one;
    }

    // trailing_bits(); // bit padding till byte alignment
    if (s->numUsedBitsInPartialByte != 0) {
        AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, 8 - s->numUsedBitsInPartialByte));
    }
    const size_t headerBytes = avifRWStreamOffset(s);

    // Chunks
    if (codecConfigSize > 0) {
        AVIF_CHECKRES(writeCodecConfig(s, &colorItem->av1C)); // unsigned int(8) main_item_codec_config[main_item_codec_config_size];
    }
    if (hasAlpha && alphaData->size != 0 && alphaCodecConfigSize != 0) {
        AVIF_CHECKRES(writeCodecConfig(s, &alphaItem->av1C)); // unsigned int(8) alpha_item_codec_config[alpha_item_codec_config_size];
    }
    if (hasHdr && hasGainmap && gainmapCodecConfigSize != 0) {
        AVIF_CHECKRES(writeCodecConfig(s, &gainmapItem->av1C)); // unsigned int(8) gainmap_item_codec_config[gainmap_item_codec_config_size];
    }

    if (hasIcc) {
        AVIF_CHECKRES(avifRWStreamWrite(s, image->icc.data, image->icc.size)); // unsigned int(8) icc_data[icc_data_size_minus1 + 1];
    }
    if (hasHdr && hasGainmap && tmapIccSize != 0) {
        AVIF_CHECKRES(avifRWStreamWrite(s, encoder->data->altImageMetadata->icc.data, tmapIccSize)); // unsigned int(8) tmap_icc_data[tmap_icc_data_size_minus1 + 1];
    }
    if (hasHdr && hasGainmap && gainmapMetadataSize != 0) {
        AVIF_CHECKRES(avifWriteGainmapMetadata(s, image->gainMap, &encoder->diag)); // unsigned int(8) gainmap_metadata[gainmap_metadata_size];
    }

    if (hasAlpha && alphaData->size != 0) {
        AVIF_CHECKRES(avifRWStreamWrite(s, alphaData->data, alphaData->size)); // unsigned int(8) alpha_item_data[alpha_item_data_size];
    }
    if (hasHdr && hasGainmap && gainmapData->size != 0) {
        AVIF_CHECKRES(avifRWStreamWrite(s, gainmapData->data, gainmapData->size)); // unsigned int(8) gainmap_item_data[gainmap_item_data_size];
    }

    AVIF_CHECKRES(avifRWStreamWrite(s, colorData->data, colorData->size)); // unsigned int(8) main_item_data[main_item_data_size_minus1 + 1];

    if (image->exif.size) {
        AVIF_CHECKRES(avifRWStreamWrite(s, image->exif.data, image->exif.size)); // unsigned int(8) exif_data[exif_data_size_minus1 + 1];
    }
    if (image->xmp.size) {
        AVIF_CHECKRES(avifRWStreamWrite(s, image->xmp.data, image->xmp.size)); // unsigned int(8) xmp_data[xmp_data_size_minus1 + 1];
    }

    const size_t expectedChunkBytes = codecConfigSize + alphaCodecConfigSize + gainmapCodecConfigSize + image->icc.size +
                                      tmapIccSize + gainmapMetadataSize + (hasAlpha ? alphaData->size : 0) +
                                      (hasGainmap ? gainmapData->size : 0) + colorData->size + image->exif.size + image->xmp.size;
    AVIF_ASSERT_OR_RETURN(avifRWStreamOffset(s) == headerBytes + expectedChunkBytes);
    avifRWStreamFinishBox(s, mini);
    return AVIF_RESULT_OK;
}
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

static avifResult avifRWStreamWriteProperties(avifItemPropertyDedup * const dedup,
                                              avifRWStream * const s,
                                              const avifEncoder * const encoder,
                                              const avifImage * const imageMetadata,
                                              const avifImage * const altImageMetadata)
{
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        const avifBool isGrid = (item->gridCols > 0);
        // Whether there is ipma to write for this item.
        avifBool hasIpmaToWrite = item->codec || isGrid;
        const avifBool isToneMappedImage = !memcmp(item->type, "tmap", 4);
        if (isToneMappedImage) {
            hasIpmaToWrite = AVIF_TRUE;
        }
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
        const avifBool isSampleTransformImage = !memcmp(item->type, "sato", 4);
        if (isSampleTransformImage) {
            hasIpmaToWrite = AVIF_TRUE;
        }
#endif
        item->associations.count = 0;
        if (!hasIpmaToWrite) {
            continue;
        }

        if (item->dimgFromID && (item->extraLayerCount == 0)) {
            avifEncoderItem * parentItem = avifEncoderDataFindItemByID(encoder->data, item->dimgFromID);
            if (parentItem && !memcmp(parentItem->type, "grid", 4)) {
                // All image cells from a grid should share the exact same properties unless they are
                // layered image which have different al1x, so see if we've already written properties
                // out for another cell in this grid, and if so, just steal their ipma and move on.
                // This is a sneaky way to provide iprp deduplication.

                avifBool foundPreviousCell = AVIF_FALSE;
                for (uint32_t dedupIndex = 0; dedupIndex < itemIndex; ++dedupIndex) {
                    avifEncoderItem * dedupItem = &encoder->data->items.item[dedupIndex];
                    if ((item->dimgFromID == dedupItem->dimgFromID) && (dedupItem->extraLayerCount == 0)) {
                        // We've already written dedup's items out. Steal their ipma indices and move on!
                        item->associations.count = 0;
                        for (uint32_t associationIndex = 0; associationIndex < dedupItem->associations.count; ++associationIndex) {
                            avifItemPropertyAssociation * association = (avifItemPropertyAssociation *)avifArrayPush(&item->associations);
                            AVIF_CHECKERR(association != NULL, AVIF_RESULT_OUT_OF_MEMORY);
                            *association = dedupItem->associations.association[associationIndex];
                        }
                        foundPreviousCell = AVIF_TRUE;
                        break;
                    }
                }
                if (foundPreviousCell) {
                    continue;
                }
            }
        }

        const avifImage * itemMetadata = imageMetadata;
        if (isToneMappedImage) {
            itemMetadata = altImageMetadata;
        } else if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
            AVIF_ASSERT_OR_RETURN(itemMetadata->gainMap && itemMetadata->gainMap->image);
            itemMetadata = itemMetadata->gainMap->image;
        }
        uint32_t imageWidth = itemMetadata->width;
        uint32_t imageHeight = itemMetadata->height;
        if (isGrid) {
            imageWidth = item->gridWidth;
            imageHeight = item->gridHeight;
        }

        // ISO/IEC 23008-12:2024 section 6.5.1:
        //   Writers should arrange the descriptive properties specified in 6.5 prior to any other properties
        //   in the sequence associating properties with an item.
        // For each item, write the descriptive properties first (and thus associate them before the
        // transformative properties).

        // Properties all image items need (coded and derived)
        // ispe = image spatial extent (width, height)
        avifItemPropertyDedupStart(dedup);
        avifBoxMarker ispe;
        AVIF_CHECKRES(avifRWStreamWriteFullBox(&dedup->s, "ispe", AVIF_BOX_SIZE_TBD, 0, 0, &ispe));
        AVIF_CHECKRES(avifRWStreamWriteU32(&dedup->s, imageWidth));  // unsigned int(32) image_width;
        AVIF_CHECKRES(avifRWStreamWriteU32(&dedup->s, imageHeight)); // unsigned int(32) image_height;
        avifRWStreamFinishBox(&dedup->s, ispe);
        AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, s, &item->associations, /*essential=*/AVIF_FALSE));

        // pixi = pixel information (depth, channel count)
        avifBool hasPixi = AVIF_TRUE;
        // Pixi is optional for the 'tmap' item.
        if (isToneMappedImage && imageMetadata->gainMap->altDepth == 0 && imageMetadata->gainMap->altPlaneCount == 0) {
            hasPixi = AVIF_FALSE;
        }
        const avifBool isAlpha = avifIsAlpha(item->itemCategory);
        uint8_t depth = (uint8_t)itemMetadata->depth;
#if defined(AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM)
        if (encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_8B_8B ||
            encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_4B ||
            encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_8B_OVERLAP_4B) {
            if (item->itemCategory == AVIF_ITEM_SAMPLE_TRANSFORM) {
                AVIF_ASSERT_OR_RETURN(depth == 16); // Only 16-bit depth is supported for now.
            } else if (encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_8B_8B) {
                depth = 8;
            } else {
                if (item->itemCategory == AVIF_ITEM_COLOR || item->itemCategory == AVIF_ITEM_ALPHA) {
                    depth = 12;
                } else {
                    AVIF_ASSERT_OR_RETURN(item->itemCategory == AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_COLOR ||
                                          item->itemCategory == AVIF_ITEM_SAMPLE_TRANSFORM_INPUT_0_ALPHA);
                    // Will be shifted to 4-bit samples at decoding for AVIF_SAMPLE_TRANSFORM_BIT_DEPTH_EXTENSION_12B_4B.
                    depth = 8;
                }
            }
        } else {
            AVIF_CHECKERR(encoder->sampleTransformRecipe == AVIF_SAMPLE_TRANSFORM_NONE, AVIF_RESULT_NOT_IMPLEMENTED);
        }
        assert(isSampleTransformImage == (item->itemCategory == AVIF_ITEM_SAMPLE_TRANSFORM));
#endif // AVIF_ENABLE_EXPERIMENTAL_SAMPLE_TRANSFORM
        if (hasPixi) {
            avifItemPropertyDedupStart(dedup);
            uint8_t channelCount = (isAlpha || (itemMetadata->yuvFormat == AVIF_PIXEL_FORMAT_YUV400)) ? 1 : 3;
            // See ISO/IEC 23008-12:2024/CDAM 2:2025 section 6.5.6.3.
            avifBoxMarker pixi;
            uint32_t flags = 0;
#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
            if (encoder->headerFormat & AVIF_HEADER_EXTENDED_PIXI) {
                flags |= 1;
            }
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&dedup->s, "pixi", AVIF_BOX_SIZE_TBD, 0, flags, &pixi));
            AVIF_CHECKRES(avifRWStreamWriteU8(&dedup->s, channelCount)); // unsigned int (8) num_channels;
            for (uint8_t chan = 0; chan < channelCount; ++chan) {
                AVIF_CHECKRES(avifRWStreamWriteU8(&dedup->s, depth)); // unsigned int (8) bits_per_channel;
            }
#if defined(AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI)
            if (flags & 1) {
                AVIF_ASSERT_OR_RETURN(item->av1C.chromaSamplePosition != AVIF_CHROMA_SAMPLE_POSITION_RESERVED);
                // Do not signal any subsampling information if the sample position is unknown because the 'pixi' box
                // does not have an enum entry for "unknown subsampling location".
                const uint8_t subsampling_flag = item->av1C.chromaSamplePosition == AVIF_CHROMA_SAMPLE_POSITION_VERTICAL ||
                                                 item->av1C.chromaSamplePosition == AVIF_CHROMA_SAMPLE_POSITION_COLOCATED;
                for (uint8_t chan = 0; chan < channelCount; ++chan) {
                    AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, 0, /*bitCount=*/3)); // unsigned int(3) channel_idc;
                    AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, 0, /*bitCount=*/1)); // unsigned int(1) reserved;
                    AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, 0, /*bitCount=*/2)); // unsigned int(2) component_format;
                    AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, subsampling_flag, /*bitCount=*/1)); // unsigned int(1) subsampling_flag;
                    AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, 0, /*bitCount=*/1)); // unsigned int(1) channel_label_flag;
                    if (subsampling_flag) {
                        const uint8_t subsamplingType = avifCodecConfigurationBoxGetSubsamplingType(&item->av1C, chan);
                        const uint8_t subsamplingLocation = subsamplingType == AVIF_PIXI_444 ? 0
                                                            : item->av1C.chromaSamplePosition == AVIF_CHROMA_SAMPLE_POSITION_VERTICAL
                                                                ? 0
                                                                : 2;
                        AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, subsamplingType, /*bitCount=*/4)); // unsigned int(4) subsampling_type;
                        AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, subsamplingLocation, /*bitCount=*/4)); // unsigned int(4) subsampling_location;
                    }
                }
            }
#endif // AVIF_ENABLE_EXPERIMENTAL_EXTENDED_PIXI
            avifRWStreamFinishBox(&dedup->s, pixi);
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, s, &item->associations, /*essential=*/AVIF_FALSE));
        }

        // Codec configuration box ('av1C' or 'av2C')
        if (item->codec) {
            avifItemPropertyDedupStart(dedup);
            AVIF_CHECKRES(writeConfigBox(&dedup->s, &item->av1C, encoder->data->configPropName));
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, s, &item->associations, /*essential=*/AVIF_TRUE));
        }

        if (isAlpha) {
            // Alpha specific properties

            avifItemPropertyDedupStart(dedup);
            avifBoxMarker auxC;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&dedup->s, "auxC", AVIF_BOX_SIZE_TBD, 0, 0, &auxC));
            AVIF_CHECKRES(avifRWStreamWriteChars(&dedup->s, alphaURN, alphaURNSize)); //  string aux_type;
            avifRWStreamFinishBox(&dedup->s, auxC);
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, s, &item->associations, /*essential=*/AVIF_FALSE));
        } else if (item->itemCategory == AVIF_ITEM_COLOR) {
            // Color specific properties
            // Note the 'tmap' (tone mapped image) item when a gain map is present also has itemCategory AVIF_ITEM_COLOR.

            AVIF_CHECKRES(avifEncoderWriteColorProperties(s, itemMetadata, &item->associations, dedup));
            AVIF_CHECKRES(avifEncoderWriteHDRProperties(&dedup->s, s, itemMetadata, &item->associations, dedup));
        } else if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
            // Gain map specific properties

            // Write the colr nclx box.
            AVIF_CHECKRES(avifEncoderWriteNclxProperty(&dedup->s, s, itemMetadata, &item->associations, dedup));

            AVIF_CHECKRES(avifEncoderWritePaspProperty(&dedup->s, s, imageMetadata, &item->associations, dedup));
        }

        if (item->extraLayerCount > 0) {
            // Layered Image Indexing Property

            avifItemPropertyDedupStart(dedup);
            avifBoxMarker a1lx;
            AVIF_CHECKRES(avifRWStreamWriteBox(&dedup->s, "a1lx", AVIF_BOX_SIZE_TBD, &a1lx));
            uint32_t layerSize[AVIF_MAX_AV1_LAYER_COUNT - 1] = { 0 };
            avifBool largeSize = AVIF_FALSE;

            for (uint32_t validLayer = 0; validLayer < item->extraLayerCount; ++validLayer) {
                uint32_t size = (uint32_t)item->encodeOutput->samples.sample[validLayer].data.size;
                layerSize[validLayer] = size;
                if (size > 0xffff) {
                    largeSize = AVIF_TRUE;
                }
            }

            AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, 0, /*bitCount=*/7));                 // unsigned int(7) reserved = 0;
            AVIF_CHECKRES(avifRWStreamWriteBits(&dedup->s, largeSize ? 1 : 0, /*bitCount=*/1)); // unsigned int(1) large_size;

            // FieldLength = (large_size + 1) * 16;
            // unsigned int(FieldLength) layer_size[3];
            for (uint32_t layer = 0; layer < AVIF_MAX_AV1_LAYER_COUNT - 1; ++layer) {
                if (largeSize) {
                    AVIF_CHECKRES(avifRWStreamWriteU32(&dedup->s, layerSize[layer]));
                } else {
                    AVIF_CHECKRES(avifRWStreamWriteU16(&dedup->s, (uint16_t)layerSize[layer]));
                }
            }
            avifRWStreamFinishBox(&dedup->s, a1lx);
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, s, &item->associations, /*essential=*/AVIF_FALSE));

            // We don't add an 'lsel' property since many decoders do not support it and will reject the image,
            // see https://github.com/AOMediaCodec/libavif/pull/2429
        }

        // Write out any opaque properties from avifImageAddOpaqueProperty() or avifImageAddUUIDProperty().
        for (size_t i = 0; i < itemMetadata->numProperties; i++) {
            avifItemPropertyDedupStart(dedup);
            const avifImageItemProperty * prop = &itemMetadata->properties[i];
            avifBoxMarker propMarker;
            AVIF_CHECKRES(avifRWStreamWriteBox(&dedup->s, (const char *)prop->boxtype, AVIF_BOX_SIZE_TBD, &propMarker));
            if (memcmp(prop->boxtype, "uuid", 4) == 0) {
                AVIF_CHECKRES(avifRWStreamWrite(&dedup->s, prop->usertype, 16));
            }
            AVIF_CHECKRES(avifRWStreamWrite(&dedup->s, prop->boxPayload.data, prop->boxPayload.size));
            avifRWStreamFinishBox(&dedup->s, propMarker);
            AVIF_CHECKRES(avifItemPropertyDedupFinish(dedup, s, &item->associations, /*essential=*/AVIF_FALSE));
        }

        // Also write the transformative properties.

        if (item->itemCategory == AVIF_ITEM_COLOR) {
            // Color specific properties
            // Note the 'tmap' (tone mapped image) item when a gain map is present also has itemCategory AVIF_ITEM_COLOR.
            AVIF_CHECKRES(avifEncoderWriteTransformativeProperties(&dedup->s, s, itemMetadata, &item->associations, dedup));
        } else if (item->itemCategory == AVIF_ITEM_GAIN_MAP) {
            // Gain map specific properties

            // For the orientation, it could be done in multiple ways:
            // - Bake the orientation in the base and gain map images.
            //   This does not allow for orientation changes without recompression.
            // - Associate 'irot'/'imir' with the 'tmap' derived image item only.
            //   If so, decoding only the base image would give a different orientation than
            //   decoding the tone-mapped image.
            // - Wrap the base image in an 'iden' derived image item and associate 'irot'/'imir'
            //   with the 'tmap' and 'iden' derived image items. 'iden' is not currently supported
            //   by libavif, reducing the backward compatibility of this solution.
            // - Associate 'irot'/'imir' with the base and gain map image items.
            //   Do not associate 'irot'/'imir' with the 'tmap' derived image item.
            //   These transformative properties are supposed to be applied at decoding on
            //   image items before these are used as input to a derived image item.
            //   libavif uses this pattern at encoding and requires it at decoding.
            //   As of today, this is forbidden by the AVIF specification:
            //     https://aomediacodec.github.io/av1-avif/v1.1.0.html#file-constraints
            //   That rule was written before 'tmap' was proposed and may be relaxed for 'tmap'.

            // 'clap' is treated as 'irot'/'imir', although it could differ between the base and
            // gain map image items if these have different dimensions.
            if (imageMetadata->transformFlags & AVIF_TRANSFORM_CLAP) {
                AVIF_CHECKERR(imageMetadata->width != itemMetadata->width || imageMetadata->height != itemMetadata->height,
                              AVIF_RESULT_NOT_IMPLEMENTED);
            }

            // 'pasp' is not a transformative property (despite AVIF_TRANSFORM_PASP being part of
            // avifTransformFlag) but it is assumed to apply to the gain map in the same way as
            // the transformative properties above.

            // Based on the explanation above, 'clap', 'irot', 'imir' and 'pasp' have to match between the base and
            // gain map image items in the container part of the encoded file.
            // To enforce that, the transformative and 'pasp' properties of the gain map cannot be set explicitly in the API.
            AVIF_CHECKERR(itemMetadata->transformFlags == AVIF_TRANSFORM_NONE, AVIF_RESULT_ENCODE_GAIN_MAP_FAILED);
            AVIF_CHECKRES(avifEncoderWriteTransformativeProperties(&dedup->s, s, imageMetadata, &item->associations, dedup));
        }
    }
    return AVIF_RESULT_OK;
}

avifResult avifEncoderFinish(avifEncoder * encoder, avifRWData * output)
{
    avifDiagnosticsClearError(&encoder->diag);
    if (encoder->data->items.count == 0) {
        return AVIF_RESULT_NO_CONTENT;
    }

    const avifCodecType codecType = avifEncoderGetCodecType(encoder);
    if (codecType == AVIF_CODEC_TYPE_UNKNOWN) {
        return AVIF_RESULT_NO_CODEC_AVAILABLE;
    }

    // -----------------------------------------------------------------------
    // Finish up encoding

    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        if (item->codec) {
            if (!item->codec->encodeFinish(item->codec, item->encodeOutput)) {
                return avifGetErrorForItemCategory(item->itemCategory);
            }

            if (item->encodeOutput->samples.count != encoder->data->frames.count) {
                return avifGetErrorForItemCategory(item->itemCategory);
            }

            if ((item->extraLayerCount > 0) && (item->encodeOutput->samples.count != item->extraLayerCount + 1)) {
                // Check whether user has sent enough frames to encoder.
                avifDiagnosticsPrintf(&encoder->diag,
                                      "Expected %u frames given to avifEncoderAddImage() to encode this layered image according to extraLayerCount, but got %u frames.",
                                      item->extraLayerCount + 1,
                                      item->encodeOutput->samples.count);
                return AVIF_RESULT_INVALID_ARGUMENT;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Harvest configuration properties from sequence headers

    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        if (item->encodeOutput->samples.count > 0) {
            const avifEncodeSample * firstSample = &item->encodeOutput->samples.sample[0];
            avifSequenceHeader sequenceHeader;
            AVIF_CHECKERR(avifSequenceHeaderParse(&sequenceHeader, (const avifROData *)&firstSample->data, codecType),
                          avifGetErrorForItemCategory(item->itemCategory));
            item->av1C = sequenceHeader.av1C;
        }
    }

    // -----------------------------------------------------------------------
    // Begin write stream

#if defined(AVIF_ENABLE_EXPERIMENTAL_MINI)
    // Decide whether to go for a reduced MinimizedImageBox or a full regular MetaBox.
    if ((encoder->headerFormat & AVIF_HEADER_MINI) && avifEncoderIsMiniCompatible(encoder)) {
        AVIF_CHECKRES(avifEncoderWriteFileTypeBoxAndMiniBox(encoder, output));
        return AVIF_RESULT_OK;
    }
#endif // AVIF_ENABLE_EXPERIMENTAL_MINI

    const avifImage * imageMetadata = encoder->data->imageMetadata;
    // The epoch for creation_time and modification_time is midnight, Jan. 1,
    // 1904, in UTC time. Add the number of seconds between that epoch and the
    // Unix epoch.
    uint64_t now = (uint64_t)time(NULL) + 2082844800;

    avifRWStream s;
    avifRWStreamStart(&s, output);

    // -----------------------------------------------------------------------
    // Write ftyp

    // Layered sequence is not supported for now.
    const avifBool isSequence = (encoder->extraLayerCount == 0) && (encoder->data->frames.count > 1);

    const char * majorBrand = "avif";
    if (isSequence) {
        majorBrand = "avis";
    }

    uint32_t minorVersion = 0;
#if defined(AVIF_CODEC_AVM)
    if (codecType == AVIF_CODEC_TYPE_AV2) {
        // TODO(yguyon): Experimental AV2-AVIF is AVIF version 2 for now (change once it is ratified).
        minorVersion = 2;
    }
#endif

    // According to section 5.2 of AV1 Image File Format specification v1.1.0:
    //   If the primary item or all the items referenced by the primary item are AV1 image items made only
    //   of Intra Frames, the brand "avio" should be used in the compatible_brands field of the FileTypeBox.
    // See https://aomediacodec.github.io/av1-avif/v1.1.0.html#image-and-image-collection-brand.
    // This rule corresponds to using the "avio" brand in all cases except for layered images, because:
    //  - Non-layered still images are always Intra Frames, even with grids;
    //  - Sequences cannot be combined with layers or grids, and the first frame of the sequence
    //    (referred to by the primary image item) is always an Intra Frame.
    avifBool useAvioBrand;
    if (isSequence) {
        // According to section 5.3 of AV1 Image File Format specification v1.1.0:
        //   Additionally, if a file contains AV1 image sequences and the brand avio is used in the
        //   compatible_brands field of the FileTypeBox, the item constraints for this brand shall be met
        //   and at least one of the AV1 image sequences shall be made only of AV1 Samples marked as sync.
        // See https://aomediacodec.github.io/av1-avif/v1.1.0.html#image-sequence-brand.
        useAvioBrand = AVIF_FALSE;
        for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
            avifEncoderItem * item = &encoder->data->items.item[itemIndex];
            if (item->encodeOutput->samples.count == 0) {
                continue; // Not a track.
            }
            avifBool onlySyncSamples = AVIF_TRUE;
            for (uint32_t sampleIndex = 0; sampleIndex < item->encodeOutput->samples.count; ++sampleIndex) {
                if (!item->encodeOutput->samples.sample[sampleIndex].sync) {
                    onlySyncSamples = AVIF_FALSE;
                    break;
                }
            }
            if (onlySyncSamples) {
                useAvioBrand = AVIF_TRUE; // at least one of the AV1 image sequences is made only of sync samples
                break;
            }
        }
    } else {
        // The gpac/ComplianceWarden tool only warns about the lack of the "avio" brand for sequences,
        // and the specification says the brand "should" be used, not "shall". Leverage that opportunity
        // to save four bytes for still images.
        useAvioBrand = AVIF_FALSE; // Should be (encoder->extraLayerCount == 0) to be fully compliant.
    }

    avifBoxMarker ftyp;
    AVIF_CHECKRES(avifRWStreamWriteBox(&s, "ftyp", AVIF_BOX_SIZE_TBD, &ftyp));
    AVIF_CHECKRES(avifRWStreamWriteChars(&s, majorBrand, 4));              // unsigned int(32) major_brand;
    AVIF_CHECKRES(avifRWStreamWriteU32(&s, minorVersion));                 // unsigned int(32) minor_version;
    AVIF_CHECKRES(avifRWStreamWriteChars(&s, "avif", 4));                  // unsigned int(32) compatible_brands[];
    if (useAvioBrand) {                                                    //
        AVIF_CHECKRES(avifRWStreamWriteChars(&s, "avio", 4));              // ... compatible_brands[]
    }                                                                      //
    if (isSequence) {                                                      //
        AVIF_CHECKRES(avifRWStreamWriteChars(&s, "avis", 4));              // ... compatible_brands[]
        AVIF_CHECKRES(avifRWStreamWriteChars(&s, "msf1", 4));              // ... compatible_brands[]
        AVIF_CHECKRES(avifRWStreamWriteChars(&s, "iso8", 4));              // ... compatible_brands[]
    }                                                                      //
    AVIF_CHECKRES(avifRWStreamWriteChars(&s, "mif1", 4));                  // ... compatible_brands[]
    AVIF_CHECKRES(avifRWStreamWriteChars(&s, "miaf", 4));                  // ... compatible_brands[]
    if ((imageMetadata->depth == 8) || (imageMetadata->depth == 10)) {     //
        if (imageMetadata->yuvFormat == AVIF_PIXEL_FORMAT_YUV420) {        //
            AVIF_CHECKRES(avifRWStreamWriteChars(&s, "MA1B", 4));          // ... compatible_brands[]
        } else if (imageMetadata->yuvFormat == AVIF_PIXEL_FORMAT_YUV444) { //
            AVIF_CHECKRES(avifRWStreamWriteChars(&s, "MA1A", 4));          // ... compatible_brands[]
        }
    }
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        if (!memcmp(encoder->data->items.item[itemIndex].type, "tmap", 4)) {
            // ISO/IEC 23008-12:2024/AMD 1:2024(E)
            // This brand enables file players to identify and decode HEIF files containing tone-map derived image
            // items. When present, this brand shall be among the brands included in the compatible_brands
            // array of the FileTypeBox.
            AVIF_CHECKRES(avifRWStreamWriteChars(&s, "tmap", 4)); // ... compatible_brands[]
            break;
        }
    }
    avifRWStreamFinishBox(&s, ftyp);

    // -----------------------------------------------------------------------
    // Start meta

    avifBoxMarker meta;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "meta", AVIF_BOX_SIZE_TBD, 0, 0, &meta));

    // -----------------------------------------------------------------------
    // Write hdlr

    AVIF_CHECKRES(avifRWStreamWriteHandlerBox(&s, "pict"));

    // -----------------------------------------------------------------------
    // Write pitm

    if (encoder->data->primaryItemID != 0) {
        AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "pitm", sizeof(uint16_t), 0, 0, /*marker=*/NULL));
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, encoder->data->primaryItemID)); //  unsigned int(16) item_ID;
    }

    // -----------------------------------------------------------------------
    // Write iloc

    avifBoxMarker iloc;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "iloc", AVIF_BOX_SIZE_TBD, 0, 0, &iloc));
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, 4, /*bitCount=*/4));                   // unsigned int(4) offset_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, 4, /*bitCount=*/4));                   // unsigned int(4) length_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, 0, /*bitCount=*/4));                   // unsigned int(4) base_offset_size;
    AVIF_CHECKRES(avifRWStreamWriteBits(&s, 0, /*bitCount=*/4));                   // unsigned int(4) reserved;
    AVIF_CHECKRES(avifRWStreamWriteU16(&s, (uint16_t)encoder->data->items.count)); // unsigned int(16) item_count;

    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, item->id)); // unsigned int(16) item_ID;
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));        // unsigned int(16) data_reference_index;

        // Layered Image, write location for all samples
        if (item->extraLayerCount > 0) {
            uint32_t layerCount = item->extraLayerCount + 1;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, (uint16_t)layerCount)); // unsigned int(16) extent_count;
            for (uint32_t i = 0; i < layerCount; ++i) {
                AVIF_CHECKRES(avifEncoderItemAddMdatFixup(item, &s));
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0 /* set later */)); // unsigned int(offset_size*8) extent_offset;
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)item->encodeOutput->samples.sample[i].data.size)); // unsigned int(length_size*8) extent_length;
            }
            continue;
        }

        uint32_t contentSize = (uint32_t)item->metadataPayload.size;
        if (item->encodeOutput->samples.count > 0) {
            // This is choosing sample 0's size as there are two cases here:
            // * This is a single image, in which case this is correct
            // * This is an image sequence, but this file should still be a valid single-image avif,
            //   so there must still be a primary item pointing at a sync sample. Since the first
            //   frame of the image sequence is guaranteed to be a sync sample, it is chosen here.
            //
            // TODO: Offer the ability for a user to specify which frame in the sequence should
            //       become the primary item's image, and force that frame to be a keyframe.
            contentSize = (uint32_t)item->encodeOutput->samples.sample[0].data.size;
        }

        AVIF_CHECKRES(avifRWStreamWriteU16(&s, 1));                     // unsigned int(16) extent_count;
        AVIF_CHECKRES(avifEncoderItemAddMdatFixup(item, &s));           //
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0 /* set later */));     // unsigned int(offset_size*8) extent_offset;
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)contentSize)); // unsigned int(length_size*8) extent_length;
    }

    avifRWStreamFinishBox(&s, iloc);

    // -----------------------------------------------------------------------
    // Write iinf

    // Section 8.11.6.2 of ISO/IEC 14496-12.
    avifBoxMarker iinf;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "iinf", AVIF_BOX_SIZE_TBD, 0, 0, &iinf));
    AVIF_CHECKRES(avifRWStreamWriteU16(&s, (uint16_t)encoder->data->items.count)); //  unsigned int(16) entry_count;

    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];

        uint32_t flags = item->hiddenImage ? 1 : 0;
        avifBoxMarker infe;
        AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "infe", AVIF_BOX_SIZE_TBD, 2, flags, &infe));
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, item->id));                             // unsigned int(16) item_ID;
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));                                    // unsigned int(16) item_protection_index;
        AVIF_CHECKRES(avifRWStreamWrite(&s, item->type, 4));                           // unsigned int(32) item_type;
        AVIF_CHECKRES(avifRWStreamWriteChars(&s, item->infeName, item->infeNameSize)); // utf8string item_name; (writing null terminator)
        if (!memcmp(item->type, "mime", 4)) {
            AVIF_CHECKRES(avifRWStreamWriteChars(&s, item->infeContentType, item->infeContentTypeSize)); // utf8string content_type; (writing null terminator)
            // utf8string content_encoding; //optional
        } else if (!memcmp(item->type, "uri ", 4)) {
            // utf8string item_uri_type;
            return AVIF_RESULT_NOT_IMPLEMENTED;
        }
        avifRWStreamFinishBox(&s, infe);
    }

    avifRWStreamFinishBox(&s, iinf);

    // -----------------------------------------------------------------------
    // Write iref boxes

    avifBoxMarker iref = 0;
    for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
        avifEncoderItem * item = &encoder->data->items.item[itemIndex];

        // Count how many other items refer to this item with dimgFromID
        uint16_t dimgCount = 0;
        for (uint32_t dimgIndex = 0; dimgIndex < encoder->data->items.count; ++dimgIndex) {
            avifEncoderItem * dimgItem = &encoder->data->items.item[dimgIndex];
            if (dimgItem->dimgFromID == item->id) {
                ++dimgCount;
            }
        }

        if (dimgCount > 0) {
            if (!iref) {
                AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "iref", AVIF_BOX_SIZE_TBD, 0, 0, &iref));
            }
            avifBoxMarker refType;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "dimg", AVIF_BOX_SIZE_TBD, &refType));
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, item->id));  // unsigned int(16) from_item_ID;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, dimgCount)); // unsigned int(16) reference_count;
            for (uint32_t dimgIndex = 0; dimgIndex < encoder->data->items.count; ++dimgIndex) {
                avifEncoderItem * dimgItem = &encoder->data->items.item[dimgIndex];
                if (dimgItem->dimgFromID == item->id) {
                    AVIF_CHECKRES(avifRWStreamWriteU16(&s, dimgItem->id)); // unsigned int(16) to_item_ID;
                }
            }
            avifRWStreamFinishBox(&s, refType);
        }

        if (item->irefToID != 0) {
            if (!iref) {
                AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "iref", AVIF_BOX_SIZE_TBD, 0, 0, &iref));
            }
            avifBoxMarker refType;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, item->irefType, AVIF_BOX_SIZE_TBD, &refType));
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, item->id));       // unsigned int(16) from_item_ID;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 1));              // unsigned int(16) reference_count;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, item->irefToID)); // unsigned int(16) to_item_ID;
            avifRWStreamFinishBox(&s, refType);
        }
    }
    if (iref) {
        avifRWStreamFinishBox(&s, iref);
    }

    // -----------------------------------------------------------------------
    // Write iprp -> ipco/ipma

    avifBoxMarker iprp;
    AVIF_CHECKRES(avifRWStreamWriteBox(&s, "iprp", AVIF_BOX_SIZE_TBD, &iprp));

    avifItemPropertyDedup * dedup = avifItemPropertyDedupCreate();
    AVIF_CHECKERR(dedup != NULL, AVIF_RESULT_OUT_OF_MEMORY);
    avifBoxMarker ipco;
    AVIF_CHECKRES(avifRWStreamWriteBox(&s, "ipco", AVIF_BOX_SIZE_TBD, &ipco));
    avifImage * altImageMetadata = NULL;
    altImageMetadata = encoder->data->altImageMetadata;
    avifResult result = avifRWStreamWriteProperties(dedup, &s, encoder, imageMetadata, altImageMetadata);
    avifItemPropertyDedupDestroy(dedup);
    AVIF_CHECKRES(result);
    avifRWStreamFinishBox(&s, ipco);
    dedup = NULL;

    avifBoxMarker ipma;
    AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "ipma", AVIF_BOX_SIZE_TBD, 0, 0, &ipma));
    {
        uint32_t ipmaCount = 0;
        for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
            avifEncoderItem * item = &encoder->data->items.item[itemIndex];
            if (item->associations.count > 0) {
                AVIF_CHECKERR(ipmaCount < UINT32_MAX, AVIF_RESULT_INVALID_ARGUMENT);
                ++ipmaCount;
            }
        }
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, ipmaCount)); // unsigned int(32) entry_count;

        for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
            avifEncoderItem * item = &encoder->data->items.item[itemIndex];
            if (item->associations.count == 0) {
                continue;
            }

            AVIF_CHECKRES(avifRWStreamWriteU16(&s, item->id)); // unsigned int(16) item_ID;
            AVIF_ASSERT_OR_RETURN(item->associations.count < (1 << 8));
            AVIF_CHECKRES(avifRWStreamWriteU8(&s, (uint8_t)item->associations.count)); // unsigned int(8) association_count;
            for (uint32_t i = 0; i < item->associations.count; ++i) {
                const avifItemPropertyAssociation * association = &item->associations.association[i];
                AVIF_CHECKRES(avifRWStreamWriteBits(&s, association->essential ? 1 : 0, /*bitCount=*/1)); // bit(1) essential;
                AVIF_ASSERT_OR_RETURN(association->property_index <= MAX_PROPERTY_INDEX);
                AVIF_CHECKRES(avifRWStreamWriteBits(&s, association->property_index, /*bitCount=*/7)); // unsigned int(7) property_index;
            }
        }
    }
    avifRWStreamFinishBox(&s, ipma);

    avifRWStreamFinishBox(&s, iprp);

    // -----------------------------------------------------------------------
    // Write grpl/altr box

    if (encoder->data->alternativeItemIDs.count) {
        // Section 8.18.3.3 of ISO 14496-12 (ISOBMFF) says:
        //   group_id is a non-negative integer assigned to the particular grouping that shall not be equal to any
        //   group_id value of any other EntityToGroupBox, any item_ID value of the hierarchy level
        //   (file, movie. or track) that contains the GroupsListBox, or any track_ID value (when the
        //   GroupsListBox is contained in the file level).
        AVIF_ASSERT_OR_RETURN(encoder->data->lastItemID < UINT16_MAX);
        ++encoder->data->lastItemID;
        const uint32_t groupID = encoder->data->lastItemID;
        AVIF_CHECKRES(avifWriteAltrGroup(&s, groupID, &encoder->data->alternativeItemIDs));
    }

    // -----------------------------------------------------------------------
    // Finish meta box

    avifRWStreamFinishBox(&s, meta);

    // -----------------------------------------------------------------------
    // Write tracks (if an image sequence)

    if (isSequence) {
        static const uint8_t unityMatrix[9][4] = {
            /* clang-format off */
            { 0x00, 0x01, 0x00, 0x00 },
            { 0 },
            { 0 },
            { 0 },
            { 0x00, 0x01, 0x00, 0x00 },
            { 0 },
            { 0 },
            { 0 },
            { 0x40, 0x00, 0x00, 0x00 }
            /* clang-format on */
        };

        if (encoder->repetitionCount < 0 && encoder->repetitionCount != AVIF_REPETITION_COUNT_INFINITE) {
            return AVIF_RESULT_INVALID_ARGUMENT;
        }

        uint64_t framesDurationInTimescales = 0;
        for (uint32_t frameIndex = 0; frameIndex < encoder->data->frames.count; ++frameIndex) {
            const avifEncoderFrame * frame = &encoder->data->frames.frame[frameIndex];
            framesDurationInTimescales += frame->durationInTimescales;
        }
        uint64_t durationInTimescales;
        if (encoder->repetitionCount == AVIF_REPETITION_COUNT_INFINITE) {
            durationInTimescales = AVIF_INDEFINITE_DURATION64;
        } else {
            uint64_t loopCount = encoder->repetitionCount + 1;
            AVIF_ASSERT_OR_RETURN(framesDurationInTimescales != 0);
            if (loopCount > UINT64_MAX / framesDurationInTimescales) {
                // The multiplication will overflow uint64_t.
                return AVIF_RESULT_INVALID_ARGUMENT;
            }
            durationInTimescales = framesDurationInTimescales * loopCount;
        }

        // -------------------------------------------------------------------
        // Start moov

        avifBoxMarker moov;
        AVIF_CHECKRES(avifRWStreamWriteBox(&s, "moov", AVIF_BOX_SIZE_TBD, &moov));

        avifBoxMarker mvhd;
        AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "mvhd", AVIF_BOX_SIZE_TBD, 1, 0, &mvhd));
        AVIF_CHECKRES(avifRWStreamWriteU64(&s, now));                          // unsigned int(64) creation_time;
        AVIF_CHECKRES(avifRWStreamWriteU64(&s, now));                          // unsigned int(64) modification_time;
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)encoder->timescale)); // unsigned int(32) timescale;
        AVIF_CHECKRES(avifRWStreamWriteU64(&s, durationInTimescales));         // unsigned int(64) duration;
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0x00010000)); // template int(32) rate = 0x00010000; // typically 1.0
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0x0100));     // template int(16) volume = 0x0100; // typically, full volume
        AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));          // const bit(16) reserved = 0;
        AVIF_CHECKRES(avifRWStreamWriteZeros(&s, 8));        // const unsigned int(32)[2] reserved = 0;
        AVIF_CHECKRES(avifRWStreamWrite(&s, unityMatrix, sizeof(unityMatrix)));
        AVIF_CHECKRES(avifRWStreamWriteZeros(&s, 24));                       // bit(32)[6] pre_defined = 0;
        AVIF_CHECKRES(avifRWStreamWriteU32(&s, encoder->data->items.count)); // unsigned int(32) next_track_ID;
        avifRWStreamFinishBox(&s, mvhd);

        // -------------------------------------------------------------------
        // Write tracks

        for (uint32_t itemIndex = 0; itemIndex < encoder->data->items.count; ++itemIndex) {
            avifEncoderItem * item = &encoder->data->items.item[itemIndex];
            if (item->encodeOutput->samples.count == 0) {
                continue;
            }

            uint32_t syncSamplesCount = 0;
            for (uint32_t sampleIndex = 0; sampleIndex < item->encodeOutput->samples.count; ++sampleIndex) {
                avifEncodeSample * sample = &item->encodeOutput->samples.sample[sampleIndex];
                if (sample->sync) {
                    ++syncSamplesCount;
                }
            }

            avifBoxMarker trak;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "trak", AVIF_BOX_SIZE_TBD, &trak));

            avifBoxMarker tkhd;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "tkhd", AVIF_BOX_SIZE_TBD, 1, 1, &tkhd));
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, now));                    // unsigned int(64) creation_time;
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, now));                    // unsigned int(64) modification_time;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, itemIndex + 1));          // unsigned int(32) track_ID;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0));                      // const unsigned int(32) reserved = 0;
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, durationInTimescales));   // unsigned int(64) duration;
            AVIF_CHECKRES(avifRWStreamWriteZeros(&s, sizeof(uint32_t) * 2)); // const unsigned int(32)[2] reserved = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));                      // template int(16) layer = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));                      // template int(16) alternate_group = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0)); // template int(16) volume = {if track_is_audio 0x0100 else 0};
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0)); // const unsigned int(16) reserved = 0;
            AVIF_CHECKRES(avifRWStreamWrite(&s, unityMatrix, sizeof(unityMatrix))); // template int(32)[9] matrix= // { 0x00010000,0,0,0,0x00010000,0,0,0,0x40000000 };
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, imageMetadata->width << 16));  // unsigned int(32) width;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, imageMetadata->height << 16)); // unsigned int(32) height;
            avifRWStreamFinishBox(&s, tkhd);

            if (item->irefToID != 0) {
                avifBoxMarker tref;
                AVIF_CHECKRES(avifRWStreamWriteBox(&s, "tref", AVIF_BOX_SIZE_TBD, &tref));
                avifBoxMarker refType;
                AVIF_CHECKRES(avifRWStreamWriteBox(&s, item->irefType, AVIF_BOX_SIZE_TBD, &refType));
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)item->irefToID));
                avifRWStreamFinishBox(&s, refType);
                avifRWStreamFinishBox(&s, tref);
            }

            avifBoxMarker edts;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "edts", AVIF_BOX_SIZE_TBD, &edts));
            uint32_t elstFlags = (encoder->repetitionCount != 0);
            avifBoxMarker elst;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "elst", AVIF_BOX_SIZE_TBD, 1, elstFlags, &elst));
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1));                          // unsigned int(32) entry_count;
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, framesDurationInTimescales)); // unsigned int(64) segment_duration;
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, 0));                          // int(64) media_time;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 1));                          // int(16) media_rate_integer;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));                          // int(16) media_rate_fraction = 0;
            avifRWStreamFinishBox(&s, elst);
            avifRWStreamFinishBox(&s, edts);

            if (item->itemCategory != AVIF_ITEM_ALPHA) {
                AVIF_CHECKRES(avifEncoderWriteTrackMetaBox(encoder, &s));
            }

            avifBoxMarker mdia;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "mdia", AVIF_BOX_SIZE_TBD, &mdia));

            avifBoxMarker mdhd;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "mdhd", AVIF_BOX_SIZE_TBD, 1, 0, &mdhd));
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, now));                          // unsigned int(64) creation_time;
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, now));                          // unsigned int(64) modification_time;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)encoder->timescale)); // unsigned int(32) timescale;
            AVIF_CHECKRES(avifRWStreamWriteU64(&s, framesDurationInTimescales));   // unsigned int(64) duration;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 21956)); // bit(1) pad = 0; unsigned int(5)[3] language; ("und")
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));     // unsigned int(16) pre_defined = 0;
            avifRWStreamFinishBox(&s, mdhd);

            AVIF_CHECKRES(avifRWStreamWriteHandlerBox(&s, (item->itemCategory == AVIF_ITEM_ALPHA) ? "auxv" : "pict"));

            avifBoxMarker minf;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "minf", AVIF_BOX_SIZE_TBD, &minf));

            avifBoxMarker vmhd;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "vmhd", AVIF_BOX_SIZE_TBD, 0, 1, &vmhd));
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0)); // template unsigned int(16) graphicsmode = 0; (copy over the existing image)
            AVIF_CHECKRES(avifRWStreamWriteZeros(&s, 6)); // template unsigned int(16)[3] opcolor = {0, 0, 0};
            avifRWStreamFinishBox(&s, vmhd);

            avifBoxMarker dinf;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "dinf", AVIF_BOX_SIZE_TBD, &dinf));
            avifBoxMarker dref;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "dref", AVIF_BOX_SIZE_TBD, 0, 0, &dref));
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1)); // unsigned int(32) entry_count;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "url ", /*contentSize=*/0, 0, 1, /*marker=*/NULL)); // flags:1 means data is in this file
            avifRWStreamFinishBox(&s, dref);
            avifRWStreamFinishBox(&s, dinf);

            // The boxes within the "stbl" box are ordered using the following recommendation in ISO/IEC 14496-12, Section 6.2.3:
            // 4) It is recommended that the boxes within the Sample Table Box be in the following order: Sample Description
            // (stsd), Time to Sample (stts), Sample to Chunk (stsc), Sample Size (stsz), Chunk Offset (stco).
            //
            // Any boxes not listed in the above line are placed in the end (after the "stco" box).
            avifBoxMarker stbl;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, "stbl", AVIF_BOX_SIZE_TBD, &stbl));

            avifBoxMarker stsd;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "stsd", AVIF_BOX_SIZE_TBD, 0, 0, &stsd));
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1)); // unsigned int(32) entry_count;
            avifBoxMarker imageItem;
            AVIF_CHECKRES(avifRWStreamWriteBox(&s, encoder->data->imageItemType, AVIF_BOX_SIZE_TBD, &imageItem));
            AVIF_CHECKRES(avifRWStreamWriteZeros(&s, 6));                             // const unsigned int(8)[6] reserved = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 1));                               // unsigned int(16) data_reference_index;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));                               // unsigned int(16) pre_defined = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0));                               // const unsigned int(16) reserved = 0;
            AVIF_CHECKRES(avifRWStreamWriteZeros(&s, sizeof(uint32_t) * 3));          // unsigned int(32)[3] pre_defined = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, (uint16_t)imageMetadata->width));  // unsigned int(16) width;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, (uint16_t)imageMetadata->height)); // unsigned int(16) height;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0x00480000));                      // template unsigned int(32) horizresolution
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0x00480000));                      // template unsigned int(32) vertresolution
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0));                               // const unsigned int(32) reserved = 0;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 1));                      // template unsigned int(16) frame_count = 1;
            AVIF_CHECKRES(avifRWStreamWriteChars(&s, "\012AOM Coding", 11)); // string[32] compressorname;
            AVIF_CHECKRES(avifRWStreamWriteZeros(&s, 32 - 11));              //
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, 0x0018));                 // template unsigned int(16) depth = 0x0018;
            AVIF_CHECKRES(avifRWStreamWriteU16(&s, (uint16_t)0xffff));       // int(16) pre_defined = -1;
            AVIF_CHECKRES(writeConfigBox(&s, &item->av1C, encoder->data->configPropName));
            if (item->itemCategory == AVIF_ITEM_COLOR) {
                AVIF_CHECKRES(avifEncoderWriteColorProperties(&s, imageMetadata, NULL, NULL));
                AVIF_CHECKRES(avifEncoderWriteHDRProperties(NULL, &s, imageMetadata, NULL, NULL));
                AVIF_CHECKRES(avifEncoderWriteTransformativeProperties(NULL, &s, imageMetadata, NULL, NULL));
            }

            avifBoxMarker ccst;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "ccst", AVIF_BOX_SIZE_TBD, 0, 0, &ccst));
            AVIF_CHECKRES(avifRWStreamWriteBits(&s, 0, /*bitCount=*/1));  // unsigned int(1) all_ref_pics_intra;
            AVIF_CHECKRES(avifRWStreamWriteBits(&s, 1, /*bitCount=*/1));  // unsigned int(1) intra_pred_used;
            AVIF_CHECKRES(avifRWStreamWriteBits(&s, 15, /*bitCount=*/4)); // unsigned int(4) max_ref_per_pic;
            AVIF_CHECKRES(avifRWStreamWriteBits(&s, 0, /*bitCount=*/26)); // unsigned int(26) reserved;
            avifRWStreamFinishBox(&s, ccst);

            if (item->itemCategory == AVIF_ITEM_ALPHA) {
                avifBoxMarker auxi;
                AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "auxi", AVIF_BOX_SIZE_TBD, 0, 0, &auxi));
                AVIF_CHECKRES(avifRWStreamWriteChars(&s, alphaURN, alphaURNSize)); //  string aux_track_type;
                avifRWStreamFinishBox(&s, auxi);
            }

            avifRWStreamFinishBox(&s, imageItem);
            avifRWStreamFinishBox(&s, stsd);

            avifBoxMarker stts;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "stts", AVIF_BOX_SIZE_TBD, 0, 0, &stts));
            size_t sttsEntryCountOffset = avifRWStreamOffset(&s);
            uint32_t sttsEntryCount = 0;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0)); // unsigned int(32) entry_count;
            for (uint32_t sampleCount = 0, frameIndex = 0; frameIndex < encoder->data->frames.count; ++frameIndex) {
                avifEncoderFrame * frame = &encoder->data->frames.frame[frameIndex];
                ++sampleCount;
                if (frameIndex < (encoder->data->frames.count - 1)) {
                    avifEncoderFrame * nextFrame = &encoder->data->frames.frame[frameIndex + 1];
                    if (frame->durationInTimescales == nextFrame->durationInTimescales) {
                        continue;
                    }
                }
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, sampleCount));                           // unsigned int(32) sample_count;
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)frame->durationInTimescales)); // unsigned int(32) sample_delta;
                sampleCount = 0;
                ++sttsEntryCount;
            }
            size_t prevOffset = avifRWStreamOffset(&s);
            avifRWStreamSetOffset(&s, sttsEntryCountOffset);
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, sttsEntryCount));
            avifRWStreamSetOffset(&s, prevOffset);
            avifRWStreamFinishBox(&s, stts);

            avifBoxMarker stsc;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "stsc", AVIF_BOX_SIZE_TBD, 0, 0, &stsc));
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1));                                 // unsigned int(32) entry_count;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1));                                 // unsigned int(32) first_chunk;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, item->encodeOutput->samples.count)); // unsigned int(32) samples_per_chunk;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1)); // unsigned int(32) sample_description_index;
            avifRWStreamFinishBox(&s, stsc);

            avifBoxMarker stsz;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "stsz", AVIF_BOX_SIZE_TBD, 0, 0, &stsz));
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 0));                                 // unsigned int(32) sample_size;
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, item->encodeOutput->samples.count)); // unsigned int(32) sample_count;
            for (uint32_t sampleIndex = 0; sampleIndex < item->encodeOutput->samples.count; ++sampleIndex) {
                avifEncodeSample * sample = &item->encodeOutput->samples.sample[sampleIndex];
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, (uint32_t)sample->data.size)); // unsigned int(32) entry_size;
            }
            avifRWStreamFinishBox(&s, stsz);

            avifBoxMarker stco;
            AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "stco", AVIF_BOX_SIZE_TBD, 0, 0, &stco));
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1));           // unsigned int(32) entry_count;
            AVIF_CHECKRES(avifEncoderItemAddMdatFixup(item, &s)); //
            AVIF_CHECKRES(avifRWStreamWriteU32(&s, 1));           // unsigned int(32) chunk_offset; (set later)
            avifRWStreamFinishBox(&s, stco);

            avifBool hasNonSyncSample = AVIF_FALSE;
            for (uint32_t sampleIndex = 0; sampleIndex < item->encodeOutput->samples.count; ++sampleIndex) {
                if (!item->encodeOutput->samples.sample[sampleIndex].sync) {
                    hasNonSyncSample = AVIF_TRUE;
                    break;
                }
            }
            // ISO/IEC 14496-12, Section 8.6.2.1:
            //   If the SyncSampleBox is not present, every sample is a sync sample.
            if (hasNonSyncSample) {
                avifBoxMarker stss;
                AVIF_CHECKRES(avifRWStreamWriteFullBox(&s, "stss", AVIF_BOX_SIZE_TBD, 0, 0, &stss));
                AVIF_CHECKRES(avifRWStreamWriteU32(&s, syncSamplesCount)); // unsigned int(32) entry_count;
                for (uint32_t sampleIndex = 0; sampleIndex < item->encodeOutput->samples.count; ++sampleIndex) {
                    avifEncodeSample * sample = &item->encodeOutput->samples.sample[sampleIndex];
                    if (sample->sync) {
                        AVIF_CHECKRES(avifRWStreamWriteU32(&s, sampleIndex + 1)); // unsigned int(32) sample_number;
                    }
                }
                avifRWStreamFinishBox(&s, stss);
            }

            avifRWStreamFinishBox(&s, stbl);

            avifRWStreamFinishBox(&s, minf);
            avifRWStreamFinishBox(&s, mdia);
            avifRWStreamFinishBox(&s, trak);
        }

        // -------------------------------------------------------------------
        // Finish moov box

        avifRWStreamFinishBox(&s, moov);
    }

    // -----------------------------------------------------------------------
    // Write mdat

    avifEncoderItemReferenceArray layeredColorItems;
    avifEncoderItemReferenceArray layeredAlphaItems;
    if (!avifArrayCreate(&layeredColorItems, sizeof(avifEncoderItemReference), 1)) {
        result = AVIF_RESULT_OUT_OF_MEMORY;
    }
    if (!avifArrayCreate(&layeredAlphaItems, sizeof(avifEncoderItemReference), 1)) {
        result = AVIF_RESULT_OUT_OF_MEMORY;
    }
    if (result == AVIF_RESULT_OK) {
        result = avifEncoderWriteMediaDataBox(encoder, &s, &layeredColorItems, &layeredAlphaItems);
    }
    avifArrayDestroy(&layeredColorItems);
    avifArrayDestroy(&layeredAlphaItems);
    AVIF_CHECKRES(result);

    // -----------------------------------------------------------------------
    // Finish up stream

    avifRWStreamFinishWrite(&s);

#if defined(AVIF_ENABLE_COMPLIANCE_WARDEN)
    AVIF_CHECKRES(avifIsCompliant(output->data, output->size));
#endif

    return AVIF_RESULT_OK;
}

avifResult avifEncoderWrite(avifEncoder * encoder, const avifImage * image, avifRWData * output)
{
    avifResult addImageResult = avifEncoderAddImage(encoder, image, 1, AVIF_ADD_IMAGE_FLAG_SINGLE);
    if (addImageResult != AVIF_RESULT_OK) {
        return addImageResult;
    }
    return avifEncoderFinish(encoder, output);
}

// Implementation of section 2.3.3 of AV1 Codec ISO Media File Format Binding specification v1.2.0.
// See https://aomediacodec.github.io/av1-isobmff/v1.2.0.html#av1codecconfigurationbox-syntax.
static avifResult writeCodecConfig(avifRWStream * s, const avifCodecConfigurationBox * cfg)
{
    const size_t av1COffset = s->offset;

    AVIF_CHECKRES(avifRWStreamWriteBits(s, 1, /*bitCount=*/1)); // unsigned int (1) marker = 1;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 1, /*bitCount=*/7)); // unsigned int (7) version = 1;

    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->seqProfile, /*bitCount=*/3));   // unsigned int (3) seq_profile;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->seqLevelIdx0, /*bitCount=*/5)); // unsigned int (5) seq_level_idx_0;

    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->seqTier0, /*bitCount=*/1));             // unsigned int (1) seq_tier_0;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->highBitdepth, /*bitCount=*/1));         // unsigned int (1) high_bitdepth;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->twelveBit, /*bitCount=*/1));            // unsigned int (1) twelve_bit;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->monochrome, /*bitCount=*/1));           // unsigned int (1) monochrome;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->chromaSubsamplingX, /*bitCount=*/1));   // unsigned int (1) chroma_subsampling_x;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->chromaSubsamplingY, /*bitCount=*/1));   // unsigned int (1) chroma_subsampling_y;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, cfg->chromaSamplePosition, /*bitCount=*/2)); // unsigned int (2) chroma_sample_position;

    AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, /*bitCount=*/3)); // unsigned int (3) reserved = 0;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, /*bitCount=*/1)); // unsigned int (1) initial_presentation_delay_present;
    AVIF_CHECKRES(avifRWStreamWriteBits(s, 0, /*bitCount=*/4)); // unsigned int (4) reserved = 0;

    // According to section 2.2.1 of AV1 Image File Format specification v1.1.0,
    // there is no need to write any OBU here.
    // See https://aomediacodec.github.io/av1-avif/v1.1.0.html#av1-configuration-item-property.
    // unsigned int (8) configOBUs[];

    AVIF_ASSERT_OR_RETURN(s->offset - av1COffset == 4); // Make sure writeCodecConfig() writes exactly 4 bytes.
    return AVIF_RESULT_OK;
}

static avifResult writeConfigBox(avifRWStream * s, const avifCodecConfigurationBox * cfg, const char * configPropName)
{
    avifBoxMarker configBox;
    AVIF_CHECKRES(avifRWStreamWriteBox(s, configPropName, AVIF_BOX_SIZE_TBD, &configBox));
    AVIF_CHECKRES(writeCodecConfig(s, cfg));
    avifRWStreamFinishBox(s, configBox);
    return AVIF_RESULT_OK;
}
