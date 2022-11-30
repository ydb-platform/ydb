#include "codec.h"

#include <contrib/libs/xdelta3/xdelta3.h>
#include <contrib/libs/xdelta3/xdelta3-internal.h>

#include <util/system/types.h>

#include <arpa/inet.h>

#include <stdlib.h>
#include <string.h>

#define IOPT_SIZE 100

#ifndef MAX
    #define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif


void* xdelta3_buffer_alloc(XDeltaContext* context, size_t items, usize_t size);

void xdelta3_buffer_free(XDeltaContext* context, void* ptr);

void init_common_config(XDeltaContext* context, xd3_config* config, usize_t iopt_size);

int merge_vcdiff_patches(
    XDeltaContext* context,
    const ui8* patch1,
    size_t patch_size1,
    const ui8* patch2,
    size_t patch_size2,
    ui8* result,
    size_t* result_size,
    size_t max_result_size);

int ProcessMemory(
    int isEncode,
    int (*func)(xd3_stream*),
    const ui8* input,
    usize_t inputSize,
    const ui8* source,
    usize_t sourceSize,
    ui8* output,
    usize_t* outputSize,
    usize_t outputSizeMax)
{
    xd3_stream stream;
    xd3_config config;
    xd3_source src;
    int ret;

    memset(&stream, 0, sizeof(stream));
    xd3_init_config(&config, XD3_NOCOMPRESS);

    if (isEncode) {
        config.winsize = inputSize;
        config.sprevsz = xd3_pow2_roundup(config.winsize);
    }

    init_common_config(NULL, &config, IOPT_SIZE); // IOPT_SIZE - key option drastically increased performance

    if ((ret = xd3_config_stream(&stream, &config)) == 0) {
        if (source != NULL || sourceSize == 0) {
            memset(&src, 0, sizeof(src));

            src.blksize = sourceSize;
            src.onblk = sourceSize;
            src.curblk = source;
            src.curblkno = 0;
            src.max_winsize = sourceSize;

            if ((ret = xd3_set_source_and_size(&stream, &src, sourceSize)) == 0) {
                ret = xd3_process_stream(
                    isEncode,
                    &stream,
                    func,
                    1,
                    input,
                    inputSize,
                    output,
                    outputSize,
                    outputSizeMax);
            }
        }
    }
    xd3_free_stream(&stream);
    return ret;
}

ui8* ComputePatch(
    XDeltaContext* context,
    const ui8* from,
    size_t fromSize,
    const ui8* to,
    size_t toSize,
    size_t* patchSize)
{
    *patchSize = 0;

    size_t maxInputSize = MAX(toSize, fromSize);
    size_t deltaSize = MAX(maxInputSize, 200u) * 1.5; // NOTE: for small data N * 1.5 does not work e.g. data 10 & 10 -> patch 31

    ui8* delta = (ui8*)xdelta3_buffer_alloc(context, deltaSize, 1);
    if (delta == NULL) {
        return NULL;
    }

    usize_t outSize = 0;
    int ret = ProcessMemory(
        1,
        &xd3_encode_input,
        to,
        toSize,
        from,
        fromSize,
        delta,
        &outSize,
        deltaSize);

    if (ret != 0) {
        xdelta3_buffer_free(context, delta);
        return NULL;
    }

    *patchSize = outSize;
    return delta;
}

ui8* ApplyPatch(
    XDeltaContext* context,
    size_t headroomSize,
    const ui8* base,
    size_t baseSize,
    const ui8* patch,
    size_t patchSize,
    size_t stateSize,
    size_t* resultSize)
{
    *resultSize = 0;

    ui8* buffer = (ui8*)xdelta3_buffer_alloc(context, headroomSize + stateSize, 1);
    if (buffer == NULL) {
        return NULL;
    }

    size_t decodedSize = 0;
    int ret = ProcessMemory(
        0,
        &xd3_decode_input,
        patch,
        patchSize,
        base,
        baseSize,
        buffer + headroomSize,
        &decodedSize,
        stateSize);

    if (ret != 0) {
        xdelta3_buffer_free(context, buffer);
        return NULL;
    }

    *resultSize = decodedSize;
    return buffer;
}

ui8* MergePatches(
    XDeltaContext* context,
    size_t headroomSize,
    const ui8* patch1,
    size_t patch1Size,
    const ui8* patch2,
    size_t patch2Size,
    size_t* patch3Size)
{
    *patch3Size = 0;

    size_t maxResultSize = headroomSize + 3 * (patch1Size + patch2Size); // estmation could be more accurate

    ui8* result = (ui8*)xdelta3_buffer_alloc(context, maxResultSize, 1);
    size_t resultSize = 0;
    int ret = merge_vcdiff_patches(
        NULL,
        patch1,
        patch1Size,
        patch2,
        patch2Size,
        result + headroomSize,
        &resultSize,
        maxResultSize - headroomSize);

    if (ret != 0) {
        xdelta3_buffer_free(context, result);
        return NULL;
    }

    *patch3Size = resultSize;
    return result;
}
