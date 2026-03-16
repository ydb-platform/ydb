/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_api_internal.h"

#if HAVE_LIBOPENJPEG

#include "openjpeg.h"

// The older versions did not have the opj_config.h file
// So we use a more recent macro to detect whether it is there.
// Also see https://github.com/uclouvain/openjpeg/issues/1514
#if defined(OPJ_IMG_INFO)
    #include "opj_config.h"
#endif

static void openjpeg_warning(const char* msg, void* client_data)
{
    grib_context_log((grib_context*)client_data, GRIB_LOG_WARNING, "openjpeg: %s", msg);
}

static void openjpeg_error(const char* msg, void* client_data)
{
    grib_context_log((grib_context*)client_data, GRIB_LOG_ERROR, "openjpeg: %s", msg);
}

static void openjpeg_info(const char* msg, void* client_data)
{
    /* grib_context_log((grib_context*)client_data,GRIB_LOG_INFO,"openjpeg: %s",msg); */
}

/* Note: The old OpenJPEG versions (e.g. v1.5.2) did not have this macro.
 * From OpenJPEG v2.1.0 onwards there is the macro OPJ_VERSION_MAJOR */
#if !defined(OPJ_VERSION_MAJOR) /* The old interface */

int grib_openjpeg_encode(grib_context* c, j2k_encode_helper* helper)
{
    int err            = GRIB_SUCCESS;
    const int numcomps = 1;

    int i;

    const double* values   = helper->values;
    long no_values         = helper->no_values;
    double reference_value = helper->reference_value;
    double divisor         = helper->divisor;
    double decimal         = helper->decimal;
    int* data;

    opj_cparameters_t parameters = {0,}; /* compression parameters */
    opj_event_mgr_t event_mgr = {0,}; /* event manager */
    opj_image_t* image            = NULL;
    opj_image_cmptparm_t cmptparm = {0,};
    opj_cio_t* cio     = NULL;
    opj_cinfo_t* cinfo = NULL;


    /* set encoding parameters to default values */
    opj_set_default_encoder_parameters(&parameters);

    grib_context_log(c, GRIB_LOG_DEBUG, "grib_openjpeg_encode: OpenJPEG version %s", opj_version());

    parameters.tcp_numlayers  = 1;
    parameters.cp_disto_alloc = 1;
    parameters.tcp_rates[0]   = helper->compression;

    /* initialize image component */
    cmptparm.prec = helper->bits_per_value;
    cmptparm.bpp  = helper->bits_per_value; /* Not sure about this one and the previous. What is the difference? */
    cmptparm.sgnd = 0;
    cmptparm.dx   = 1;
    cmptparm.dy   = 1;
    cmptparm.w    = helper->width;
    cmptparm.h    = helper->height;

    /* create the image */
    image = opj_image_create(numcomps, &cmptparm, CLRSPC_GRAY);

    if (!image) {
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }

    image->x0 = 0;
    image->y0 = 0;
    image->x1 = helper->width;
    image->y1 = helper->height;

    ECCODES_ASSERT(cmptparm.prec <= sizeof(image->comps[0].data[0]) * 8 - 1); /* BR: -1 because I don't know what happens if the sign bit is set */

    ECCODES_ASSERT(helper->no_values == image->comps[0].h * image->comps[0].w);

    /* Simple packing */
    data = image->comps[0].data;
    for (i = 0; i < no_values; i++) {
        unsigned long unsigned_val = (unsigned long)((((values[i] * decimal) - (reference_value)) * divisor) + 0.5);
        data[i]                    = unsigned_val;
    }

    /* get a J2K compressor handle */
    cinfo = opj_create_compress(CODEC_J2K);

    /* catch events using our callbacks and give a local context */
    event_mgr.error_handler   = openjpeg_error;
    event_mgr.info_handler    = openjpeg_info;
    event_mgr.warning_handler = openjpeg_warning;
    opj_set_event_mgr((opj_common_ptr)cinfo, &event_mgr, c);

    /* setup the encoder parameters using the current image and user parameters */
    opj_setup_encoder(cinfo, &parameters, image);

    /* open a byte stream for writing */
    /* allocate memory for all tiles */
    cio = opj_cio_open((opj_common_ptr)cinfo, NULL, 0);

    /* encode image */
    if (!opj_encode(cinfo, cio, image, NULL)) {
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }

    helper->jpeg_length = cio_tell(cio);
    memcpy(helper->jpeg_buffer, cio->buffer, helper->jpeg_length);

cleanup:
    if (cio)
        opj_cio_close(cio);
    if (cinfo)
        opj_destroy_compress(cinfo);
    if (image)
        opj_image_destroy(image);

    return err;
}

int grib_openjpeg_decode(grib_context* c, unsigned char* buf, const size_t* buflen, double* val, const size_t* n_vals)
{
    int err = GRIB_SUCCESS;
    int i;
    unsigned long mask;
    int* data;
    size_t count;

    opj_dparameters_t parameters = {0,};   /* decompression parameters */
    opj_dinfo_t* dinfo        = NULL; /* handle to a decompressor */
    opj_event_mgr_t event_mgr = {0,}; /* event manager */
    opj_cio_t* cio        = NULL;
    opj_image_t* image    = NULL;
    opj_image_comp_t comp = {0,};

    /* set decoding parameters to default values */
    opj_set_default_decoder_parameters(&parameters);

    /* JPEG-2000 codestream */
    grib_context_log(c, GRIB_LOG_DEBUG, "grib_openjpeg_decode: OpenJPEG version %s", opj_version());

    /* get a decoder handle */
    dinfo = opj_create_decompress(CODEC_J2K);

    /* catch events using our callbacks and give a local context */
    event_mgr.error_handler   = openjpeg_error;
    event_mgr.info_handler    = openjpeg_info;
    event_mgr.warning_handler = openjpeg_warning;

    opj_set_event_mgr((opj_common_ptr)dinfo, &event_mgr, c);

    /* setup the decoder decoding parameters using user parameters */
    opj_setup_decoder(dinfo, &parameters);

    /* open a byte stream */
    cio = opj_cio_open((opj_common_ptr)dinfo, buf, *buflen);

    image = opj_decode(dinfo, cio);

    if (!image) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed to decode image");
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    if (!(*n_vals <= image->comps[0].w * image->comps[0].h)) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    if ((image->numcomps != 1) || !(image->x1 * image->y1)) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    ECCODES_ASSERT(image->comps[0].sgnd == 0);
    ECCODES_ASSERT(comp.prec <= sizeof(image->comps[0].data[0]) * 8 - 1); /* BR: -1 because I don't know what happens if the sign bit is set */

    ECCODES_ASSERT(image->comps[0].prec < sizeof(mask) * 8 - 1);

    data = image->comps[0].data;
    mask = (1 << image->comps[0].prec) - 1;

    count = image->comps[0].w * image->comps[0].h;

    for (i = 0; i < count; i++)
        val[i] = data[i] & mask;

cleanup:
    /* close the byte stream */
    if (cio)
        opj_cio_close(cio);
    if (dinfo)
        opj_destroy_decompress(dinfo);
    if (image)
        opj_image_destroy(image);

    return err;
}

#else /* OPENJPEG VERSION 2 - macro OPJ_VERSION_MAJOR is defined */

/* OpenJPEG 2.1 version of grib_openjpeg_encoding.c */

/* opj_* Helper code from
 * https://groups.google.com/forum/#!topic/openjpeg/8cebr0u7JgY
 */
/* These routines are added to use memory instead of a file for input and output */
/* struct need to treat memory as a stream */
typedef struct
{
    OPJ_UINT8* pData;    /* our data */
    OPJ_SIZE_T dataSize; /* how big is our data */
    OPJ_SIZE_T offset;   /* where we are currently in our data */
    j2k_encode_helper* helper;
} opj_memory_stream;

/* This will read from our memory to the buffer */
static OPJ_SIZE_T opj_memory_stream_read(void* buffer, OPJ_SIZE_T nb_bytes, void* p_user_data)
{
    opj_memory_stream* mstream = (opj_memory_stream*)p_user_data; /* Our data */
    OPJ_SIZE_T nb_bytes_read   = nb_bytes;                        /* Amount to move to buffer */

    /* Check if the current offset is outside our data buffer */
    if (mstream->offset >= mstream->dataSize)
        return (OPJ_SIZE_T)-1;

    /* Check if we are reading more than we have */
    if (nb_bytes > (mstream->dataSize - mstream->offset))
        nb_bytes_read = mstream->dataSize - mstream->offset;

    memcpy(buffer, &(mstream->pData[mstream->offset]), nb_bytes_read);
    mstream->offset += nb_bytes_read; /* Update the pointer to the new location */
    return nb_bytes_read;
}

/* Write from the buffer to our memory */
static OPJ_SIZE_T opj_memory_stream_write(void* buffer, OPJ_SIZE_T nb_bytes, void* user_data)
{
    opj_memory_stream* mstream = (opj_memory_stream*)user_data; /* our data */
    OPJ_SIZE_T nb_bytes_write  = nb_bytes;                      /* Amount to move to buffer */

    /* Check if the current offset is outside our data buffer */
    if (mstream->offset >= mstream->dataSize)
        return (OPJ_SIZE_T)-1;

    /* Check if we are writing more than we have space for */
    if (nb_bytes > (mstream->dataSize - mstream->offset))
        nb_bytes_write = mstream->dataSize - mstream->offset;

    /* Copy the data from the internal buffer */
    memcpy(&(mstream->pData[mstream->offset]), buffer, nb_bytes_write);
    mstream->offset += nb_bytes_write; /* Update the pointer to the new location */
    return nb_bytes_write;
}

/* Moves the pointer forward, but never more than we have */
static OPJ_OFF_T opj_memory_stream_skip(OPJ_OFF_T nb_bytes, void* user_data)
{
    opj_memory_stream* mstream = (opj_memory_stream*)user_data;
    OPJ_SIZE_T l_nb_bytes;

    if (nb_bytes < 0)
        return -1;                     /* No skipping backwards */
    l_nb_bytes = (OPJ_SIZE_T)nb_bytes; /* Allowed because it is positive */
    /* Do not allow jumping past the end */
    if (l_nb_bytes > mstream->dataSize - mstream->offset)
        l_nb_bytes = mstream->dataSize - mstream->offset; /* Jump the max. */
    mstream->offset += l_nb_bytes; /* Make the jump */
    return l_nb_bytes; /* Return how far we jumped */
}

/* Sets the pointer to anywhere in the memory */
static OPJ_BOOL opj_memory_stream_seek(OPJ_OFF_T nb_bytes, void* user_data)
{
    opj_memory_stream* mstream = (opj_memory_stream*)user_data;

    if (nb_bytes < 0)
        return OPJ_FALSE; /* Not before the buffer */
    if (nb_bytes > (OPJ_OFF_T)mstream->dataSize)
        return OPJ_FALSE;                   /* Not after the buffer */
    mstream->offset = (OPJ_SIZE_T)nb_bytes; /* Move to new position */
    return OPJ_TRUE;
}

static void opj_memory_stream_do_nothing(void* p_user_data)
{
    OPJ_ARG_NOT_USED(p_user_data);
}

/* Create a stream to use memory as the input or output */
static opj_stream_t* opj_stream_create_default_memory_stream(opj_memory_stream* memoryStream, OPJ_BOOL is_read_stream)
{
    opj_stream_t* stream;

    if (!(stream = opj_stream_default_create(is_read_stream)))
        return (NULL);
    /* Set how to work with the frame buffer */
    if (is_read_stream)
        opj_stream_set_read_function(stream, opj_memory_stream_read);
    else
        opj_stream_set_write_function(stream, opj_memory_stream_write);

    opj_stream_set_seek_function(stream, opj_memory_stream_seek);
    opj_stream_set_skip_function(stream, opj_memory_stream_skip);
    opj_stream_set_user_data(stream, memoryStream, opj_memory_stream_do_nothing);
    opj_stream_set_user_data_length(stream, memoryStream->dataSize);
    return stream;
}

int grib_openjpeg_encode(grib_context* c, j2k_encode_helper* helper)
{
    int err            = GRIB_SUCCESS;
    const int numcomps = 1;
    int i;

    const double* values   = helper->values;
    long no_values         = helper->no_values;
    double reference_value = helper->reference_value;
    double divisor         = helper->divisor;
    double decimal         = helper->decimal;
    int* data;

    opj_cparameters_t parameters = {0,}; /* compression parameters */
    opj_codec_t* codec            = NULL;
    opj_image_t* image            = NULL;
    opj_image_cmptparm_t cmptparm = {0,};
    opj_stream_t* stream = NULL;
    opj_memory_stream mstream;

    /* set encoding parameters to default values */
    opj_set_default_encoder_parameters(&parameters);

    grib_context_log(c, GRIB_LOG_DEBUG, "grib_openjpeg_encode: OpenJPEG version %s", opj_version());

    parameters.tcp_numlayers  = 1;
    parameters.cp_disto_alloc = 1;
    /* parameters.numresolution =  1; */
    parameters.tcp_rates[0] = helper->compression;

    /* By default numresolution = 6 (must be between 1 and 32)
     * This may be too large for some of our datasets, eg. 1xn, so adjust ...
     */
    parameters.numresolution = 6;
    while ((helper->width < (OPJ_UINT32)(1 << (parameters.numresolution - 1U))) ||
           (helper->height < (OPJ_UINT32)(1 << (parameters.numresolution - 1U)))) {
        parameters.numresolution--;
    }

    /* initialize image component */
    cmptparm.prec = helper->bits_per_value;
    cmptparm.sgnd = 0;
    cmptparm.dx   = 1;
    cmptparm.dy   = 1;
    cmptparm.w    = helper->width;
    cmptparm.h    = helper->height;

    /* create the image */
    image = opj_image_create(numcomps, &cmptparm, OPJ_CLRSPC_GRAY);
    if (!image) {
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }
    image->x0 = 0;
    image->y0 = 0;
    image->x1 = helper->width;
    image->y1 = helper->height;

    ECCODES_ASSERT(cmptparm.prec <= sizeof(image->comps[0].data[0]) * 8 - 1); /* BR: -1 because I don't know what happens if the sign bit is set */
    ECCODES_ASSERT(helper->no_values == image->comps[0].h * image->comps[0].w);

    /* Simple packing */
    data = image->comps[0].data;
    for (i = 0; i < no_values; i++) {
        unsigned long unsigned_val = (unsigned long)((((values[i] * decimal) - (reference_value)) * divisor) + 0.5);
        data[i]                    = unsigned_val;
    }

    /* get a J2K compressor handle */
    codec = opj_create_compress(OPJ_CODEC_J2K);

    opj_set_info_handler(codec, openjpeg_info, c);
    opj_set_warning_handler(codec, openjpeg_warning, c);
    opj_set_error_handler(codec, openjpeg_error, c);

    /* setup the encoder parameters using the current image and user parameters */
    if (!opj_setup_encoder(codec, &parameters, image)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed to setup encoder");
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }

    /* open a byte stream for writing */
    mstream.helper   = helper;
    mstream.pData    = (OPJ_UINT8*)helper->jpeg_buffer;
    mstream.offset   = 0;
    mstream.dataSize = helper->buffer_size;
    stream           = opj_stream_create_default_memory_stream(&mstream, OPJ_STREAM_WRITE);
    if (stream == NULL) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed create default memory stream");
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }
    if (!opj_start_compress(codec, image, stream)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed to setup encoder");
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }

    /* encode image */
    if (!opj_encode(codec, stream)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: opj_encode failed");
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }

    if (!opj_end_compress(codec, stream)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: opj_end_compress failed");
        err = GRIB_ENCODING_ERROR;
        goto cleanup;
    }
    helper->jpeg_length = mstream.offset;

cleanup:
    if (codec)
        opj_destroy_codec(codec);
    if (stream)
        opj_stream_destroy(stream);
    if (image)
        opj_image_destroy(image);

    return err;
}

int grib_openjpeg_decode(grib_context* c, unsigned char* buf, const size_t* buflen, double* val, const size_t* n_vals)
{
    int err = GRIB_SUCCESS;
    int i;
    unsigned long mask;
    int* data;
    size_t count;

    opj_dparameters_t parameters = {0,}; /* decompression parameters */
    opj_stream_t* stream = NULL;
    opj_memory_stream mstream;
    opj_image_t* image    = NULL;
    opj_codec_t* codec    = NULL;
    opj_image_comp_t comp = {0,};

    /* set decoding parameters to default values */
    opj_set_default_decoder_parameters(&parameters);
    parameters.decod_format = 1; /* JP2_FMT */

    /* JPEG-2000 codestream */
    grib_context_log(c, GRIB_LOG_DEBUG, "grib_openjpeg_decode: OpenJPEG version %s", opj_version());

    /* get a decoder handle */
    codec = opj_create_decompress(OPJ_CODEC_J2K);

    /* catch events using our callbacks and give a local context */
    opj_set_info_handler(codec, openjpeg_info, c);
    opj_set_warning_handler(codec, openjpeg_warning, c);
    opj_set_error_handler(codec, openjpeg_error, c);

    /* initialize our memory stream */
    mstream.pData    = buf;
    mstream.dataSize = *buflen;
    mstream.offset   = 0;
    /* open a byte stream from memory stream */
    stream = opj_stream_create_default_memory_stream(&mstream, OPJ_STREAM_READ);

    /* setup the decoder decoding parameters using user parameters */
    if (!opj_setup_decoder(codec, &parameters)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed to setup decoder");
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }
    if (!opj_read_header(stream, codec, &image)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed to read the header");
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }
    if (!opj_decode(codec, stream, image)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed to decode");
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    if (!(*n_vals <= image->comps[0].w * image->comps[0].h)) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }
    if ((image->numcomps != 1) || (image->x1 * image->y1) == 0) {
        err = GRIB_DECODING_ERROR;
        goto cleanup;
    }

    ECCODES_ASSERT(image->comps[0].sgnd == 0);
    ECCODES_ASSERT(comp.prec <= sizeof(image->comps[0].data[0]) * 8 - 1); /* BR: -1 because I don't know what happens if the sign bit is set */

    ECCODES_ASSERT(image->comps[0].prec < sizeof(mask) * 8 - 1);

    data = image->comps[0].data;
    mask = (1 << image->comps[0].prec) - 1;

    count = image->comps[0].w * image->comps[0].h;

    for (i = 0; i < count; i++)
        val[i] = data[i] & mask;

    if (!opj_end_decompress(codec, stream)) {
        grib_context_log(c, GRIB_LOG_ERROR, "openjpeg: failed in opj_end_decompress");
        err = GRIB_DECODING_ERROR;
    }

cleanup:
    /* close the byte stream */
    if (codec)
        opj_destroy_codec(codec);
    if (stream)
        opj_stream_destroy(stream);
    if (image)
        opj_image_destroy(image);

    return err;
}

#endif /* OPENJPEG_VERSION */

#else /* No OpenJPEG */

int grib_openjpeg_decode(grib_context* c, unsigned char* buf, const size_t* buflen, double* val, const size_t* n_vals)
{
    grib_context_log(c, GRIB_LOG_ERROR, "grib_openjpeg_decode: OpenJPEG JPEG support not enabled.");
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}

int grib_openjpeg_encode(grib_context* c, j2k_encode_helper* helper)
{
    grib_context_log(c, GRIB_LOG_ERROR, "grib_openjpeg_encode: OpenJPEG JPEG support not enabled.");
    return GRIB_FUNCTIONALITY_NOT_ENABLED;
}

#endif
