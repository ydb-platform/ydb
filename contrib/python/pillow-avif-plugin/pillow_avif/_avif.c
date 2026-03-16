#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include "avif/avif.h"

#if AVIF_VERSION < 80300
#define AVIF_CHROMA_UPSAMPLING_AUTOMATIC AVIF_CHROMA_UPSAMPLING_BILINEAR
#define AVIF_CHROMA_UPSAMPLING_BEST_QUALITY AVIF_CHROMA_UPSAMPLING_BILINEAR
#define AVIF_CHROMA_UPSAMPLING_FASTEST AVIF_CHROMA_UPSAMPLING_NEAREST
#endif

// Encoder type
typedef struct {
    PyObject_HEAD
    avifEncoder *encoder;
    avifImage *image;
    PyObject *icc_bytes;
    PyObject *exif_bytes;
    PyObject *xmp_bytes;
    int first_frame;
} AvifEncoderObject;

static PyTypeObject AvifEncoder_Type;

// Decoder type
typedef struct {
    PyObject_HEAD
    avifDecoder *decoder;
    PyObject *data;
    avifChromaUpsampling upsampling;
} AvifDecoderObject;

static PyTypeObject AvifDecoder_Type;

static int default_max_threads = 0;

static void
init_max_threads(void) {
    PyObject *os = NULL;
    PyObject *n = NULL;
    long num_cpus;

#if PY_VERSION_HEX >= 0x03000000
    os = PyImport_ImportModule("os");
#else
    os = PyImport_ImportModule("multiprocessing");
#endif
    if (os == NULL) {
        goto error;
    }

    if (PyObject_HasAttrString(os, "sched_getaffinity")) {
        n = PyObject_CallMethod(os, "sched_getaffinity", "i", 0);
        if (n == NULL) {
            goto error;
        }
        num_cpus = PySet_Size(n);
    } else {
        n = PyObject_CallMethod(os, "cpu_count", NULL);
        if (n == NULL) {
            goto error;
        }
#if PY_VERSION_HEX >= 0x03000000
        num_cpus = PyLong_AsLong(n);
#else
        num_cpus = PyInt_AsLong(n);
#endif
    }

    if (num_cpus < 1) {
        goto error;
    }

    default_max_threads = (int)num_cpus;

done:
    Py_XDECREF(os);
    Py_XDECREF(n);
    return;

error:
    if (PyErr_Occurred()) {
        PyErr_Clear();
    }
    PyErr_WarnEx(
        PyExc_RuntimeWarning, "could not get cpu count: using max_threads=1", 1);
    goto done;
}

#if AVIF_VERSION < 1000000
static int
normalize_quantize_value(int qvalue) {
    if (qvalue < AVIF_QUANTIZER_BEST_QUALITY) {
        return AVIF_QUANTIZER_BEST_QUALITY;
    } else if (qvalue > AVIF_QUANTIZER_WORST_QUALITY) {
        return AVIF_QUANTIZER_WORST_QUALITY;
    } else {
        return qvalue;
    }
}
#endif

static int
normalize_tiles_log2(int value) {
    if (value < 0) {
        return 0;
    } else if (value > 6) {
        return 6;
    } else {
        return value;
    }
}

static PyObject *
exc_type_for_avif_result(avifResult result) {
    switch (result) {
        case AVIF_RESULT_INVALID_EXIF_PAYLOAD:
        case AVIF_RESULT_INVALID_CODEC_SPECIFIC_OPTION:
            return PyExc_ValueError;
        case AVIF_RESULT_INVALID_FTYP:
        case AVIF_RESULT_BMFF_PARSE_FAILED:
        case AVIF_RESULT_TRUNCATED_DATA:
        case AVIF_RESULT_NO_CONTENT:
            return PyExc_SyntaxError;
        default:
            return PyExc_RuntimeError;
    }
}

static uint8_t
irot_imir_to_exif_orientation(const avifImage *image) {
    uint8_t axis;
#if AVIF_VERSION_MAJOR >= 1
    axis = image->imir.axis;
#else
    axis = image->imir.mode;
#endif
    int imir = image->transformFlags & AVIF_TRANSFORM_IMIR;
    int irot = image->transformFlags & AVIF_TRANSFORM_IROT;
    if (irot) {
        uint8_t angle = image->irot.angle;
        if (angle == 1) {
            if (imir) {
                return axis ? 7   // 90 degrees anti-clockwise then swap left and right.
                            : 5;  // 90 degrees anti-clockwise then swap top and bottom.
            }
            return 6;  // 90 degrees anti-clockwise.
        }
        if (angle == 2) {
            if (imir) {
                return axis
                           ? 4   // 180 degrees anti-clockwise then swap left and right.
                           : 2;  // 180 degrees anti-clockwise then swap top and bottom.
            }
            return 3;  // 180 degrees anti-clockwise.
        }
        if (angle == 3) {
            if (imir) {
                return axis
                           ? 5   // 270 degrees anti-clockwise then swap left and right.
                           : 7;  // 270 degrees anti-clockwise then swap top and bottom.
            }
            return 8;  // 270 degrees anti-clockwise.
        }
    }
    if (imir) {
        return axis ? 2   // Swap left and right.
                    : 4;  // Swap top and bottom.
    }
    return 1;  // Default orientation ("top-left", no-op).
}

static void
exif_orientation_to_irot_imir(avifImage *image, int orientation) {
    // Mapping from Exif orientation as defined in JEITA CP-3451C section 4.6.4.A
    // Orientation to irot and imir boxes as defined in HEIF ISO/IEC 28002-12:2021
    // sections 6.5.10 and 6.5.12.
    switch (orientation) {
        case 2:  // The 0th row is at the visual top of the image, and the 0th column is
                 // the visual right-hand side.
            image->transformFlags |= AVIF_TRANSFORM_IMIR;
#if AVIF_VERSION_MAJOR >= 1
            image->imir.axis = 1;
#else
            image->imir.mode = 1;
#endif
            break;
        case 3:  // The 0th row is at the visual bottom of the image, and the 0th column
                 // is the visual right-hand side.
            image->transformFlags |= AVIF_TRANSFORM_IROT;
            image->irot.angle = 2;
            break;
        case 4:  // The 0th row is at the visual bottom of the image, and the 0th column
                 // is the visual left-hand side.
            image->transformFlags |= AVIF_TRANSFORM_IMIR;
            break;
        case 5:  // The 0th row is the visual left-hand side of the image, and the 0th
                 // column is the visual top.
            image->transformFlags |= AVIF_TRANSFORM_IROT | AVIF_TRANSFORM_IMIR;
            image->irot.angle = 1;  // applied before imir according to MIAF spec
                                    // ISO/IEC 28002-12:2021 - section 7.3.6.7
            break;
        case 6:  // The 0th row is the visual right-hand side of the image, and the 0th
                 // column is the visual top.
            image->transformFlags |= AVIF_TRANSFORM_IROT;
            image->irot.angle = 3;
            break;
        case 7:  // The 0th row is the visual right-hand side of the image, and the 0th
                 // column is the visual bottom.
            image->transformFlags |= AVIF_TRANSFORM_IROT | AVIF_TRANSFORM_IMIR;
            image->irot.angle = 3;  // applied before imir according to MIAF spec
                                    // ISO/IEC 28002-12:2021 - section 7.3.6.7
            break;
        case 8:  // The 0th row is the visual left-hand side of the image, and the 0th
                 // column is the visual bottom.
            image->transformFlags |= AVIF_TRANSFORM_IROT;
            image->irot.angle = 1;
            break;
    }
}

static int
_codec_available(const char *name, avifCodecFlags flags) {
    avifCodecChoice codec = avifCodecChoiceFromName(name);
    if (codec == AVIF_CODEC_CHOICE_AUTO) {
        return 0;
    }
    const char *codec_name = avifCodecName(codec, flags);
    return (codec_name == NULL) ? 0 : 1;
}

PyObject *
_decoder_codec_available(PyObject *self, PyObject *args) {
    char *codec_name;
    if (!PyArg_ParseTuple(args, "s", &codec_name)) {
        return NULL;
    }
    int is_available = _codec_available(codec_name, AVIF_CODEC_FLAG_CAN_DECODE);
    return PyBool_FromLong(is_available);
}

PyObject *
_encoder_codec_available(PyObject *self, PyObject *args) {
    char *codec_name;
    if (!PyArg_ParseTuple(args, "s", &codec_name)) {
        return NULL;
    }
    int is_available = _codec_available(codec_name, AVIF_CODEC_FLAG_CAN_ENCODE);
    return PyBool_FromLong(is_available);
}

#if AVIF_VERSION >= 80200
static int
_add_codec_specific_options(avifEncoder *encoder, PyObject *opts) {
    Py_ssize_t i, size;
    PyObject *keyval, *py_key, *py_val;
    if (!PyTuple_Check(opts)) {
        PyErr_SetString(PyExc_ValueError, "Invalid advanced codec options");
        return 1;
    }
    size = PyTuple_GET_SIZE(opts);

    for (i = 0; i < size; i++) {
        keyval = PyTuple_GetItem(opts, i);
        if (!PyTuple_Check(keyval) || PyTuple_GET_SIZE(keyval) != 2) {
            PyErr_SetString(PyExc_ValueError, "Invalid advanced codec options");
            return 1;
        }
        py_key = PyTuple_GetItem(keyval, 0);
        py_val = PyTuple_GetItem(keyval, 1);
        if (!PyBytes_Check(py_key) || !PyBytes_Check(py_val)) {
            PyErr_SetString(PyExc_ValueError, "Invalid advanced codec options");
            return 1;
        }
        const char *key = PyBytes_AsString(py_key);
        const char *val = PyBytes_AsString(py_val);
        if (key == NULL || val == NULL) {
            PyErr_SetString(PyExc_ValueError, "Invalid advanced codec options");
            return 1;
        }

#if AVIF_VERSION < 1000000
        avifEncoderSetCodecSpecificOption(encoder, key, val);
#else
        avifResult result = avifEncoderSetCodecSpecificOption(encoder, key, val);
        if (result != AVIF_RESULT_OK) {
            PyErr_Format(
                exc_type_for_avif_result(result),
                "Setting advanced codec options failed: %s",
                avifResultToString(result));
            return 1;
        }
#endif
    }
    return 0;
}
#endif

// Encoder functions
PyObject *
AvifEncoderNew(PyObject *self_, PyObject *args) {
    unsigned int width, height;
    AvifEncoderObject *self = NULL;
    avifEncoder *encoder = NULL;

    char *subsampling;
    int quality;
    int qmin;
    int qmax;
    int speed;
    int exif_orientation;
    int max_threads;
    PyObject *icc_bytes;
    PyObject *exif_bytes;
    PyObject *xmp_bytes;
    PyObject *alpha_premultiplied;
    PyObject *autotiling;
    int tile_rows_log2;
    int tile_cols_log2;

    char *codec;
    char *range;

    PyObject *advanced;
    int error = 0;

    if (!PyArg_ParseTuple(
            args,
            "IIsiiiiissiiOOSSiSO",
            &width,
            &height,
            &subsampling,
            &qmin,
            &qmax,
            &quality,
            &speed,
            &max_threads,
            &codec,
            &range,
            &tile_rows_log2,
            &tile_cols_log2,
            &alpha_premultiplied,
            &autotiling,
            &icc_bytes,
            &exif_bytes,
            &exif_orientation,
            &xmp_bytes,
            &advanced)) {
        return NULL;
    }

    // Create a new animation encoder and picture frame
    avifImage *image = avifImageCreateEmpty();
    if (image == NULL) {
        PyErr_SetString(PyExc_ValueError, "Image creation failed");
        error = 1;
        goto end;
    }

    // Set these in advance so any upcoming RGB -> YUV use the proper coefficients
    if (strcmp(range, "full") == 0) {
        image->yuvRange = AVIF_RANGE_FULL;
    } else if (strcmp(range, "limited") == 0) {
        image->yuvRange = AVIF_RANGE_LIMITED;
    } else {
        PyErr_SetString(PyExc_ValueError, "Invalid range");
        error = 1;
        goto end;
    }
    if (strcmp(subsampling, "4:0:0") == 0) {
        image->yuvFormat = AVIF_PIXEL_FORMAT_YUV400;
    } else if (strcmp(subsampling, "4:2:0") == 0) {
        image->yuvFormat = AVIF_PIXEL_FORMAT_YUV420;
    } else if (strcmp(subsampling, "4:2:2") == 0) {
        image->yuvFormat = AVIF_PIXEL_FORMAT_YUV422;
    } else if (strcmp(subsampling, "4:4:4") == 0) {
        image->yuvFormat = AVIF_PIXEL_FORMAT_YUV444;
    } else {
        PyErr_Format(PyExc_ValueError, "Invalid subsampling: %s", subsampling);
        error = 1;
        goto end;
    }

    // Validate canvas dimensions
    if (width == 0 || height == 0) {
        PyErr_SetString(PyExc_ValueError, "invalid canvas dimensions");
        error = 1;
        goto end;
    }
    image->width = width;
    image->height = height;

    image->depth = 8;
#if AVIF_VERSION >= 90000
    image->alphaPremultiplied = alpha_premultiplied == Py_True ? AVIF_TRUE : AVIF_FALSE;
#endif

    encoder = avifEncoderCreate();
    if (!encoder) {
        PyErr_SetString(PyExc_MemoryError, "Can't allocate encoder");
        error = 1;
        goto end;
    }

    if (max_threads == 0) {
        if (default_max_threads == 0) {
            init_max_threads();
        }
        max_threads = default_max_threads;
    }

    int is_aom_encode = strcmp(codec, "aom") == 0 ||
                        (strcmp(codec, "auto") == 0 &&
                         _codec_available("aom", AVIF_CODEC_FLAG_CAN_ENCODE));
    encoder->maxThreads = is_aom_encode && max_threads > 64 ? 64 : max_threads;

#if AVIF_VERSION < 1000000
    if (qmin != -1 && qmax != -1) {
        encoder->minQuantizer = qmin;
        encoder->maxQuantizer = qmax;
    } else {
        encoder->minQuantizer = normalize_quantize_value(64 - quality);
        encoder->maxQuantizer = normalize_quantize_value(100 - quality);
    }
#else
    encoder->quality = quality;
#endif

    if (strcmp(codec, "auto") == 0) {
        encoder->codecChoice = AVIF_CODEC_CHOICE_AUTO;
    } else {
        encoder->codecChoice = avifCodecChoiceFromName(codec);
    }

    if (speed < AVIF_SPEED_SLOWEST) {
        speed = AVIF_SPEED_SLOWEST;
    } else if (speed > AVIF_SPEED_FASTEST) {
        speed = AVIF_SPEED_FASTEST;
    }
    encoder->speed = speed;
    encoder->timescale = (uint64_t)1000;

#if AVIF_VERSION >= 110000
    if (PyObject_IsTrue(autotiling)) {
        encoder->autoTiling = AVIF_TRUE;
    } else {
        encoder->autoTiling = AVIF_FALSE;
        encoder->tileRowsLog2 = normalize_tiles_log2(tile_rows_log2);
        encoder->tileColsLog2 = normalize_tiles_log2(tile_cols_log2);
    }
#else
    encoder->tileRowsLog2 = normalize_tiles_log2(tile_rows_log2);
    encoder->tileColsLog2 = normalize_tiles_log2(tile_cols_log2);
#endif

    if (advanced != Py_None) {
#if AVIF_VERSION >= 80200
        if (_add_codec_specific_options(encoder, advanced)) {
            error = 1;
            goto end;
        }
#else
        PyErr_SetString(
            PyExc_ValueError, "Advanced codec options require libavif >= 0.8.2");
        error = 1;
        goto end;
#endif
    }

    self = PyObject_New(AvifEncoderObject, &AvifEncoder_Type);
    if (!self) {
        PyErr_SetString(PyExc_RuntimeError, "could not create encoder object");
        error = 1;
        goto end;
    }
    self->first_frame = 1;
    self->icc_bytes = NULL;
    self->exif_bytes = NULL;
    self->xmp_bytes = NULL;

    avifResult result;
    if (PyBytes_GET_SIZE(icc_bytes)) {
        self->icc_bytes = icc_bytes;
        Py_INCREF(icc_bytes);
#if AVIF_VERSION < 1000000
        avifImageSetProfileICC(
            image,
            (uint8_t *)PyBytes_AS_STRING(icc_bytes),
            PyBytes_GET_SIZE(icc_bytes));
#else
        result = avifImageSetProfileICC(
            image,
            (uint8_t *)PyBytes_AS_STRING(icc_bytes),
            PyBytes_GET_SIZE(icc_bytes));
        if (result != AVIF_RESULT_OK) {
            PyErr_Format(
                exc_type_for_avif_result(result),
                "Setting ICC profile failed: %s",
                avifResultToString(result));
            error = 1;
            goto end;
        }
#endif
        // colorPrimaries and transferCharacteristics are ignored when an ICC
        // profile is present, so set them to UNSPECIFIED.
        image->colorPrimaries = AVIF_COLOR_PRIMARIES_UNSPECIFIED;
        image->transferCharacteristics = AVIF_TRANSFER_CHARACTERISTICS_UNSPECIFIED;
    } else {
        image->colorPrimaries = AVIF_COLOR_PRIMARIES_BT709;
        image->transferCharacteristics = AVIF_TRANSFER_CHARACTERISTICS_SRGB;
    }
    image->matrixCoefficients = AVIF_MATRIX_COEFFICIENTS_BT601;

    if (PyBytes_GET_SIZE(exif_bytes)) {
        self->exif_bytes = exif_bytes;
        Py_INCREF(exif_bytes);
#if AVIF_VERSION < 1000000
        avifImageSetMetadataExif(
            image,
            (uint8_t *)PyBytes_AS_STRING(exif_bytes),
            PyBytes_GET_SIZE(exif_bytes));
#else
        result = avifImageSetMetadataExif(
            image,
            (uint8_t *)PyBytes_AS_STRING(exif_bytes),
            PyBytes_GET_SIZE(exif_bytes));
        if (result != AVIF_RESULT_OK) {
            PyErr_Format(
                exc_type_for_avif_result(result),
                "Setting EXIF data failed: %s",
                avifResultToString(result));
            error = 1;
            goto end;
        }
#endif
    }

    if (PyBytes_GET_SIZE(xmp_bytes)) {
        self->xmp_bytes = xmp_bytes;
        Py_INCREF(xmp_bytes);
#if AVIF_VERSION < 1000000
        avifImageSetMetadataXMP(
            image,
            (uint8_t *)PyBytes_AS_STRING(xmp_bytes),
            PyBytes_GET_SIZE(xmp_bytes));
#else
        result = avifImageSetMetadataXMP(
            image,
            (uint8_t *)PyBytes_AS_STRING(xmp_bytes),
            PyBytes_GET_SIZE(xmp_bytes));
        if (result != AVIF_RESULT_OK) {
            PyErr_Format(
                exc_type_for_avif_result(result),
                "Setting XMP data failed: %s",
                avifResultToString(result));
            error = 1;
            goto end;
        }
#endif
    }

    if (exif_orientation) {
        exif_orientation_to_irot_imir(image, exif_orientation);
    }

    self->image = image;
    self->encoder = encoder;

end:
    if (error) {
        if (image) {
            avifImageDestroy(image);
        }
        if (encoder) {
            avifEncoderDestroy(encoder);
        }
        if (self) {
            Py_XDECREF(self->icc_bytes);
            Py_XDECREF(self->exif_bytes);
            Py_XDECREF(self->xmp_bytes);
            PyObject_Del(self);
        }
        return NULL;
    }

    return (PyObject *)self;
}

PyObject *
_encoder_dealloc(AvifEncoderObject *self) {
    if (self->encoder) {
        avifEncoderDestroy(self->encoder);
    }
    if (self->image) {
        avifImageDestroy(self->image);
    }
    Py_XDECREF(self->icc_bytes);
    Py_XDECREF(self->exif_bytes);
    Py_XDECREF(self->xmp_bytes);
    Py_RETURN_NONE;
}

PyObject *
_encoder_add(AvifEncoderObject *self, PyObject *args) {
    uint8_t *rgb_bytes;
    Py_ssize_t size;
    unsigned int duration;
    unsigned int width;
    unsigned int height;
    char *mode;
    PyObject *is_single_frame = NULL;
    int error = 0;

    avifRGBImage rgb;
    avifResult result;

    avifEncoder *encoder = self->encoder;
    avifImage *image = self->image;
    avifImage *frame = NULL;

    if (!PyArg_ParseTuple(
            args,
            "z#IIIsO",
            (char **)&rgb_bytes,
            &size,
            &duration,
            &width,
            &height,
            &mode,
            &is_single_frame)) {
        return NULL;
    }

    if (image->width != width || image->height != height) {
        PyErr_Format(
            PyExc_ValueError,
            "Image sequence dimensions mismatch, %ux%u != %ux%u",
            image->width,
            image->height,
            width,
            height);
        return NULL;
    }

    if (self->first_frame) {
        // If we don't have an image populated with yuv planes, this is the first
        // frame
        frame = image;
    } else {
        frame = avifImageCreateEmpty();
        if (image == NULL) {
            PyErr_SetString(PyExc_ValueError, "Image creation failed");
            return NULL;
        }

        frame->width = width;
        frame->height = height;
        frame->colorPrimaries = image->colorPrimaries;
        frame->transferCharacteristics = image->transferCharacteristics;
        frame->matrixCoefficients = image->matrixCoefficients;
        frame->yuvRange = image->yuvRange;
        frame->yuvFormat = image->yuvFormat;
        frame->depth = image->depth;
#if AVIF_VERSION >= 90000
        frame->alphaPremultiplied = image->alphaPremultiplied;
#endif
    }

    avifRGBImageSetDefaults(&rgb, frame);

    if (strcmp(mode, "RGBA") == 0) {
        rgb.format = AVIF_RGB_FORMAT_RGBA;
    } else {
        rgb.format = AVIF_RGB_FORMAT_RGB;
    }

#if AVIF_VERSION < 1000000
    avifRGBImageAllocatePixels(&rgb);
#else
    result = avifRGBImageAllocatePixels(&rgb);
    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Pixel allocation failed: %s",
            avifResultToString(result));
        error = 1;
        goto end;
    }
#endif

    if (rgb.rowBytes * rgb.height != size) {
        PyErr_Format(
            PyExc_RuntimeError,
            "rgb data has incorrect size: %u * %u (%u) != %u",
            rgb.rowBytes,
            rgb.height,
            rgb.rowBytes * rgb.height,
            size);
        error = 1;
        goto end;
    }

    // rgb.pixels is safe for writes
    memcpy(rgb.pixels, rgb_bytes, size);

    Py_BEGIN_ALLOW_THREADS;
    result = avifImageRGBToYUV(frame, &rgb);
    Py_END_ALLOW_THREADS;

    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Conversion to YUV failed: %s",
            avifResultToString(result));
        error = 1;
        goto end;
    }

    uint32_t addImageFlags = PyObject_IsTrue(is_single_frame)
                                 ? AVIF_ADD_IMAGE_FLAG_SINGLE
                                 : AVIF_ADD_IMAGE_FLAG_NONE;

    Py_BEGIN_ALLOW_THREADS;
    result = avifEncoderAddImage(encoder, frame, duration, addImageFlags);
    Py_END_ALLOW_THREADS;

    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Failed to encode image: %s",
            avifResultToString(result));
        error = 1;
        goto end;
    }

end:
    if (&rgb) {
        avifRGBImageFreePixels(&rgb);
    }
    if (!self->first_frame) {
        avifImageDestroy(frame);
    }

    if (error) {
        return NULL;
    }
    self->first_frame = 0;
    Py_RETURN_NONE;
}

PyObject *
_encoder_finish(AvifEncoderObject *self) {
    avifEncoder *encoder = self->encoder;

    avifRWData raw = AVIF_DATA_EMPTY;
    avifResult result;
    PyObject *ret = NULL;

    Py_BEGIN_ALLOW_THREADS;
    result = avifEncoderFinish(encoder, &raw);
    Py_END_ALLOW_THREADS;

    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Failed to finish encoding: %s",
            avifResultToString(result));
        avifRWDataFree(&raw);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize((char *)raw.data, raw.size);

    avifRWDataFree(&raw);

    return ret;
}

// Decoder functions
PyObject *
AvifDecoderNew(PyObject *self_, PyObject *args) {
    PyObject *avif_bytes;
    AvifDecoderObject *self = NULL;
    avifDecoder *decoder;

    char *upsampling_str;
    char *codec_str;
    avifCodecChoice codec;
    avifChromaUpsampling upsampling;
    int max_threads = 0;

    avifResult result;

    if (!PyArg_ParseTuple(
            args, "Sssi", &avif_bytes, &codec_str, &upsampling_str, &max_threads)) {
        return NULL;
    }

    if (!strcmp(upsampling_str, "auto")) {
        upsampling = AVIF_CHROMA_UPSAMPLING_AUTOMATIC;
    } else if (!strcmp(upsampling_str, "fastest")) {
        upsampling = AVIF_CHROMA_UPSAMPLING_FASTEST;
    } else if (!strcmp(upsampling_str, "best")) {
        upsampling = AVIF_CHROMA_UPSAMPLING_BEST_QUALITY;
    } else if (!strcmp(upsampling_str, "nearest")) {
        upsampling = AVIF_CHROMA_UPSAMPLING_NEAREST;
    } else if (!strcmp(upsampling_str, "bilinear")) {
        upsampling = AVIF_CHROMA_UPSAMPLING_BILINEAR;
    } else {
        PyErr_Format(PyExc_ValueError, "Invalid upsampling option: %s", upsampling_str);
        return NULL;
    }

    if (strcmp(codec_str, "auto") == 0) {
        codec = AVIF_CODEC_CHOICE_AUTO;
    } else {
        codec = avifCodecChoiceFromName(codec_str);
    }

    self = PyObject_New(AvifDecoderObject, &AvifDecoder_Type);
    if (!self) {
        PyErr_SetString(PyExc_RuntimeError, "could not create decoder object");
        return NULL;
    }

    self->upsampling = upsampling;

    decoder = avifDecoderCreate();
    if (!decoder) {
        PyErr_SetString(PyExc_MemoryError, "Can't allocate decoder");
        PyObject_Del(self);
        return NULL;
    }

#if AVIF_VERSION >= 80400
    if (max_threads == 0) {
        if (default_max_threads == 0) {
            init_max_threads();
        }
        max_threads = default_max_threads;
    }
    decoder->maxThreads = max_threads;
#endif
#if AVIF_VERSION >= 90200
    // Turn off libavif's 'clap' (clean aperture) property validation.
    decoder->strictFlags &= ~AVIF_STRICT_CLAP_VALID;
    // Allow the PixelInformationProperty ('pixi') to be missing in AV1 image
    // items. libheif v1.11.0 and older does not add the 'pixi' item property to
    // AV1 image items.
    decoder->strictFlags &= ~AVIF_STRICT_PIXI_REQUIRED;
#endif
    decoder->codecChoice = codec;

    Py_INCREF(avif_bytes);

    result = avifDecoderSetIOMemory(
        decoder,
        (uint8_t *)PyBytes_AS_STRING(avif_bytes),
        PyBytes_GET_SIZE(avif_bytes));

    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Setting IO memory failed: %s",
            avifResultToString(result));
        avifDecoderDestroy(decoder);
        Py_XDECREF(avif_bytes);
        PyObject_Del(self);
        return NULL;
    }

    result = avifDecoderParse(decoder);
    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Failed to decode image: %s",
            avifResultToString(result));
        avifDecoderDestroy(decoder);
        Py_XDECREF(avif_bytes);
        PyObject_Del(self);
        return NULL;
    }

    self->decoder = decoder;
    self->data = avif_bytes;

    return (PyObject *)self;
}

PyObject *
_decoder_dealloc(AvifDecoderObject *self) {
    if (self->decoder) {
        avifDecoderDestroy(self->decoder);
    }
    Py_XDECREF(self->data);
    Py_RETURN_NONE;
}

PyObject *
_decoder_get_info(AvifDecoderObject *self) {
    avifDecoder *decoder = self->decoder;
    avifImage *image = decoder->image;

    PyObject *icc = NULL;
    PyObject *exif = NULL;
    PyObject *xmp = NULL;
    PyObject *ret = NULL;

    if (image->xmp.size) {
        xmp = PyBytes_FromStringAndSize((const char *)image->xmp.data, image->xmp.size);
    }

    if (image->exif.size) {
        exif =
            PyBytes_FromStringAndSize((const char *)image->exif.data, image->exif.size);
    }

    if (image->icc.size) {
        icc = PyBytes_FromStringAndSize((const char *)image->icc.data, image->icc.size);
    }

    ret = Py_BuildValue(
        "IIIsSSIS",
        image->width,
        image->height,
        decoder->imageCount,
        decoder->alphaPresent ? "RGBA" : "RGB",
        NULL == icc ? Py_None : icc,
        NULL == exif ? Py_None : exif,
        irot_imir_to_exif_orientation(image),
        NULL == xmp ? Py_None : xmp);

    Py_XDECREF(xmp);
    Py_XDECREF(exif);
    Py_XDECREF(icc);

    return ret;
}

PyObject *
_decoder_get_frame(AvifDecoderObject *self, PyObject *args) {
    PyObject *bytes;
    PyObject *ret;
    Py_ssize_t size;
    avifResult result;
    avifRGBImage rgb;
    avifDecoder *decoder;
    avifImage *image;
    uint32_t frame_index;

    decoder = self->decoder;

    if (!PyArg_ParseTuple(args, "I", &frame_index)) {
        return NULL;
    }

    result = avifDecoderNthImage(decoder, frame_index);
    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Failed to decode frame %u: %s",
            frame_index,
            avifResultToString(result));
        return NULL;
    }

    image = decoder->image;

    avifRGBImageSetDefaults(&rgb, image);

    rgb.depth = 8;
    rgb.format = decoder->alphaPresent ? AVIF_RGB_FORMAT_RGBA : AVIF_RGB_FORMAT_RGB;
    rgb.chromaUpsampling = self->upsampling;

#if AVIF_VERSION < 1000000
    avifRGBImageAllocatePixels(&rgb);
#else
    result = avifRGBImageAllocatePixels(&rgb);
    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Pixel allocation failed: %s",
            avifResultToString(result));
        return NULL;
    }
#endif

    Py_BEGIN_ALLOW_THREADS;
    result = avifImageYUVToRGB(image, &rgb);
    Py_END_ALLOW_THREADS;

    if (result != AVIF_RESULT_OK) {
        PyErr_Format(
            exc_type_for_avif_result(result),
            "Conversion from YUV failed: %s",
            avifResultToString(result));
        avifRGBImageFreePixels(&rgb);
        return NULL;
    }

    if (rgb.height > PY_SSIZE_T_MAX / rgb.rowBytes) {
        PyErr_SetString(PyExc_MemoryError, "Integer overflow in pixel size");
        return NULL;
    }

    size = rgb.rowBytes * rgb.height;

    bytes = PyBytes_FromStringAndSize((char *)rgb.pixels, size);
    avifRGBImageFreePixels(&rgb);

    ret = Py_BuildValue(
        "SKKK",
        bytes,
        (unsigned PY_LONG_LONG)decoder->timescale,
        (unsigned PY_LONG_LONG)decoder->imageTiming.ptsInTimescales,
        (unsigned PY_LONG_LONG)decoder->imageTiming.durationInTimescales);

    Py_DECREF(bytes);

    return ret;
}

/* -------------------------------------------------------------------- */
/* Type Definitions                                                     */
/* -------------------------------------------------------------------- */

// AvifEncoder methods
static struct PyMethodDef _encoder_methods[] = {
    {"add", (PyCFunction)_encoder_add, METH_VARARGS},
    {"finish", (PyCFunction)_encoder_finish, METH_NOARGS},
    {NULL, NULL} /* sentinel */
};

// AvifDecoder type definition
static PyTypeObject AvifEncoder_Type = {
    // clang-format off
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "AvifEncoder",
    // clang-format on
    .tp_basicsize = sizeof(AvifEncoderObject),
    .tp_dealloc = (destructor)_encoder_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = _encoder_methods,
};

// AvifDecoder methods
static struct PyMethodDef _decoder_methods[] = {
    {"get_info", (PyCFunction)_decoder_get_info, METH_NOARGS},
    {"get_frame", (PyCFunction)_decoder_get_frame, METH_VARARGS},
    {NULL, NULL} /* sentinel */
};

// AvifDecoder type definition
static PyTypeObject AvifDecoder_Type = {
    // clang-format off
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "AvifDecoder",
    // clang-format on
    .tp_basicsize = sizeof(AvifDecoderObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)_decoder_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = _decoder_methods,
};

PyObject *
#if PY_VERSION_HEX >= 0x03000000
AvifCodecVersions(PyObject *self, PyObject *args) {
#else
AvifCodecVersions() {
#endif
    char codecVersions[256];
    avifCodecVersions(codecVersions);
    return PyUnicode_FromString(codecVersions);
}

/* -------------------------------------------------------------------- */
/* Module Setup                                                         */
/* -------------------------------------------------------------------- */

#if PY_VERSION_HEX >= 0x03000000
#define MOD_ERROR_VAL NULL
#define MOD_SUCCESS_VAL(val) val
#define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
#define MOD_DEF(ob, name, methods)          \
    static struct PyModuleDef moduledef = { \
        PyModuleDef_HEAD_INIT,              \
        name,                               \
        NULL,                               \
        -1,                                 \
        methods,                            \
    };                                      \
    ob = PyModule_Create(&moduledef);
#else
#define MOD_ERROR_VAL
#define MOD_SUCCESS_VAL(val)
#define MOD_INIT(name) void init##name(void)
#define MOD_DEF(ob, name, methods) ob = Py_InitModule(name, methods);
#endif

static PyMethodDef avifMethods[] = {
    {"AvifDecoder", AvifDecoderNew, METH_VARARGS},
    {"AvifEncoder", AvifEncoderNew, METH_VARARGS},
    {"AvifCodecVersions", AvifCodecVersions, METH_NOARGS},
    {"decoder_codec_available", _decoder_codec_available, METH_VARARGS},
    {"encoder_codec_available", _encoder_codec_available, METH_VARARGS},
    {NULL, NULL}};

static int
setup_module(PyObject *m) {
    PyObject *d = PyModule_GetDict(m);

    PyObject *v = PyUnicode_FromString(avifVersion());
    if (PyDict_SetItemString(d, "libavif_version", v) < 0) {
        Py_DECREF(v);
        return -1;
    }
    Py_DECREF(v);

    v = Py_BuildValue(
        "(iii)", AVIF_VERSION_MAJOR, AVIF_VERSION_MINOR, AVIF_VERSION_PATCH);

    if (PyDict_SetItemString(d, "VERSION", v) < 0) {
        Py_DECREF(v);
        return -1;
    }
    Py_DECREF(v);

    if (PyType_Ready(&AvifDecoder_Type) < 0 || PyType_Ready(&AvifEncoder_Type) < 0) {
        return -1;
    }
    return 0;
}

MOD_INIT(_avif) {
    PyObject *m;

    MOD_DEF(m, "_avif", avifMethods)

    if (m == NULL || setup_module(m) < 0) {
        return MOD_ERROR_VAL;
    }

#ifdef Py_GIL_DISABLED
    PyUnstable_Module_SetGIL(m, Py_MOD_GIL_NOT_USED);
#endif

    return MOD_SUCCESS_VAL(m);
}
