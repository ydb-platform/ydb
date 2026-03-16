#define PY_SSIZE_T_CLEAN

#include "Python.h"
#include "libheif/heif.h"
#if LIBHEIF_HAVE_VERSION(1,20,0)
    #include "libheif/heif_properties.h"
#endif
#include "_ph_postprocess.h"

/* =========== Free-threading support ======== */

#ifdef Py_GIL_DISABLED
#define MUTEX_LOCK(m) PyMutex_Lock(m)
#define MUTEX_UNLOCK(m) PyMutex_Unlock(m)
#else
#define MUTEX_LOCK(m)
#define MUTEX_UNLOCK(m)
#endif

/* =========== Common stuff ======== */

#define MAX_ENCODERS 20
#define MAX_DECODERS 20

static struct heif_error heif_error_no = { .code = 0, .subcode = 0, .message = "" };

int check_error(struct heif_error error) {
    if (error.code == heif_error_Ok) {
        return 0;
    }

    PyObject* e;
    switch (error.code) {
        case heif_error_Decoder_plugin_error:
            if (error.subcode == 100) {
                e = PyExc_EOFError;
                break;
            }
        case heif_error_Invalid_input:
        case heif_error_Usage_error:
            e = PyExc_ValueError;
            break;
        case heif_error_Unsupported_filetype:
        case heif_error_Unsupported_feature:
        case heif_error_Color_profile_does_not_exist:
            e = PyExc_SyntaxError;
            break;
        default:
            e = PyExc_RuntimeError;
    }
    PyErr_SetString(e, error.message);
    return 1;
}

int __PyDict_SetItemString(PyObject *p, const char *key, PyObject *val) {
    int r = PyDict_SetItemString(p, key, val);
    Py_DECREF(val);
    return r;
}

enum ph_image_type {
    PhHeifImage = 0,
    PhHeifDepthImage = 2,
};

/* =========== Objects ======== */

typedef struct {
    PyObject_HEAD
    enum heif_chroma chroma;
    struct heif_image* image;
    struct heif_image_handle* handle;
    struct heif_color_profile_nclx* output_nclx_color_profile;
} CtxWriteImageObject;

static PyTypeObject CtxWriteImage_Type;

typedef struct {
    PyObject_HEAD
    struct heif_context* ctx;               // libheif context
    struct heif_encoder* encoder;           // encoder
    size_t size;                            // number of bytes in `data`
    void* data;                             // encoded data if success
} CtxWriteObject;

static PyTypeObject CtxWrite_Type;

typedef struct {
    PyObject_HEAD
    enum ph_image_type image_type;              // 0 - standard, 2 - depth image
    int width;                                  // size[0];
    int height;                                 // size[1];
    int bits;                                   // one of: 8, 10, 12.
    int alpha;                                  // one of: 0, 1.
    char mode[8];                               // one of: L, RGB, RGBA, RGBa, BGR, BGRA, BGRa + Optional[;10/12/16]
    int n_channels;                             // 1, 2, 3, 4.
    int primary;                                // one of: 0, 1.
    enum heif_colorspace colorspace;
    enum heif_chroma chroma;
    int hdr_to_8bit;                            // private. decode option.
    int bgr_mode;                               // private. decode option.
    int remove_stride;                          // private. decode option.
    int hdr_to_16bit;                           // private. decode option.
    int reload_size;                            // private. decode option.
    char decoder_id[64];                        // private. decode option. optional
    struct heif_image_handle *handle;           // private
    struct heif_image *heif_image;              // private
    const struct heif_depth_representation_info* depth_metadata; // only for image_type == 2
    uint8_t *data;                              // pointer to data after decoding
    int stride;                                 // time when it get filled depends on `remove_stride` value
    PyObject *file_bytes;                       // private
#ifdef Py_GIL_DISABLED
    PyMutex decode_mutex;                       // protects lazy decode in free-threaded builds
#endif
} CtxImageObject;

static PyTypeObject CtxImage_Type;

int get_stride(CtxImageObject *ctx_image) {
    int stride = ctx_image->width * ctx_image->n_channels;
    if ((ctx_image->bits > 8) && (!ctx_image->hdr_to_8bit))
        stride = stride * 2;
    return stride;
}

/* =========== CtxWriteImage ======== */

static void _CtxWriteImage_destructor(CtxWriteImageObject* self) {
    if (self->handle)
        heif_image_handle_release(self->handle);
    if (self->image)
        heif_image_release(self->image);
    if (self->output_nclx_color_profile)
        heif_nclx_color_profile_free(self->output_nclx_color_profile);
    PyObject_Del(self);
}

static PyObject* _CtxWriteImage_add_plane(CtxWriteImageObject* self, PyObject* args) {
    /* (size), depth: int, depth_in: int, data: bytes, bgr_mode: int */
    int width, height, depth, depth_in, stride_out, stride_in, real_stride, bgr_mode;
    Py_buffer buffer;
    uint8_t* plane_data;

    if (!PyArg_ParseTuple(args, "(ii)iiy*ii", &width, &height, &depth, &depth_in, &buffer, &bgr_mode, &stride_in))
        return NULL;

    int with_alpha = 0;
    if ((self->chroma == heif_chroma_interleaved_RGBA) || (self->chroma == heif_chroma_interleaved_RRGGBBAA_LE)) {
        real_stride = width * 4;
        with_alpha = 1;
    }
    else
        real_stride = width * 3;
    if (depth > 8)
        real_stride = real_stride * 2;
    if (stride_in == 0)
        stride_in = real_stride;
    if ((Py_ssize_t)stride_in * height > buffer.len) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError, "image plane does not contain enough data");
        return NULL;
    }

    if (check_error(heif_image_add_plane(self->image, heif_channel_interleaved, width, height, depth))) {
        PyBuffer_Release(&buffer);
        return NULL;
    }

    plane_data = heif_image_get_plane(self->image, heif_channel_interleaved, &stride_out);
    if (!plane_data) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_RuntimeError, "heif_image_get_plane failed");
        return NULL;
    }

    int invalid_mode = 0;
    Py_BEGIN_ALLOW_THREADS
    uint8_t *out = plane_data;
    uint8_t *in = buffer.buf;
    uint16_t *out_word = (uint16_t *)plane_data;
    uint16_t *in_word = (uint16_t *)buffer.buf;
    if (!bgr_mode) {
        if ((depth_in == depth) && (stride_in == stride_out))
            memcpy(out, in, stride_out * height);
        else if ((depth_in == depth) && (stride_in != stride_out))
            for (int i = 0; i < height; i++)
                memcpy(out + stride_out * i, in + stride_in * i, real_stride);
        else if ((depth_in == 16) && (depth == 12) && (!with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 3 + 0] = in_word[i2 * 3 + 0] >> 4;
                    out_word[i2 * 3 + 1] = in_word[i2 * 3 + 1] >> 4;
                    out_word[i2 * 3 + 2] = in_word[i2 * 3 + 2] >> 4;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 12) && (with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 4 + 0] = in_word[i2 * 4 + 0] >> 4;
                    out_word[i2 * 4 + 1] = in_word[i2 * 4 + 1] >> 4;
                    out_word[i2 * 4 + 2] = in_word[i2 * 4 + 2] >> 4;
                    out_word[i2 * 4 + 3] = in_word[i2 * 4 + 3] >> 4;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 10) && (!with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 3 + 0] = in_word[i2 * 3 + 0] >> 6;
                    out_word[i2 * 3 + 1] = in_word[i2 * 3 + 1] >> 6;
                    out_word[i2 * 3 + 2] = in_word[i2 * 3 + 2] >> 6;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 10) && (with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 4 + 0] = in_word[i2 * 4 + 0] >> 6;
                    out_word[i2 * 4 + 1] = in_word[i2 * 4 + 1] >> 6;
                    out_word[i2 * 4 + 2] = in_word[i2 * 4 + 2] >> 6;
                    out_word[i2 * 4 + 3] = in_word[i2 * 4 + 3] >> 6;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else
            invalid_mode = 1;
    }
    else {
        if ((depth <= 8) && (depth_in == depth) && (!with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out[i2 * 3 + 0] = in[i2 * 3 + 2];
                    out[i2 * 3 + 1] = in[i2 * 3 + 1];
                    out[i2 * 3 + 2] = in[i2 * 3 + 0];
                }
                in += stride_in;
                out += stride_out;
            }
        else if ((depth <= 8) && (depth_in == depth) && (with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out[i2 * 4 + 0] = in[i2 * 4 + 2];
                    out[i2 * 4 + 1] = in[i2 * 4 + 1];
                    out[i2 * 4 + 2] = in[i2 * 4 + 0];
                    out[i2 * 4 + 3] = in[i2 * 4 + 3];
                }
                in += stride_in;
                out += stride_out;
            }
        else if ((depth_in == depth) && (!with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 3 + 0] = in_word[i2 * 3 + 2];
                    out_word[i2 * 3 + 1] = in_word[i2 * 3 + 1];
                    out_word[i2 * 3 + 2] = in_word[i2 * 3 + 0];
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == depth) && (with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 4 + 0] = in_word[i2 * 4 + 2];
                    out_word[i2 * 4 + 1] = in_word[i2 * 4 + 1];
                    out_word[i2 * 4 + 2] = in_word[i2 * 4 + 0];
                    out_word[i2 * 4 + 3] = in_word[i2 * 4 + 3];
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 10) && (!with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 3 + 0] = in_word[i2 * 3 + 2] >> 6;
                    out_word[i2 * 3 + 1] = in_word[i2 * 3 + 1] >> 6;
                    out_word[i2 * 3 + 2] = in_word[i2 * 3 + 0] >> 6;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 10) && (with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 4 + 0] = in_word[i2 * 4 + 2] >> 6;
                    out_word[i2 * 4 + 1] = in_word[i2 * 4 + 1] >> 6;
                    out_word[i2 * 4 + 2] = in_word[i2 * 4 + 0] >> 6;
                    out_word[i2 * 4 + 3] = in_word[i2 * 4 + 3] >> 6;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 12) && (!with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 3 + 0] = in_word[i2 * 3 + 2] >> 4;
                    out_word[i2 * 3 + 1] = in_word[i2 * 3 + 1] >> 4;
                    out_word[i2 * 3 + 2] = in_word[i2 * 3 + 0] >> 4;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else if ((depth_in == 16) && (depth == 12) && (with_alpha))
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    out_word[i2 * 4 + 0] = in_word[i2 * 4 + 2] >> 4;
                    out_word[i2 * 4 + 1] = in_word[i2 * 4 + 1] >> 4;
                    out_word[i2 * 4 + 2] = in_word[i2 * 4 + 0] >> 4;
                    out_word[i2 * 4 + 3] = in_word[i2 * 4 + 3] >> 4;
                }
                in_word += stride_in / 2;
                out_word += stride_out / 2;
            }
        else
            invalid_mode = 1;
    }
    Py_END_ALLOW_THREADS
    PyBuffer_Release(&buffer);
    if (invalid_mode) {
        PyErr_SetString(PyExc_ValueError, "invalid plane mode value");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_add_plane_la(CtxWriteImageObject* self, PyObject* args) {
    /* (size), depth: int, depth_in: int, data: bytes */
    int width, height, depth, depth_in, stride_y, stride_alpha, stride_in, real_stride;
    Py_buffer buffer;
    uint8_t *plane_data_y, *plane_data_alpha;

    if (!PyArg_ParseTuple(args, "(ii)iiy*i", &width, &height, &depth, &depth_in, &buffer, &stride_in))
        return NULL;

    real_stride = width * 2;
    if (depth > 8)
        real_stride = real_stride * 2;
    if (stride_in == 0)
        stride_in = real_stride;
    if ((Py_ssize_t)stride_in * height > buffer.len) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError, "image plane does not contain enough data");
        return NULL;
    }

    if (check_error(heif_image_add_plane(self->image, heif_channel_Y, width, height, depth))) {
        PyBuffer_Release(&buffer);
        return NULL;
    }

    if (check_error(heif_image_add_plane(self->image, heif_channel_Alpha, width, height, depth))) {
        PyBuffer_Release(&buffer);
        return NULL;
    }

    plane_data_y = heif_image_get_plane(self->image, heif_channel_Y, &stride_y);
    if (!plane_data_y) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_RuntimeError, "heif_image_get_plane(Y) failed");
        return NULL;
    }

    plane_data_alpha = heif_image_get_plane(self->image, heif_channel_Alpha, &stride_alpha);
    if (!plane_data_alpha) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_RuntimeError, "heif_image_get_plane(Alpha) failed");
        return NULL;
    }

    int invalid_mode = 0;
    Py_BEGIN_ALLOW_THREADS
    uint16_t *out_word_y = (uint16_t *)plane_data_y;
    uint16_t *out_word_alpha = (uint16_t *)plane_data_alpha;
    uint16_t *in_word = (uint16_t *)buffer.buf;
    if ((depth_in == depth) && (depth <= 8)) {
        uint8_t *out_y = plane_data_y;
        uint8_t *out_alpha = plane_data_alpha;
        uint8_t *in = buffer.buf;
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                out_y[i2] = in[i2 * 2 + 0];
                out_alpha[i2] = in[i2 * 2 + 1];
            }
            in += stride_in;
            out_y += stride_y;
            out_alpha += stride_alpha;
        }
    }
    else if (depth_in == depth) {
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                out_word_y[i2] = in_word[i2 * 2 + 0];
                out_word_alpha[i2] = in_word[i2 * 2 + 1];
            }
            in_word += stride_in / 2;
            out_word_y += stride_y / 2;
            out_word_alpha += stride_alpha / 2;
        }
    }
    else if ((depth_in == 16) && (depth == 10))
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                out_word_y[i2] = in_word[i2 * 2 + 0] >> 6;
                out_word_alpha[i2] = in_word[i2 * 2 + 1] >> 6;
            }
            in_word += stride_in / 2;
            out_word_y += stride_y / 2;
            out_word_alpha += stride_alpha / 2;
        }
    else if ((depth_in == 16) && (depth == 12))
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                out_word_y[i2] = in_word[i2 * 2 + 0] >> 4;
                out_word_alpha[i2] = in_word[i2 * 2 + 1] >> 4;
            }
            in_word += stride_in / 2;
            out_word_y += stride_y / 2;
            out_word_alpha += stride_alpha / 2;
        }
    else
        invalid_mode = 1;
    Py_END_ALLOW_THREADS
    PyBuffer_Release(&buffer);
    if (invalid_mode) {
        PyErr_SetString(PyExc_ValueError, "invalid plane mode value");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_add_plane_l(CtxWriteImageObject* self, PyObject* args) {
    /* (size), depth: int, depth_in: int, data: bytes */
    int width, height, depth, depth_in, stride_out, stride_in, real_stride, target_heif_channel;
    Py_buffer buffer;
    uint8_t *plane_data;

    if (!PyArg_ParseTuple(args, "(ii)iiy*ii", &width, &height, &depth, &depth_in, &buffer, &stride_in, &target_heif_channel))
        return NULL;

    real_stride = width;
    if (depth > 8)
        real_stride = real_stride * 2;
    if (stride_in == 0)
        stride_in = real_stride;
    if ((Py_ssize_t)stride_in * height > buffer.len) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_ValueError, "image plane does not contain enough data");
        return NULL;
    }

    if (check_error(heif_image_add_plane(self->image, target_heif_channel, width, height, depth))) {
        PyBuffer_Release(&buffer);
        return NULL;
    }

    plane_data = heif_image_get_plane(self->image, target_heif_channel, &stride_out);
    if (!plane_data) {
        PyBuffer_Release(&buffer);
        PyErr_SetString(PyExc_RuntimeError, "heif_image_get_plane(Y) failed");
        return NULL;
    }

    int invalid_mode = 0;
    Py_BEGIN_ALLOW_THREADS
    uint8_t *out = plane_data;
    uint8_t *in = buffer.buf;
    uint16_t *out_word = (uint16_t *)plane_data;
    uint16_t *in_word = (uint16_t *)buffer.buf;
    if ((depth_in == depth) && (stride_in == stride_out))
        memcpy(out, in, stride_out * height);
    else if ((depth_in == depth) && (stride_in != stride_out))
        for (int i = 0; i < height; i++)
            memcpy(out + stride_out * i, in + stride_in * i, real_stride);
    else if ((depth_in == 16) && (depth == 10))
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++)
                out_word[i2] = in_word[i2] >> 6;
            in_word += stride_in / 2;
            out_word += stride_out / 2;
        }
    else if ((depth_in == 16) && (depth == 12))
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++)
                out_word[i2] = in_word[i2] >> 4;
            in_word += stride_in / 2;
            out_word += stride_out / 2;
        }
    else
        invalid_mode = 1;
    Py_END_ALLOW_THREADS
    PyBuffer_Release(&buffer);
    if (invalid_mode) {
        PyErr_SetString(PyExc_ValueError, "invalid plane mode value");
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_set_icc_profile(CtxWriteImageObject* self, PyObject* args) {
    /* type: str, color_profile: bytes */
    const char* type;
    Py_buffer buffer;
    struct heif_error error;

    if (!PyArg_ParseTuple(args, "sy*", &type, &buffer))
        return NULL;

    error = heif_image_set_raw_color_profile(self->image, type, buffer.buf, (int)buffer.len);
    PyBuffer_Release(&buffer);
    if (check_error(error))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_set_nclx_profile(CtxWriteImageObject* self, PyObject* args) {
    /* color_primaries: int, transfer_characteristics: int, matrix_coefficients: int, full_range_flag: int */
    struct heif_error error;
    int color_primaries, transfer_characteristics, matrix_coefficients, full_range_flag;

    if (!PyArg_ParseTuple(args, "iiii",
        &color_primaries, &transfer_characteristics, &matrix_coefficients, &full_range_flag))
        return NULL;

    struct heif_color_profile_nclx* nclx_color_profile = heif_nclx_color_profile_alloc();
    nclx_color_profile->color_primaries = color_primaries;
    nclx_color_profile->transfer_characteristics = transfer_characteristics;
    nclx_color_profile->matrix_coefficients = matrix_coefficients;
    nclx_color_profile->full_range_flag = full_range_flag;
    error = heif_image_set_nclx_color_profile(self->image, nclx_color_profile);
    heif_nclx_color_profile_free(nclx_color_profile);
    if (check_error(error))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_set_pixel_aspect_ratio(CtxWriteImageObject* self, PyObject* args) {
    unsigned int aspect_h, aspect_v;
    if (!PyArg_ParseTuple(args, "II", &aspect_h, &aspect_v))
        return NULL;
    heif_image_set_pixel_aspect_ratio(self->image, aspect_h, aspect_v);
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_encode(CtxWriteImageObject* self, PyObject* args) {
    /* ctx: CtxWriteObject, primary: int */
    CtxWriteObject* ctx_write;
    int primary, image_orientation,
        save_nclx, color_primaries, transfer_characteristics, matrix_coefficients, full_range_flag;
    struct heif_error error;
    struct heif_encoding_options* options;

    if (!PyArg_ParseTuple(args, "Oiiiiiii",
        (PyObject*)&ctx_write, &primary,
        &save_nclx, &color_primaries, &transfer_characteristics, &matrix_coefficients, &full_range_flag,
        &image_orientation
    ))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    options = heif_encoding_options_alloc();
    options->macOS_compatibility_workaround_no_nclx_profile = !save_nclx;
    if (
        (color_primaries != -1) ||
        (transfer_characteristics != -1) ||
        (matrix_coefficients != -1) ||
        (full_range_flag != -1)
       ) {
        options->output_nclx_profile = heif_nclx_color_profile_alloc();
        if (color_primaries != -1)
            options->output_nclx_profile->color_primaries = color_primaries;
        if (transfer_characteristics != -1)
            options->output_nclx_profile->transfer_characteristics = transfer_characteristics;
        if (matrix_coefficients != -1)
            options->output_nclx_profile->matrix_coefficients = matrix_coefficients;
        if (full_range_flag != -1)
            options->output_nclx_profile->full_range_flag = full_range_flag;
    }
    options->image_orientation = image_orientation;
    error = heif_context_encode_image(ctx_write->ctx, self->image, ctx_write->encoder, options, &self->handle);
    if (options->output_nclx_profile)
        heif_nclx_color_profile_free(options->output_nclx_profile);
    heif_encoding_options_free(options);
    Py_END_ALLOW_THREADS
    if (check_error(error))
        return NULL;

    if (primary)
        heif_context_set_primary_image(ctx_write->ctx, self->handle);
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_set_exif(CtxWriteImageObject* self, PyObject* args) {
    /* ctx: CtxWriteObject, data: bytes */
    CtxWriteObject* ctx_write;
    Py_buffer buffer;
    struct heif_error error;

    if (!PyArg_ParseTuple(args, "Oy*", (PyObject*)&ctx_write, &buffer))
        return NULL;

    error = heif_context_add_exif_metadata(ctx_write->ctx, self->handle, buffer.buf, (int)buffer.len);
    PyBuffer_Release(&buffer);
    if (check_error(error))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_set_xmp(CtxWriteImageObject* self, PyObject* args) {
    /* ctx: CtxWriteObject, data: bytes */
    CtxWriteObject* ctx_write;
    Py_buffer buffer;
    struct heif_error error;

    if (!PyArg_ParseTuple(args, "Oy*", (PyObject*)&ctx_write, &buffer))
        return NULL;

    error = heif_context_add_XMP_metadata(ctx_write->ctx, self->handle, buffer.buf, (int)buffer.len);
    PyBuffer_Release(&buffer);
    if (check_error(error))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_set_metadata(CtxWriteImageObject* self, PyObject* args) {
    /* ctx: CtxWriteObject, type: str, content_type: str, data: bytes */
    CtxWriteObject* ctx_write;
    const char *type, *content_type;
    Py_buffer buffer;
    struct heif_error error;

    if (!PyArg_ParseTuple(args, "Ossy*", (PyObject*)&ctx_write, &type, &content_type, &buffer))
        return NULL;

    error = heif_context_add_generic_metadata(ctx_write->ctx, self->handle, buffer.buf, (int)buffer.len, type, content_type);
    PyBuffer_Release(&buffer);
    if (check_error(error))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_encode_thumbnail(CtxWriteImageObject* self, PyObject* args) {
    /* ctx: CtxWriteObject, thumb_box: int */
    struct heif_error error;
    struct heif_image_handle* thumb_handle;
    struct heif_encoding_options* options;
    CtxWriteObject* ctx_write;
    int thumb_box, image_orientation;

    if (!PyArg_ParseTuple(args, "Oii", (PyObject*)&ctx_write, &thumb_box, &image_orientation))
        return NULL;

    Py_BEGIN_ALLOW_THREADS
    options = heif_encoding_options_alloc();
    options->image_orientation = image_orientation;
    error = heif_context_encode_thumbnail(
        ctx_write->ctx,
        self->image,
        self->handle,
        ctx_write->encoder,
        options,
        thumb_box,
        &thumb_handle);
    heif_encoding_options_free(options);
    Py_END_ALLOW_THREADS
    if (check_error(error))
        return NULL;
    heif_image_handle_release(thumb_handle);
    Py_RETURN_NONE;
}

static struct PyMethodDef _CtxWriteImage_methods[] = {
    {"add_plane", (PyCFunction)_CtxWriteImage_add_plane, METH_VARARGS},
    {"add_plane_l", (PyCFunction)_CtxWriteImage_add_plane_l, METH_VARARGS},
    {"add_plane_la", (PyCFunction)_CtxWriteImage_add_plane_la, METH_VARARGS},
    {"set_icc_profile", (PyCFunction)_CtxWriteImage_set_icc_profile, METH_VARARGS},
    {"set_nclx_profile", (PyCFunction)_CtxWriteImage_set_nclx_profile, METH_VARARGS},
    {"set_pixel_aspect_ratio", (PyCFunction)_CtxWriteImage_set_pixel_aspect_ratio, METH_VARARGS},
    {"encode", (PyCFunction)_CtxWriteImage_encode, METH_VARARGS},
    {"set_exif", (PyCFunction)_CtxWriteImage_set_exif, METH_VARARGS},
    {"set_xmp", (PyCFunction)_CtxWriteImage_set_xmp, METH_VARARGS},
    {"set_metadata", (PyCFunction)_CtxWriteImage_set_metadata, METH_VARARGS},
    {"encode_thumbnail", (PyCFunction)_CtxWriteImage_encode_thumbnail, METH_VARARGS},
    {NULL, NULL}
};

/* =========== CtxWrite ======== */

static struct heif_error ctx_write_callback(struct heif_context* ctx, const void* data, size_t size, void* userdata) {
    *((PyObject**)userdata) = PyBytes_FromStringAndSize((char*)data, size);
    return heif_error_no;
}

static struct heif_writer ctx_writer = { .writer_api_version = 1, .write = &ctx_write_callback };

static void _CtxWrite_destructor(CtxWriteObject* self) {
    if (self->data)
        free(self->data);
    if (self->encoder)
        heif_encoder_release(self->encoder);
    heif_context_free(self->ctx);
    PyObject_Del(self);
}

static PyObject* _CtxWrite_set_parameter(CtxWriteObject* self, PyObject* args) {
    char *key, *value;
    if (!PyArg_ParseTuple(args, "ss", &key, &value))
        return NULL;
    if (check_error(heif_encoder_set_parameter(self->encoder, key, value)))
        return NULL;
    Py_RETURN_NONE;
}

static PyObject* _CtxWriteImage_create(CtxWriteObject* self, PyObject* args) {
    /* (size), color: int, chroma: int, premultiplied: int */
    struct heif_image* image;
    int width, height, colorspace, chroma, premultiplied;
    if (!PyArg_ParseTuple(args, "(ii)iii", &width, &height, &colorspace, &chroma, &premultiplied))
        return NULL;

    if (check_error(heif_image_create(width, height, colorspace, chroma, &image)))
        return NULL;
    if (premultiplied)
        heif_image_set_premultiplied_alpha(image, 1);

    CtxWriteImageObject* ctx_write_image = PyObject_New(CtxWriteImageObject, &CtxWriteImage_Type);
    if (!ctx_write_image) {
        heif_image_release(image);
        return NULL;
    }
    ctx_write_image->chroma = chroma;
    ctx_write_image->image = image;
    ctx_write_image->handle = NULL;
    ctx_write_image->output_nclx_color_profile = NULL;
    return (PyObject*)ctx_write_image;
}

static PyObject* _CtxWrite_finalize(CtxWriteObject* self) {
    PyObject *ret = NULL;
    struct heif_error error = heif_context_write(self->ctx, &ctx_writer, &ret);
    if (!check_error(error)) {
        if (ret != NULL)
            return ret;
        PyErr_SetString(PyExc_RuntimeError, "Unknown runtime or memory error");
    }
    return NULL;
}

static struct PyMethodDef _CtxWrite_methods[] = {
    {"set_parameter", (PyCFunction)_CtxWrite_set_parameter, METH_VARARGS},
    {"create_image", (PyCFunction)_CtxWriteImage_create, METH_VARARGS},
    {"finalize", (PyCFunction)_CtxWrite_finalize, METH_NOARGS},
    {NULL, NULL}
};

/* =========== CtxAuxImage ======== */

static const char* _colorspace_to_str(enum heif_colorspace colorspace) {
    switch (colorspace) {
        case heif_colorspace_undefined:
            return "undefined";
        case heif_colorspace_monochrome:
            return "monochrome";
        case heif_colorspace_RGB:
            return "RGB";
        case heif_colorspace_YCbCr:
            return "YCbCr";
        default:  // note: this means the upstream API has changed
            return "unknown";
    }
}

PyObject* _CtxAuxImage(struct heif_image_handle* main_handle, heif_item_id aux_image_id,
                       int remove_stride, int hdr_to_16bit, PyObject* file_bytes,
                       const char* decoder_id) {
    struct heif_image_handle* aux_handle;
    if (check_error(heif_image_handle_get_auxiliary_image_handle(main_handle, aux_image_id, &aux_handle))) {
        return NULL;
    }
    int luma_bits = heif_image_handle_get_luma_bits_per_pixel(aux_handle);
    enum heif_colorspace colorspace;
    enum heif_chroma chroma;
    if (check_error(heif_image_handle_get_preferred_decoding_colorspace(aux_handle, &colorspace, &chroma))) {
        heif_image_handle_release(aux_handle);
        return NULL;
    }
    if (luma_bits != 8) {
        PyErr_Format(
            PyExc_NotImplementedError,
            "Only 8-bit AUX images are currently supported. Got %d-bit image.",
            luma_bits);
        heif_image_handle_release(aux_handle);
        return NULL;
    }

    CtxImageObject *ctx_image = PyObject_New(CtxImageObject, &CtxImage_Type);
    if (!ctx_image) {
        heif_image_handle_release(aux_handle);
        return NULL;
    }

    if (colorspace == heif_colorspace_monochrome) {
        ctx_image->colorspace = heif_colorspace_monochrome;
        ctx_image->chroma = heif_chroma_monochrome;
        strcpy(ctx_image->mode, "L");
        ctx_image->n_channels = 1;
    }
    else if (colorspace == heif_colorspace_YCbCr) {
        ctx_image->colorspace = heif_colorspace_RGB;
        ctx_image->chroma = heif_chroma_interleaved_RGB;
        strcpy(ctx_image->mode, "RGB");
        ctx_image->n_channels = 3;
    }
    else {
        const char* colorspace_str = _colorspace_to_str(colorspace);
        PyErr_Format(
            PyExc_NotImplementedError,
            "Only monochrome or YCbCr auxiliary images are currently supported. Got %d-bit %s image. "
            "Please consider filing an issue with an example HEIF file.",
            luma_bits, colorspace_str);
        heif_image_handle_release(aux_handle);
        PyObject_Del(ctx_image);
        return NULL;
    }

    ctx_image->depth_metadata = NULL;
    ctx_image->image_type = PhHeifImage;
    ctx_image->width = heif_image_handle_get_width(aux_handle);
    ctx_image->height = heif_image_handle_get_height(aux_handle);
    ctx_image->alpha = 0;
    ctx_image->bits = 8;
    ctx_image->hdr_to_8bit = 0;
    ctx_image->bgr_mode = 0;
    ctx_image->handle = aux_handle;
    ctx_image->heif_image = NULL;
    ctx_image->data = NULL;
    ctx_image->remove_stride = remove_stride;
    ctx_image->hdr_to_16bit = hdr_to_16bit;
    ctx_image->reload_size = 1;
    ctx_image->file_bytes = file_bytes;
    ctx_image->stride = get_stride(ctx_image);
    strcpy(ctx_image->decoder_id, decoder_id);
#ifdef Py_GIL_DISABLED
    ctx_image->decode_mutex = (PyMutex){0};
#endif
    Py_INCREF(file_bytes);
    return (PyObject*)ctx_image;
}

/* =========== CtxDepthImage ======== */

PyObject* _CtxDepthImage(struct heif_image_handle* main_handle, heif_item_id depth_image_id,
                         int remove_stride, int hdr_to_16bit, PyObject* file_bytes,
                         const char* decoder_id) {
    struct heif_image_handle* depth_handle;
    if (check_error(heif_image_handle_get_depth_image_handle(main_handle, depth_image_id, &depth_handle))) {
        return NULL;
    }
    CtxImageObject *ctx_image = PyObject_New(CtxImageObject, &CtxImage_Type);
    if (!ctx_image) {
        heif_image_handle_release(depth_handle);
        return NULL;
    }
    if (!heif_image_handle_get_depth_image_representation_info(main_handle, depth_image_id, &ctx_image->depth_metadata))
        ctx_image->depth_metadata = NULL;
    ctx_image->image_type = PhHeifDepthImage;
    ctx_image->width = heif_image_handle_get_width(depth_handle);
    ctx_image->height = heif_image_handle_get_height(depth_handle);
    ctx_image->alpha = 0;
    ctx_image->n_channels = 1;
    ctx_image->bits = heif_image_handle_get_luma_bits_per_pixel(depth_handle);
    strcpy(ctx_image->mode, "L");
    if (ctx_image->bits > 8) {
        if (hdr_to_16bit) {
            strcpy(ctx_image->mode, "I;16");
        }
        else if (ctx_image->bits == 10) {
            strcpy(ctx_image->mode, "I;10");
        }
        else {
            strcpy(ctx_image->mode, "I;12");
        }
    }
    ctx_image->hdr_to_8bit = 0;
    ctx_image->bgr_mode = 0;
    ctx_image->colorspace = heif_colorspace_monochrome;
    ctx_image->chroma = heif_chroma_monochrome;
    ctx_image->handle = depth_handle;
    ctx_image->heif_image = NULL;
    ctx_image->data = NULL;
    ctx_image->remove_stride = remove_stride;
    ctx_image->hdr_to_16bit = hdr_to_16bit;
    ctx_image->reload_size = 1;
    ctx_image->file_bytes = file_bytes;
    ctx_image->stride = get_stride(ctx_image);
    strcpy(ctx_image->decoder_id, decoder_id);
#ifdef Py_GIL_DISABLED
    ctx_image->decode_mutex = (PyMutex){0};
#endif
    Py_INCREF(file_bytes);
    return (PyObject*)ctx_image;
}

/* =========== CtxImage ======== */

static void _CtxImage_destructor(CtxImageObject* self) {
    if (self->heif_image)
        heif_image_release(self->heif_image);
    if (self->handle)
        heif_image_handle_release(self->handle);
    if (self->depth_metadata)
        heif_depth_representation_info_free(self->depth_metadata);
    Py_DECREF(self->file_bytes);
    PyObject_Del(self);
}

PyObject* _CtxImage(struct heif_image_handle* handle, int hdr_to_8bit,
                    int bgr_mode, int remove_stride, int hdr_to_16bit,
                    int reload_size, int primary, PyObject* file_bytes,
                    const char *decoder_id,
                    enum heif_colorspace colorspace, enum heif_chroma chroma
                    ) {
    CtxImageObject *ctx_image = PyObject_New(CtxImageObject, &CtxImage_Type);
    if (!ctx_image) {
        heif_image_handle_release(handle);
        return NULL;
    }
    ctx_image->depth_metadata = NULL;
    ctx_image->image_type = PhHeifImage;
    ctx_image->width = heif_image_handle_get_width(handle);
    ctx_image->height = heif_image_handle_get_height(handle);
    ctx_image->alpha = heif_image_handle_has_alpha_channel(handle);
    ctx_image->bits = heif_image_handle_get_luma_bits_per_pixel(handle);
    if ((chroma == heif_chroma_monochrome) && (colorspace == heif_colorspace_monochrome) && (!ctx_image->alpha)) {
        strcpy(ctx_image->mode, "L");
        if (ctx_image->bits > 8) {
            if (hdr_to_16bit) {
                strcpy(ctx_image->mode, "I;16");
            }
            else if (ctx_image->bits == 10) {
                strcpy(ctx_image->mode, "I;10");
            }
            else {
                strcpy(ctx_image->mode, "I;12");
            }
        }
        ctx_image->n_channels = 1;
        bgr_mode = 0;
        hdr_to_8bit = 0;
    } else {
        strcpy(ctx_image->mode, bgr_mode ? "BGR" : "RGB");
        ctx_image->n_channels = 3;
        if (ctx_image->alpha) {
            strcat(ctx_image->mode, heif_image_handle_is_premultiplied_alpha(handle) ? "a" : "A");
            ctx_image->n_channels += 1;
        }
        if ((ctx_image->bits > 8) && (!hdr_to_8bit)) {
            if (hdr_to_16bit) {
                strcat(ctx_image->mode, ";16");
            }
            else if (ctx_image->bits == 10) {
                strcat(ctx_image->mode, ";10");
            }
            else {
                strcat(ctx_image->mode, ";12");
            }
        }
    }
    ctx_image->hdr_to_8bit = hdr_to_8bit;
    ctx_image->bgr_mode = bgr_mode;
    ctx_image->handle = handle;
    ctx_image->heif_image = NULL;
    ctx_image->data = NULL;
    ctx_image->remove_stride = remove_stride;
    ctx_image->hdr_to_16bit = hdr_to_16bit;
    ctx_image->reload_size = reload_size;
    ctx_image->primary = primary;
    ctx_image->colorspace = colorspace;
    ctx_image->chroma = chroma;
    ctx_image->file_bytes = file_bytes;
    ctx_image->stride = get_stride(ctx_image);
    strcpy(ctx_image->decoder_id, decoder_id);
#ifdef Py_GIL_DISABLED
    ctx_image->decode_mutex = (PyMutex){0};
#endif
    Py_INCREF(file_bytes);
    return (PyObject*)ctx_image;
}

static PyObject* _CtxImage_size_mode(CtxImageObject* self, void* closure) {
    return Py_BuildValue("(ii)s", self->width, self->height, self->mode);
}

static PyObject* _CtxImage_primary(CtxImageObject* self, void* closure) {
    return Py_BuildValue("i", self->primary);
}

static PyObject* _CtxImage_bit_depth(CtxImageObject* self, void* closure) {
    return Py_BuildValue("i", self->bits);
}

static PyObject* _CtxImage_colorspace(CtxImageObject* self, void* closure) {
    return Py_BuildValue("i", self->colorspace);
}

static PyObject* _CtxImage_chroma(CtxImageObject* self, void* closure) {
    return Py_BuildValue("i", self->chroma);
}

static PyObject* _CtxImage_color_profile(CtxImageObject* self, void* closure) {
    enum heif_color_profile_type profile_type = heif_image_handle_get_color_profile_type(self->handle);
    if (profile_type == heif_color_profile_type_not_present)
        return PyDict_New();

    if (profile_type == heif_color_profile_type_nclx) {
        struct heif_color_profile_nclx* nclx_profile;
        if (check_error(heif_image_handle_get_nclx_color_profile(self->handle, &nclx_profile)))
            return NULL;

        PyObject* result = PyDict_New();
        if (!result) {
            heif_nclx_color_profile_free(nclx_profile);
            return NULL;
        }
        __PyDict_SetItemString(result, "type", PyUnicode_FromString("nclx"));
        PyObject* d = PyDict_New();
        if (!d) {
            heif_nclx_color_profile_free(nclx_profile);
            Py_DECREF(result);
            return NULL;
        }
        __PyDict_SetItemString(d, "color_primaries", PyLong_FromLong(nclx_profile->color_primaries));
        __PyDict_SetItemString(d, "transfer_characteristics", PyLong_FromLong(nclx_profile->transfer_characteristics));
        __PyDict_SetItemString(d, "matrix_coefficients", PyLong_FromLong(nclx_profile->matrix_coefficients));
        __PyDict_SetItemString(d, "full_range_flag", PyLong_FromLong(nclx_profile->full_range_flag));
        __PyDict_SetItemString(d, "color_primary_red_x", PyFloat_FromDouble(nclx_profile->color_primary_red_x));
        __PyDict_SetItemString(d, "color_primary_red_y", PyFloat_FromDouble(nclx_profile->color_primary_red_y));
        __PyDict_SetItemString(d, "color_primary_green_x", PyFloat_FromDouble(nclx_profile->color_primary_green_x));
        __PyDict_SetItemString(d, "color_primary_green_y", PyFloat_FromDouble(nclx_profile->color_primary_green_y));
        __PyDict_SetItemString(d, "color_primary_blue_x", PyFloat_FromDouble(nclx_profile->color_primary_blue_x));
        __PyDict_SetItemString(d, "color_primary_blue_y", PyFloat_FromDouble(nclx_profile->color_primary_blue_y));
        __PyDict_SetItemString(d, "color_primary_white_x", PyFloat_FromDouble(nclx_profile->color_primary_white_x));
        __PyDict_SetItemString(d, "color_primary_white_y", PyFloat_FromDouble(nclx_profile->color_primary_white_y));
        heif_nclx_color_profile_free(nclx_profile);
        __PyDict_SetItemString(result, "data", d);
        return result;
    }

    PyObject* result = PyDict_New();
    if (!result) {
        return NULL;
    }
    __PyDict_SetItemString(
        result, "type", PyUnicode_FromString(profile_type == heif_color_profile_type_rICC ? "rICC" : "prof"));
    size_t size = heif_image_handle_get_raw_color_profile_size(self->handle);
    if (!size)
        __PyDict_SetItemString(result, "data", PyBytes_FromString(""));
    else {
        void* data = malloc(size);
        if (!data) {
            Py_DECREF(result);
            result = NULL;
            PyErr_NoMemory();
        }
        else {
            if (!check_error(heif_image_handle_get_raw_color_profile(self->handle, data)))
                __PyDict_SetItemString(result, "data", PyBytes_FromStringAndSize(data, size));
            else {
                Py_DECREF(result);
                result = NULL;
            }
            free(data);
        }
    }
    return result;
}

static PyObject* _CtxImage_metadata(CtxImageObject* self, void* closure) {
    if (self->image_type == PhHeifImage) {
        PyObject *meta_item_info;
        const char *type, *content_type;
        size_t size;
        void* data;
        struct heif_error error;

        int n_metas = heif_image_handle_get_number_of_metadata_blocks(self->handle, NULL);
        if (!n_metas)
            return PyList_New(0);

        heif_item_id* meta_ids  = (heif_item_id*)malloc(n_metas * sizeof(heif_item_id));
        if (!meta_ids)
            return PyErr_NoMemory();

        n_metas = heif_image_handle_get_list_of_metadata_block_IDs(self->handle, NULL, meta_ids, n_metas);
        PyObject* meta_list = PyList_New(n_metas);
        if (!meta_list) {
            free(meta_ids);
            return NULL;
        }

        for (int i = 0; i < n_metas; i++) {
            meta_item_info = NULL;
            type = heif_image_handle_get_metadata_type(self->handle, meta_ids[i]);
            content_type = heif_image_handle_get_metadata_content_type(self->handle, meta_ids[i]);
            size = heif_image_handle_get_metadata_size(self->handle, meta_ids[i]);
            data = malloc(size);
            if (!data) {
                Py_DECREF(meta_list);
                free(meta_ids);
                return PyErr_NoMemory();
            }
            error = heif_image_handle_get_metadata(self->handle, meta_ids[i], data);
            if (error.code == heif_error_Ok) {
                meta_item_info = PyDict_New();
                if (!meta_item_info) {
                    free(data);
                    Py_DECREF(meta_list);
                    free(meta_ids);
                    return NULL;
                }
                __PyDict_SetItemString(meta_item_info, "type", PyUnicode_FromString(type));
                __PyDict_SetItemString(meta_item_info, "content_type", PyUnicode_FromString(content_type));
                __PyDict_SetItemString(meta_item_info, "data", PyBytes_FromStringAndSize((char*)data, size));
            }
            free(data);
            if (!meta_item_info) {
                meta_item_info = Py_None;
                Py_INCREF(meta_item_info);
            }
            PyList_SET_ITEM(meta_list, i, meta_item_info);
        }
        free(meta_ids);
        return meta_list;
    }
    else if (self->image_type == PhHeifDepthImage) {
        PyObject* meta = PyDict_New();
        if (!meta)
            return NULL;
        if (!self->depth_metadata)
            return meta;

        if (self->depth_metadata->has_z_near)
            __PyDict_SetItemString(meta, "z_near", PyFloat_FromDouble(self->depth_metadata->z_near));
        if (self->depth_metadata->has_z_far)
            __PyDict_SetItemString(meta, "z_far", PyFloat_FromDouble(self->depth_metadata->z_far));
        if (self->depth_metadata->has_d_min)
            __PyDict_SetItemString(meta, "d_min", PyFloat_FromDouble(self->depth_metadata->d_min));
        if (self->depth_metadata->has_d_max)
            __PyDict_SetItemString(meta, "d_max", PyFloat_FromDouble(self->depth_metadata->d_max));

        __PyDict_SetItemString(
            meta, "representation_type", PyLong_FromUnsignedLong(self->depth_metadata->depth_representation_type));
        __PyDict_SetItemString(
            meta, "disparity_reference_view", PyLong_FromUnsignedLong(self->depth_metadata->disparity_reference_view));

        __PyDict_SetItemString(
            meta, "nonlinear_representation_model_size",
            PyLong_FromUnsignedLong(self->depth_metadata->depth_nonlinear_representation_model_size));
        // need an example file with this info. need info about depth_nonlinear_representation_model
        return meta;
    }
    Py_RETURN_NONE;
}

static PyObject* _CtxImage_thumbnails(CtxImageObject* self, void* closure) {
    int n_images = heif_image_handle_get_number_of_thumbnails(self->handle);
    if (n_images == 0)
        return PyList_New(0);
    heif_item_id* images_ids = (heif_item_id*)malloc(n_images * sizeof(heif_item_id));
    if (!images_ids)
        return PyList_New(0);

    n_images = heif_image_handle_get_list_of_thumbnail_IDs(self->handle, images_ids, n_images);
    PyObject* images_list = PyList_New(n_images);
    if (!images_list) {
        free(images_ids);
        return NULL;
    }

    struct heif_image_handle* handle;
    struct heif_error error;
    for (int i = 0; i < n_images; i++) {
        int box = 0;
        error = heif_image_handle_get_thumbnail(self->handle, images_ids[i], &handle);
        if (error.code == heif_error_Ok) {
            int width = heif_image_handle_get_width(handle);
            int height = heif_image_handle_get_height(handle);
            heif_image_handle_release(handle);
            box = width >= height ? width : height;
        }
        PyList_SET_ITEM(images_list, i, PyLong_FromSsize_t(box));
    }
    free(images_ids);
    return images_list;
}

int decode_image(CtxImageObject* self) {
    struct heif_error error;
    int bytes_in_cc;
    enum heif_colorspace colorspace;
    enum heif_chroma chroma;
    enum heif_channel channel;

    Py_BEGIN_ALLOW_THREADS
    struct heif_decoding_options *decode_options = heif_decoding_options_alloc();
    decode_options->convert_hdr_to_8bit = self->hdr_to_8bit;
    if (self->n_channels == 1) {
        channel = heif_channel_Y;
        colorspace = heif_colorspace_monochrome;
        chroma = heif_chroma_monochrome;
    }
    else {
        channel = heif_channel_interleaved;
        colorspace = heif_colorspace_RGB;
        if ((self->bits == 8) || (self->hdr_to_8bit)) {
            chroma = self->alpha ? heif_chroma_interleaved_RGBA : heif_chroma_interleaved_RGB;
        }
        else {
            chroma = self->alpha ? heif_chroma_interleaved_RRGGBBAA_LE : heif_chroma_interleaved_RRGGBB_LE;
        }
    }
    if ((self->bits == 8) || (self->hdr_to_8bit)) {
        bytes_in_cc = 1;
    }
    else {
        bytes_in_cc = 2;
    }

    if (strlen(self->decoder_id) > 0) {
        decode_options->decoder_id = self->decoder_id;
    }
    error = heif_decode_image(self->handle, &self->heif_image, colorspace, chroma, decode_options);
    heif_decoding_options_free(decode_options);
    Py_END_ALLOW_THREADS
    if (check_error(error))
        return 0;

    int stride;
    self->data = heif_image_get_plane(self->heif_image, channel, &stride);
    if (!self->data) {
        heif_image_release(self->heif_image);
        self->heif_image = NULL;
        PyErr_SetString(PyExc_RuntimeError, "heif_image_get_plane failed");
        return 0;
    }

    int decoded_width = heif_image_get_primary_width(self->heif_image);
    int decoded_height = heif_image_get_primary_height(self->heif_image);
    if (self->reload_size) {
        self->width = decoded_width;
        self->height = decoded_height;
    }
    else if ((self->width > decoded_width) || (self->height > decoded_height)) {
        heif_image_release(self->heif_image);
        self->heif_image = NULL;
        PyErr_Format(PyExc_ValueError,
                    "corrupted image(dimensions in header: (%d, %d), decoded dimensions: (%d, %d)). "
                    "Set ALLOW_INCORRECT_HEADERS to True if you need to load them.",
                    self->width, self->height, decoded_width, decoded_height);
        return 0;
    }

    self->stride = self->remove_stride ? get_stride(self) : stride;

    int remove_stride = ((self->remove_stride) && (self->stride != stride));
    int shift_size = ((self->hdr_to_16bit) && (self->bits > 8) && (!self->hdr_to_8bit)) ? 16 - self->bits : 0;

    if ((self->bgr_mode) && (!remove_stride))
        postprocess__bgr(self->width, self->height, self->data, stride,
                         bytes_in_cc, self->n_channels, shift_size);
    else if ((self->bgr_mode) && (remove_stride))
        postprocess__bgr_stride(self->width, self->height, self->data, stride, self->stride,
                                bytes_in_cc, self->n_channels, shift_size);
    else if ((!self->bgr_mode) && (!remove_stride))
        postprocess(self->width, self->height, self->data, stride,
                    bytes_in_cc, self->n_channels, shift_size);
    else if ((!self->bgr_mode) && (remove_stride))
        postprocess__stride(self->width, self->height, self->data, stride, self->stride,
                            bytes_in_cc, self->n_channels, shift_size);
    else {
        PyErr_SetString(PyExc_ValueError, "internal error, invalid postprocess condition");
        return 0;
    }
    return 1;
}

static PyObject* _CtxImage_stride(CtxImageObject* self, void* closure) {
    MUTEX_LOCK(&self->decode_mutex);
    if (!self->data) {
        if (!decode_image(self)) {
            MUTEX_UNLOCK(&self->decode_mutex);
            return NULL;
        }
    }
    MUTEX_UNLOCK(&self->decode_mutex);
    return PyLong_FromSsize_t(self->stride);
}

static PyObject* _CtxImage_data(CtxImageObject* self, void* closure) {
    MUTEX_LOCK(&self->decode_mutex);
    if (!self->data) {
        if (!decode_image(self)) {
            MUTEX_UNLOCK(&self->decode_mutex);
            return NULL;
        }
    }
    MUTEX_UNLOCK(&self->decode_mutex);
    return PyMemoryView_FromMemory((char*)self->data, self->stride * self->height, PyBUF_READ);
}

static PyObject* _CtxImage_depth_image_list(CtxImageObject* self, void* closure) {
    int n_images = heif_image_handle_get_number_of_depth_images(self->handle);
    if (n_images == 0)
        return PyList_New(0);
    heif_item_id* images_ids = (heif_item_id*)malloc(n_images * sizeof(heif_item_id));
    if (!images_ids)
        return PyErr_NoMemory();

    n_images = heif_image_handle_get_list_of_depth_image_IDs(self->handle, images_ids, n_images);
    PyObject* images_list = PyList_New(n_images);
    if (!images_list) {
        free(images_ids);
        return NULL;
    }

    for (int i = 0; i < n_images; i++) {
        PyObject* ctx_depth_image = _CtxDepthImage(
            self->handle, images_ids[i], self->remove_stride, self->hdr_to_16bit, self->file_bytes,
            self->decoder_id);
        if (!ctx_depth_image) {
            Py_DECREF(images_list);
            free(images_ids);
            return NULL;
        }
        PyList_SET_ITEM(images_list, i, ctx_depth_image);
    }
    free(images_ids);
    return images_list;
}

static PyObject* _CtxImage_aux_image_ids(CtxImageObject* self, void* closure) {
    int aux_filter = LIBHEIF_AUX_IMAGE_FILTER_OMIT_ALPHA | LIBHEIF_AUX_IMAGE_FILTER_OMIT_DEPTH;
    int n_images = heif_image_handle_get_number_of_auxiliary_images(self->handle, aux_filter);
    if (n_images == 0)
        return PyList_New(0);
    heif_item_id* images_ids = (heif_item_id*)malloc(n_images * sizeof(heif_item_id));
    if (!images_ids)
        return PyErr_NoMemory();

    n_images = heif_image_handle_get_list_of_auxiliary_image_IDs(self->handle, aux_filter, images_ids, n_images);
    PyObject* images_list = PyList_New(n_images);
    if (!images_list) {
        free(images_ids);
        return PyErr_NoMemory();
    }
    for (int i = 0; i < n_images; i++) {
        PyList_SET_ITEM(images_list, i, PyLong_FromUnsignedLong(images_ids[i]));
    }
    free(images_ids);
    return images_list;
}

static PyObject* _CtxImage_get_aux_image(CtxImageObject* self, PyObject* arg_image_id) {
    heif_item_id aux_image_id = (heif_item_id)PyLong_AsUnsignedLong(arg_image_id);
    return _CtxAuxImage(
        self->handle, aux_image_id, self->remove_stride, self->hdr_to_16bit, self->file_bytes,
        self->decoder_id
    );
}

static PyObject* _get_aux_type(const struct heif_image_handle* aux_handle) {
    const char* aux_type_c = NULL;
    struct heif_error error = heif_image_handle_get_auxiliary_type(aux_handle, &aux_type_c);
    if (check_error(error))
        return NULL;
    PyObject *aux_type = PyUnicode_FromString(aux_type_c);
    heif_image_handle_release_auxiliary_type(aux_handle, &aux_type_c);
    return aux_type;
}

static PyObject* _CtxImage_get_aux_type(CtxImageObject* self, PyObject* arg_image_id) {
    heif_item_id aux_image_id = (heif_item_id)PyLong_AsUnsignedLong(arg_image_id);
    struct heif_image_handle* aux_handle;
    if (check_error(heif_image_handle_get_auxiliary_image_handle(self->handle, aux_image_id, &aux_handle)))
        return NULL;
    PyObject* aux_type = _get_aux_type(aux_handle);
    if (!aux_type)
        return NULL;
    heif_image_handle_release(aux_handle);
    return aux_type;
}

static PyObject* _CtxImage_pixel_aspect_ratio(CtxImageObject* self, void* closure) {
    #if LIBHEIF_HAVE_VERSION(1,19,0)
        uint32_t aspect_h, aspect_v;
        int has_pasp = heif_image_handle_get_pixel_aspect_ratio(self->handle, &aspect_h, &aspect_v);
        if (has_pasp) {
            return Py_BuildValue("(II)", aspect_h, aspect_v);
        }
    #endif
    Py_RETURN_NONE;
}

/* =========== CtxImage Experimental Part ======== */

static PyObject* _CtxImage_camera_intrinsic_matrix(CtxImageObject* self, void* closure) {
    #if LIBHEIF_HAVE_VERSION(1,18,0)
        struct heif_camera_intrinsic_matrix camera_intrinsic_matrix;

        if (!heif_image_handle_has_camera_intrinsic_matrix(self->handle)) {
            Py_RETURN_NONE;
        }
        if (check_error(heif_image_handle_get_camera_intrinsic_matrix(self->handle, &camera_intrinsic_matrix))) {
            return NULL;
        }
        return Py_BuildValue(
            "(ddddd)",
            camera_intrinsic_matrix.focal_length_x,
            camera_intrinsic_matrix.focal_length_y,
            camera_intrinsic_matrix.principal_point_x,
            camera_intrinsic_matrix.principal_point_y,
            camera_intrinsic_matrix.skew
            );
    #else
        Py_RETURN_NONE;
    #endif
}

static PyObject* _CtxImage_camera_extrinsic_matrix_rot(CtxImageObject* self, void* closure) {
    #if LIBHEIF_HAVE_VERSION(1,18,0)
        struct heif_camera_extrinsic_matrix* camera_extrinsic_matrix;
        double rot[9];
        struct heif_error error;

        if (!heif_image_handle_has_camera_extrinsic_matrix(self->handle)) {
            Py_RETURN_NONE;
        }
        if (check_error(heif_image_handle_get_camera_extrinsic_matrix(self->handle, &camera_extrinsic_matrix))) {
            return NULL;
        }
        error = heif_camera_extrinsic_matrix_get_rotation_matrix(camera_extrinsic_matrix, rot);
        heif_camera_extrinsic_matrix_release(camera_extrinsic_matrix);
        if (check_error(error)) {
            return NULL;
        }
        return Py_BuildValue("(ddddddddd)", rot[0], rot[1], rot[2], rot[3], rot[4], rot[5], rot[6], rot[7], rot[8]);
    #else
        Py_RETURN_NONE;
    #endif
}

/* =========== CtxImage properties available to Python Part ======== */

static struct PyGetSetDef _CtxImage_getseters[] = {
    {"size_mode", (getter)_CtxImage_size_mode, NULL, NULL, NULL},
    {"primary", (getter)_CtxImage_primary, NULL, NULL, NULL},
    {"bit_depth", (getter)_CtxImage_bit_depth, NULL, NULL, NULL},
    {"colorspace", (getter)_CtxImage_colorspace, NULL, NULL, NULL},
    {"chroma", (getter)_CtxImage_chroma, NULL, NULL, NULL},
    {"color_profile", (getter)_CtxImage_color_profile, NULL, NULL, NULL},
    {"metadata", (getter)_CtxImage_metadata, NULL, NULL, NULL},
    {"thumbnails", (getter)_CtxImage_thumbnails, NULL, NULL, NULL},
    {"stride", (getter)_CtxImage_stride, NULL, NULL, NULL},
    {"data", (getter)_CtxImage_data, NULL, NULL, NULL},
    {"depth_image_list", (getter)_CtxImage_depth_image_list, NULL, NULL, NULL},
    {"aux_image_ids", (getter)_CtxImage_aux_image_ids, NULL, NULL, NULL},
    {"pixel_aspect_ratio", (getter)_CtxImage_pixel_aspect_ratio, NULL, NULL, NULL},
    {"camera_intrinsic_matrix", (getter)_CtxImage_camera_intrinsic_matrix, NULL, NULL, NULL},
    {"camera_extrinsic_matrix_rot", (getter)_CtxImage_camera_extrinsic_matrix_rot, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static struct PyMethodDef _CtxImage_methods[] = {
    {"get_aux_image", (PyCFunction)_CtxImage_get_aux_image, METH_O},
    {"get_aux_type", (PyCFunction)_CtxImage_get_aux_type, METH_O},
    {NULL, NULL}
};

/* =========== Functions ======== */

static PyObject* _CtxWrite(PyObject* self, PyObject* args) {
    /* compression_format: int, quality: int, encoder_id: str */
    struct heif_encoder* encoder;
    struct heif_error error;
    int compression_format, quality;
    const char *encoder_id;
    const struct heif_encoder_descriptor* encoders[1];

    if (!PyArg_ParseTuple(args, "iis", &compression_format, &quality, &encoder_id))
        return NULL;

    struct heif_context* ctx = heif_context_alloc();
    if ((strlen(encoder_id) > 0) &&
        (heif_get_encoder_descriptors(heif_compression_undefined, encoder_id, encoders, 1) == 1)
        ) {
        error = heif_context_get_encoder(ctx, encoders[0], &encoder);
    }
    else {
        error = heif_context_get_encoder_for_format(ctx, compression_format, &encoder);
    }

    if (check_error(error)) {
        heif_context_free(ctx);
        return NULL;
    }

    if (quality == -1)
        error = heif_encoder_set_lossless(encoder, 1);
    else if (quality >= 0)
        error = heif_encoder_set_lossy_quality(encoder, quality);
    if (check_error(error)) {
        heif_encoder_release(encoder);
        heif_context_free(ctx);
        return NULL;
    }

    CtxWriteObject* ctx_write = PyObject_New(CtxWriteObject, &CtxWrite_Type);
    if (!ctx_write) {
        heif_encoder_release(encoder);
        heif_context_free(ctx);
        return NULL;
    }
    ctx_write->ctx = ctx;
    ctx_write->encoder = encoder;
    ctx_write->size = 0;
    ctx_write->data = NULL;
    return (PyObject*)ctx_write;
}

static PyObject* _load_file(PyObject* self, PyObject* args) {
    int hdr_to_8bit, threads_count, bgr_mode, remove_stride, hdr_to_16bit, reload_size, disable_security_limits;
    PyObject *heif_bytes;
    const char *decoder_id;

    if (!PyArg_ParseTuple(args,
                          "Oiiiiiisi",
                          &heif_bytes,
                          &threads_count,
                          &hdr_to_8bit,
                          &bgr_mode,
                          &remove_stride,
                          &hdr_to_16bit,
                          &reload_size,
                          &decoder_id,
                          &disable_security_limits))
        return NULL;

    struct heif_context* heif_ctx = heif_context_alloc();

    #if LIBHEIF_HAVE_VERSION(1,19,0)
    if (disable_security_limits) {
        heif_context_set_security_limits(heif_ctx, heif_get_disabled_security_limits());
    }
    #endif

    if (check_error(heif_context_read_from_memory_without_copy(
                        heif_ctx, (void*)PyBytes_AS_STRING(heif_bytes), PyBytes_GET_SIZE(heif_bytes), NULL))) {
        heif_context_free(heif_ctx);
        return NULL;
    }

    heif_context_set_max_decoding_threads(heif_ctx, threads_count);

    heif_item_id primary_image_id;
    if (check_error(heif_context_get_primary_image_ID(heif_ctx, &primary_image_id))) {
        heif_context_free(heif_ctx);
        return NULL;
    }

    int n_images = heif_context_get_number_of_top_level_images(heif_ctx);
    heif_item_id* images_ids = (heif_item_id*)malloc(n_images * sizeof(heif_item_id));
    if (!images_ids) {
        heif_context_free(heif_ctx);
        return PyErr_NoMemory();
    }
    n_images = heif_context_get_list_of_top_level_image_IDs(heif_ctx, images_ids, n_images);
    PyObject* images_list = PyList_New(n_images);
    if (!images_list) {
        free(images_ids);
        heif_context_free(heif_ctx);
        return NULL;
    }

    enum heif_colorspace colorspace;
    enum heif_chroma chroma;
    struct heif_image_handle* handle;
    struct heif_error error;
    for (int i = 0; i < n_images; i++) {
        int primary = 0;
        if (images_ids[i] == primary_image_id) {
            error = heif_context_get_primary_image_handle(heif_ctx, &handle);
            primary = 1;
        }
        else
            error = heif_context_get_image_handle(heif_ctx, images_ids[i], &handle);
        if (error.code == heif_error_Ok) {
            error = heif_image_handle_get_preferred_decoding_colorspace(handle, &colorspace, &chroma);
            if (error.code == heif_error_Ok) {
                PyObject* ctx_image = _CtxImage(
                    handle, hdr_to_8bit, bgr_mode, remove_stride, hdr_to_16bit, reload_size, primary, heif_bytes,
                    decoder_id, colorspace, chroma);
                if (!ctx_image) {
                    Py_DECREF(images_list);
                    heif_image_handle_release(handle);
                    free(images_ids);
                    heif_context_free(heif_ctx);
                    return NULL;
                }
                PyList_SET_ITEM(images_list, i, ctx_image);
            } else {
                heif_image_handle_release(handle);
            }
        }
        if (error.code != heif_error_Ok) {
            Py_INCREF(Py_None);
            PyList_SET_ITEM(images_list, i, Py_None);
        }
    }
    free(images_ids);
    heif_context_free(heif_ctx);
    return images_list;
}

static PyObject* _get_lib_info(PyObject* self) {
    PyObject* lib_info_dict = PyDict_New();
    if (!lib_info_dict) {
        return NULL;
    }
    PyObject* encoders_dict = PyDict_New();
    if (!encoders_dict) {
        Py_DECREF(lib_info_dict);
        return NULL;
    }
    PyObject* decoders_dict = PyDict_New();
    if (!decoders_dict) {
        Py_DECREF(encoders_dict);
        Py_DECREF(lib_info_dict);
        return NULL;
    }
    __PyDict_SetItemString(lib_info_dict, "libheif", PyUnicode_FromString(heif_get_version()));

    const struct heif_encoder_descriptor* encoder_descriptor;
    const char* x265_version = "";
    if (heif_get_encoder_descriptors(heif_compression_HEVC, NULL, &encoder_descriptor, 1))
        x265_version = heif_encoder_descriptor_get_name(encoder_descriptor);
    __PyDict_SetItemString(lib_info_dict, "HEIF", PyUnicode_FromString(x265_version));
    const char* aom_version = "";
    if (heif_get_encoder_descriptors(heif_compression_AV1, NULL, &encoder_descriptor, 1))
        aom_version = heif_encoder_descriptor_get_name(encoder_descriptor);
    __PyDict_SetItemString(lib_info_dict, "AVIF", PyUnicode_FromString(aom_version));

    __PyDict_SetItemString(lib_info_dict, "encoders", encoders_dict);
    __PyDict_SetItemString(lib_info_dict, "decoders", decoders_dict);

    const struct heif_encoder_descriptor* encoders[MAX_ENCODERS];
    int encoders_count = heif_get_encoder_descriptors(heif_compression_undefined, NULL, encoders, MAX_ENCODERS);
    for (int i = 0; i < encoders_count; i++) {
        __PyDict_SetItemString(encoders_dict, heif_encoder_descriptor_get_id_name(encoders[i]), PyUnicode_FromString(heif_encoder_descriptor_get_name(encoders[i])));
    }

    const struct heif_decoder_descriptor* decoders[MAX_DECODERS];
    int decoders_count = heif_get_decoder_descriptors(heif_compression_undefined, decoders, MAX_DECODERS);
    for (int i = 0; i < decoders_count; i++) {
        __PyDict_SetItemString(decoders_dict, heif_decoder_descriptor_get_id_name(decoders[i]), PyUnicode_FromString(heif_decoder_descriptor_get_name(decoders[i])));
    }

    return lib_info_dict;
}

static PyObject* _load_plugins(PyObject* self, PyObject* args) {
    const char *plugins_directory;
    if (!PyArg_ParseTuple(args, "s", &plugins_directory))
        return NULL;

    struct heif_error error = heif_load_plugins(plugins_directory, NULL, NULL, 0);
    if (check_error(error)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject* _load_plugin(PyObject* self, PyObject* args) {
    const char *plugin_path;
    if (!PyArg_ParseTuple(args, "s", &plugin_path))
        return NULL;

    const struct heif_plugin_info* info = NULL;
    struct heif_error error = heif_load_plugin(plugin_path, &info);
    if (check_error(error)) {
        return NULL;
    }
    Py_RETURN_NONE;
}

/* =========== Module =========== */

static PyMethodDef heifMethods[] = {
    {"CtxWrite", (PyCFunction)_CtxWrite, METH_VARARGS},
    {"load_file", (PyCFunction)_load_file, METH_VARARGS},
    {"get_lib_info", (PyCFunction)_get_lib_info, METH_NOARGS},
    {"load_plugins", (PyCFunction)_load_plugins, METH_VARARGS},
    {"load_plugin", (PyCFunction)_load_plugin, METH_VARARGS},
    {NULL, NULL}
};

static PyTypeObject CtxWriteImage_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "CtxWriteImage",
    .tp_basicsize = sizeof(CtxWriteImageObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)_CtxWriteImage_destructor,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = _CtxWriteImage_methods,
};

static PyTypeObject CtxWrite_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "CtxWrite",
    .tp_basicsize = sizeof(CtxWriteObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)_CtxWrite_destructor,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = _CtxWrite_methods,
};

static PyTypeObject CtxImage_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "CtxImage",
    .tp_basicsize = sizeof(CtxImageObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)_CtxImage_destructor,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_getset = _CtxImage_getseters,
    .tp_methods = _CtxImage_methods,
};

static int setup_module(PyObject* m) {
    if (PyType_Ready(&CtxWriteImage_Type) < 0)
        return -1;

    if (PyType_Ready(&CtxWrite_Type) < 0)
        return -1;

    if (PyType_Ready(&CtxImage_Type) < 0)
        return -1;

    heif_init(NULL);
    return 0;
}

static PyModuleDef_Slot module_slots[] = {
    {Py_mod_exec, setup_module},
#ifdef Py_GIL_DISABLED
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}
};

PyMODINIT_FUNC PyInit__pillow_heif(void) {
    static PyModuleDef module_def = {
        PyModuleDef_HEAD_INIT,
        .m_name = "_pillow_heif",
        .m_methods = heifMethods,
        .m_slots = module_slots,
    };

    return PyModuleDef_Init(&module_def);
}
