/* =========== Decode postprocess stuff ======== */

void postprocess__bgr__byte(int width, int height, uint8_t* data, int stride, int channels) {
    uint8_t tmp;
    if (channels == 3) {
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                tmp = data[i2 * 3 + 0];
                data[i2 * 3 + 0] = data[i2 * 3 + 2];
                data[i2 * 3 + 2] = tmp;
            }
            data += stride;
        }
    }
    else {
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                tmp = data[i2 * 4 + 0];
                data[i2 * 4 + 0] = data[i2 * 4 + 2];
                data[i2 * 4 + 2] = tmp;
            }
            data += stride;
        }
    }
}

void postprocess__bgr__word(int width, int height, uint16_t* data, int stride, int channels, int shift_size) {
    uint16_t tmp;
    if (channels == 3) {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data[i2 * 3 + 0];
                    data[i2 * 3 + 0] = data[i2 * 3 + 2] << 4;
                    data[i2 * 3 + 1] = data[i2 * 3 + 1] << 4;
                    data[i2 * 3 + 2] = tmp << 4;
                }
                data += stride / 2;
            }
        }
        else if (shift_size == 6) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data[i2 * 3 + 0];
                    data[i2 * 3 + 0] = data[i2 * 3 + 2] << 6;
                    data[i2 * 3 + 1] = data[i2 * 3 + 1] << 6;
                    data[i2 * 3 + 2] = tmp << 6;
                }
                data += stride / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data[i2 * 3 + 0];
                    data[i2 * 3 + 0] = data[i2 * 3 + 2];
                    data[i2 * 3 + 2] = tmp;
                }
                data += stride / 2;
            }
        }
    }
    else {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data[i2 * 4 + 0];
                    data[i2 * 4 + 0] = data[i2 * 4 + 2] << 4;
                    data[i2 * 4 + 1] = data[i2 * 4 + 1] << 4;
                    data[i2 * 4 + 2] = tmp << 4;
                    data[i2 * 4 + 3] = data[i2 * 4 + 3] << 4;
                }
                data += stride / 2;
            }
        }
        else if (shift_size == 6) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data[i2 * 4 + 0];
                    data[i2 * 4 + 0] = data[i2 * 4 + 2] << 6;
                    data[i2 * 4 + 1] = data[i2 * 4 + 1] << 6;
                    data[i2 * 4 + 2] = tmp << 6;
                    data[i2 * 4 + 3] = data[i2 * 4 + 3] << 6;
                }
                data += stride / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data[i2 * 4 + 0];
                    data[i2 * 4 + 0] = data[i2 * 4 + 2];
                    data[i2 * 4 + 2] = tmp;
                }
                data += stride / 2;
            }
        }
    }
}

void postprocess__bgr_stride__byte(int width, int height, uint8_t* data, int stride_in, int stride_out, int channels) {
    uint8_t *data_in = data, *data_out = data, tmp;
    if (channels == 3) {
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                tmp = data_in[i2 * 3 + 0];
                data_out[i2 * 3 + 0] = data_in[i2 * 3 + 2];
                data_out[i2 * 3 + 1] = data_in[i2 * 3 + 1];
                data_out[i2 * 3 + 2] = tmp;
            }
            data_in += stride_in;
            data_out += stride_out;
        }
    }
    else {
        for (int i = 0; i < height; i++) {
            for (int i2 = 0; i2 < width; i2++) {
                tmp = data_in[i2 * 4 + 0];
                data_out[i2 * 4 + 0] = data_in[i2 * 4 + 2];
                data_out[i2 * 4 + 1] = data_in[i2 * 4 + 1];
                data_out[i2 * 4 + 2] = tmp;
                data_out[i2 * 4 + 3] = data_in[i2 * 4 + 3];
            }
            data_in += stride_in;
            data_out += stride_out;
        }
    }
}

void postprocess__bgr_stride__word(int width, int height, uint16_t* data, int stride_in, int stride_out,
                                   int channels, int shift_size) {
    uint16_t *data_in = data, *data_out = data, tmp;
    if (channels == 3) {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data_in[i2 * 3 + 0];
                    data_out[i2 * 3 + 0] = data_in[i2 * 3 + 2] << 4;
                    data_out[i2 * 3 + 1] = data_in[i2 * 3 + 1] << 4;
                    data_out[i2 * 3 + 2] = tmp << 4;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else if (shift_size == 6) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data_in[i2 * 3 + 0];
                    data_out[i2 * 3 + 0] = data_in[i2 * 3 + 2] << 6;
                    data_out[i2 * 3 + 1] = data_in[i2 * 3 + 1] << 6;
                    data_out[i2 * 3 + 2] = tmp << 6;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data_in[i2 * 3 + 0];
                    data_out[i2 * 3 + 0] = data_in[i2 * 3 + 2];
                    data_out[i2 * 3 + 1] = data_in[i2 * 3 + 1];
                    data_out[i2 * 3 + 2] = tmp;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
    }
    else {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data_in[i2 * 4 + 0];
                    data_out[i2 * 4 + 0] = data_in[i2 * 4 + 2] << 4;
                    data_out[i2 * 4 + 1] = data_in[i2 * 4 + 1] << 4;
                    data_out[i2 * 4 + 2] = tmp << 4;
                    data_out[i2 * 4 + 3] = data_in[i2 * 4 + 3] << 4;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else if (shift_size == 6) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data_in[i2 * 4 + 0];
                    data_out[i2 * 4 + 0] = data_in[i2 * 4 + 2] << 6;
                    data_out[i2 * 4 + 1] = data_in[i2 * 4 + 1] << 6;
                    data_out[i2 * 4 + 2] = tmp << 6;
                    data_out[i2 * 4 + 3] = data_in[i2 * 4 + 3] << 6;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    tmp = data_in[i2 * 4 + 0];
                    data_out[i2 * 4 + 0] = data_in[i2 * 4 + 2];
                    data_out[i2 * 4 + 1] = data_in[i2 * 4 + 1];
                    data_out[i2 * 4 + 2] = tmp;
                    data_out[i2 * 4 + 3] = data_in[i2 * 4 + 3];
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
    }
}

void postprocess__word(int width, int height, uint16_t* data, int stride_elements, int channels, int shift_size) {
    if (channels == 1) {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data[i2 * 1 + 0] = data[i2 * 1 + 0] << 4;
                }
                data += stride_elements;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data[i2 * 1 + 0] = data[i2 * 1 + 0] << 6;
                }
                data += stride_elements;
            }
        }
    }
    else if (channels == 3) {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data[i2 * 3 + 0] = data[i2 * 3 + 0] << 4;
                    data[i2 * 3 + 1] = data[i2 * 3 + 1] << 4;
                    data[i2 * 3 + 2] = data[i2 * 3 + 2] << 4;
                }
                data += stride_elements;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data[i2 * 3 + 0] = data[i2 * 3 + 0] << 6;
                    data[i2 * 3 + 1] = data[i2 * 3 + 1] << 6;
                    data[i2 * 3 + 2] = data[i2 * 3 + 2] << 6;
                }
                data += stride_elements;
            }
        }
    }
    else {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data[i2 * 4 + 0] = data[i2 * 4 + 0] << 4;
                    data[i2 * 4 + 1] = data[i2 * 4 + 1] << 4;
                    data[i2 * 4 + 2] = data[i2 * 4 + 2] << 4;
                    data[i2 * 4 + 3] = data[i2 * 4 + 3] << 4;
                }
                data += stride_elements;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data[i2 * 4 + 0] = data[i2 * 4 + 0] << 6;
                    data[i2 * 4 + 1] = data[i2 * 4 + 1] << 6;
                    data[i2 * 4 + 2] = data[i2 * 4 + 2] << 6;
                    data[i2 * 4 + 3] = data[i2 * 4 + 3] << 6;
                }
                data += stride_elements;
            }
        }
    }
}

void postprocess__stride__byte(int width, int height, uint8_t* data, int stride_in, int stride_out) {
    uint8_t *data_in = data, *data_out = data;
    for (int i = 0; i < height; i++) {
        memmove(data_out, data_in, stride_out); // possible will change to memcpy and set -D_FORTIFY_SOURCE=0
        data_in += stride_in;
        data_out += stride_out;
    }
}

void postprocess__stride__word(int width, int height, uint16_t* data, int stride_in, int stride_out,
                               int channels, int shift_size) {
    uint16_t *data_in = data, *data_out = data;
    if (shift_size == 0) {
        for (int i = 0; i < height; i++) {
            memmove(data_out, data_in, stride_out); // possible will change to memcpy and set -D_FORTIFY_SOURCE=0
            data_in += stride_in / 2;
            data_out += stride_out / 2;
        }
        return;
    }

    if (channels == 1) {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data_out[i2 * 1 + 0] = data_in[i2 * 1 + 0] << 4;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data_out[i2 * 1 + 0] = data_in[i2 * 1 + 0] << 6;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
    }
    else if (channels == 3) {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data_out[i2 * 3 + 0] = data_in[i2 * 3 + 0] << 4;
                    data_out[i2 * 3 + 1] = data_in[i2 * 3 + 1] << 4;
                    data_out[i2 * 3 + 2] = data_in[i2 * 3 + 2] << 4;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data_out[i2 * 3 + 0] = data_in[i2 * 3 + 0] << 6;
                    data_out[i2 * 3 + 1] = data_in[i2 * 3 + 1] << 6;
                    data_out[i2 * 3 + 2] = data_in[i2 * 3 + 2] << 6;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
    }
    else {
        if (shift_size == 4) {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data_out[i2 * 4 + 0] = data_in[i2 * 4 + 0] << 4;
                    data_out[i2 * 4 + 1] = data_in[i2 * 4 + 1] << 4;
                    data_out[i2 * 4 + 2] = data_in[i2 * 4 + 2] << 4;
                    data_out[i2 * 4 + 3] = data_in[i2 * 4 + 3] << 4;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                for (int i2 = 0; i2 < width; i2++) {
                    data_out[i2 * 4 + 0] = data_in[i2 * 4 + 0] << 6;
                    data_out[i2 * 4 + 1] = data_in[i2 * 4 + 1] << 6;
                    data_out[i2 * 4 + 2] = data_in[i2 * 4 + 2] << 6;
                    data_out[i2 * 4 + 3] = data_in[i2 * 4 + 3] << 6;
                }
                data_in += stride_in / 2;
                data_out += stride_out / 2;
            }
        }
    }
}

// Top Level Postprocess Functions

void postprocess__bgr(int width, int height, void* data, int stride,
                      int bytes_in_cc, int channels, int shift_size) {
    Py_BEGIN_ALLOW_THREADS
    if (bytes_in_cc == 1)
        postprocess__bgr__byte(width, height, (uint8_t*)data, stride, channels);
    else
        postprocess__bgr__word(width, height, (uint16_t*)data, stride, channels, shift_size);
    Py_END_ALLOW_THREADS
}

void postprocess__bgr_stride(int width, int height, void* data, int stride_in, int stride_out,
                             int bytes_in_cc, int channels, int shift_size) {
    Py_BEGIN_ALLOW_THREADS
    if (bytes_in_cc == 1)
        postprocess__bgr_stride__byte(width, height, (uint8_t*)data, stride_in, stride_out, channels);
    else
        postprocess__bgr_stride__word(width, height, (uint16_t*)data, stride_in, stride_out, channels, shift_size);
    Py_END_ALLOW_THREADS
}

void postprocess(int width, int height, void* data, int stride,
                 int bytes_in_cc, int channels, int shift_size) {
    if ((bytes_in_cc == 1) || (shift_size == 0))
        return;
    Py_BEGIN_ALLOW_THREADS
    postprocess__word(width, height, (uint16_t*)data, stride / 2, channels, shift_size);
    Py_END_ALLOW_THREADS
}

void postprocess__stride(int width, int height, void* data, int stride_in, int stride_out,
                         int bytes_in_cc, int channels, int shift_size) {
   Py_BEGIN_ALLOW_THREADS
    if (bytes_in_cc == 1)
        postprocess__stride__byte(width, height, (uint8_t*)data, stride_in, stride_out);
    else
        postprocess__stride__word(width, height, (uint16_t*)data, stride_in, stride_out, channels, shift_size);
    Py_END_ALLOW_THREADS
}
