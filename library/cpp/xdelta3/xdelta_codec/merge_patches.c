#include "codec.h"

#include <contrib/libs/xdelta3/xdelta3.h>
#include <contrib/libs/xdelta3/xdelta3-internal.h>

#include <util/system/types.h>

// routines extracted from xdelta3-main.h

// from xdelta3.c
#define VCD_ADLER32 (1U << 2) /* has adler32 checksum */

#define UNUSED(x) (void)(x)

typedef enum {
    CMD_MERGE_ARG,
    CMD_MERGE,
} xd3_cmd;

void* xdelta3_buffer_alloc(void* context, size_t items, usize_t size)
{
    if (!context) {
        return malloc(items * size);
    }
    XDeltaContext* xd3_context = (XDeltaContext*) context;
    return xd3_context->allocate(xd3_context->opaque, items * size);
}

void xdelta3_buffer_free(void* context, void* ptr)
{
    if (!context) {
        free(ptr);
        return;
    }
    XDeltaContext* xd3_context = (XDeltaContext*) context;
    xd3_context->free(xd3_context->opaque, ptr);
}

void init_common_config(XDeltaContext* context, xd3_config* config, usize_t iopt_size)
{
    config->alloc = xdelta3_buffer_alloc;
    config->freef = xdelta3_buffer_free;
    config->opaque = context;
    if (iopt_size) { // option should be 0 in case of patches merge, critical for patch calculation performance
        config->iopt_size = iopt_size;
    }
}

int init_recode_stream(XDeltaContext* context, xd3_stream* recode_stream)
{
    int ret;
    int stream_flags = XD3_ADLER32_NOVER | XD3_SKIP_EMIT;
    int recode_flags;
    xd3_config recode_config;

    recode_flags = (stream_flags & XD3_SEC_TYPE);

    xd3_init_config(&recode_config, recode_flags);

    init_common_config(context, &recode_config, 0);

    if ((ret = xd3_config_stream(recode_stream, &recode_config)) ||
        (ret = xd3_encode_init_partial(recode_stream)) ||
        (ret = xd3_whole_state_init(recode_stream)))
    {
        xd3_free_stream(recode_stream);
        return ret;
    }

    return 0;
}

int merge_func(xd3_stream* stream, ui8* out_data, size_t* out_size)
{
    UNUSED(out_data);
    UNUSED(out_size);

    return xd3_whole_append_window(stream);
}

int write_output(xd3_stream* stream, ui8* out_data, size_t* out_size, size_t max_osize)
{
    if (stream->avail_out > 0) {
        if (*out_size + stream->avail_out > max_osize) {
            return ENOMEM;
        }

        memcpy(out_data + *out_size, stream->next_out, stream->avail_out);
        *out_size += stream->avail_out;
    }

    return 0;
}

int merge_output(
    XDeltaContext* context,
    xd3_stream* stream,
    xd3_stream* recode_stream,
    xd3_stream* merge_stream,
    ui8** buffer,
    size_t buffer_size,
    ui8* out_data,
    size_t* out_size,
    size_t max_osize)
{
    int ret;
    usize_t inst_pos = 0;
    xoff_t output_pos = 0;
    xd3_source recode_source;
    usize_t window_num = 0;
    int at_least_once = 0;

    /* merge_stream is set if there were arguments.  this stream's input
     * needs to be applied to the merge_stream source. */
    if ((merge_stream != NULL) &&
        (ret = xd3_merge_input_output(stream, &merge_stream->whole_target)))
    {
        return ret;
    }

    /* Enter the ENC_INPUT state and bypass the next_in == NULL test
     * and (leftover) input buffering logic. */
    XD3_ASSERT(recode_stream->enc_state == ENC_INIT);
    recode_stream->enc_state = ENC_INPUT;
    recode_stream->next_in = *buffer;
    recode_stream->flags |= XD3_FLUSH;

    /* This encodes the entire target. */
    while (inst_pos < stream->whole_target.instlen || !at_least_once) {
        xoff_t window_start = output_pos;
        int window_srcset = 0;
        xoff_t window_srcmin = 0;
        xoff_t window_srcmax = 0;
        usize_t window_pos = 0;
        usize_t window_size;

        /* at_least_once ensures that we encode at least one window,
         * which handles the 0-byte case. */
        at_least_once = 1;

        XD3_ASSERT(recode_stream->enc_state == ENC_INPUT);

        if ((ret = xd3_encode_input(recode_stream)) != XD3_WINSTART) {
            return XD3_INVALID;
        }

        /* Window sizes must match from the input to the output, so that
         * target copies are in-range (and so that checksums carry
         * over). */
        XD3_ASSERT(window_num < stream->whole_target.wininfolen);
        window_size = stream->whole_target.wininfo[window_num].length;

        /* Output position should also match. */
        if (output_pos != stream->whole_target.wininfo[window_num].offset) {
            // internal merge error: offset mismatch
            return XD3_INVALID;
        }

        // NOTE: check if delta_codecs can decode this. option_use_checksum = 1
        if ((stream->dec_win_ind & VCD_ADLER32) != 0) {
            recode_stream->flags |= XD3_ADLER32_RECODE;
            recode_stream->recode_adler32 = stream->whole_target.wininfo[window_num].adler32;
        }

        window_num++;

        if (buffer_size < window_size) {
            xdelta3_buffer_free(context, *buffer);
            *buffer = NULL;
            buffer_size = 0;
            if ((*buffer = (ui8*)xdelta3_buffer_alloc(context, window_size, 1)) == NULL) {
                return ENOMEM;
            }
            recode_stream->next_in = *buffer; // re-setting stream buffer
            buffer_size = window_size;
        }

        /* This encodes a single target window. */
        while (window_pos < window_size && inst_pos < stream->whole_target.instlen) {
            xd3_winst* inst = &stream->whole_target.inst[inst_pos];
            usize_t take = xd3_min(inst->size, window_size - window_pos);
            xoff_t addr;

            switch (inst->type) {
                case XD3_RUN:
                    if ((ret = xd3_emit_run(recode_stream, window_pos, take, &stream->whole_target.adds[inst->addr]))) {
                        return ret;
                    }
                    break;

                case XD3_ADD:
                    /* Adds are implicit, put them into the input buffer. */
                    memcpy(*buffer + window_pos,
                           stream->whole_target.adds + inst->addr, take);
                    break;

                default: /* XD3_COPY + copy mode */
                    if (inst->mode != 0) {
                        if (window_srcset) {
                            window_srcmin = xd3_min(window_srcmin, inst->addr);
                            window_srcmax = xd3_max(window_srcmax,
                                                    inst->addr + take);
                        } else {
                            window_srcset = 1;
                            window_srcmin = inst->addr;
                            window_srcmax = inst->addr + take;
                        }
                        addr = inst->addr;
                    } else {
                        XD3_ASSERT(inst->addr >= window_start);
                        addr = inst->addr - window_start;
                    }

                    if ((ret = xd3_found_match(recode_stream, window_pos, take, addr, inst->mode != 0))) {
                        return ret;
                    }
                    break;
            }

            window_pos += take;
            output_pos += take;

            if (take == inst->size) {
                inst_pos += 1;
            } else {
                /* Modify the instruction for the next pass. */
                if (inst->type != XD3_RUN) {
                    inst->addr += take;
                }
                inst->size -= take;
            }
        }

        xd3_avail_input(recode_stream, *buffer, window_pos);

        recode_stream->enc_state = ENC_INSTR;

        if (window_srcset) {
            recode_stream->srcwin_decided = 1;
            recode_stream->src = &recode_source;
            recode_source.srclen = (usize_t)(window_srcmax - window_srcmin);
            recode_source.srcbase = window_srcmin;
            recode_stream->taroff = recode_source.srclen;

            XD3_ASSERT(recode_source.srclen != 0);
        } else {
            recode_stream->srcwin_decided = 0;
            recode_stream->src = NULL;
            recode_stream->taroff = 0;
        }

        for (;;) {
            switch ((ret = xd3_encode_input(recode_stream))) {
                case XD3_INPUT: {
                    goto done_window;
                }
                case XD3_OUTPUT: {
                    /* main_file_write below */
                    break;
                }
                case XD3_GOTHEADER:
                case XD3_WINSTART:
                case XD3_WINFINISH: {
                    /* ignore */
                    continue;
                }
                case XD3_GETSRCBLK:
                case 0: {
                    return XD3_INTERNAL;
                }
                default:
                    return ret;
            }

            if ((ret = write_output(recode_stream, out_data, out_size, max_osize))) {
                return ret;
            }

            xd3_consume_output(recode_stream);
        }
    done_window:
        (void)0;
    }

    return 0;
}

int process_input(
    XDeltaContext* context,
    xd3_cmd cmd,
    xd3_stream* recode_stream,
    xd3_stream* merge_stream,
    const ui8* patch,
    size_t patch_size,
    ui8* out_data,
    size_t* out_size,
    size_t max_out_size)
{
    int ret;
    xd3_stream stream;
    int stream_flags = 0;
    xd3_config config;
    xd3_source source;

    int (*input_func)(xd3_stream*);
    int (*output_func)(xd3_stream*, ui8*, size_t*);

    memset(&stream, 0, sizeof(stream));
    memset(&source, 0, sizeof(source));
    memset(&config, 0, sizeof(config));

    stream_flags |= XD3_ADLER32;
    /* main_input setup. */
    stream_flags |= XD3_ADLER32_NOVER | XD3_SKIP_EMIT; // TODO: add nocompress
    input_func = xd3_decode_input;

    if ((ret = init_recode_stream(context, recode_stream))) {
        return EXIT_FAILURE;
    }

    config.winsize = patch_size;
    config.sprevsz = xd3_pow2_roundup(config.winsize);
    config.flags = stream_flags;
    init_common_config(context, &config, 0);

    output_func = merge_func;

    if ((ret = xd3_config_stream(&stream, &config)) || (ret = xd3_whole_state_init(&stream))) {
        return EXIT_FAILURE;
    }

    /* If we've reached EOF tell the stream to flush. */
    stream.flags |= XD3_FLUSH;

    xd3_avail_input(&stream, patch, patch_size);

    /* Main input loop. */

    int again = 0;
    do {
        ret = input_func(&stream);

        switch (ret) {
            case XD3_INPUT:
                again = 0;
                break;

            case XD3_GOTHEADER: {
                XD3_ASSERT(stream.current_window == 0);
            }
                /* FALLTHROUGH */
            case XD3_WINSTART: {
                /* e.g., set or unset XD3_SKIP_WINDOW. */
                again = 1;
                break;
            }

            case XD3_OUTPUT: {
                if (ret = output_func(&stream, out_data, out_size)) {
                    xd3_free_stream(&stream);
                    return EXIT_FAILURE;
                }

                xd3_consume_output(&stream);
                again = 1;
                break;
            }

            case XD3_WINFINISH: {
                again = 1;
                break;
            }

            default:
                /* input_func() error */
                xd3_free_stream(&stream);
                return EXIT_FAILURE;
        }
    } while (again);

    if (cmd == CMD_MERGE) {
        ui8* buffer = NULL;
        size_t buffer_size = patch_size;
        if ((buffer = (ui8*)xdelta3_buffer_alloc(context, buffer_size, 1)) == NULL) {
            xd3_free_stream(&stream);
            return EXIT_FAILURE;
        }

        ret = merge_output(
            context,
            &stream,
            recode_stream,
            merge_stream,
            &buffer,
            buffer_size,
            out_data,
            out_size,
            max_out_size);

        xdelta3_buffer_free(context, buffer);

        if (ret) {
            xd3_free_stream(&stream);
            return EXIT_FAILURE;
        }
    } else if (cmd == CMD_MERGE_ARG) {
        xd3_swap_whole_state(&stream.whole_target, &recode_stream->whole_target);
    }

    if ((ret = xd3_close_stream(&stream))) {
        return EXIT_FAILURE;
    }

    xd3_free_stream(&stream);
    return EXIT_SUCCESS;
}

int patch_to_stream(
    XDeltaContext* context,
    const ui8* patch,
    size_t patch_size,
    xd3_stream* recode_stream,
    xd3_stream* merge_stream)
{
    xd3_stream merge_input;
    int ret;

    xd3_config config;
    memset(&config, 0, sizeof(config));
    init_common_config(context, &config, 0);

    if ((ret = xd3_config_stream(&merge_input, &config)) || (ret = xd3_whole_state_init(&merge_input))) {
        return ret;
    }

    ret = process_input(
        context,
        CMD_MERGE_ARG,
        recode_stream,
        merge_stream,
        patch,
        patch_size,
        NULL,
        NULL,
        0);

    if (ret == 0) {
        xd3_swap_whole_state(&recode_stream->whole_target, &merge_input.whole_target);
    }

    xd3_free_stream(recode_stream);

    if (ret != 0) {
        xd3_free_stream(&merge_input);
        return ret;
    }

    if ((ret = xd3_config_stream(merge_stream, &config)) || (ret = xd3_whole_state_init(merge_stream))) {
        xd3_free_stream(&merge_input);
        return ret;
    }

    xd3_swap_whole_state(&merge_stream->whole_target, &merge_input.whole_target);
    ret = 0;
    xd3_free_stream(&merge_input);
    return ret;
}

int merge_vcdiff_patches(
    XDeltaContext* context,
    const ui8* patch1,
    size_t patch_size1,
    const ui8* patch2,
    size_t patch_size2,
    ui8* result,
    size_t* result_size,
    size_t max_result_size)
{
    xd3_stream recode_stream;
    memset(&recode_stream, 0, sizeof(xd3_stream));
    xd3_stream merge_stream;
    memset(&merge_stream, 0, sizeof(xd3_stream));

    int ret;
    ret = patch_to_stream(context, patch1, patch_size1, &recode_stream, &merge_stream);
    if (!ret) {
        ret = process_input(context, CMD_MERGE, &recode_stream, &merge_stream, patch2, patch_size2, result, result_size, max_result_size);
    }

    xd3_free_stream(&recode_stream);
    xd3_free_stream(&merge_stream);

    return ret;
}
