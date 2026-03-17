static inline int unpack_container_header(unpack_context* ctx, const char* data, Py_ssize_t len, Py_ssize_t* off)
{
    assert(len >= *off);
    uint32_t size;
    const unsigned char *const p = (unsigned char*)data + *off;

#define inc_offset(inc) \
    if (len - *off < inc) \
        return 0; \
    *off += inc;

    switch (*p) {
    case var_offset:
        inc_offset(3);
        size = _msgpack_load16(uint16_t, p + 1);
        break;
    case var_offset + 1:
        inc_offset(5);
        size = _msgpack_load32(uint32_t, p + 1);
        break;
#ifdef USE_CASE_RANGE
    case fixed_offset + 0x0 ... fixed_offset + 0xf:
#else
    case fixed_offset + 0x0:
    case fixed_offset + 0x1:
    case fixed_offset + 0x2:
    case fixed_offset + 0x3:
    case fixed_offset + 0x4:
    case fixed_offset + 0x5:
    case fixed_offset + 0x6:
    case fixed_offset + 0x7:
    case fixed_offset + 0x8:
    case fixed_offset + 0x9:
    case fixed_offset + 0xa:
    case fixed_offset + 0xb:
    case fixed_offset + 0xc:
    case fixed_offset + 0xd:
    case fixed_offset + 0xe:
    case fixed_offset + 0xf:
#endif
        ++*off;
        size = ((unsigned int)*p) & 0x0f;
        break;
    default:
        PyErr_SetString(PyExc_ValueError, "Unexpected type header on stream");
        return -1;
    }
    unpack_callback_uint32(&ctx->user, size, &ctx->stack[0].obj);
    return 1;
}

