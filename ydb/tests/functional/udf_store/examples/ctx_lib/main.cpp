//! Shared context library for filter examples.
//! Uploaded as library_source name "ctx_lib".
//! Built with NO_LIBC (like helpers) — no stdlib/string includes.

enum { CtxLibMax = 64 };

typedef struct TCtxSlot {
    int used;
    long long a;
    long long b;
} TCtxSlot;

static TCtxSlot Slots[CtxLibMax];
static unsigned long long NextHandle = 1;

static TCtxSlot* SlotFor(unsigned long long handle) {
    if (handle == 0 || handle > CtxLibMax) {
        return 0;
    }
    TCtxSlot* slot = &Slots[handle - 1];
    return slot->used ? slot : 0;
}

extern "C" {

__attribute__((visibility("default"))) unsigned long long ctx_lib_create(void) {
    for (unsigned i = 0; i < CtxLibMax; ++i) {
        if (!Slots[i].used) {
            Slots[i].used = 1;
            Slots[i].a = 0;
            Slots[i].b = 0;
            const unsigned long long handle = i + 1;
            if (NextHandle <= handle) {
                NextHandle = handle + 1;
            }
            return handle;
        }
    }
    return 0;
}

__attribute__((visibility("default"))) void ctx_lib_destroy(unsigned long long handle) {
    TCtxSlot* slot = SlotFor(handle);
    if (slot) {
        slot->used = 0;
        slot->a = 0;
        slot->b = 0;
    }
}

__attribute__((visibility("default"))) void ctx_lib_inc_a(unsigned long long handle) {
    TCtxSlot* slot = SlotFor(handle);
    if (slot) {
        ++slot->a;
    }
}

__attribute__((visibility("default"))) void ctx_lib_inc_b(unsigned long long handle) {
    TCtxSlot* slot = SlotFor(handle);
    if (slot) {
        ++slot->b;
    }
}

//! Writes "a=<n>;b=<m>" into caller-provided buffer; returns length (excl. NUL).
__attribute__((visibility("default"))) int ctx_lib_format(
    unsigned long long handle,
    char* buf,
    int bufLen)
{
    TCtxSlot* slot = SlotFor(handle);
    if (!slot || !buf || bufLen < 16) {
        return 0;
    }
    int n = 0;
    buf[n++] = 'a';
    buf[n++] = '=';
    long long v = slot->a;
    char tmp[32];
    int t = 0;
    if (v == 0) {
        tmp[t++] = '0';
    } else {
        if (v < 0) {
            buf[n++] = '-';
            v = -v;
        }
        while (v > 0) {
            tmp[t++] = (char)('0' + (v % 10));
            v /= 10;
        }
        while (t > 0) {
            buf[n++] = tmp[--t];
        }
    }
    buf[n++] = ';';
    buf[n++] = 'b';
    buf[n++] = '=';
    v = slot->b;
    t = 0;
    if (v == 0) {
        tmp[t++] = '0';
    } else {
        if (v < 0) {
            buf[n++] = '-';
            v = -v;
        }
        while (v > 0) {
            tmp[t++] = (char)('0' + (v % 10));
            v /= 10;
        }
        while (t > 0) {
            buf[n++] = tmp[--t];
        }
    }
    if (n >= bufLen) {
        return 0;
    }
    buf[n] = 0;
    return n;
}

} // extern "C"
