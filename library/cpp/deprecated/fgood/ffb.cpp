#include "ffb.h"

#include <util/string/util.h> // str_spn
#include <util/system/compat.h>
#include <util/generic/yexception.h>

#include <cstdio>
#include <algorithm>

#include <ctype.h>

#ifdef _win_
#include <io.h>
#else
#include <unistd.h>
#endif

ffb::ffb(FILE* file)
    : TFILEPtr(file)
{
    if (file && !isatty(fileno(file)) && BUFSIZ < 512 * 1024)
        setvbuf(file, nullptr, _IOFBF, 512 * 1024);
}

void ffb::operator=(FILE* f) {
    TFILEPtr::operator=(f);
    if (f && !isatty(fileno(f)) && BUFSIZ < 512 * 1024)
        setvbuf(f, nullptr, _IOFBF, 512 * 1024);
}

void ffb::open(const char* name, const char* mode) {
    TFILEPtr::open(name, mode);
    if (!isatty(fileno(*this)) && BUFSIZ < 512 * 1024)
        setvbuf(*this, nullptr, _IOFBF, 512 * 1024);
}

int sf(char** fb, char* buf) { //don't want to call sf(fb, buf, 32)
    if (!(*buf && *buf != 10)) {
        *fb = nullptr;
        return 0;
    }
    int n = 1;
    fb[0] = buf;
    while (*buf && *buf != 10 && n < 31) {
        if (*buf == '\t') {
            *buf++ = 0;
            fb[n++] = buf;
            continue;
        }
        buf++;
    }
    if (*buf == 10 && buf[-1] == 13)
        buf[-1] = 0;
    *buf = 0;
    fb[n] = nullptr;
    return n;
}

int sf(char** fb, char* buf, size_t fb_sz) {
    if (!(*buf && *buf != 10)) {
        *fb = nullptr;
        return 0;
    }
    fb_sz--;
    int n = 1;
    fb[0] = buf;
    while (*buf && *buf != 10 && n < (int)fb_sz) {
        if (*buf == '\t') {
            *buf++ = 0;
            fb[n++] = buf;
            continue;
        }
        buf++;
    }
    if (*buf == 10 && buf[-1] == 13)
        buf[-1] = 0;
    *buf = 0;
    fb[n] = nullptr;
    return n;
}

inline int sf_blank(char** fb, char* buf, size_t fb_sz) {
    while (isspace((ui8)*buf))
        buf++;
    if (!*buf) {
        *fb = nullptr;
        return 0;
    }
    fb_sz--;
    int n = 1;
    fb[0] = buf;
    while (*buf && *buf != 10 && n < (int)fb_sz) {
        if (isspace((ui8)*buf)) {
            *buf++ = 0;
            while (isspace((ui8)*buf))
                buf++;
            if (*buf)
                fb[n++] = buf;
            continue;
        }
        buf++;
    }
    if (*buf == 10 && buf[-1] == 13)
        buf[-1] = 0;
    *buf = 0;
    fb[n] = nullptr;
    return n;
}

int sf(char fs, char** fb, char* buf, size_t fb_sz) {
    if (fs == ' ')
        return sf_blank(fb, buf, fb_sz);
    while (*buf == fs)
        buf++;
    if (!(*buf && *buf != 10)) {
        *fb = nullptr;
        return 0;
    }
    fb_sz--;
    int n = 1;
    fb[0] = buf;
    while (*buf && *buf != 10 && n < (int)fb_sz) {
        if (*buf == fs) {
            *buf++ = 0;
            while (*buf == fs)
                buf++;
            fb[n++] = buf;
            continue;
        }
        buf++;
    }
    if (*buf == 10 && buf[-1] == 13)
        buf[-1] = 0;
    *buf = 0;
    fb[n] = nullptr;
    return n;
}

int sf(const char* fs, char** fb, char* buf, size_t fb_sz) {
    if (!(*buf && *buf != 10)) {
        *fb = nullptr;
        return 0;
    }
    int fs_len = strlen(fs);
    fb_sz--;
    int n = 1;
    fb[0] = buf;
    while (*buf && *buf != 10 && n < (int)fb_sz) {
        if (*buf == *fs && !strncmp(buf + 1, fs + 1, fs_len - 1)) {
            *buf = 0;
            buf += fs_len;
            fb[n++] = buf;
            continue;
        }
        buf++;
    }
    if (*buf == 10 && buf[-1] == 13)
        buf[-1] = 0;
    *buf = 0;
    fb[n] = nullptr;
    return n;
}

inline bool is_end(const char* p) {
    return !p || !p[0];
}

int sf(const char* seps, char* buf, char** fb, size_t fb_sz) {
    if (fb_sz < 1 || is_end(buf)) {
        *fb = nullptr;
        return 0;
    }
    str_spn sseps(seps);
    fb[0] = nullptr;
    int n = 0;
    // skip leading delimeters
    buf = sseps.cbrk(buf);
    if (is_end(buf))
        return 0;
    // store fields
    while (n < (int)fb_sz) {
        fb[n++] = buf;
        // find delimeters
        buf = sseps.brk(buf + 1);
        if (is_end(buf))
            break;
        *buf = 0;
        // skip delimiters
        buf = sseps.cbrk(buf + 1);
        if (is_end(buf))
            break;
    }
    fb[n] = nullptr;
    return n;
}

void TLineSplitter::operator()(char* p, TVector<char*>& fields) const {
    if (!p || !*p)
        return;
    char* q = p;
    while (1) {
        p = Sep.brk(p);
        if (q && (p - q || !SkipEmpty()))
            fields.push_back(q);
        q = nullptr;
        if (!*p)
            break;
        if (SepStrLen == 1 || (SepStrLen > 1 && !strncmp(p + 1, SepStr + 1, SepStrLen - 1))) {
            *p = 0;
            p += SepStrLen;
            q = p;
        } else
            p++;
    }
}

void TLineSplitter::operator()(const char* p, TVector<std::pair<const char*, size_t>>& fields) const {
    if (!p || !*p)
        return;
    const char* q = p;
    while (1) {
        p = Sep.brk(p);
        if (q && (p - q || !SkipEmpty()))
            fields.push_back(std::make_pair(q, p - q));
        q = nullptr;
        if (!*p)
            break;
        if (SepStrLen == 1 || (SepStrLen > 1 && !strncmp(p + 1, SepStr + 1, SepStrLen - 1))) {
            p += SepStrLen;
            q = p;
        } else
            p++;
    }
}

TSFReader::TSFReader(const char* fname, char sep, i32 nfrq) // if sep == ' ' isspace will be imitated (for compat)
    : Split(str_spn(sep == ' ' ? "\t\n\v\f\r " : TString(1, sep).data()), sep == ' ')
    , OpenPipe(false)
{
    Open(fname, nfrq);
}

TSFReader::TSFReader(const char* fname, const char* sep, i32 nfrq)
    : Split(sep, false)
    , OpenPipe(false)
{
    Open(fname, nfrq);
}

TSFReader::TSFReader(const char* fname, const TLineSplitter& spl, i32 nfrq)
    : Split(spl)
    , OpenPipe(false)
{
    Open(fname, nfrq);
}

void TSFReader::Open(const char* fname, i32 nfrq, size_t vbuf_size) {
    FieldsRequired = nfrq;
    NF = NR = 0;

    if (IsOpen())
        File.close();

    if (!fname)
        return;

    if (!strcmp(fname, "/dev/stdin")) {
        File.assign(stdin, "/dev/stdin");
    } else {
        if (OpenPipe)
            File.popen(fname, "r");
        else
            File.open(fname, "r");
    }
    OpenPipe = false;
    if (!isatty(fileno(File)))
        setvbuf(File, nullptr, _IOFBF, vbuf_size);
}

void TSFReader::Popen(const char* pname, i32 nfrq, size_t vbuf_size) {
    OpenPipe = true;
    Open(pname, nfrq, vbuf_size);
}

bool TSFReader::NextLine(segmented_string_pool* pool) {
    size_t line_len = 0;

#ifdef __FreeBSD__
    char* ptr = fgetln(File, &line_len);
    if (!ptr)
        return false;
    if (!line_len || ptr[line_len - 1] != '\n') { // last line w/o newline
        Buf.AssignNoAlias(ptr, line_len);
        ptr = Buf.begin();
    } else {
        // can safely replace newline with \0
        ptr[line_len - 1] = 0;
        --line_len;
    }
#else
    if (!getline(File, Buf))
        return false;
    char* ptr = Buf.begin();
    line_len = Buf.size();
#endif
    if (line_len && ptr[line_len - 1] == '\r')
        ptr[line_len - 1] = 0;

    if (pool) {
        char* nptr = pool->append(ptr);
        Y_ASSERT(!strcmp(ptr, nptr));
        ptr = nptr;
    }

    ++NR;
    Fields.clear();
    Split(ptr, Fields);
    NF = Fields.size();

    if (FieldsRequired != -1 && FieldsRequired != (int)NF)
        ythrow yexception() << File.name() << " line " << NR << ": " << NF << " fields, expected " << FieldsRequired;

    return true;
}

int prnstr::f(const char* c, ...) {
    va_list params;
    int n = asize - pos, k;
    va_start(params, c);
    while ((k = vsnprintf(buf + pos, n, c, params)) >= n) {
        n += asize, asize *= 2;
        while (k + pos >= n)
            n += asize, asize *= 2;
        char* t = new char[asize];
        memcpy(t, buf, pos);
        delete[] buf;
        buf = t;
        va_end(params);
        va_start(params, c);
    }
    pos += k;
    va_end(params);
    return k;
}
int prnstr::s(const char* c, size_t k) {
    if (!c)
        return 0;
    size_t n = asize - pos;
    if (k >= n) {
        n += asize, asize *= 2;
        while (k + pos >= n)
            n += asize, asize *= 2;
        char* t = new char[asize];
        memcpy(t, buf, pos);
        delete[] buf;
        buf = t;
    }
    memcpy(buf + pos, c, k);
    pos += k;
    buf[pos] = 0;
    return k;
}
void prnstr::clear() {
    pos = 0;
    if (asize > 32768) {
        asize = 32768;
        delete[] buf;
        buf = new char[asize];
    }
}

void prnstr::swap(prnstr& w) {
    std::swap(buf, w.buf);
    std::swap(pos, w.pos);
    std::swap(asize, w.asize);
}

FILE* read_or_die(const char* fname) {
    FILE* f = fopen(fname, "rb");
    if (!f)
        err(1, "%s", fname);
    return f;
}
FILE* write_or_die(const char* fname) {
    FILE* f = fopen(fname, "wb");
    if (!f)
        err(1, "%s", fname);
    return f;
}
FILE* fopen_or_die(const char* fname, const char* mode) {
    FILE* f = fopen(fname, mode);
    if (!f)
        err(1, "%s (mode '%s')", fname, mode);
    return f;
}

FILE* fopen_chk(const char* fname, const char* mode) {
    FILE* f = fopen(fname, mode);
    if (!f)
        ythrow yexception() << fname << " (mode '" << mode << "'): " << LastSystemErrorText();
    return f;
}

void fclose_chk(FILE* f, const char* fname) {
    if (fclose(f))
        ythrow yexception() << "file " << fname << ": " << LastSystemErrorText();
}
