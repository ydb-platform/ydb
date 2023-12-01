#pragma once

#include "fgood.h"

#include <util/string/util.h>  // str_spn
#include <util/string/split.h> // str_spn
#include <util/memory/segmented_string_pool.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/noncopyable.h>

#include <utility>

#include <cstdarg>
#include <cstring>

struct ffb: public TFILEPtr {
    ffb() {
    }
    ffb(FILE* file);
    ffb(const char* name, const char* mode) {
        open(name, mode);
    }
    void operator=(FILE* f); // take ownership
    void open(const char* name, const char* mode);
    int f(const char* c, ...) {
        va_list args;
        va_start(args, c);
        return vfprintf(*this, c, args);
    }
    void s(const char* c) {
        fsput(c, strlen(c));
    }
    void b(const void* cc, int n) {
        fsput((const char*)cc, n);
    }
    void B(const void* cc, int N) {
        fsput((const char*)cc, N);
    }
    void c(char c) {
        fputc(c);
    }
    void cbe(wchar16 c) {     // big endian utf-16
        fputc(char(c >> 8));  //Hi8
        fputc(char(c & 255)); //Lo8
    }
    void sbe(const wchar16* c) {
        for (; *c; c++)
            cbe(*c);
    }
    void fclose() {
        close();
    }
};

// split fields of tab-delimited line of text
// here and below fb actual size must be fb_sz + 1 to allow fb[fb_sz] be zero
int sf(char** fb, char* buf, size_t fb_sz);
int sf(char** fb, char* buf /* fb_sz == 32 */);

// split fields of char-delimited line of text
// Achtung: delim = ' ' imitates awk: initial separators are skipped,
// repeated seps treated as one, all chars less than ' ' treated as separators.
int sf(char fs, char** fb, char* buf, size_t fb_sz = 32);

// split fields of string-delimited line of text (fs is NOT a regexp)
// (usually fs is "@@")
int sf(const char* fs, char** fb, char* buf, size_t fb_sz = 32);

// split fields of char-delimited line of text, set of char-separators is given
// Achtung: repeated seps treated as one, initial seps are skipped
// newlines are NOT ignored.
int sf(const char* seps, char* buf, char** fb, size_t fb_sz = 32);

inline char* chomp(char* buf) {
    char* c = buf + strlen(buf);
    if (c > buf && c[-1] == '\n') {
        *--c = 0;
#ifdef _win32_
        if (c > buf && c[-1] == '\r')
            *--c = 0;
#endif
    }
    return buf;
}

inline char* chomp_cr(char* buf) {
    char* c = buf + strlen(buf);
    if (c > buf && c[-1] == '\n')
        *--c = 0;
    if (c > buf && c[-1] == '\r')
        *--c = 0;
    return buf;
}

class TLineSplitter {
protected:
    enum {                 // Default: Split string by SepStr
        SplitByAnySep = 1, // Split string by Sep
        NoEmptyFields = 2  // Skip all empty fields between separators
    };

private:
    ui32 Flags;
    const str_spn Sep;  // collection of separators
    const char* SepStr; // pointer exact string to separate by
    size_t SepStrLen;   // length of separator string

public:
    TLineSplitter(const char* sep, bool noEmpty)
        : Flags(noEmpty ? NoEmptyFields : 0)
        , Sep(TString(sep, 1).data())
        , SepStr(sep)
        , SepStrLen(strlen(sep))
    {
    }
    TLineSplitter(const str_spn& sep, bool noEmpty = false)
        : Flags(SplitByAnySep | (noEmpty ? NoEmptyFields : 0))
        , Sep(sep)
        , SepStr(nullptr)
        , SepStrLen(1)
    {
    }
    bool AnySep() const {
        return Flags & SplitByAnySep;
    }
    bool SkipEmpty() const {
        return Flags & NoEmptyFields;
    }
    /// Separates string onto tokens
    /// Expecting a zero-terminated string
    /// By default returns empty fields between sequential separators
    void operator()(char* p, TVector<char*>& fields) const;
    /// Same, but for const string - fills vector of pairs (pointer, length)
    void operator()(const char* p, TVector<std::pair<const char*, size_t>>& fields) const;
};

/**
 * Use library/cpp/map_text_file/map_tsv_file.h instead.
 */
class TSFReader {
    TString Buf; // buffer used for non-'\n'-terminated string and for non-freebsd work
    TLineSplitter Split;
    TVector<char*> Fields;
    size_t NF; // Fields.size()
    size_t NR;

    TFILEPtr File;

    bool OpenPipe; // internal flag that turns open() to popen()

    i32 FieldsRequired; // if != -1, != nf, terminate program

public:
    // char separator
    // Achtung: delim = ' ' imitates awk: initial separators are skipped,
    // all chars less than ' ' treated as separators.
    TSFReader(const char* fname = nullptr, char sep = '\t', i32 nf_reqired = -1);
    // exact string separator
    TSFReader(const char* fname, const char* sep, i32 nf_reqired = -1);
    // fully customizable
    TSFReader(const char* fname, const TLineSplitter& spl, i32 nf_reqired = -1);

    void Open(const char* fname, i32 nf_reqired = -1, size_t vbufsize = 1u << 21); // use "/dev/stdin" for stdin
    void Popen(const char* pname, i32 nf_reqired = -1, size_t vbufsize = 1u << 21);

    bool NextLine(segmented_string_pool* pool = nullptr);

    bool IsOpen() const {
        return (FILE*)File != nullptr;
    }
    bool IsEof() const {
        return feof(File);
    }
    void Close() {
        File.close();
    }
    void Rewind() {
        File.seek(0, SEEK_SET);
    }
    void Seek(i64 offset, int mode = SEEK_SET) {
        File.seek(offset, mode);
    }
    i64 Tell() const {
        return ftell(File);
    }
    char*& operator[](size_t ind) {
        //if (ind >= NF)
        //    throw yexception("Can't return reference to unexisting field %" PRISZT, ind);
        return Fields[ind];
    }
    const char* operator[](size_t ind) const {
        if (ind >= NF)
            return nullptr;
        return Fields[ind];
    }
    operator int() const { // note: empty input line makes 0 fields
        return (int)NF;
    }
    const char* Name() const {
        return File.name().data();
    }
    size_t Line() const {
        return NR;
    }
    const TVector<char*>& GetFields() const {
        return Fields;
    }
};

struct prnstr {
    char* buf;
    int pos;
    int asize;
    prnstr()
        : pos(0)
    {
        asize = 32;
        buf = new char[asize];
    }
    explicit prnstr(int asz)
        : pos(0)
    {
        asize = asz;
        buf = new char[asize];
    }
    int f(const char* c, ...);
    int s(const char* c1, const char* c2);
    int s(const char* c1, const char* c2, const char* c3);
    int s(const char* c, size_t len);
    //int s(const char *c);
    int s(const char* c) {
        return c ? s(c, strlen(c)) : 0;
    }
    int s(const TString& c);
    int s_htmesc(const char* c, bool enc_utf = false);
    int s_htmesc_w(const char* c);
    int c(char c);
    int cu(wchar32 c); //for utf-8
    void restart() {
        *buf = 0;
        pos = 0;
    }
    const char* operator~() const {
        return buf;
    }
    int operator+() const {
        return pos;
    }
    ~prnstr() {
        delete[] buf;
    }
    void clear();
    void swap(prnstr& w);
};

// functions that terminate program upon failure
FILE* read_or_die(const char* fname);
FILE* write_or_die(const char* fname);
FILE* fopen_or_die(const char* fname, const char* mode);

// functions that throw upon failure
FILE* fopen_chk(const char* fname, const char* mode);
void fclose_chk(FILE* f, const char* fname_dbg);
