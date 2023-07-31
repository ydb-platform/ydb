#pragma once

#include <util/system/yassert.h>
#include <util/system/defaults.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/ptr.h>

#include "fput.h"

#include <cstdio>

#include <fcntl.h>

#ifdef _unix_
extern "C" int __ungetc(int, FILE*);
#endif

#if (!defined(__FreeBSD__) && !defined(__linux__) && !defined(_darwin_) && !defined(_cygwin_)) || defined(_bionic_)
#define feof_unlocked(_stream) feof(_stream)
#define ferror_unlocked(_stream) ferror(_stream)
#endif

#ifndef _unix_
#if defined(_MSC_VER) && (_MSC_VER < 1900)
#define getc_unlocked(_stream) (--(_stream)->_cnt >= 0 ? 0xff & *(_stream)->_ptr++ : _filbuf(_stream))
#define putc_unlocked(_c, _stream) (--(_stream)->_cnt >= 0 ? 0xff & (*(_stream)->_ptr++ = (char)(_c)) : _flsbuf((_c), (_stream)))
#else
#define getc_unlocked(_stream) getc(_stream)
#define putc_unlocked(_c, _stream) putc(_c, _stream)
#endif
#endif

inline bool fgood(FILE* f) {
    return !feof_unlocked(f) && !ferror_unlocked(f);
}

#ifdef _win32_
// These functions will work only with static MSVC runtime linkage. For dynamic linkage,
// fseeki64.c and ftelli64.c from CRT sources should be included in project
extern "C" int __cdecl _fseeki64(FILE*, __int64, int);
extern "C" __int64 __cdecl _ftelli64(FILE*);

inline i64 ftello(FILE* stream) {
    return _ftelli64(stream);
}

inline int fseeko(FILE* stream, i64 offset, int origin) {
    return _fseeki64(stream, offset, origin);
}
#endif

class TFILEPtr {
private:
    enum { SHOULD_CLOSE = 1,
           IS_PIPE = 2 };
    FILE* m_file;
    int m_Flags;
    TString Name;

public:
    TFILEPtr() noexcept {
        m_file = nullptr;
        m_Flags = 0;
    }
    TFILEPtr(const TString& name, const char* mode) {
        m_file = nullptr;
        m_Flags = 0;
        open(name, mode);
    }
    TFILEPtr(const TFILEPtr& src) noexcept {
        m_file = src.m_file;
        m_Flags = 0;
    }
    TFILEPtr& operator=(const TFILEPtr& src) {
        if (src.m_file != m_file) {
            close();
            m_file = src.m_file;
            m_Flags = 0;
        }
        return *this;
    }
    explicit TFILEPtr(FILE* f) noexcept { // take ownership
        m_file = f;
        m_Flags = SHOULD_CLOSE;
    }
    TFILEPtr& operator=(FILE* f) { // take ownership
        if (f != m_file) {
            close();
            m_file = f;
            m_Flags = SHOULD_CLOSE;
        }
        return *this;
    }
    const TString& name() const {
        return Name;
    }
    operator FILE*() const noexcept {
        return m_file;
    }
    FILE* operator->() const noexcept {
        return m_file;
    }
    bool operator!() const noexcept {
        return m_file == nullptr;
    }
    bool operator!=(FILE* f) const noexcept {
        return m_file != f;
    }
    bool operator==(FILE* f) const noexcept {
        return m_file == f;
    }
    ~TFILEPtr() {
        close();
    }
    void Y_PRINTF_FORMAT(2, 3) check(const char* message, ...) const {
        if (Y_UNLIKELY(!fgood(m_file))) {
            va_list args;
            va_start(args, message);
            char buf[512];
            vsnprintf(buf, 512, message, args);
            // XXX: errno is undefined here
            ythrow yexception() << buf << ": " << LastSystemErrorText() << ", " << Name.data() << " at offset " << (i64)ftell();
        }
    }
    TFILEPtr& assign(FILE* f, const char* name = nullptr) { // take ownership and have a name
        *this = f;
        if (name)
            Name = name;
        return *this;
    }
    void open(const TString& name, const char* mode) {
        Y_ASSERT(!name.empty());
        Y_ASSERT(m_file == nullptr);
        m_file = ::fopen(name.data(), mode);
        if (!m_file)
            ythrow yexception() << "can't open \'" << name << "\' with mode \'" << mode << "\': " << LastSystemErrorText();
        m_Flags = SHOULD_CLOSE;
        Name = name;
    }
    void popen(const TString& command, const char* mode) {
        Y_ASSERT(!command.empty());
        Y_ASSERT(m_file == nullptr);
        m_file = ::popen(command.data(), mode);
        if (!m_file)
            ythrow yexception() << "can't execute \'" << command << "\' with mode \'" << mode << "\': " << LastSystemErrorText();
        m_Flags = IS_PIPE | SHOULD_CLOSE;
        Name = command;
    }
    void close() {
        if (m_file != nullptr && (m_Flags & SHOULD_CLOSE)) {
            if ((m_Flags & IS_PIPE) ? ::pclose(m_file) : ::fclose(m_file)) {
                m_file = nullptr;
                m_Flags = 0;
                if (!UncaughtException())
                    ythrow yexception() << "can't close file " << Name.data() << ": " << LastSystemErrorText();
            }
        }
        m_file = nullptr;
        m_Flags = 0;
        Name.clear();
    }
    size_t write(const void* buffer, size_t size, size_t count) const {
        Y_ASSERT(m_file != nullptr);
        size_t r = ::fwrite(buffer, size, count, m_file);
        check("can't write %lu bytes", (unsigned long)size * count);
        return r;
    }
    size_t read(void* buffer, size_t size, size_t count) const {
        Y_ASSERT(m_file != nullptr);
        size_t r = ::fread(buffer, size, count, m_file);
        if (ferror_unlocked(m_file))
            ythrow yexception() << "can't read " << (unsigned long)size * count << " bytes: " << LastSystemErrorText() << ", " << Name.data() << " at offset " << (i64)ftell();
        return r;
    }
    char* fgets(char* buffer, int size) const {
        Y_ASSERT(m_file != nullptr);
        char* r = ::fgets(buffer, size, m_file);
        if (ferror_unlocked(m_file))
            ythrow yexception() << "can't read string of maximum size " << size << ": " << LastSystemErrorText() << ", " << Name.data() << " at offset " << (i64)ftell();
        return r;
    }
    void Y_PRINTF_FORMAT(2, 3) fprintf(const char* format, ...) {
        Y_ASSERT(m_file != nullptr);
        va_list args;
        va_start(args, format);
        vfprintf(m_file, format, args);
        check("can't write");
    }
    void seek(i64 offset, int origin) const {
        Y_ASSERT(m_file != nullptr);
#if defined(_unix_) || defined(_win32_)
        if (fseeko(m_file, offset, origin) != 0)
#else
        Y_ASSERT(offset == (i64)(i32)offset);
        if (::fseek(m_file, (long)offset, origin) != 0)
#endif
            ythrow yexception() << "can't seek " << Name.data() << " by " << offset << ": " << LastSystemErrorText();
    }
    i64 length() const; // uses various system headers -> in fileptr.cpp

    void setDirect() const {
#if !defined(_win_) && !defined(_darwin_)
        if (!m_file)
            ythrow yexception() << "file not open";
        if (fcntl(fileno(m_file), F_SETFL, O_DIRECT) == -1)
            ythrow yexception() << "Cannot set O_DIRECT flag";
#endif
    }

    // for convenience

    i64 ftell() const noexcept {
#if defined(_unix_) || defined(_win32_)
        return ftello(m_file);
#else
        return ftell(m_file);
#endif
    }
    bool eof() const noexcept {
        Y_ASSERT(m_file != nullptr);
        return feof_unlocked(m_file) != 0;
    }
    int fputc(int c) {
        Y_ASSERT(m_file != nullptr);
        return putc_unlocked(c, m_file);
    }
    size_t fputs(const char* buffer) const {
        return write(buffer, strlen(buffer), 1);
    }
    int fgetc() {
        Y_ASSERT(m_file != nullptr);
        return getc_unlocked(m_file);
    }
    int ungetc(int c) {
        Y_ASSERT(m_file != nullptr);
        return ::ungetc(c, m_file);
    }
    template <class T>
    size_t fput(const T& a) {
        Y_ASSERT(m_file != nullptr);
        return ::fput(m_file, a);
    }
    template <class T>
    size_t fget(T& a) {
        Y_ASSERT(m_file != nullptr);
        return ::fget(m_file, a);
    }
    size_t fsput(const char* s, size_t l) {
        Y_ASSERT(m_file != nullptr);
        return ::fsput(m_file, s, l);
    }
    size_t fsget(char* s, size_t l) {
        Y_ASSERT(m_file != nullptr);
        return ::fsget(m_file, s, l);
    }

    void fflush() {
        ::fflush(m_file);
    }

    /* This block contains some TFile/TStream - compatible names */
    size_t Read(void* bufferIn, size_t numBytes) {
        size_t r = fsget((char*)bufferIn, numBytes);
        if (Y_UNLIKELY(ferror_unlocked(m_file)))
            ythrow yexception() << "can't read " << numBytes << " bytes: " << LastSystemErrorText() << ", " << Name << " at offset " << (i64)ftell();
        return r;
    }
    void Write(const void* buffer, size_t numBytes) {
        write(buffer, 1, numBytes);
    }
    i64 Seek(i64 offset, int origin /*SeekDir*/) {
        seek(offset, origin);
        return ftell();
    }
    i64 GetPosition() const noexcept {
        return ftell();
    }
    i64 GetLength() const noexcept {
        return length();
    }
    bool ReadLine(TString& st);

    /* Similar to TAutoPtr::Release - return pointer and forget about it. */
    FILE* Release() noexcept {
        FILE* result = m_file;
        m_file = nullptr;
        m_Flags = 0;
        Name.clear();
        return result;
    }
};

inline void fclose(TFILEPtr& F) {
    F.close();
}

inline void fseek(const TFILEPtr& F, i64 offset, int whence) {
    F.seek(offset, whence);
}

#ifdef _freebsd_ // fgetln
inline bool getline(TFILEPtr& f, TString& s) {
    size_t len;
    char* buf = fgetln(f, &len);
    if (!buf)
        return false;
    if (len && buf[len - 1] == '\n')
        len--;
    s.AssignNoAlias(buf, len);
    return true;
}
#else
bool getline(TFILEPtr& f, TString& s);
#endif //_freebsd_

inline bool TFILEPtr::ReadLine(TString& st) {
    return getline(*this, st);
}

FILE* OpenFILEOrFail(const TString& name, const char* mode);

//Should be used with THolder
struct TFILECloser {
    static void Destroy(FILE* file);
};

using TFILEHolder = THolder<FILE, TFILECloser>;
