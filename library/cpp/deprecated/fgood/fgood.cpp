#include "fgood.h"

#include <util/generic/cast.h>
#include <util/string/cast.h>
#include <util/system/fstat.h>

#ifdef _win32_
#include <io.h>
#endif

i64 TFILEPtr::length() const {
#ifdef _win32_
    FHANDLE fd = (FHANDLE)_get_osfhandle(fileno(m_file));
#else
    FHANDLE fd = fileno(m_file);
#endif
    i64 rv = GetFileLength(fd);
    if (rv < 0)
        ythrow yexception() << "TFILEPtr::length() " << Name.data() << ": " << LastSystemErrorText();
    return rv;
}

FILE* OpenFILEOrFail(const TString& name, const char* mode) {
    FILE* res = ::fopen(name.data(), mode);
    if (!res) {
        ythrow yexception() << "can't open \'" << name << "\' with mode \'" << mode << "\': " << LastSystemErrorText();
    }
    return res;
}

void TFILECloser::Destroy(FILE* file) {
    ::fclose(file);
}

#ifdef _freebsd_ // fgetln
#define getline getline_alt_4test
#endif // _freebsd_

bool getline(TFILEPtr& f, TString& s) {
    char buf[4096];
    char* buf_ptr;
    if (s.capacity() > sizeof(buf)) {
        s.resize(s.capacity());
        if ((buf_ptr = fgets(s.begin(), IntegerCast<int>(s.capacity()), f)) == nullptr)
            return false;
    } else {
        if ((buf_ptr = fgets(buf, sizeof(buf), f)) == nullptr)
            return false;
    }
    size_t buf_len = strlen(buf_ptr);
    bool line_complete = buf_len && buf_ptr[buf_len - 1] == '\n';
    if (line_complete)
        buf_len--;
    if (buf_ptr == s.begin())
        s.resize(buf_len);
    else
        s.AssignNoAlias(buf, buf_len);
    if (line_complete)
        return true;
    while (fgets(buf, sizeof(buf), f)) {
        size_t buf_len2 = strlen(buf);
        if (buf_len2 && buf[buf_len2 - 1] == '\n') {
            buf[buf_len2 - 1] = 0;
            s.append(buf, buf_len2 - 1);
            return true;
        }
        s.append(buf, buf_len2);
    }
    return true;
}
