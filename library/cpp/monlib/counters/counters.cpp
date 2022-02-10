 
#include "counters.h" 
 
namespace NMonitoring { 
    char* PrettyNumShort(i64 val, char* buf, size_t size) {
        static const char shorts[] = {' ', 'K', 'M', 'G', 'T', 'P', 'E'};
        unsigned i = 0; 
        i64 major = val; 
        i64 minor = 0; 
        const unsigned imax = sizeof(shorts) / sizeof(char);
        for (i = 0; i < imax; i++) {
            if (major >> 10 == 0) 
                break; 
            else { 
                minor = major - (major >> 10 << 10);
                major = major >> 10; 
            } 
        } 
        minor = (minor * 10) >> 10;
 
        if (i == 0 || i >= imax)
            *buf = '\0'; 
        else 
            snprintf(buf, size, "%" PRId64 ".%" PRId64 "%c", major, minor, shorts[i]);
 
        return buf; 
    } 
 
    char* PrettyNum(i64 val, char* buf, size_t size) {
        Y_ASSERT(buf);
        if (size < 4) {
            buf[0] = 0;
            return buf;
        }
        PrettyNumShort(val, buf + 2, size - 3);
        if (buf[2] == 0) {
            *buf = '\0';
        } else {
            size_t len = 2 + strnlen(buf + 2, size - 4);
            Y_ASSERT(len < size);
            buf[0] = ' ';
            buf[1] = '(';
            buf[len] = ')';
            buf[len + 1] = '\0';
        }

        return buf;
    }
}
