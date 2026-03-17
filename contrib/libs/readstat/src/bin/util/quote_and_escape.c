#include <stdlib.h>

static int escape(const char *s, char* dest) {
    char c = s[0];
    if (c == '\\') {
        if (dest) {
            dest[0] = '\\';
            dest[1] = '\\';
        }
        return 2 + escape(&s[1], dest ? &dest[2] : NULL);
    } else if (c == '"') {
        if (dest) {
            dest[0] = '\\';
            dest[1] = '"';
        }
        return 2 + escape(&s[1], dest ? &dest[2] : NULL);
    } else if (c) {
        if (dest) {
            dest[0] = c;
        }
        return 1 + escape(&s[1], dest ? &dest[1] : NULL);
    } else {
        if (dest) {
            dest[0] = '"';
            dest[1] = 0;
        }
        return 1;
    }
}

char* quote_and_escape(const char *src) {
    int newlen = 2 + escape(src, NULL);
    char *dest = malloc(newlen);
    dest[0] = '"';
    escape(src, &dest[1]);
    return dest;
}

