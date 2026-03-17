#include <stdio.h>
#include <string.h>

int double_decimals(double value) {
    char buf[255];
    snprintf(buf, sizeof(buf), "%.14f", value);

    int len = strlen(buf);

    int dot_pos = 0;
    int relevant_decimal_pos = 0;

    for (int i=0; i<len; i++) {
        if (buf[i] == '.') {
            dot_pos = i;
            relevant_decimal_pos = i;
        } else if (dot_pos && buf[i] != '0') {
            relevant_decimal_pos = i;
        }
    }

    int relevant_decimals = relevant_decimal_pos - dot_pos;

    return relevant_decimals;
}
