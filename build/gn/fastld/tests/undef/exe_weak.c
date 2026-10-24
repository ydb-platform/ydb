#include <stdio.h>
const char *present(void);
int weak_gone_is_null(void);
int main(void) {
    printf("%s, weak ref to removed symbol is NULL: %s\n", present(), weak_gone_is_null() ? "yes" : "no");
    return 0;
}
