#include <stdio.h>
const char *present(void), *gone(void);
int main(int argc, char **argv) {
    (void)argv;
    printf("main reached\n");
    if (argc > 5) printf("%s\n", gone()); /* never called: BIND_NOW still fails */
    printf("%s\n", present());
    return 0;
}
