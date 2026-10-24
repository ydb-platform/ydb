#include <stdio.h>
const char *present(void), *mid_strong(void);
int main(int argc, char **argv) {
    (void)argv;
    printf("main reached\n");
    if (argc > 5) printf("%s\n", mid_strong());
    printf("%s\n", present());
    return 0;
}
