#include <stdlib.h>
#include <stdio.h>


// True main for all platforms
int portable_main(int argc, char *argv[]);


#if defined _WIN32
#include <windows.h>
    // Standard way of decoding wide-string command-line arguments on Windows.
    // Call portable_main with UTF-8 strings.
    int main(int unused_argc, char *unused_argv[]) {
        int argc;
        int ret = 1;
        wchar_t** utf16_argv = NULL;
        char** utf8_argv = NULL;

        // Manual standard argument decoding needed since wmain is not supported by MinGW by default.
        utf16_argv = CommandLineToArgvW(GetCommandLineW(), &argc);

        if(utf16_argv == NULL) {
            fprintf(stderr, "Fatal error: command line argument extraction failure\n");
            goto cleanup;
        }

        utf8_argv = calloc(argc, sizeof(char*));

        for (int i=0; i<argc; ++i) {
            const int len = WideCharToMultiByte(CP_UTF8, 0, utf16_argv[i], -1, NULL, 0, NULL, NULL);

            if (len <= 0) {
                fprintf(stderr, "Fatal error: command line encoding failure (argument %d)\n", i+1);
                goto cleanup;
            }

            utf8_argv[i] = malloc(len + 1);
            const size_t ret = WideCharToMultiByte(CP_UTF8, 0, utf16_argv[i], -1, utf8_argv[i], len, NULL, NULL);

            if (ret <= 0) {
                fprintf(stderr, "Fatal error: command line encoding failure (argument %d)\n", i+1);
                goto cleanup;
            }

            utf8_argv[i][len] = 0;
        }

        ret = portable_main(argc, utf8_argv);

    cleanup:
        if(utf8_argv != NULL)
            for(int i=0; i<argc; ++i)
                free(utf8_argv[i]);

        free(utf8_argv);
        LocalFree(utf16_argv);
        return ret;
    }
#else
    // Proxy main.
    // On Mac, argv encoding is the current local encoding.
    // On Linux, argv encoding is difficult to know, but it 
    // should often be the current local encoding (generally UTF-8).
    int main(int argc, char *argv[])
    {
        portable_main(argc, argv);
        return 0;
    }
#endif

