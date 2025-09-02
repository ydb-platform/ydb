#include <dlfcn.h>
#include <linux/limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern "C" {
    void __llvm_profile_initialize_file(void);
    int __llvm_profile_write_file(void);
    void __llvm_profile_set_filename(const char* filename_pattern);

    // there might no llmv rt, for example for the targets from contrib
    __attribute__((weak)) int __llvm_profile_write_file(void) {
        return 0;
    }
    __attribute__((weak)) void __llvm_profile_initialize_file(void) {
    }
    __attribute__((weak)) void __llvm_profile_set_filename(const char*) {
    }
}

namespace {
    void dummy() {
    }
}

bool getSoName(char* buff, size_t size) {
    // returns so name for shared objects and exe_name for binaries
    Dl_info dl_info = {0, 0, 0, 0};
    if (dladdr((void*)(intptr_t)dummy, &dl_info) != 0) {
        if (dl_info.dli_fname) {
            const char* name = dl_info.dli_fname;
            char real_path[PATH_MAX];
            const char* resolved = realpath(name, real_path);
            if (resolved != NULL)
                name = real_path;
            const char* lastSlash = strrchr(name, '/');
            if (!!lastSlash) {
                name = lastSlash + 1;
            }
            strncpy(buff, name, size);
            return true;
        }
    }
    return false;
}

bool getExeName(char* buff, size_t size) {
    ssize_t len = readlink("/proc/self/exe", buff, size);
    if (len <= 0) {
        return false;
    }

    buff[len] = '\0';
    char* lastSlash = strrchr(buff, '/');
    if (!!lastSlash) {
        strncpy(buff, lastSlash + 1, size);
        buff[(buff + len) - lastSlash] = '\0';
    }
    return true;
}

bool getSelfName(char* buff, size_t size) {
#if defined(_musl_)
    return getExeName(buff, size);
#else
    return getSoName(buff, size);
#endif
}

void replaceFirst(char* data, size_t dsize, const char* pat, size_t psize, const char* repl, size_t rsize) {
    char* patPtr = strstr(data, pat);
    if (!patPtr) {
        return;
    }

    char tmp[PATH_MAX] = {0};
    char* tmpPtr = &tmp[0];

    strcpy(tmpPtr, patPtr + psize);
    strcpy(patPtr, repl);
    strcpy(patPtr + rsize, tmpPtr);
    data[dsize - psize + rsize] = '\0';
}

// Adds support of the specifier '%e' (executable filename (without path prefix)) to the LLVM_PROFILE_FILE
void parseAndSetFilename() {
    const char* profile_file = getenv("LLVM_PROFILE_FILE");
    if (!profile_file)
        return;

    // __llvm_profile_set_filename doesn't copy name, so it must remain valid
    static char pattern[PATH_MAX] = {0};
    char* patternPtr = &pattern[0];

    strncpy(patternPtr, profile_file, PATH_MAX - 1);

    if (!!strstr(patternPtr, "%e")) {
        char buff[PATH_MAX] = {0};
        char* buffPtr = &buff[0];

        if (getSelfName(buffPtr, PATH_MAX)) {
            size_t patternSize = strlen(patternPtr);
            size_t buffSize = strlen(buffPtr);

            if (patternSize + buffSize >= PATH_MAX) {
                abort();
            }
            replaceFirst(patternPtr, patternSize, "%e", 2, buffPtr, buffSize);
        }
    }

    __llvm_profile_set_filename(patternPtr);
}

void __attribute__((constructor)) premain() {
    parseAndSetFilename();
    if (getenv("YA_COVERAGE_DUMP_PROFILE_AND_EXIT")) {
        __llvm_profile_initialize_file();
        int rc = __llvm_profile_write_file();
        if (!rc && getenv("YA_COVERAGE_DUMP_PROFILE_EXIT_CODE")) {
            if (const char* token = getenv("YA_COVERAGE_DUMP_PROFILE_RELIABILITY_TOKEN")) {
                fprintf(stdout, "%s", token);
                fflush(stdout);
            }
            rc = atoi(getenv("YA_COVERAGE_DUMP_PROFILE_EXIT_CODE"));
        }
        exit(rc);
    }
}
