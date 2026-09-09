#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>

#if defined(_WIN32)
    #include <process.h>
#else
    #include <unistd.h>
#endif

#if defined(__linux__)
    #include <sys/prctl.h>
#endif

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: process_guard PARENT_PID COMMAND [ARG...]\n");
        return 2;
    }

    char* end = nullptr;
    errno = 0;
    const long parsedParent = std::strtol(argv[1], &end, 10);
    if (errno != 0 || end == argv[1] || *end != '\0' || parsedParent <= 0) {
        std::fprintf(stderr, "invalid parent pid: %s\n", argv[1]);
        return 2;
    }

#if defined(__linux__)
    if (parsedParent > std::numeric_limits<pid_t>::max()) {
        std::fprintf(stderr, "invalid parent pid: %s\n", argv[1]);
        return 2;
    }
    const pid_t parent = static_cast<pid_t>(parsedParent);
    if (getppid() != parent) {
        return 125;
    }
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) == -1) {
        std::fprintf(stderr, "cannot set parent-death signal: %s\n", std::strerror(errno));
        return 126;
    }
    if (getppid() != parent) {
        return 125;
    }
#endif

#if defined(_WIN32)
    _execvp(argv[2], reinterpret_cast<const char* const*>(argv + 2));
#else
    execvp(argv[2], argv + 2);
#endif
    std::fprintf(stderr, "cannot execute %s: %s\n", argv[2], std::strerror(errno));
    return 127;
}
