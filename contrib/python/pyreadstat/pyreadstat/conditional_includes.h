/* The include and declarations in this file exists because when cython translates to c it cannot translate an IF UNAME_SYSNAME=="Windows" to
 * a c #ifdef. That means the resulting c file will be different if produced on windows or on unix. We want our c files to be portable and 
 * therefore do the right includes for windows here, and just declare dummy symbols for unix. 
 */
 
#ifdef _WIN32

    #include <fcntl.h>
    #include <share.h>
    #include <io.h>
    #include <sys/stat.h>
    
    // Stuff for handling paths with international characters on windows
    void assign_fd(void *io_ctx, int fd) { ((unistd_io_ctx_t*)io_ctx)->fd = fd; }
    //ssize_t write(int fd, const void *buf, size_t nbyte){return 0;};
    //int close(int fd);
        
#else

    #include<sys/stat.h>
    #include<unistd.h>
    #include<fcntl.h>

    //int open(const char *path, int oflag, int mode);
    //int close(int fd);
    #define _wsopen(...) 0
    #define _O_RDONLY 0
    #define _O_BINARY 0
    #define _O_WRONLY 0
    #define _O_TRUNC 0
    #define _O_CREAT 0
    #define _SH_DENYRW 0
    #define _SH_DENYWR 0
    #define _SH_DENYRD 0
    #define _SH_DENYNO 0
    #define _S_IWRITE 0
    #define _S_IREAD 0
    #define assign_fd(...) {}
    #define _close(...) 0
    #define _write(...) 0
    
#endif
