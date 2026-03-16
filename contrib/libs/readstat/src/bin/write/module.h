typedef int (*rs_mod_will_write_file)(const char *filename);
typedef void * (*rs_mod_ctx_init)(const char *filename);
typedef void (*rs_mod_finish_file)(void *ctx);

typedef struct rs_module_s {
    rs_mod_will_write_file  accept;
    rs_mod_ctx_init         init;
    rs_mod_finish_file      finish;
    readstat_callbacks_t    handle;
} rs_module_t;

