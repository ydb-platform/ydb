/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__fop_AUTO_H
#define	__fop_AUTO_H
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___fop_create_42	143
typedef struct ___fop_create_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	name;
	u_int32_t	appname;
	u_int32_t	mode;
} __fop_create_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_create_42_desc[];
static inline int __fop_create_42_read(ENV *env, 
    void *data, __fop_create_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_create_42_desc, sizeof(__fop_create_42_args), (void**)arg));
}
#define	DB___fop_create	143
typedef struct ___fop_create_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	name;
	DBT	dirname;
	u_int32_t	appname;
	u_int32_t	mode;
} __fop_create_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_create_desc[];
static inline int
__fop_create_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *name, const DBT *dirname, u_int32_t appname, u_int32_t mode)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___fop_create, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(name) + LOG_DBT_SIZE(dirname) + sizeof(u_int32_t) +
	    sizeof(u_int32_t),
	    __fop_create_desc,
	    name, dirname, appname, mode));
}

static inline int __fop_create_read(ENV *env, 
    void *data, __fop_create_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_create_desc, sizeof(__fop_create_args), (void**)arg));
}
#define	DB___fop_remove	144
typedef struct ___fop_remove_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	name;
	DBT	fid;
	u_int32_t	appname;
} __fop_remove_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_remove_desc[];
static inline int
__fop_remove_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *name, const DBT *fid, u_int32_t appname)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___fop_remove, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(name) + LOG_DBT_SIZE(fid) + sizeof(u_int32_t),
	    __fop_remove_desc,
	    name, fid, appname));
}

static inline int __fop_remove_read(ENV *env, 
    void *data, __fop_remove_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_remove_desc, sizeof(__fop_remove_args), (void**)arg));
}
#define	DB___fop_write_42	145
typedef struct ___fop_write_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	name;
	u_int32_t	appname;
	u_int32_t	pgsize;
	db_pgno_t	pageno;
	u_int32_t	offset;
	DBT	page;
	u_int32_t	flag;
} __fop_write_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_write_42_desc[];
static inline int __fop_write_42_read(ENV *env, 
    void *data, __fop_write_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_write_42_desc, sizeof(__fop_write_42_args), (void**)arg));
}
#define	DB___fop_write	145
typedef struct ___fop_write_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	name;
	DBT	dirname;
	u_int32_t	appname;
	u_int32_t	pgsize;
	db_pgno_t	pageno;
	u_int32_t	offset;
	DBT	page;
	u_int32_t	flag;
} __fop_write_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_write_desc[];
static inline int
__fop_write_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *name, const DBT *dirname, u_int32_t appname, u_int32_t pgsize, db_pgno_t pageno,
    u_int32_t offset, const DBT *page, u_int32_t flag)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___fop_write, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(name) + LOG_DBT_SIZE(dirname) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(page) + sizeof(u_int32_t),
	    __fop_write_desc,
	    name, dirname, appname, pgsize, pageno, offset, page, flag));
}

static inline int __fop_write_read(ENV *env, 
    void *data, __fop_write_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_write_desc, sizeof(__fop_write_args), (void**)arg));
}
#define	DB___fop_rename_42	146
#define	DB___fop_rename_noundo_46	150
typedef struct ___fop_rename_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	oldname;
	DBT	newname;
	DBT	fileid;
	u_int32_t	appname;
} __fop_rename_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_rename_42_desc[];
static inline int __fop_rename_42_read(ENV *env, 
    void *data, __fop_rename_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_rename_42_desc, sizeof(__fop_rename_42_args), (void**)arg));
}
extern __DB_IMPORT DB_LOG_RECSPEC __fop_rename_noundo_46_desc[];
static inline int __fop_rename_noundo_46_read(ENV *env, 
    void *data, __fop_rename_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_rename_noundo_46_desc, sizeof(__fop_rename_42_args), (void**)arg));
}
#define	DB___fop_rename	146
#define	DB___fop_rename_noundo	150
typedef struct ___fop_rename_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	oldname;
	DBT	newname;
	DBT	dirname;
	DBT	fileid;
	u_int32_t	appname;
} __fop_rename_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_rename_desc[];
static inline int
__fop_rename_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *oldname, const DBT *newname, const DBT *dirname, const DBT *fileid, u_int32_t appname)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___fop_rename, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(oldname) + LOG_DBT_SIZE(newname) + LOG_DBT_SIZE(dirname) +
	    LOG_DBT_SIZE(fileid) + sizeof(u_int32_t),
	    __fop_rename_desc,
	    oldname, newname, dirname, fileid, appname));
}

static inline int __fop_rename_read(ENV *env, 
    void *data, __fop_rename_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_rename_desc, sizeof(__fop_rename_args), (void**)arg));
}
extern __DB_IMPORT DB_LOG_RECSPEC __fop_rename_noundo_desc[];
static inline int
__fop_rename_noundo_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *oldname, const DBT *newname, const DBT *dirname, const DBT *fileid, u_int32_t appname)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___fop_rename_noundo, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(oldname) + LOG_DBT_SIZE(newname) + LOG_DBT_SIZE(dirname) +
	    LOG_DBT_SIZE(fileid) + sizeof(u_int32_t),
	    __fop_rename_noundo_desc,
	    oldname, newname, dirname, fileid, appname));
}

static inline int __fop_rename_noundo_read(ENV *env, 
    void *data, __fop_rename_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_rename_noundo_desc, sizeof(__fop_rename_args), (void**)arg));
}
#define	DB___fop_file_remove	141
typedef struct ___fop_file_remove_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	real_fid;
	DBT	tmp_fid;
	DBT	name;
	u_int32_t	appname;
	u_int32_t	child;
} __fop_file_remove_args;

extern __DB_IMPORT DB_LOG_RECSPEC __fop_file_remove_desc[];
static inline int
__fop_file_remove_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *real_fid, const DBT *tmp_fid, const DBT *name, u_int32_t appname, u_int32_t child)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___fop_file_remove, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(real_fid) + LOG_DBT_SIZE(tmp_fid) + LOG_DBT_SIZE(name) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __fop_file_remove_desc,
	    real_fid, tmp_fid, name, appname, child));
}

static inline int __fop_file_remove_read(ENV *env, 
    void *data, __fop_file_remove_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __fop_file_remove_desc, sizeof(__fop_file_remove_args), (void**)arg));
}
#endif
