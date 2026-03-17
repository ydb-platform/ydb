/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__crdel_AUTO_H
#define	__crdel_AUTO_H
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___crdel_metasub	142
typedef struct ___crdel_metasub_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DBT	page;
	DB_LSN	lsn;
} __crdel_metasub_args;

extern __DB_IMPORT DB_LOG_RECSPEC __crdel_metasub_desc[];
static inline int
__crdel_metasub_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, const DBT *page, DB_LSN * lsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___crdel_metasub, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(page) +
	    sizeof(*lsn),
	    __crdel_metasub_desc, pgno, page, lsn));
}

static inline int __crdel_metasub_read(ENV *env, 
    DB **dbpp, void *td, void *data, __crdel_metasub_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __crdel_metasub_desc, sizeof(__crdel_metasub_args), (void**)arg));
}
#define	DB___crdel_inmem_create	138
typedef struct ___crdel_inmem_create_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DBT	name;
	DBT	fid;
	u_int32_t	pgsize;
} __crdel_inmem_create_args;

extern __DB_IMPORT DB_LOG_RECSPEC __crdel_inmem_create_desc[];
static inline int
__crdel_inmem_create_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    int32_t fileid, const DBT *name, const DBT *fid, u_int32_t pgsize)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___crdel_inmem_create, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + LOG_DBT_SIZE(name) + LOG_DBT_SIZE(fid) +
	    sizeof(u_int32_t),
	    __crdel_inmem_create_desc,
	    fileid, name, fid, pgsize));
}

static inline int __crdel_inmem_create_read(ENV *env, 
    void *data, __crdel_inmem_create_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __crdel_inmem_create_desc, sizeof(__crdel_inmem_create_args), (void**)arg));
}
#define	DB___crdel_inmem_rename	139
typedef struct ___crdel_inmem_rename_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	oldname;
	DBT	newname;
	DBT	fid;
} __crdel_inmem_rename_args;

extern __DB_IMPORT DB_LOG_RECSPEC __crdel_inmem_rename_desc[];
static inline int
__crdel_inmem_rename_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *oldname, const DBT *newname, const DBT *fid)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___crdel_inmem_rename, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(oldname) + LOG_DBT_SIZE(newname) + LOG_DBT_SIZE(fid),
	    __crdel_inmem_rename_desc,
	    oldname, newname, fid));
}

static inline int __crdel_inmem_rename_read(ENV *env, 
    void *data, __crdel_inmem_rename_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __crdel_inmem_rename_desc, sizeof(__crdel_inmem_rename_args), (void**)arg));
}
#define	DB___crdel_inmem_remove	140
typedef struct ___crdel_inmem_remove_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DBT	name;
	DBT	fid;
} __crdel_inmem_remove_args;

extern __DB_IMPORT DB_LOG_RECSPEC __crdel_inmem_remove_desc[];
static inline int
__crdel_inmem_remove_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    const DBT *name, const DBT *fid)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___crdel_inmem_remove, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    LOG_DBT_SIZE(name) + LOG_DBT_SIZE(fid),
	    __crdel_inmem_remove_desc,
	    name, fid));
}

static inline int __crdel_inmem_remove_read(ENV *env, 
    void *data, __crdel_inmem_remove_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __crdel_inmem_remove_desc, sizeof(__crdel_inmem_remove_args), (void**)arg));
}
#endif
