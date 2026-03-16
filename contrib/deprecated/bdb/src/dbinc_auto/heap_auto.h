/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__heap_AUTO_H
#define	__heap_AUTO_H
#ifdef HAVE_HEAP
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___heap_addrem	151
typedef struct ___heap_addrem_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	indx;
	u_int32_t	nbytes;
	DBT	hdr;
	DBT	dbt;
	DB_LSN	pagelsn;
} __heap_addrem_args;

extern __DB_IMPORT DB_LOG_RECSPEC __heap_addrem_desc[];
static inline int
__heap_addrem_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, db_pgno_t pgno, u_int32_t indx, u_int32_t nbytes,
    const DBT *hdr, const DBT *dbt, DB_LSN * pagelsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___heap_addrem, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(hdr) +
	    LOG_DBT_SIZE(dbt) + sizeof(*pagelsn),
	    __heap_addrem_desc,
	    opcode, pgno, indx, nbytes, hdr, dbt, pagelsn));
}

static inline int __heap_addrem_read(ENV *env, 
    DB **dbpp, void *td, void *data, __heap_addrem_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __heap_addrem_desc, sizeof(__heap_addrem_args), (void**)arg));
}
#define	DB___heap_pg_alloc	152
typedef struct ___heap_pg_alloc_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	meta_lsn;
	db_pgno_t	meta_pgno;
	db_pgno_t	pgno;
	u_int32_t	ptype;
	db_pgno_t	last_pgno;
} __heap_pg_alloc_args;

extern __DB_IMPORT DB_LOG_RECSPEC __heap_pg_alloc_desc[];
static inline int
__heap_pg_alloc_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, DB_LSN * meta_lsn, db_pgno_t meta_pgno, db_pgno_t pgno, u_int32_t ptype,
    db_pgno_t last_pgno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___heap_pg_alloc, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*meta_lsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __heap_pg_alloc_desc, meta_lsn, meta_pgno, pgno, ptype, last_pgno));
}

static inline int __heap_pg_alloc_read(ENV *env, 
    DB **dbpp, void *td, void *data, __heap_pg_alloc_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __heap_pg_alloc_desc, sizeof(__heap_pg_alloc_args), (void**)arg));
}
#define	DB___heap_trunc_meta	153
typedef struct ___heap_trunc_meta_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	last_pgno;
	u_int32_t	key_count;
	u_int32_t	record_count;
	u_int32_t	curregion;
	u_int32_t	nregions;
	DB_LSN	pagelsn;
} __heap_trunc_meta_args;

extern __DB_IMPORT DB_LOG_RECSPEC __heap_trunc_meta_desc[];
static inline int
__heap_trunc_meta_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, u_int32_t last_pgno, u_int32_t key_count, u_int32_t record_count,
    u_int32_t curregion, u_int32_t nregions, DB_LSN * pagelsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___heap_trunc_meta, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(*pagelsn),
	    __heap_trunc_meta_desc, pgno, last_pgno, key_count, record_count, curregion, nregions, pagelsn));
}

static inline int __heap_trunc_meta_read(ENV *env, 
    DB **dbpp, void *td, void *data, __heap_trunc_meta_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __heap_trunc_meta_desc, sizeof(__heap_trunc_meta_args), (void**)arg));
}
#define	DB___heap_trunc_page	154
typedef struct ___heap_trunc_page_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DBT	old_data;
	u_int32_t	is_region;
	DB_LSN	pagelsn;
} __heap_trunc_page_args;

extern __DB_IMPORT DB_LOG_RECSPEC __heap_trunc_page_desc[];
static inline int
__heap_trunc_page_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, const DBT *old_data, u_int32_t is_region, DB_LSN * pagelsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___heap_trunc_page, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(old_data) +
	    sizeof(u_int32_t) + sizeof(*pagelsn),
	    __heap_trunc_page_desc, pgno, old_data, is_region, pagelsn));
}

static inline int __heap_trunc_page_read(ENV *env, 
    DB **dbpp, void *td, void *data, __heap_trunc_page_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __heap_trunc_page_desc, sizeof(__heap_trunc_page_args), (void**)arg));
}
#endif /* HAVE_HEAP */
#endif
