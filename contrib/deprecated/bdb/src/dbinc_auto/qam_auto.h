/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__qam_AUTO_H
#define	__qam_AUTO_H
#ifdef HAVE_QUEUE
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___qam_incfirst	84
typedef struct ___qam_incfirst_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_recno_t	recno;
	db_pgno_t	meta_pgno;
} __qam_incfirst_args;

extern __DB_IMPORT DB_LOG_RECSPEC __qam_incfirst_desc[];
static inline int
__qam_incfirst_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_recno_t recno, db_pgno_t meta_pgno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___qam_incfirst, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __qam_incfirst_desc, recno, meta_pgno));
}

static inline int __qam_incfirst_read(ENV *env, 
    DB **dbpp, void *td, void *data, __qam_incfirst_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __qam_incfirst_desc, sizeof(__qam_incfirst_args), (void**)arg));
}
#define	DB___qam_mvptr	85
typedef struct ___qam_mvptr_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	fileid;
	db_recno_t	old_first;
	db_recno_t	new_first;
	db_recno_t	old_cur;
	db_recno_t	new_cur;
	DB_LSN	metalsn;
	db_pgno_t	meta_pgno;
} __qam_mvptr_args;

extern __DB_IMPORT DB_LOG_RECSPEC __qam_mvptr_desc[];
static inline int
__qam_mvptr_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, db_recno_t old_first, db_recno_t new_first, db_recno_t old_cur,
    db_recno_t new_cur, DB_LSN * metalsn, db_pgno_t meta_pgno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___qam_mvptr, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(*metalsn) + sizeof(u_int32_t),
	    __qam_mvptr_desc,
	    opcode, old_first, new_first, old_cur, new_cur, metalsn, meta_pgno));
}

static inline int __qam_mvptr_read(ENV *env, 
    DB **dbpp, void *td, void *data, __qam_mvptr_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __qam_mvptr_desc, sizeof(__qam_mvptr_args), (void**)arg));
}
#define	DB___qam_del	79
typedef struct ___qam_del_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	lsn;
	db_pgno_t	pgno;
	u_int32_t	indx;
	db_recno_t	recno;
} __qam_del_args;

extern __DB_IMPORT DB_LOG_RECSPEC __qam_del_desc[];
static inline int
__qam_del_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, DB_LSN * lsn, db_pgno_t pgno, u_int32_t indx, db_recno_t recno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___qam_del, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*lsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __qam_del_desc, lsn, pgno, indx, recno));
}

static inline int __qam_del_read(ENV *env, 
    DB **dbpp, void *td, void *data, __qam_del_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __qam_del_desc, sizeof(__qam_del_args), (void**)arg));
}
#define	DB___qam_add	80
typedef struct ___qam_add_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	lsn;
	db_pgno_t	pgno;
	u_int32_t	indx;
	db_recno_t	recno;
	DBT	data;
	u_int32_t	vflag;
	DBT	olddata;
} __qam_add_args;

extern __DB_IMPORT DB_LOG_RECSPEC __qam_add_desc[];
static inline int
__qam_add_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, DB_LSN * lsn, db_pgno_t pgno, u_int32_t indx, db_recno_t recno,
    const DBT *data, u_int32_t vflag, const DBT *olddata)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___qam_add, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*lsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(data) +
	    sizeof(u_int32_t) + LOG_DBT_SIZE(olddata),
	    __qam_add_desc, lsn, pgno, indx, recno, data, vflag, olddata));
}

static inline int __qam_add_read(ENV *env, 
    DB **dbpp, void *td, void *data, __qam_add_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __qam_add_desc, sizeof(__qam_add_args), (void**)arg));
}
#define	DB___qam_delext	83
typedef struct ___qam_delext_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	lsn;
	db_pgno_t	pgno;
	u_int32_t	indx;
	db_recno_t	recno;
	DBT	data;
} __qam_delext_args;

extern __DB_IMPORT DB_LOG_RECSPEC __qam_delext_desc[];
static inline int
__qam_delext_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, DB_LSN * lsn, db_pgno_t pgno, u_int32_t indx, db_recno_t recno,
    const DBT *data)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___qam_delext, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*lsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(data),
	    __qam_delext_desc, lsn, pgno, indx, recno, data));
}

static inline int __qam_delext_read(ENV *env, 
    DB **dbpp, void *td, void *data, __qam_delext_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __qam_delext_desc, sizeof(__qam_delext_args), (void**)arg));
}
#endif /* HAVE_QUEUE */
#endif
