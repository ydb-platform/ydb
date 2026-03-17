/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__dbreg_AUTO_H
#define	__dbreg_AUTO_H
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___dbreg_register	2
typedef struct ___dbreg_register_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	DBT	name;
	DBT	uid;
	int32_t	fileid;
	DBTYPE	ftype;
	db_pgno_t	meta_pgno;
	u_int32_t	id;
} __dbreg_register_args;

extern __DB_IMPORT DB_LOG_RECSPEC __dbreg_register_desc[];
static inline int
__dbreg_register_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, const DBT *name, const DBT *uid, int32_t fileid, DBTYPE ftype,
    db_pgno_t meta_pgno, u_int32_t id)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___dbreg_register, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + LOG_DBT_SIZE(name) + LOG_DBT_SIZE(uid) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t),
	    __dbreg_register_desc,
	    opcode, name, uid, fileid, ftype, meta_pgno, id));
}

static inline int __dbreg_register_read(ENV *env, 
    void *data, __dbreg_register_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __dbreg_register_desc, sizeof(__dbreg_register_args), (void**)arg));
}
#endif
