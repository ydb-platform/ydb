/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__repmgr_AUTO_H
#define	__repmgr_AUTO_H
#ifdef HAVE_REPLICATION_THREADS
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___repmgr_member	200
typedef struct ___repmgr_member_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	version;
	u_int32_t	prev_status;
	u_int32_t	status;
	DBT	host;
	u_int32_t	port;
} __repmgr_member_args;

extern __DB_IMPORT DB_LOG_RECSPEC __repmgr_member_desc[];
static inline int
__repmgr_member_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t version, u_int32_t prev_status, u_int32_t status, const DBT *host, u_int32_t port)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___repmgr_member, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(host) + sizeof(u_int32_t),
	    __repmgr_member_desc,
	    version, prev_status, status, host, port));
}

static inline int __repmgr_member_read(ENV *env, 
    void *data, __repmgr_member_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __repmgr_member_desc, sizeof(__repmgr_member_args), (void**)arg));
}
#endif /* HAVE_REPLICATION_THREADS */
#endif
