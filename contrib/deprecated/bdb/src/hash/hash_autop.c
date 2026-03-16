/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#ifdef HAVE_HASH
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/hash.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __ham_insdel_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_insdel_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_insdel", __ham_insdel_desc, info));
}

/*
 * PUBLIC: int __ham_insdel_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_insdel_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_insdel_42", __ham_insdel_42_desc, info));
}

/*
 * PUBLIC: int __ham_newpage_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_newpage_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_newpage", __ham_newpage_desc, info));
}

/*
 * PUBLIC: int __ham_splitdata_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_splitdata_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_splitdata", __ham_splitdata_desc, info));
}

/*
 * PUBLIC: int __ham_replace_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_replace_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_replace", __ham_replace_desc, info));
}

/*
 * PUBLIC: int __ham_replace_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_replace_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_replace_42", __ham_replace_42_desc, info));
}

/*
 * PUBLIC: int __ham_copypage_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_copypage_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_copypage", __ham_copypage_desc, info));
}

/*
 * PUBLIC: int __ham_metagroup_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_metagroup_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_metagroup_42", __ham_metagroup_42_desc, info));
}

/*
 * PUBLIC: int __ham_metagroup_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_metagroup_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_metagroup", __ham_metagroup_desc, info));
}

/*
 * PUBLIC: int __ham_groupalloc_42_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_groupalloc_42_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_groupalloc_42", __ham_groupalloc_42_desc, info));
}

/*
 * PUBLIC: int __ham_groupalloc_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_groupalloc_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_groupalloc", __ham_groupalloc_desc, info));
}

/*
 * PUBLIC: int __ham_changeslot_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_changeslot_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_changeslot", __ham_changeslot_desc, info));
}

/*
 * PUBLIC: int __ham_contract_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_contract_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_contract", __ham_contract_desc, info));
}

/*
 * PUBLIC: int __ham_curadj_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_curadj_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_curadj", __ham_curadj_desc, info));
}

/*
 * PUBLIC: int __ham_chgpg_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__ham_chgpg_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__ham_chgpg", __ham_chgpg_desc, info));
}

/*
 * PUBLIC: int __ham_init_print __P((ENV *, DB_DISTAB *));
 */
int
__ham_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_insdel_print, DB___ham_insdel)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_newpage_print, DB___ham_newpage)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_splitdata_print, DB___ham_splitdata)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_replace_print, DB___ham_replace)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_copypage_print, DB___ham_copypage)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_metagroup_print, DB___ham_metagroup)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_groupalloc_print, DB___ham_groupalloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_changeslot_print, DB___ham_changeslot)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_contract_print, DB___ham_contract)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_curadj_print, DB___ham_curadj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_chgpg_print, DB___ham_chgpg)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_HASH */
