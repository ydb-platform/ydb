/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc_auto/repmgr_auto.h"

DB_LOG_RECSPEC __repmgr_member_desc[] = {
	{LOGREC_ARG, SSZ(__repmgr_member_args, version), "version", "%lu"},
	{LOGREC_ARG, SSZ(__repmgr_member_args, prev_status), "prev_status", "%lu"},
	{LOGREC_ARG, SSZ(__repmgr_member_args, status), "status", "%lu"},
	{LOGREC_DBT, SSZ(__repmgr_member_args, host), "host", ""},
	{LOGREC_ARG, SSZ(__repmgr_member_args, port), "port", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __repmgr_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__repmgr_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __repmgr_member_recover, DB___repmgr_member)) != 0)
		return (ret);
	return (0);
}
