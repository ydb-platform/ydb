/* Do not edit: automatically built by gen_msg.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_swap.h"

/*
 * PUBLIC: void __repmgr_handshake_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_handshake_args *, u_int8_t *));
 */
void
__repmgr_handshake_marshal(env, argp, bp)
	ENV *env;
	__repmgr_handshake_args *argp;
	u_int8_t *bp;
{
	DB_HTONS_COPYOUT(env, bp, argp->port);
	DB_HTONS_COPYOUT(env, bp, argp->alignment);
	DB_HTONL_COPYOUT(env, bp, argp->ack_policy);
	DB_HTONL_COPYOUT(env, bp, argp->flags);
}

/*
 * PUBLIC: int __repmgr_handshake_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_handshake_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_handshake_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_handshake_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_HANDSHAKE_SIZE)
		goto too_few;
	DB_NTOHS_COPYIN(env, argp->port, bp);
	DB_NTOHS_COPYIN(env, argp->alignment, bp);
	DB_NTOHL_COPYIN(env, argp->ack_policy, bp);
	DB_NTOHL_COPYIN(env, argp->flags, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_handshake message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_v3handshake_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_v3handshake_args *, u_int8_t *));
 */
void
__repmgr_v3handshake_marshal(env, argp, bp)
	ENV *env;
	__repmgr_v3handshake_args *argp;
	u_int8_t *bp;
{
	DB_HTONS_COPYOUT(env, bp, argp->port);
	DB_HTONL_COPYOUT(env, bp, argp->priority);
	DB_HTONL_COPYOUT(env, bp, argp->flags);
}

/*
 * PUBLIC: int __repmgr_v3handshake_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_v3handshake_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_v3handshake_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_v3handshake_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_V3HANDSHAKE_SIZE)
		goto too_few;
	DB_NTOHS_COPYIN(env, argp->port, bp);
	DB_NTOHL_COPYIN(env, argp->priority, bp);
	DB_NTOHL_COPYIN(env, argp->flags, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_v3handshake message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_v2handshake_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_v2handshake_args *, u_int8_t *));
 */
void
__repmgr_v2handshake_marshal(env, argp, bp)
	ENV *env;
	__repmgr_v2handshake_args *argp;
	u_int8_t *bp;
{
	DB_HTONS_COPYOUT(env, bp, argp->port);
	DB_HTONL_COPYOUT(env, bp, argp->priority);
}

/*
 * PUBLIC: int __repmgr_v2handshake_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_v2handshake_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_v2handshake_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_v2handshake_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_V2HANDSHAKE_SIZE)
		goto too_few;
	DB_NTOHS_COPYIN(env, argp->port, bp);
	DB_NTOHL_COPYIN(env, argp->priority, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_v2handshake message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_parm_refresh_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_parm_refresh_args *, u_int8_t *));
 */
void
__repmgr_parm_refresh_marshal(env, argp, bp)
	ENV *env;
	__repmgr_parm_refresh_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->ack_policy);
	DB_HTONL_COPYOUT(env, bp, argp->flags);
}

/*
 * PUBLIC: int __repmgr_parm_refresh_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_parm_refresh_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_parm_refresh_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_parm_refresh_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_PARM_REFRESH_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->ack_policy, bp);
	DB_NTOHL_COPYIN(env, argp->flags, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_parm_refresh message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_permlsn_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_permlsn_args *, u_int8_t *));
 */
void
__repmgr_permlsn_marshal(env, argp, bp)
	ENV *env;
	__repmgr_permlsn_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->generation);
	DB_HTONL_COPYOUT(env, bp, argp->lsn.file);
	DB_HTONL_COPYOUT(env, bp, argp->lsn.offset);
}

/*
 * PUBLIC: int __repmgr_permlsn_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_permlsn_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_permlsn_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_permlsn_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_PERMLSN_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->generation, bp);
	DB_NTOHL_COPYIN(env, argp->lsn.file, bp);
	DB_NTOHL_COPYIN(env, argp->lsn.offset, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_permlsn message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_version_proposal_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_version_proposal_args *, u_int8_t *));
 */
void
__repmgr_version_proposal_marshal(env, argp, bp)
	ENV *env;
	__repmgr_version_proposal_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->min);
	DB_HTONL_COPYOUT(env, bp, argp->max);
}

/*
 * PUBLIC: int __repmgr_version_proposal_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_version_proposal_args *, u_int8_t *, size_t,
 * PUBLIC:	 u_int8_t **));
 */
int
__repmgr_version_proposal_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_version_proposal_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_VERSION_PROPOSAL_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->min, bp);
	DB_NTOHL_COPYIN(env, argp->max, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_version_proposal message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_version_confirmation_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_version_confirmation_args *, u_int8_t *));
 */
void
__repmgr_version_confirmation_marshal(env, argp, bp)
	ENV *env;
	__repmgr_version_confirmation_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->version);
}

/*
 * PUBLIC: int __repmgr_version_confirmation_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_version_confirmation_args *, u_int8_t *, size_t,
 * PUBLIC:	 u_int8_t **));
 */
int
__repmgr_version_confirmation_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_version_confirmation_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_VERSION_CONFIRMATION_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->version, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_version_confirmation message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_msg_hdr_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_msg_hdr_args *, u_int8_t *));
 */
void
__repmgr_msg_hdr_marshal(env, argp, bp)
	ENV *env;
	__repmgr_msg_hdr_args *argp;
	u_int8_t *bp;
{
	*bp++ = argp->type;
	DB_HTONL_COPYOUT(env, bp, argp->word1);
	DB_HTONL_COPYOUT(env, bp, argp->word2);
}

/*
 * PUBLIC: int __repmgr_msg_hdr_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_msg_hdr_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_msg_hdr_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_msg_hdr_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_MSG_HDR_SIZE)
		goto too_few;
	argp->type = *bp++;
	DB_NTOHL_COPYIN(env, argp->word1, bp);
	DB_NTOHL_COPYIN(env, argp->word2, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_msg_hdr message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_msg_metadata_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_msg_metadata_args *, u_int8_t *));
 */
void
__repmgr_msg_metadata_marshal(env, argp, bp)
	ENV *env;
	__repmgr_msg_metadata_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->tag);
	DB_HTONL_COPYOUT(env, bp, argp->limit);
	DB_HTONL_COPYOUT(env, bp, argp->flags);
}

/*
 * PUBLIC: int __repmgr_msg_metadata_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_msg_metadata_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_msg_metadata_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_msg_metadata_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_MSG_METADATA_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->tag, bp);
	DB_NTOHL_COPYIN(env, argp->limit, bp);
	DB_NTOHL_COPYIN(env, argp->flags, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_msg_metadata message"));
	return (EINVAL);
}

/*
 * PUBLIC: int __repmgr_membership_key_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_membership_key_args *, u_int8_t *, size_t, size_t *));
 */
int
__repmgr_membership_key_marshal(env, argp, bp, max, lenp)
	ENV *env;
	__repmgr_membership_key_args *argp;
	u_int8_t *bp;
	size_t *lenp, max;
{
	u_int8_t *start;

	if (max < __REPMGR_MEMBERSHIP_KEY_SIZE
	    + (size_t)argp->host.size)
		return (ENOMEM);
	start = bp;

	DB_HTONL_COPYOUT(env, bp, argp->host.size);
	if (argp->host.size > 0) {
		memcpy(bp, argp->host.data, argp->host.size);
		bp += argp->host.size;
	}
	DB_HTONS_COPYOUT(env, bp, argp->port);

	*lenp = (size_t)(bp - start);
	return (0);
}

/*
 * PUBLIC: int __repmgr_membership_key_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_membership_key_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_membership_key_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_membership_key_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	size_t needed;

	needed = __REPMGR_MEMBERSHIP_KEY_SIZE;
	if (max < needed)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->host.size, bp);
	if (argp->host.size == 0)
		argp->host.data = NULL;
	else
		argp->host.data = bp;
	needed += (size_t)argp->host.size;
	if (max < needed)
		goto too_few;
	bp += argp->host.size;
	DB_NTOHS_COPYIN(env, argp->port, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_membership_key message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_membership_data_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_membership_data_args *, u_int8_t *));
 */
void
__repmgr_membership_data_marshal(env, argp, bp)
	ENV *env;
	__repmgr_membership_data_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->flags);
}

/*
 * PUBLIC: int __repmgr_membership_data_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_membership_data_args *, u_int8_t *, size_t,
 * PUBLIC:	 u_int8_t **));
 */
int
__repmgr_membership_data_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_membership_data_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_MEMBERSHIP_DATA_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->flags, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_membership_data message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_member_metadata_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_member_metadata_args *, u_int8_t *));
 */
void
__repmgr_member_metadata_marshal(env, argp, bp)
	ENV *env;
	__repmgr_member_metadata_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->format);
	DB_HTONL_COPYOUT(env, bp, argp->version);
}

/*
 * PUBLIC: int __repmgr_member_metadata_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_member_metadata_args *, u_int8_t *, size_t,
 * PUBLIC:	 u_int8_t **));
 */
int
__repmgr_member_metadata_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_member_metadata_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_MEMBER_METADATA_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->format, bp);
	DB_NTOHL_COPYIN(env, argp->version, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_member_metadata message"));
	return (EINVAL);
}

/*
 * PUBLIC: int __repmgr_gm_fwd_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_gm_fwd_args *, u_int8_t *, size_t, size_t *));
 */
int
__repmgr_gm_fwd_marshal(env, argp, bp, max, lenp)
	ENV *env;
	__repmgr_gm_fwd_args *argp;
	u_int8_t *bp;
	size_t *lenp, max;
{
	u_int8_t *start;

	if (max < __REPMGR_GM_FWD_SIZE
	    + (size_t)argp->host.size)
		return (ENOMEM);
	start = bp;

	DB_HTONL_COPYOUT(env, bp, argp->host.size);
	if (argp->host.size > 0) {
		memcpy(bp, argp->host.data, argp->host.size);
		bp += argp->host.size;
	}
	DB_HTONS_COPYOUT(env, bp, argp->port);
	DB_HTONL_COPYOUT(env, bp, argp->gen);

	*lenp = (size_t)(bp - start);
	return (0);
}

/*
 * PUBLIC: int __repmgr_gm_fwd_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_gm_fwd_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_gm_fwd_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_gm_fwd_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	size_t needed;

	needed = __REPMGR_GM_FWD_SIZE;
	if (max < needed)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->host.size, bp);
	if (argp->host.size == 0)
		argp->host.data = NULL;
	else
		argp->host.data = bp;
	needed += (size_t)argp->host.size;
	if (max < needed)
		goto too_few;
	bp += argp->host.size;
	DB_NTOHS_COPYIN(env, argp->port, bp);
	DB_NTOHL_COPYIN(env, argp->gen, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_gm_fwd message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_membr_vers_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_membr_vers_args *, u_int8_t *));
 */
void
__repmgr_membr_vers_marshal(env, argp, bp)
	ENV *env;
	__repmgr_membr_vers_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->version);
	DB_HTONL_COPYOUT(env, bp, argp->gen);
}

/*
 * PUBLIC: int __repmgr_membr_vers_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_membr_vers_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_membr_vers_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_membr_vers_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_MEMBR_VERS_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->version, bp);
	DB_NTOHL_COPYIN(env, argp->gen, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_membr_vers message"));
	return (EINVAL);
}

/*
 * PUBLIC: int __repmgr_site_info_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_site_info_args *, u_int8_t *, size_t, size_t *));
 */
int
__repmgr_site_info_marshal(env, argp, bp, max, lenp)
	ENV *env;
	__repmgr_site_info_args *argp;
	u_int8_t *bp;
	size_t *lenp, max;
{
	u_int8_t *start;

	if (max < __REPMGR_SITE_INFO_SIZE
	    + (size_t)argp->host.size)
		return (ENOMEM);
	start = bp;

	DB_HTONL_COPYOUT(env, bp, argp->host.size);
	if (argp->host.size > 0) {
		memcpy(bp, argp->host.data, argp->host.size);
		bp += argp->host.size;
	}
	DB_HTONS_COPYOUT(env, bp, argp->port);
	DB_HTONL_COPYOUT(env, bp, argp->flags);

	*lenp = (size_t)(bp - start);
	return (0);
}

/*
 * PUBLIC: int __repmgr_site_info_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_site_info_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_site_info_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_site_info_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	size_t needed;

	needed = __REPMGR_SITE_INFO_SIZE;
	if (max < needed)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->host.size, bp);
	if (argp->host.size == 0)
		argp->host.data = NULL;
	else
		argp->host.data = bp;
	needed += (size_t)argp->host.size;
	if (max < needed)
		goto too_few;
	bp += argp->host.size;
	DB_NTOHS_COPYIN(env, argp->port, bp);
	DB_NTOHL_COPYIN(env, argp->flags, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_site_info message"));
	return (EINVAL);
}

/*
 * PUBLIC: void __repmgr_connect_reject_marshal __P((ENV *,
 * PUBLIC:	 __repmgr_connect_reject_args *, u_int8_t *));
 */
void
__repmgr_connect_reject_marshal(env, argp, bp)
	ENV *env;
	__repmgr_connect_reject_args *argp;
	u_int8_t *bp;
{
	DB_HTONL_COPYOUT(env, bp, argp->version);
	DB_HTONL_COPYOUT(env, bp, argp->gen);
}

/*
 * PUBLIC: int __repmgr_connect_reject_unmarshal __P((ENV *,
 * PUBLIC:	 __repmgr_connect_reject_args *, u_int8_t *, size_t, u_int8_t **));
 */
int
__repmgr_connect_reject_unmarshal(env, argp, bp, max, nextp)
	ENV *env;
	__repmgr_connect_reject_args *argp;
	u_int8_t *bp;
	size_t max;
	u_int8_t **nextp;
{
	if (max < __REPMGR_CONNECT_REJECT_SIZE)
		goto too_few;
	DB_NTOHL_COPYIN(env, argp->version, bp);
	DB_NTOHL_COPYIN(env, argp->gen, bp);

	if (nextp != NULL)
		*nextp = bp;
	return (0);

too_few:
	__db_errx(env, DB_STR("3675",
	    "Not enough input bytes to fill a __repmgr_connect_reject message"));
	return (EINVAL);
}

