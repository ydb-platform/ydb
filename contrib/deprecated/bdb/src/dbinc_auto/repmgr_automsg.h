/* Do not edit: automatically built by gen_msg.awk. */

#ifndef	__repmgr_AUTOMSG_H
#define	__repmgr_AUTOMSG_H

/*
 * Message sizes are simply the sum of field sizes (not
 * counting variable size parts, when DBTs are present),
 * and may be different from struct sizes due to padding.
 */
#define	__REPMGR_HANDSHAKE_SIZE	12
typedef struct ___repmgr_handshake_args {
	u_int16_t	port;
	u_int16_t	alignment;
	u_int32_t	ack_policy;
	u_int32_t	flags;
} __repmgr_handshake_args;

#define	__REPMGR_V3HANDSHAKE_SIZE	10
typedef struct ___repmgr_v3handshake_args {
	u_int16_t	port;
	u_int32_t	priority;
	u_int32_t	flags;
} __repmgr_v3handshake_args;

#define	__REPMGR_V2HANDSHAKE_SIZE	6
typedef struct ___repmgr_v2handshake_args {
	u_int16_t	port;
	u_int32_t	priority;
} __repmgr_v2handshake_args;

#define	__REPMGR_PARM_REFRESH_SIZE	8
typedef struct ___repmgr_parm_refresh_args {
	u_int32_t	ack_policy;
	u_int32_t	flags;
} __repmgr_parm_refresh_args;

#define	__REPMGR_PERMLSN_SIZE	12
typedef struct ___repmgr_permlsn_args {
	u_int32_t	generation;
	DB_LSN		lsn;
} __repmgr_permlsn_args;

#define	__REPMGR_VERSION_PROPOSAL_SIZE	8
typedef struct ___repmgr_version_proposal_args {
	u_int32_t	min;
	u_int32_t	max;
} __repmgr_version_proposal_args;

#define	__REPMGR_VERSION_CONFIRMATION_SIZE	4
typedef struct ___repmgr_version_confirmation_args {
	u_int32_t	version;
} __repmgr_version_confirmation_args;

#define	__REPMGR_MSG_HDR_SIZE	9
typedef struct ___repmgr_msg_hdr_args {
	u_int8_t	type;
	u_int32_t	word1;
	u_int32_t	word2;
} __repmgr_msg_hdr_args;

#define	__REPMGR_MSG_METADATA_SIZE	12
typedef struct ___repmgr_msg_metadata_args {
	u_int32_t	tag;
	u_int32_t	limit;
	u_int32_t	flags;
} __repmgr_msg_metadata_args;

#define	__REPMGR_MEMBERSHIP_KEY_SIZE	6
typedef struct ___repmgr_membership_key_args {
	DBT		host;
	u_int16_t	port;
} __repmgr_membership_key_args;

#define	__REPMGR_MEMBERSHIP_DATA_SIZE	4
typedef struct ___repmgr_membership_data_args {
	u_int32_t	flags;
} __repmgr_membership_data_args;

#define	__REPMGR_MEMBER_METADATA_SIZE	8
typedef struct ___repmgr_member_metadata_args {
	u_int32_t	format;
	u_int32_t	version;
} __repmgr_member_metadata_args;

#define	__REPMGR_GM_FWD_SIZE	10
typedef struct ___repmgr_gm_fwd_args {
	DBT		host;
	u_int16_t	port;
	u_int32_t	gen;
} __repmgr_gm_fwd_args;

#define	__REPMGR_MEMBR_VERS_SIZE	8
typedef struct ___repmgr_membr_vers_args {
	u_int32_t	version;
	u_int32_t	gen;
} __repmgr_membr_vers_args;

#define	__REPMGR_SITE_INFO_SIZE	10
typedef struct ___repmgr_site_info_args {
	DBT		host;
	u_int16_t	port;
	u_int32_t	flags;
} __repmgr_site_info_args;

#define	__REPMGR_CONNECT_REJECT_SIZE	8
typedef struct ___repmgr_connect_reject_args {
	u_int32_t	version;
	u_int32_t	gen;
} __repmgr_connect_reject_args;

#define	__REPMGR_MAXMSG_SIZE	12
#endif
