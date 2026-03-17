PREFIX	__repmgr

INCLUDE #include "db_int.h"
INCLUDE #include "dbinc/db_swap.h"
INCLUDE

BEGIN_MSG handshake
ARG	port		u_int16_t
ARG	alignment	u_int16_t
ARG	ack_policy	u_int32_t
ARG	flags		u_int32_t
END

BEGIN_MSG v3handshake
ARG	port		u_int16_t
ARG	priority	u_int32_t
ARG	flags		u_int32_t
END

BEGIN_MSG v2handshake
ARG	port		u_int16_t
ARG	priority	u_int32_t
END

BEGIN_MSG parm_refresh
ARG	ack_policy	u_int32_t
ARG	flags		u_int32_t
END

BEGIN_MSG permlsn
ARG	generation	u_int32_t
ARG	lsn		DB_LSN
END

BEGIN_MSG version_proposal
ARG	min		u_int32_t
ARG	max		u_int32_t
END

BEGIN_MSG version_confirmation
ARG	version		u_int32_t
END

BEGIN_MSG msg_hdr
ARG	type		u_int8_t
ARG	word1		u_int32_t
ARG	word2		u_int32_t
END

/* Metadata that goes along with user message on a DB_CHANNEL. */
BEGIN_MSG msg_metadata
ARG	tag		u_int32_t
ARG	limit		u_int32_t
ARG	flags		u_int32_t
END

/*
 * The membership database has a record for each site in the group, plus one
 * extra meta-data record.  The key of the meta-data record has a zero-length
 * host, and a port value of 0.
 */
BEGIN_MSG membership_key check_length
ARG	host		DBT
ARG	port		u_int16_t
END

BEGIN_MSG membership_data
ARG	flags           u_int32_t
END

/*
 * The "format" identifies the content and layout of the records within the
 * membership database (i.e., some of the items defined here in this *.msg
 * file).  It should be incremented when the layouts change in future Berkeley
 * DB releases.  The "version" counts group changes that the application makes
 * by adding or removing sites; thus it varies dynamically thoughout the life of
 * a group, during a single release of Berkeley DB.
 */ 
BEGIN_MSG member_metadata
ARG	format		u_int32_t
ARG	version		u_int32_t
END

/*
 * When a new site wants to join a group, it "guesses" that the configured
 * "helper" site is the master, and sends the request there.  When that guess
 * is wrong, the helper site responds with the location of the current master,
 * in effect "forwarding" the request.
 */
BEGIN_MSG gm_fwd check_length
ARG	host		DBT
ARG	port		u_int16_t
ARG	gen		u_int32_t
END

/* Membership list version header: */
BEGIN_MSG membr_vers
ARG	version		u_int32_t
ARG	gen		u_int32_t
END
BEGIN_MSG site_info check_length
ARG	host		DBT
ARG	port		u_int16_t
ARG	flags		u_int32_t
END

/*
 * If site A breaks or rejects a connection from site B, it first
 * tries to send B this message containing site A's currently known
 * membership DB version.  Site B can use this to decide what to do.
 * If site B knows of a later version, it should retry the connection
 * to site A later, polling at it until site A catches up.  However, if
 * site B's known version is less, it means that site B is no longer in 
 * the group, and so instead it should shut down and notify the application.
 */
BEGIN_MSG connect_reject
ARG	version		u_int32_t
ARG	gen		u_int32_t
END
