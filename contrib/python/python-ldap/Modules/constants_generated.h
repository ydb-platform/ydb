/*
 * Generated with:
 *   python Lib/ldap/constants.py > Modules/constants_generated.h
 *
 * Please do any modifications there, then re-generate this file
 */

add_err(ADMINLIMIT_EXCEEDED);
add_err(AFFECTS_MULTIPLE_DSAS);
add_err(ALIAS_DEREF_PROBLEM);
add_err(ALIAS_PROBLEM);
add_err(ALREADY_EXISTS);
add_err(AUTH_METHOD_NOT_SUPPORTED);
add_err(AUTH_UNKNOWN);
add_err(BUSY);
add_err(CLIENT_LOOP);
add_err(COMPARE_FALSE);
add_err(COMPARE_TRUE);
add_err(CONFIDENTIALITY_REQUIRED);
add_err(CONNECT_ERROR);
add_err(CONSTRAINT_VIOLATION);
add_err(CONTROL_NOT_FOUND);
add_err(DECODING_ERROR);
add_err(ENCODING_ERROR);
add_err(FILTER_ERROR);
add_err(INAPPROPRIATE_AUTH);
add_err(INAPPROPRIATE_MATCHING);
add_err(INSUFFICIENT_ACCESS);
add_err(INVALID_CREDENTIALS);
add_err(INVALID_DN_SYNTAX);
add_err(INVALID_SYNTAX);
add_err(IS_LEAF);
add_err(LOCAL_ERROR);
add_err(LOOP_DETECT);
add_err(MORE_RESULTS_TO_RETURN);
add_err(NAMING_VIOLATION);
add_err(NO_MEMORY);
add_err(NO_OBJECT_CLASS_MODS);
add_err(NO_OBJECT_CLASS_MODS);
add_err(NO_RESULTS_RETURNED);
add_err(NO_SUCH_ATTRIBUTE);
add_err(NO_SUCH_OBJECT);
add_err(NOT_ALLOWED_ON_NONLEAF);
add_err(NOT_ALLOWED_ON_RDN);
add_err(NOT_SUPPORTED);
add_err(OBJECT_CLASS_VIOLATION);
add_err(OPERATIONS_ERROR);
add_err(OTHER);
add_err(PARAM_ERROR);
add_err(PARTIAL_RESULTS);
add_err(PROTOCOL_ERROR);
add_err(REFERRAL);
add_err(REFERRAL_LIMIT_EXCEEDED);
add_err(RESULTS_TOO_LARGE);
add_err(SASL_BIND_IN_PROGRESS);
add_err(SERVER_DOWN);
add_err(SIZELIMIT_EXCEEDED);
add_err(STRONG_AUTH_NOT_SUPPORTED);
add_err(STRONG_AUTH_REQUIRED);
add_err(SUCCESS);
add_err(TIMELIMIT_EXCEEDED);
add_err(TIMEOUT);
add_err(TYPE_OR_VALUE_EXISTS);
add_err(UNAVAILABLE);
add_err(UNAVAILABLE_CRITICAL_EXTENSION);
add_err(UNDEFINED_TYPE);
add_err(UNWILLING_TO_PERFORM);
add_err(USER_CANCELLED);
add_err(VLV_ERROR);
add_err(X_PROXY_AUTHZ_FAILURE);

#if defined(LDAP_API_FEATURE_CANCEL)
add_err(CANCELLED);
add_err(NO_SUCH_OPERATION);
add_err(TOO_LATE);
add_err(CANNOT_CANCEL);
#endif


#if defined(LDAP_ASSERTION_FAILED)
add_err(ASSERTION_FAILED);
#endif


#if defined(LDAP_PROXIED_AUTHORIZATION_DENIED)
add_err(PROXIED_AUTHORIZATION_DENIED);
#endif

add_int(API_VERSION);
add_int(VENDOR_VERSION);
add_int(PORT);
add_int(VERSION1);
add_int(VERSION2);
add_int(VERSION3);
add_int(VERSION_MIN);
add_int(VERSION);
add_int(VERSION_MAX);
add_int(TAG_MESSAGE);
add_int(TAG_MSGID);
add_int(REQ_BIND);
add_int(REQ_UNBIND);
add_int(REQ_SEARCH);
add_int(REQ_MODIFY);
add_int(REQ_ADD);
add_int(REQ_DELETE);
add_int(REQ_MODRDN);
add_int(REQ_COMPARE);
add_int(REQ_ABANDON);
add_int(TAG_LDAPDN);
add_int(TAG_LDAPCRED);
add_int(TAG_CONTROLS);
add_int(TAG_REFERRAL);
add_int(REQ_EXTENDED);

#if LDAP_API_VERSION >= 2004
add_int(TAG_NEWSUPERIOR);
add_int(TAG_EXOP_REQ_OID);
add_int(TAG_EXOP_REQ_VALUE);
add_int(TAG_EXOP_RES_OID);
add_int(TAG_EXOP_RES_VALUE);

#if defined(HAVE_SASL)
add_int(TAG_SASL_RES_CREDS);
#endif

#endif

add_int(SASL_AUTOMATIC);
add_int(SASL_INTERACTIVE);
add_int(SASL_QUIET);
add_int(RES_BIND);
add_int(RES_SEARCH_ENTRY);
add_int(RES_SEARCH_RESULT);
add_int(RES_MODIFY);
add_int(RES_ADD);
add_int(RES_DELETE);
add_int(RES_MODRDN);
add_int(RES_COMPARE);
add_int(RES_ANY);
add_int(RES_SEARCH_REFERENCE);
add_int(RES_EXTENDED);
add_int(RES_UNSOLICITED);
add_int(RES_INTERMEDIATE);
add_int(AUTH_NONE);
add_int(AUTH_SIMPLE);
add_int(SCOPE_BASE);
add_int(SCOPE_ONELEVEL);
add_int(SCOPE_SUBTREE);

#if defined(LDAP_SCOPE_SUBORDINATE)
add_int(SCOPE_SUBORDINATE);
#endif

add_int(MOD_ADD);
add_int(MOD_DELETE);
add_int(MOD_REPLACE);
add_int(MOD_INCREMENT);
add_int(MOD_BVALUES);
add_int(MSG_ONE);
add_int(MSG_ALL);
add_int(MSG_RECEIVED);
add_int(DEREF_NEVER);
add_int(DEREF_SEARCHING);
add_int(DEREF_FINDING);
add_int(DEREF_ALWAYS);
add_int(NO_LIMIT);
add_int(OPT_API_INFO);
add_int(OPT_DEREF);
add_int(OPT_SIZELIMIT);
add_int(OPT_TIMELIMIT);

#if defined(LDAP_OPT_REFERRALS)
add_int(OPT_REFERRALS);
#endif

add_int(OPT_RESULT_CODE);
add_int(OPT_ERROR_NUMBER);
add_int(OPT_RESTART);
add_int(OPT_PROTOCOL_VERSION);
add_int(OPT_SERVER_CONTROLS);
add_int(OPT_CLIENT_CONTROLS);
add_int(OPT_API_FEATURE_INFO);
add_int(OPT_HOST_NAME);
add_int(OPT_DESC);
add_int(OPT_DIAGNOSTIC_MESSAGE);
add_int(OPT_ERROR_STRING);
add_int(OPT_MATCHED_DN);
add_int(OPT_DEBUG_LEVEL);
add_int(OPT_TIMEOUT);
add_int(OPT_REFHOPLIMIT);
add_int(OPT_NETWORK_TIMEOUT);

#if defined(LDAP_OPT_TCP_USER_TIMEOUT)
add_int(OPT_TCP_USER_TIMEOUT);
#endif

add_int(OPT_URI);

#if defined(LDAP_OPT_DEFBASE)
add_int(OPT_DEFBASE);
#endif


#if HAVE_TLS

#if defined(LDAP_OPT_X_TLS)
add_int(OPT_X_TLS);
#endif

add_int(OPT_X_TLS_CTX);
add_int(OPT_X_TLS_CACERTFILE);
add_int(OPT_X_TLS_CACERTDIR);
add_int(OPT_X_TLS_CERTFILE);
add_int(OPT_X_TLS_KEYFILE);
add_int(OPT_X_TLS_REQUIRE_CERT);
add_int(OPT_X_TLS_CIPHER_SUITE);
add_int(OPT_X_TLS_RANDOM_FILE);
add_int(OPT_X_TLS_DHFILE);
add_int(OPT_X_TLS_NEVER);
add_int(OPT_X_TLS_HARD);
add_int(OPT_X_TLS_DEMAND);
add_int(OPT_X_TLS_ALLOW);
add_int(OPT_X_TLS_TRY);

#if defined(LDAP_OPT_X_TLS_VERSION)
add_int(OPT_X_TLS_VERSION);
#endif


#if defined(LDAP_OPT_X_TLS_CIPHER)
add_int(OPT_X_TLS_CIPHER);
#endif


#if defined(LDAP_OPT_X_TLS_PEERCERT)
add_int(OPT_X_TLS_PEERCERT);
#endif


#if defined(LDAP_OPT_X_TLS_CRLCHECK)
add_int(OPT_X_TLS_CRLCHECK);
#endif


#if defined(LDAP_OPT_X_TLS_CRLFILE)
add_int(OPT_X_TLS_CRLFILE);
#endif

add_int(OPT_X_TLS_CRL_NONE);
add_int(OPT_X_TLS_CRL_PEER);
add_int(OPT_X_TLS_CRL_ALL);

#if defined(LDAP_OPT_X_TLS_NEWCTX)
add_int(OPT_X_TLS_NEWCTX);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_MIN)
add_int(OPT_X_TLS_PROTOCOL_MIN);
#endif


#if defined(LDAP_OPT_X_TLS_PACKAGE)
add_int(OPT_X_TLS_PACKAGE);
#endif


#if defined(LDAP_OPT_X_TLS_ECNAME)
add_int(OPT_X_TLS_ECNAME);
#endif


#if defined(LDAP_OPT_X_TLS_REQUIRE_SAN)
add_int(OPT_X_TLS_REQUIRE_SAN);
#endif


#if defined(LDAP_OPT_X_TLS_PEERCERT)
add_int(OPT_X_TLS_PEERCERT);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_MAX)
add_int(OPT_X_TLS_PROTOCOL_MAX);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_SSL3)
add_int(OPT_X_TLS_PROTOCOL_SSL3);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_TLS1_0)
add_int(OPT_X_TLS_PROTOCOL_TLS1_0);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_TLS1_1)
add_int(OPT_X_TLS_PROTOCOL_TLS1_1);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_TLS1_2)
add_int(OPT_X_TLS_PROTOCOL_TLS1_2);
#endif


#if defined(LDAP_OPT_X_TLS_PROTOCOL_TLS1_3)
add_int(OPT_X_TLS_PROTOCOL_TLS1_3);
#endif

#endif

add_int(OPT_X_SASL_MECH);
add_int(OPT_X_SASL_REALM);
add_int(OPT_X_SASL_AUTHCID);
add_int(OPT_X_SASL_AUTHZID);
add_int(OPT_X_SASL_SSF);
add_int(OPT_X_SASL_SSF_EXTERNAL);
add_int(OPT_X_SASL_SECPROPS);
add_int(OPT_X_SASL_SSF_MIN);
add_int(OPT_X_SASL_SSF_MAX);

#if defined(LDAP_OPT_X_SASL_NOCANON)
add_int(OPT_X_SASL_NOCANON);
#endif


#if defined(LDAP_OPT_X_SASL_USERNAME)
add_int(OPT_X_SASL_USERNAME);
#endif


#if defined(LDAP_OPT_CONNECT_ASYNC)
add_int(OPT_CONNECT_ASYNC);
#endif


#if defined(LDAP_OPT_X_KEEPALIVE_IDLE)
add_int(OPT_X_KEEPALIVE_IDLE);
#endif


#if defined(LDAP_OPT_X_KEEPALIVE_PROBES)
add_int(OPT_X_KEEPALIVE_PROBES);
#endif


#if defined(LDAP_OPT_X_KEEPALIVE_INTERVAL)
add_int(OPT_X_KEEPALIVE_INTERVAL);
#endif

add_int(DN_FORMAT_LDAP);
add_int(DN_FORMAT_LDAPV3);
add_int(DN_FORMAT_LDAPV2);
add_int(DN_FORMAT_DCE);
add_int(DN_FORMAT_UFN);
add_int(DN_FORMAT_AD_CANONICAL);
add_int(DN_FORMAT_MASK);
add_int(DN_PRETTY);
add_int(DN_SKIP);
add_int(DN_P_NOLEADTRAILSPACES);
add_int(DN_P_NOSPACEAFTERRDN);
add_int(DN_PEDANTIC);
add_int(AVA_NULL);
add_int(AVA_STRING);
add_int(AVA_BINARY);
add_int(AVA_NONPRINTABLE);
add_int(OPT_SUCCESS);
add_int(URL_ERR_BADSCOPE);
add_int(URL_ERR_MEM);

#ifdef HAVE_SASL
if (PyModule_AddIntConstant(m, "SASL_AVAIL", 1) != 0) return -1;
#else
if (PyModule_AddIntConstant(m, "SASL_AVAIL", 0) != 0) return -1;
#endif


#ifdef HAVE_TLS
if (PyModule_AddIntConstant(m, "TLS_AVAIL", 1) != 0) return -1;
#else
if (PyModule_AddIntConstant(m, "TLS_AVAIL", 0) != 0) return -1;
#endif


#ifdef HAVE_LDAP_INIT_FD
if (PyModule_AddIntConstant(m, "INIT_FD_AVAIL", 1) != 0) return -1;
#else
if (PyModule_AddIntConstant(m, "INIT_FD_AVAIL", 0) != 0) return -1;
#endif

add_string(CONTROL_MANAGEDSAIT);
add_string(CONTROL_PROXY_AUTHZ);
add_string(CONTROL_SUBENTRIES);
add_string(CONTROL_VALUESRETURNFILTER);
add_string(CONTROL_ASSERT);
add_string(CONTROL_PRE_READ);
add_string(CONTROL_POST_READ);
add_string(CONTROL_SORTREQUEST);
add_string(CONTROL_SORTRESPONSE);
add_string(CONTROL_PAGEDRESULTS);
add_string(CONTROL_SYNC);
add_string(CONTROL_SYNC_STATE);
add_string(CONTROL_SYNC_DONE);
add_string(SYNC_INFO);
add_string(CONTROL_PASSWORDPOLICYREQUEST);
add_string(CONTROL_PASSWORDPOLICYRESPONSE);
add_string(CONTROL_RELAX);
