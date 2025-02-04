$osquery_data = [
    <|dt: 1688910000, host: 'fqdn1', ev_type: 'someEv', ev_status: '', user: '', vpn: FALSE,|>,
    <|dt: 1688910050, host: 'fqdn2', ev_type: 'login', ev_status: 'success', user: '', vpn: TRUE,|>,
    <|dt: 1688910100, host: 'fqdn1', ev_type: 'login', ev_status: 'success', user: '', vpn: TRUE,|>,
    <|dt: 1688910220, host: 'fqdn1', ev_type: 'login', ev_status: 'success', user: '', vpn: FALSE,|>,
    <|dt: 1688910300, host: 'fqdn1', ev_type: 'delete_all', ev_status: '', user: '', vpn: FALSE,|>,
    <|dt: 1688910400, host: 'fqdn2', ev_type: 'delete_all', ev_status: '', user: '', vpn: FALSE,|>,
    <|dt: 1688910500, host: 'fqdn1', ev_type: 'login', ev_status: 'failed', user: 'user1', vpn: FALSE,|>,
    <|dt: 1688910500, host: 'fqdn1', ev_type: 'login', ev_status: 'failed', user: 'user2', vpn: FALSE,|>,
    <|dt: 1688910600, host: 'fqdn', ev_type: 'someEv', ev_status: '', user: 'user1', vpn: FALSE,|>,
    <|dt: 1688910800, host: 'fqdn2', ev_type: 'login', ev_status: 'failed', user: 'user1', vpn: FALSE,|>,
    <|dt: 1688910900, host: 'fqdn2', ev_type: 'login', ev_status: 'failed', user: 'user2', vpn: FALSE,|>,
    <|dt: 1688911000, host: 'fqdn2', ev_type: 'login', ev_status: 'success', user: 'user1', vpn: FALSE,|>,
    <|dt: 1688911001, host: 'fqdn2', ev_type: 'login', ev_status: 'success', user: 'user1', vpn: FALSE,|>,
];

PRAGMA FeatureR010 = 'prototype';
PRAGMA config.flags('MatchRecognizeStream', 'force');

SELECT
    *
FROM
    AS_TABLE($osquery_data) MATCH_RECOGNIZE (
        MEASURES
            LAST(LOGIN_SUCCESS_REMOTE.host) AS remote_login_host,
            LAST(LOGIN_SUCCESS_REMOTE.user) AS remote_login_user,
            LAST(LOGIN_SUCCESS_REMOTE.dt) AS remote_login_dt,
            LAST(SUSPICIOUS_ACTION_SOON.dt) AS suspicious_action_dt,
            FIRST(LOGIN_FAILED_SAME_USER.dt) AS brutforce_begin,
            FIRST(LOGIN_SUCCESS_SAME_USER.dt) AS brutforce_end,
            LAST(LOGIN_SUCCESS_SAME_USER.user) AS brutforce_login
        ONE ROW PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN (LOGIN_SUCCESS_REMOTE ANY_ROW1 * SUSPICIOUS_ACTION_SOON | (LOGIN_FAILED_SAME_USER ANY_ROW2 *) {2,} LOGIN_SUCCESS_SAME_USER)
        DEFINE
            LOGIN_SUCCESS_REMOTE AS LOGIN_SUCCESS_REMOTE.ev_type == 'login'
            AND LOGIN_SUCCESS_REMOTE.ev_status == 'success'
            AND LOGIN_SUCCESS_REMOTE.vpn == TRUE
            AND COALESCE(LOGIN_SUCCESS_REMOTE.dt - FIRST(LOGIN_FAILED_SAME_USER.dt) <= 500, TRUE),
            ANY_ROW1 AS COALESCE(ANY_ROW1.dt - FIRST(LOGIN_SUCCESS_REMOTE.dt) <= 500, TRUE),
            SUSPICIOUS_ACTION_SOON AS SUSPICIOUS_ACTION_SOON.host == LAST(LOGIN_SUCCESS_REMOTE.host)
            AND SUSPICIOUS_ACTION_SOON.ev_type == 'delete_all'
            AND COALESCE(SUSPICIOUS_ACTION_SOON.dt - FIRST(LOGIN_SUCCESS_REMOTE.dt) <= 500, TRUE),
            LOGIN_FAILED_SAME_USER AS LOGIN_FAILED_SAME_USER.ev_type == 'login'
            AND LOGIN_FAILED_SAME_USER.ev_status != 'success'
            AND (
                LAST(LOGIN_FAILED_SAME_USER.user) IS NULL
                OR LAST(LOGIN_FAILED_SAME_USER.user) == LOGIN_FAILED_SAME_USER.user
            ) AND COALESCE(LOGIN_FAILED_SAME_USER.dt - FIRST(LOGIN_FAILED_SAME_USER.dt) <= 500, TRUE),
            ANY_ROW2 AS COALESCE(ANY_ROW2.dt - FIRST(LOGIN_FAILED_SAME_USER.dt) <= 500, TRUE),
            LOGIN_SUCCESS_SAME_USER AS LOGIN_SUCCESS_SAME_USER.ev_type == 'login'
            AND LOGIN_SUCCESS_SAME_USER.ev_status == 'success'
            AND LOGIN_SUCCESS_SAME_USER.user == LAST(LOGIN_FAILED_SAME_USER.user)
            AND COALESCE(LOGIN_SUCCESS_SAME_USER.dt - FIRST(LOGIN_FAILED_SAME_USER.dt) <= 500, TRUE)
    ) AS MATCHED
;
