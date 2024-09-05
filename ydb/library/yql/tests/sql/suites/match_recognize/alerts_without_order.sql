$osquery_data = [
<|dt:1688910000, host:"fqdn1", ev_type:"someEv",     ev_status:"",        user:"",       vpn:false, |>,
<|dt:1688910050, host:"fqdn2", ev_type:"login",      ev_status:"success", user:"",       vpn:true,  |>,
<|dt:1688910100, host:"fqdn1", ev_type:"login",      ev_status:"success", user:"",       vpn:true,  |>,
<|dt:1688910220, host:"fqdn1", ev_type:"login",      ev_status:"success", user:"",       vpn:false, |>,
<|dt:1688910300, host:"fqdn1", ev_type:"delete_all", ev_status:"",        user:"",       vpn:false, |>,
<|dt:1688910400, host:"fqdn2", ev_type:"delete_all", ev_status:"",        user:"",       vpn:false, |>,
<|dt:1688910500, host:"fqdn1", ev_type:"login",      ev_status:"failed",  user:"user1",  vpn:false, |>,
<|dt:1688910500, host:"fqdn1", ev_type:"login",      ev_status:"failed",  user:"user2",  vpn:false, |>,
<|dt:1688910600, host:"fqdn",  ev_type:"someEv",     ev_status:"",        user:"user1",  vpn:false, |>,
<|dt:1688910800, host:"fqdn2", ev_type:"login",      ev_status:"failed",  user:"user1",  vpn:false, |>,
<|dt:1688910900, host:"fqdn2", ev_type:"login",      ev_status:"failed",  user:"user2",  vpn:false, |>,
<|dt:1688911000, host:"fqdn2", ev_type:"login",      ev_status:"success",  user:"user1",  vpn:false, |>,
];

pragma FeatureR010="prototype";
pragma config.flags("MatchRecognizeStream", "disable");

SELECT *
FROM AS_TABLE($osquery_data) MATCH_RECOGNIZE(
    MEASURES 
      LAST(SUSPICIOUS_ACTION_SOON.dt) as suspicious_action_dt,
      LAST(LOGIN_SUCCESS_REMOTE.host) as remote_login_host,
      LAST(LOGIN_SUCCESS_REMOTE.user) as remote_login_user,
      LAST(LOGIN_SUCCESS_REMOTE.dt) as t,
      FIRST(LOGIN_FAILED_SAME_USER.dt) as brutforce_begin,
      FIRST(LOGIN_SUCCESS_SAME_USER.dt) as brutforce_end,
      LAST(LOGIN_SUCCESS_SAME_USER.user) as brutforce_login

    ONE ROW PER MATCH
    PATTERN (
      LOGIN_SUCCESS_REMOTE ANY_ROW* (SUSPICIOUS_ACTION_SOON | SUSPICIOUS_ACTION_TIMEOUT) |
      (LOGIN_FAILED_SAME_USER ANY_ROW*){2,} LOGIN_SUCCESS_SAME_USER
    )
    DEFINE 
        LOGIN_SUCCESS_REMOTE as 
            LOGIN_SUCCESS_REMOTE.ev_type = "login" and 
            LOGIN_SUCCESS_REMOTE.ev_status = "success" and 
            LOGIN_SUCCESS_REMOTE.vpn = true,
        SUSPICIOUS_ACTION_SOON as 
            SUSPICIOUS_ACTION_SOON.host = LAST(LOGIN_SUCCESS_REMOTE.host) and 
            SUSPICIOUS_ACTION_SOON.ev_type = "delete_all" and
            SUSPICIOUS_ACTION_SOON.dt - LAST(LOGIN_SUCCESS_REMOTE.dt) < 1000,
        SUSPICIOUS_ACTION_TIMEOUT as
            SUSPICIOUS_ACTION_TIMEOUT.dt - LAST(LOGIN_SUCCESS_REMOTE.dt) >= 1000,
            
        LOGIN_FAILED_SAME_USER as 
            LOGIN_FAILED_SAME_USER.ev_type = "login" and 
            LOGIN_FAILED_SAME_USER.ev_status <> "success" and 
            (LAST(LOGIN_FAILED_SAME_USER.user) IS NULL 
                or LAST(LOGIN_FAILED_SAME_USER.user) = LOGIN_FAILED_SAME_USER.user
            ),
        LOGIN_SUCCESS_SAME_USER as 
            LOGIN_SUCCESS_SAME_USER.ev_type = "login" and 
            LOGIN_SUCCESS_SAME_USER.ev_status = "success" and 
            LOGIN_SUCCESS_SAME_USER.user = LAST(LOGIN_FAILED_SAME_USER.user)
) AS MATCHED 
;

