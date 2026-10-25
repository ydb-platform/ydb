use super::super::*;

extern "C" {
    #[cfg_attr(arcadia_openssl_renamed, link_name = "v1_NCONF_new")]
    pub fn NCONF_new(meth: *mut CONF_METHOD) -> *mut CONF;
    #[cfg_attr(arcadia_openssl_renamed, link_name = "v1_NCONF_default")]
    pub fn NCONF_default() -> *mut CONF_METHOD;
    #[cfg_attr(arcadia_openssl_renamed, link_name = "v1_NCONF_free")]
    pub fn NCONF_free(conf: *mut CONF);
}
