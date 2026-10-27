// Take a look at the license at the top of the repository in the LICENSE file.

use glib::{prelude::*, translate::*};

use crate::{ProxyResolver, SimpleProxyResolver, ffi};

impl SimpleProxyResolver {
    #[doc(alias = "g_simple_proxy_resolver_new")]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(default_proxy: Option<&str>, ignore_hosts: impl IntoStrV) -> ProxyResolver {
        unsafe {
            ignore_hosts.run_with_strv(|ignore_hosts| {
                from_glib_full(ffi::g_simple_proxy_resolver_new(
                    default_proxy.to_glib_none().0,
                    ignore_hosts.as_ptr() as *mut _,
                ))
            })
        }
    }
}

pub trait SimpleProxyResolverExtManual: IsA<SimpleProxyResolver> + 'static {
    #[doc(alias = "g_simple_proxy_resolver_set_ignore_hosts")]
    fn set_ignore_hosts(&self, ignore_hosts: impl IntoStrV) {
        unsafe {
            ignore_hosts.run_with_strv(|ignore_hosts| {
                ffi::g_simple_proxy_resolver_set_ignore_hosts(
                    self.as_ref().to_glib_none().0,
                    ignore_hosts.as_ptr() as *mut _,
                );
            })
        }
    }
}

impl<O: IsA<SimpleProxyResolver>> SimpleProxyResolverExtManual for O {}
