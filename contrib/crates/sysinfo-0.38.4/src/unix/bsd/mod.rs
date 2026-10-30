// Take a look at the license at the top of the repository in the LICENSE file.

cfg_if! {
    if #[cfg(target_os = "freebsd")] {
        pub(crate) mod freebsd;
        #[allow(unused_imports)]
        pub(crate) use freebsd::*;

        #[allow(unused_imports)]
        pub(crate) use libc::__error as libc_errno;
    } else if #[cfg(target_os = "netbsd")] {
        pub(crate) mod netbsd;
        #[allow(unused_imports)]
        pub(crate) use netbsd::*;

        #[allow(unused_imports)]
        pub(crate) use libc::__errno as libc_errno;
    } else {
        compile_error!("Invalid cfg!");
    }
}

cfg_if! {
    if #[cfg(feature = "system")] {
        pub mod system_common;

        pub use self::system_common::*;
    }
    if #[cfg(feature = "network")] {
        pub mod network_common;

        pub(crate) use self::network_common::*;
    }
}

mod common;

// Little trick to ensure `rustfmt` works on all files.
#[cfg(any())]
mod freebsd;
#[cfg(any())]
mod netbsd;
#[cfg(any())]
mod network_common;
#[cfg(any())]
mod system_common;

#[doc = include_str!("../../../md_doc/is_supported.md")]
pub const IS_SUPPORTED_SYSTEM: bool = true;
