// Take a look at the license at the top of the repository in the LICENSE file.

pub type GSocketFamily = libc::c_int;
pub type GSocketMsgFlags = libc::c_int;

#[cfg(target_family = "windows")]
mod windows_constants {
    pub const G_SOCKET_FAMILY_INVALID: super::GSocketFamily =
        windows_sys::Win32::Networking::WinSock::AF_UNSPEC as super::GSocketFamily;
    pub const G_SOCKET_FAMILY_UNIX: super::GSocketFamily =
        windows_sys::Win32::Networking::WinSock::AF_UNIX as super::GSocketFamily;
    pub const G_SOCKET_FAMILY_IPV4: super::GSocketFamily =
        windows_sys::Win32::Networking::WinSock::AF_INET as super::GSocketFamily;
    pub const G_SOCKET_FAMILY_IPV6: super::GSocketFamily =
        windows_sys::Win32::Networking::WinSock::AF_INET6 as super::GSocketFamily;

    pub const G_SOCKET_MSG_NONE: super::GSocketMsgFlags = 0;
    pub const G_SOCKET_MSG_OOB: super::GSocketMsgFlags =
        windows_sys::Win32::Networking::WinSock::MSG_OOB;
    pub const G_SOCKET_MSG_PEEK: super::GSocketMsgFlags =
        windows_sys::Win32::Networking::WinSock::MSG_PEEK;
    pub const G_SOCKET_MSG_DONTROUTE: super::GSocketMsgFlags =
        windows_sys::Win32::Networking::WinSock::MSG_DONTROUTE;
}
#[cfg(target_family = "windows")]
pub use windows_constants::*;

#[cfg(not(target_family = "windows"))]
mod libc_constants {
    pub const G_SOCKET_FAMILY_INVALID: super::GSocketFamily = libc::AF_UNSPEC;
    pub const G_SOCKET_FAMILY_UNIX: super::GSocketFamily = libc::AF_UNIX;
    pub const G_SOCKET_FAMILY_IPV4: super::GSocketFamily = libc::AF_INET;
    pub const G_SOCKET_FAMILY_IPV6: super::GSocketFamily = libc::AF_INET6;

    pub const G_SOCKET_MSG_NONE: super::GSocketMsgFlags = 0;
    pub const G_SOCKET_MSG_OOB: super::GSocketMsgFlags = libc::MSG_OOB;
    pub const G_SOCKET_MSG_PEEK: super::GSocketMsgFlags = libc::MSG_PEEK;
    pub const G_SOCKET_MSG_DONTROUTE: super::GSocketMsgFlags = libc::MSG_DONTROUTE;
}
#[cfg(not(target_family = "windows"))]
pub use libc_constants::*;
