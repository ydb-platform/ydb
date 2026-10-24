// Take a look at the license at the top of the repository in the LICENSE file.

use std::net::IpAddr;

use glib::{prelude::*, translate::*};

use crate::{InetAddress, SocketFamily, ffi, prelude::*};

#[derive(Debug)]
pub enum InetAddressBytes<'a> {
    V4(&'a [u8; 4]),
    V6(&'a [u8; 16]),
}

impl InetAddressBytes<'_> {
    #[inline]
    fn deref(&self) -> &[u8] {
        use self::InetAddressBytes::*;

        match *self {
            V4(bytes) => bytes,
            V6(bytes) => bytes,
        }
    }
}

impl InetAddress {
    #[doc(alias = "g_inet_address_new_from_bytes")]
    pub fn from_bytes(inet_address_bytes: InetAddressBytes) -> Self {
        let bytes = inet_address_bytes.deref();

        let family = match inet_address_bytes {
            InetAddressBytes::V4(_) => SocketFamily::Ipv4,
            InetAddressBytes::V6(_) => SocketFamily::Ipv6,
        };
        unsafe {
            from_glib_full(ffi::g_inet_address_new_from_bytes(
                bytes.to_glib_none().0,
                family.into_glib(),
            ))
        }
    }

    #[cfg(feature = "v2_86")]
    #[cfg_attr(docsrs, doc(cfg(feature = "v2_86")))]
    #[doc(alias = "g_inet_address_new_from_bytes_with_ipv6_info")]
    #[doc(alias = "new_from_bytes_with_ipv6_info")]
    pub fn from_bytes_with_ipv6_info(
        inet_address_bytes: InetAddressBytes,
        flowinfo: u32,
        scope_id: u32,
    ) -> InetAddress {
        let bytes = inet_address_bytes.deref();

        let family = match inet_address_bytes {
            InetAddressBytes::V4(_) => SocketFamily::Ipv4,
            InetAddressBytes::V6(_) => SocketFamily::Ipv6,
        };
        unsafe {
            from_glib_full(ffi::g_inet_address_new_from_bytes_with_ipv6_info(
                bytes.to_glib_none().0,
                family.into_glib(),
                flowinfo,
                scope_id,
            ))
        }
    }
}

pub trait InetAddressExtManual: IsA<InetAddress> + 'static {
    // rustdoc-stripper-ignore-next
    /// Returns `None` in case the address has a native size different than 4 and 16.
    #[doc(alias = "g_inet_address_to_bytes")]
    #[inline]
    fn to_bytes(&self) -> Option<InetAddressBytes<'_>> {
        let size = self.native_size();
        unsafe {
            let bytes = ffi::g_inet_address_to_bytes(self.as_ref().to_glib_none().0);
            if size == 4 {
                Some(InetAddressBytes::V4(&*(bytes as *const [u8; 4])))
            } else if size == 16 {
                Some(InetAddressBytes::V6(&*(bytes as *const [u8; 16])))
            } else {
                None
            }
        }
    }
}

impl<O: IsA<InetAddress>> InetAddressExtManual for O {}

impl From<IpAddr> for InetAddress {
    fn from(addr: IpAddr) -> Self {
        match addr {
            IpAddr::V4(v4) => Self::from_bytes(InetAddressBytes::V4(&v4.octets())),
            IpAddr::V6(v6) => Self::from_bytes(InetAddressBytes::V6(&v6.octets())),
        }
    }
}

impl From<InetAddress> for IpAddr {
    fn from(addr: InetAddress) -> Self {
        match addr.to_bytes() {
            Some(InetAddressBytes::V4(bytes)) => IpAddr::from(*bytes),
            Some(InetAddressBytes::V6(bytes)) => IpAddr::from(*bytes),
            None => panic!("Unknown IP kind"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use crate::InetAddress;

    #[test]
    fn test_ipv6_to_rust() {
        let rust_addr = "2606:50c0:8000::153".parse::<IpAddr>().unwrap();
        assert!(rust_addr.is_ipv6());
        let gio_addr = InetAddress::from(rust_addr);
        assert_eq!(rust_addr, IpAddr::from(gio_addr));
    }

    #[test]
    fn test_ipv4_to_rust() {
        let rust_addr = "185.199.108.153".parse::<IpAddr>().unwrap();
        assert!(rust_addr.is_ipv4());
        let gio_addr = InetAddress::from(rust_addr);
        assert_eq!(rust_addr, IpAddr::from(gio_addr));
    }
}
