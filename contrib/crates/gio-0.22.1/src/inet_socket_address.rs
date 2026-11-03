// Take a look at the license at the top of the repository in the LICENSE file.

use std::net::SocketAddr;

use crate::{InetAddress, InetSocketAddress, prelude::*};

impl From<SocketAddr> for InetSocketAddress {
    fn from(addr: SocketAddr) -> Self {
        Self::new(&InetAddress::from(addr.ip()), addr.port())
    }
}

impl From<InetSocketAddress> for SocketAddr {
    fn from(addr: InetSocketAddress) -> Self {
        Self::new(addr.address().into(), addr.port())
    }
}
