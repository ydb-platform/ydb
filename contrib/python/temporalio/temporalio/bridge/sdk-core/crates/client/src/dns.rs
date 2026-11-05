use crate::{
    add_tls_to_channel,
    errors::ClientConnectError,
    options_structs::{
        ClientKeepAliveOptions, ConnectionOptions, DnsLoadBalancingOptions, TlsOptions,
    },
};
use http::Uri;
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint, channel::Change};
use url::Url;

const MIN_DNS_RESOLUTION_INTERVAL: Duration = Duration::from_secs(1);

/// Validates DNS load balancing configuration and returns the options if DNS LB should be used.
///
/// Returns `Err` if `dns_load_balancing` is set alongside `service_override` or
/// `http_connect_proxy`. Returns `Ok(None)` if DNS LB is disabled or the target is an IP literal.
pub(crate) fn validate_and_get_dns_lb(
    options: &ConnectionOptions,
) -> Result<Option<&DnsLoadBalancingOptions>, ClientConnectError> {
    let Some(dns_opts) = options.dns_load_balancing.as_ref() else {
        return Ok(None);
    };

    if options.service_override.is_some() {
        return Err(ClientConnectError::InvalidConfig(
            "dns_load_balancing cannot be used with service_override".to_owned(),
        ));
    }
    if options.http_connect_proxy.is_some() {
        return Err(ClientConnectError::InvalidConfig(
            "dns_load_balancing cannot be used with http_connect_proxy".to_owned(),
        ));
    }

    let host = options
        .target
        .host()
        .ok_or_else(|| ClientConnectError::InvalidConfig("target URL has no host".to_owned()))?;

    if dns_opts.resolution_interval < MIN_DNS_RESOLUTION_INTERVAL {
        return Err(ClientConnectError::InvalidConfig(format!(
            "dns_load_balancing.resolution_interval must be at least \
             {MIN_DNS_RESOLUTION_INTERVAL:?}, got {:?}",
            dns_opts.resolution_interval
        )));
    }

    match host {
        url::Host::Domain("localhost") => Ok(None),
        url::Host::Domain(_) => Ok(Some(dns_opts)),
        url::Host::Ipv4(_) | url::Host::Ipv6(_) => Ok(None),
    }
}

async fn resolve_host(host: &str, port: u16) -> Result<Vec<SocketAddr>, std::io::Error> {
    tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map(|addrs| addrs.collect())
}

fn endpoint_uri(addr: SocketAddr, scheme: &str) -> String {
    match addr {
        SocketAddr::V4(v4) => format!("{scheme}://{v4}"),
        SocketAddr::V6(v6) => format!("{scheme}://[{}]:{}", v6.ip(), v6.port()),
    }
}

async fn build_endpoint(
    addr: SocketAddr,
    original_host: &str,
    scheme: &str,
    tls_options: Option<&TlsOptions>,
    keep_alive: Option<&ClientKeepAliveOptions>,
    override_origin: Option<&Uri>,
    connect_timeout: Option<Duration>,
) -> Result<Endpoint, ClientConnectError> {
    let uri = endpoint_uri(addr, scheme);
    let channel = Channel::from_shared(uri)?;

    let channel = if let Some(timeout) = connect_timeout {
        channel.connect_timeout(timeout)
    } else {
        channel
    };

    // When connecting to an IP with TLS, SNI must use the original hostname.
    let tls_for_ip = tls_options.map(|tls| {
        if tls.domain.is_some() {
            tls.clone()
        } else {
            let mut patched = tls.clone();
            patched.domain = Some(original_host.to_owned());
            patched
        }
    });
    let tls_result = add_tls_to_channel(tls_for_ip.as_ref().or(tls_options), channel).await?;

    let channel = match tls_result {
        crate::TlsConfigResult::Standard(ep) => ep,
        #[cfg(feature = "dynamic-tls")]
        crate::TlsConfigResult::CustomConnector { .. } => {
            return Err(ClientConnectError::InvalidConfig(
                "client_cert_resolver is not yet supported with dns_load_balancing. \
                 Disable dns_load_balancing or use static client_tls_options instead."
                    .to_owned(),
            ));
        }
    };

    let channel = if let Some(keep_alive) = keep_alive {
        channel
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(keep_alive.interval)
            .keep_alive_timeout(keep_alive.timeout)
    } else {
        channel
    };

    let channel = if let Some(origin) = override_origin.cloned() {
        channel.origin(origin)
    } else {
        channel
    };

    Ok(channel)
}

/// Creates a balanced channel backed by all DNS-resolved addresses for the target.
pub(crate) async fn create_balanced_channel(
    options: &ConnectionOptions,
) -> Result<(Channel, mpsc::Sender<Change<SocketAddr, Endpoint>>), ClientConnectError> {
    let host = options
        .target
        .host_str()
        .ok_or_else(|| ClientConnectError::InvalidConfig("target URL has no host".to_owned()))?;
    let port = options.target.port_or_known_default().unwrap_or(7233);
    let scheme = options.target.scheme();

    let addrs = resolve_host(host, port).await.map_err(|source| {
        ClientConnectError::DnsResolutionError {
            host: host.to_owned(),
            source,
        }
    })?;
    if addrs.is_empty() {
        return Err(ClientConnectError::DnsResolutionError {
            host: host.to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "DNS resolution returned no addresses",
            ),
        });
    }

    let (channel, sender) = Channel::balance_channel(addrs.len());

    for addr in addrs {
        let endpoint = build_endpoint(
            addr,
            host,
            scheme,
            options.tls_options.as_ref(),
            options.keep_alive.as_ref(),
            options.override_origin.as_ref(),
            options.connect_timeout,
        )
        .await?;
        // Unbounded-ish send into the freshly-created channel; can't realistically fail.
        let _ = sender.send(Change::Insert(addr, endpoint)).await;
    }

    Ok((channel, sender))
}

/// Handle that aborts the DNS re-resolution task when dropped.
#[derive(Debug)]
pub(crate) struct DnsReresolutionHandle {
    abort_handle: tokio::task::AbortHandle,
}

impl Drop for DnsReresolutionHandle {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

/// Spawns a background task that periodically re-resolves DNS and updates the balanced channel.
pub(crate) fn spawn_dns_reresolution(
    sender: mpsc::Sender<Change<SocketAddr, Endpoint>>,
    target: Url,
    tls_options: Option<TlsOptions>,
    keep_alive: Option<ClientKeepAliveOptions>,
    override_origin: Option<Uri>,
    resolution_interval: Duration,
    connect_timeout: Option<Duration>,
) -> Arc<DnsReresolutionHandle> {
    let host = target.host_str().unwrap_or("").to_owned();
    let port = target.port_or_known_default().unwrap_or(7233);
    let scheme = target.scheme().to_owned();

    let handle = tokio::spawn(async move {
        let mut current_addrs: HashSet<SocketAddr> = HashSet::new();
        // Populate initial set from the channel we already seeded
        if let Ok(initial) = resolve_host(&host, port).await {
            current_addrs.extend(initial);
        }

        loop {
            tokio::time::sleep(resolution_interval).await;

            let new_addrs = match resolve_host(&host, port).await {
                Ok(addrs) => addrs.into_iter().collect::<HashSet<_>>(),
                Err(e) => {
                    warn!(
                        host = %host,
                        error = %e,
                        "DNS re-resolution failed, keeping existing endpoints"
                    );
                    continue;
                }
            };

            if new_addrs.is_empty() {
                warn!(
                    host = %host,
                    "DNS re-resolution returned no addresses, keeping existing endpoints"
                );
                continue;
            }

            // Remove stale endpoints
            for addr in current_addrs.difference(&new_addrs) {
                if sender.send(Change::Remove(*addr)).await.is_err() {
                    return;
                }
            }

            // Add new endpoints
            for addr in new_addrs.difference(&current_addrs) {
                match build_endpoint(
                    *addr,
                    &host,
                    &scheme,
                    tls_options.as_ref(),
                    keep_alive.as_ref(),
                    override_origin.as_ref(),
                    connect_timeout,
                )
                .await
                {
                    Ok(endpoint) => {
                        if sender.send(Change::Insert(*addr, endpoint)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(
                            addr = %addr,
                            error = %e,
                            "Failed to build endpoint for resolved address"
                        );
                    }
                }
            }

            current_addrs = new_addrs;
        }
    });

    Arc::new(DnsReresolutionHandle {
        abort_handle: handle.abort_handle(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_v4_target_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://1.2.3.4:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn ip_v6_target_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://[::1]:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn domain_target_returns_some() {
        let opts =
            ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_some());
    }

    #[test]
    fn disabled_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap())
            .dns_load_balancing(None)
            .build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn service_override_with_dns_lb_is_error() {
        use crate::callback_based::CallbackBasedGrpcService;

        let svc = CallbackBasedGrpcService {
            callback: Arc::new(|_| Box::pin(async { unreachable!() })),
        };
        let opts = ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap())
            .service_override(svc)
            .build();
        assert!(validate_and_get_dns_lb(&opts).is_err());
    }

    #[test]
    fn localhost_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn endpoint_uri_v4() {
        let addr: SocketAddr = "1.2.3.4:7233".parse().unwrap();
        assert_eq!(endpoint_uri(addr, "https"), "https://1.2.3.4:7233");
    }

    #[test]
    fn endpoint_uri_v6() {
        let addr: SocketAddr = "[::1]:7233".parse().unwrap();
        assert_eq!(endpoint_uri(addr, "https"), "https://[::1]:7233");
    }

    #[test]
    fn zero_resolution_interval_is_error() {
        let opts = ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap())
            .dns_load_balancing(Some(
                DnsLoadBalancingOptions::builder()
                    .resolution_interval(Duration::ZERO)
                    .build(),
            ))
            .build();
        assert!(validate_and_get_dns_lb(&opts).is_err());
    }

    #[test]
    fn sub_minimum_resolution_interval_is_error() {
        let opts = ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap())
            .dns_load_balancing(Some(
                DnsLoadBalancingOptions::builder()
                    .resolution_interval(Duration::from_millis(500))
                    .build(),
            ))
            .build();
        assert!(validate_and_get_dns_lb(&opts).is_err());
    }
}
