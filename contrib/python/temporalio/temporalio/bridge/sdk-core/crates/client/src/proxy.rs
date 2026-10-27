use base64::prelude::*;
use http_body_util::Empty;
use hyper::{body::Bytes, header};
use hyper_util::{
    client::legacy::{
        Client,
        connect::{Connected, Connection},
    },
    rt::{TokioExecutor, TokioIo},
};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tonic::transport::{Channel, Endpoint};
use tower::{Service, service_fn};

#[cfg(unix)]
use tokio::net::UnixStream;

/// Options for HTTP CONNECT proxy.
#[derive(Clone, Debug, bon::Builder)]
#[builder(start_fn = new, on(String, into))]
#[non_exhaustive]
pub struct HttpConnectProxyOptions {
    /// The host:port to proxy through for TCP, or unix:/path/to/unix.sock for
    /// Unix socket (which means it must start with "unix:/").
    #[builder(start_fn)]
    pub target_addr: String,
    /// Optional HTTP basic auth for the proxy as user/pass tuple.
    pub basic_auth: Option<(String, String)>,
}

impl HttpConnectProxyOptions {
    /// Create a channel from the given endpoint that uses the HTTP CONNECT proxy.
    pub async fn connect_endpoint(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Channel, tonic::transport::Error> {
        let proxy_options = self.clone();
        let svc_fn = service_fn(move |uri: tonic::transport::Uri| {
            let proxy_options = proxy_options.clone();
            async move { proxy_options.connect(uri).await }
        });
        endpoint.connect_with_connector(svc_fn).await
    }

    async fn connect(
        &self,
        uri: tonic::transport::Uri,
    ) -> anyhow::Result<hyper::upgrade::Upgraded> {
        let uri = ensure_connect_authority_port(uri);
        debug!("Connecting to {} via proxy at {}", uri, self.target_addr);
        // Create CONNECT request
        let mut req_build = hyper::Request::builder().method("CONNECT").uri(uri);
        if let Some((user, pass)) = &self.basic_auth {
            let creds = BASE64_STANDARD.encode(format!("{user}:{pass}"));
            req_build = req_build.header(header::PROXY_AUTHORIZATION, format!("Basic {creds}"));
        }
        let req = req_build.body(Empty::<Bytes>::new())?;

        // We have to create a client with a specific connector because Hyper is
        // not letting us change the HTTP/2 authority
        let client = Client::builder(TokioExecutor::new())
            .build(OverrideAddrConnector(self.target_addr.clone()));

        // Send request
        let res = client.request(req).await?;
        if res.status().is_success() {
            Ok(hyper::upgrade::on(res).await?)
        } else {
            Err(anyhow::anyhow!(
                "CONNECT call failed with status: {}",
                res.status()
            ))
        }
    }
}

#[derive(Clone)]
struct OverrideAddrConnector(String);

impl Service<hyper::Uri> for OverrideAddrConnector {
    type Response = TokioIo<ProxyStream>;

    type Error = anyhow::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _ctx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::Uri) -> Self::Future {
        let target_addr = self.0.clone();
        let fut = async move {
            Ok(TokioIo::new(
                ProxyStream::connect(target_addr.as_str()).await?,
            ))
        };
        Box::pin(fut)
    }
}

/// Visible only for tests
#[doc(hidden)]
pub enum ProxyStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl ProxyStream {
    async fn connect(target_addr: &str) -> anyhow::Result<Self> {
        if target_addr.starts_with("unix:/") {
            #[cfg(unix)]
            {
                Ok(ProxyStream::Unix(
                    UnixStream::connect(&target_addr[5..]).await?,
                ))
            }
            #[cfg(not(unix))]
            {
                Err(anyhow::anyhow!(
                    "Unix sockets are not supported on this platform"
                ))
            }
        } else {
            Ok(ProxyStream::Tcp(TcpStream::connect(target_addr).await?))
        }
    }
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            ProxyStream::Tcp(s) => s.is_write_vectored(),
            #[cfg(unix)]
            ProxyStream::Unix(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            ProxyStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            ProxyStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Connection for ProxyStream {
    fn connected(&self) -> Connected {
        match self {
            ProxyStream::Tcp(s) => s.connected(),
            // There is no special connected metadata for Unix sockets
            #[cfg(unix)]
            ProxyStream::Unix(_) => Connected::new(),
        }
    }
}

/// Ensure the URI authority includes an explicit port so that hyper emits a
/// RFC 9110-compliant CONNECT request-target (authority-form requires host:port).
fn ensure_connect_authority_port(uri: tonic::transport::Uri) -> tonic::transport::Uri {
    if uri.port().is_some() {
        return uri;
    }
    let port = match uri.scheme_str() {
        Some("https") => 443,
        Some("http") => 80,
        _ => return uri,
    };
    let mut parts = uri.into_parts();
    if let Some(ref authority) = parts.authority
        && let Ok(new_auth) = format!("{}:{}", authority.host(), port).parse()
    {
        parts.authority = Some(new_auth);
    }
    tonic::transport::Uri::from_parts(parts).expect("adding port to valid URI should not fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };

    struct CapturedConnect {
        request_line: String,
        headers: Vec<String>,
    }

    async fn mock_proxy() -> (String, tokio::task::JoinHandle<CapturedConnect>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).await.unwrap();
            let mut headers = Vec::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line == "\r\n" {
                    break;
                }
                headers.push(line.trim_end().to_string());
            }
            reader
                .into_inner()
                .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
                .await
                .unwrap();
            CapturedConnect {
                request_line,
                headers,
            }
        });
        (addr, handle)
    }

    #[rstest::rstest]
    #[case("https://example.com/some/path", "CONNECT example.com:443 HTTP/1.1")]
    #[case("http://example.com", "CONNECT example.com:80 HTTP/1.1")]
    #[case("https://example.com:7233", "CONNECT example.com:7233 HTTP/1.1")]
    #[tokio::test]
    async fn connect_request_line(#[case] uri: &str, #[case] expected: &str) {
        let (proxy_addr, handle) = mock_proxy().await;
        let opts = HttpConnectProxyOptions::new(proxy_addr).build();
        let uri: tonic::transport::Uri = uri.parse().unwrap();
        let _ = opts.connect(uri).await;

        let captured = handle.await.unwrap();
        assert_eq!(captured.request_line.trim(), expected);
    }

    #[tokio::test]
    async fn connect_includes_basic_auth() {
        let (proxy_addr, handle) = mock_proxy().await;
        let opts = HttpConnectProxyOptions::new(proxy_addr)
            .basic_auth(("user".to_string(), "pass".to_string()))
            .build();
        let uri: tonic::transport::Uri = "https://example.com:7233".parse().unwrap();
        let _ = opts.connect(uri).await;

        let captured = handle.await.unwrap();
        let creds = BASE64_STANDARD.encode("user:pass");
        let auth_header = captured
            .headers
            .iter()
            .find(|h| h.to_lowercase().starts_with("proxy-authorization:"))
            .expect("missing proxy-authorization header");
        assert_eq!(
            auth_header.trim(),
            format!("proxy-authorization: Basic {creds}")
        );
    }
}
