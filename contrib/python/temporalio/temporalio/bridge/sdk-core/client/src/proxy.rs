use base64::prelude::*;
use http_body_util::Empty;
use hyper::{body::Bytes, header};
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::net::TcpStream;
use tonic::transport::{Channel, Endpoint};
use tower::{Service, service_fn};

/// Options for HTTP CONNECT proxy.
#[derive(Clone, Debug)]
pub struct HttpConnectProxyOptions {
    /// The host:port to proxy through.
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
    type Response = TokioIo<TcpStream>;

    type Error = anyhow::Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _ctx: &mut Context<'_>) -> Poll<anyhow::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::Uri) -> Self::Future {
        let target_addr = self.0.clone();
        let fut = async move { Ok(TokioIo::new(TcpStream::connect(target_addr).await?)) };
        Box::pin(fut)
    }
}
