use crate::telemetry::prometheus_meter::Registry;
use http_body_util::Full;
use hyper::{Method, Request, Response, body::Bytes, header::CONTENT_TYPE, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use prometheus::{Encoder, TextEncoder};
use std::{
    net::{SocketAddr, TcpListener},
    sync::Arc,
};
use temporal_sdk_core_api::telemetry::PrometheusExporterOptions;
use tokio::{io, task::AbortHandle};

pub struct StartedPromServer {
    pub meter: Arc<crate::telemetry::prometheus_meter::CorePrometheusMeter>,
    pub bound_addr: SocketAddr,
    pub abort_handle: AbortHandle,
}

/// Builds and runs a prometheus endpoint which can be scraped by prom instances for metrics export.
/// Returns the meter that can be used as a [CoreMeter].
///
/// Requires a Tokio runtime to exist, and will block briefly while binding the server endpoint.
pub fn start_prometheus_metric_exporter(
    opts: PrometheusExporterOptions,
) -> Result<StartedPromServer, anyhow::Error> {
    let srv = PromServer::new(&opts)?;
    let meter = Arc::new(
        crate::telemetry::prometheus_meter::CorePrometheusMeter::new(
            srv.registry().clone(),
            opts.use_seconds_for_durations,
            opts.unit_suffix,
            opts.histogram_bucket_overrides,
        ),
    );
    let bound_addr = srv.bound_addr()?;
    let handle = tokio::spawn(async move { srv.run().await });
    Ok(StartedPromServer {
        meter,
        bound_addr,
        abort_handle: handle.abort_handle(),
    })
}

/// Exposes prometheus metrics for scraping
pub(super) struct PromServer {
    listener: TcpListener,
    registry: Registry,
}

impl PromServer {
    pub(super) fn new(opts: &PrometheusExporterOptions) -> Result<Self, anyhow::Error> {
        let registry = Registry::new(opts.global_tags.clone());
        Ok(Self {
            listener: TcpListener::bind(opts.socket_addr)?,
            registry,
        })
    }

    pub(super) fn registry(&self) -> &Registry {
        &self.registry
    }

    pub(super) async fn run(self) -> Result<(), anyhow::Error> {
        // Spin up hyper server to serve metrics for scraping. We use hyper since we already depend
        // on it via Tonic.
        self.listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(self.listener)?;
        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let regclone = self.registry.clone();
            tokio::task::spawn(async move {
                let server = auto::Builder::new(TokioExecutor::new());
                if let Err(e) = server
                    .serve_connection(
                        io,
                        service_fn(move |req| metrics_req(req, regclone.clone())),
                    )
                    .await
                {
                    warn!("Error serving metrics connection: {:?}", e);
                }
            });
        }
    }

    pub(super) fn bound_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

/// Serves prometheus metrics in the expected format for scraping
async fn metrics_req(
    req: Request<hyper::body::Incoming>,
    registry: Registry,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(buffer.into())
                .unwrap()
        }
        _ => Response::builder()
            .status(404)
            .body(vec![].into())
            .expect("Can't fail to construct empty resp"),
    };
    Ok(response)
}
