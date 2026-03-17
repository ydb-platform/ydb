//! This module implements support for callback-based gRPC service that has a callback invoked for
//! every gRPC call instead of directly using the network.

use anyhow::anyhow;
use bytes::{BufMut, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::stream;
use http::{HeaderMap, Request, Response};
use http_body_util::{BodyExt, StreamBody, combinators::BoxBody};
use hyper::body::{Bytes, Frame};
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tonic::{Status, metadata::GRPC_CONTENT_TYPE};
use tower::Service;

/// gRPC request for use by a callback.
pub struct GrpcRequest {
    /// Fully qualified gRPC service name.
    pub service: String,
    /// RPC name.
    pub rpc: String,
    /// Request headers.
    pub headers: HeaderMap,
    /// Protobuf bytes of the request.
    pub proto: Bytes,
}

/// Successful gRPC response returned by a callback.
pub struct GrpcSuccessResponse {
    /// Response headers.
    pub headers: HeaderMap,

    /// Response proto bytes.
    pub proto: Vec<u8>,
}

/// gRPC service that invokes the given callback on each call.
#[derive(Clone)]
pub struct CallbackBasedGrpcService {
    /// Callback to invoke on each RPC call.
    #[allow(clippy::type_complexity)] // Signature is not that complex
    pub callback: Arc<
        dyn Fn(GrpcRequest) -> BoxFuture<'static, Result<GrpcSuccessResponse, Status>>
            + Send
            + Sync,
    >,
}

impl Service<Request<tonic::body::Body>> for CallbackBasedGrpcService {
    type Response = http::Response<tonic::body::Body>;
    type Error = anyhow::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<tonic::body::Body>) -> Self::Future {
        let callback = self.callback.clone();

        Box::pin(async move {
            // Build req
            let (parts, body) = req.into_parts();
            let mut path_parts = parts.uri.path().trim_start_matches('/').split('/');
            let req_body = body.collect().await.map_err(|e| anyhow!(e))?.to_bytes();
            // Body is flag saying whether compressed (we do not support that), then 32-bit length,
            // then the actual proto.
            if req_body.len() < 5 {
                return Err(anyhow!("Too few request bytes: {}", req_body.len()));
            } else if req_body[0] != 0 {
                return Err(anyhow!("Compression not supported"));
            }
            let req_proto_len =
                u32::from_be_bytes([req_body[1], req_body[2], req_body[3], req_body[4]]) as usize;
            if req_body.len() < 5 + req_proto_len {
                return Err(anyhow!(
                    "Expected request body length at least {}, got {}",
                    5 + req_proto_len,
                    req_body.len()
                ));
            }
            let req = GrpcRequest {
                service: path_parts.next().unwrap_or_default().to_owned(),
                rpc: path_parts.next().unwrap_or_default().to_owned(),
                headers: parts.headers,
                proto: req_body.slice(5..5 + req_proto_len),
            };

            // Invoke and handle response
            match (callback)(req).await {
                Ok(success) => {
                    // Create body bytes which requires a flag saying whether compressed, then
                    // message len, then actual message. So we create a Bytes for those 5 prepend
                    // parts, then stream it alongside the user-provided Vec. This allows us to
                    // avoid copying the vec
                    let mut body_prepend = BytesMut::with_capacity(5);
                    body_prepend.put_u8(0); // 0 means no compression
                    body_prepend.put_u32(success.proto.len() as u32);
                    let stream = stream::iter(vec![
                        Ok::<_, Status>(Frame::data(Bytes::from(body_prepend))),
                        Ok::<_, Status>(Frame::data(Bytes::from(success.proto))),
                    ]);
                    let stream_body = StreamBody::new(stream);
                    let full_body = BoxBody::new(stream_body).boxed();

                    // Build response appending headers
                    let mut resp_builder = Response::builder()
                        .status(200)
                        .header(http::header::CONTENT_TYPE, GRPC_CONTENT_TYPE);
                    for (key, value) in success.headers.iter() {
                        resp_builder = resp_builder.header(key, value);
                    }
                    Ok(resp_builder
                        .body(tonic::body::Body::new(full_body))
                        .map_err(|e| anyhow!(e))?)
                }
                Err(status) => Ok(status.into_http()),
            }
        })
    }
}
