use futures_util::{FutureExt, future::BoxFuture};
use temporalio_common::{
    data_converters::{PayloadCodec, PayloadConversionError, SerializationContextData},
    protos::temporal::api::common::v1::Payload,
};

pub(crate) struct XorCodec;

pub(crate) struct FailingCodec;

impl PayloadCodec for XorCodec {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move {
            Ok(payloads
                .into_iter()
                .map(|mut payload| {
                    payload.data.iter_mut().for_each(|byte| *byte ^= 0x42);
                    payload
                })
                .collect())
        }
        .boxed()
    }

    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        self.encode(context, payloads)
    }
}

impl PayloadCodec for FailingCodec {
    fn encode(
        &self,
        _: &SerializationContextData,
        _: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move {
            Err(PayloadConversionError::EncodingError(
                "codec encode failed".into(),
            ))
        }
        .boxed()
    }

    fn decode(
        &self,
        _: &SerializationContextData,
        _: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        async move {
            Err(PayloadConversionError::EncodingError(
                "codec decode failed".into(),
            ))
        }
        .boxed()
    }
}
