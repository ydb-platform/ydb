use crate::{
    trace::{Span, SpanContext, Status},
    KeyValue,
};
use std::borrow::Cow;

#[derive(Debug)]
pub struct TestSpan(pub SpanContext);

impl Span for TestSpan {
    fn add_event_with_timestamp<T>(
        &mut self,
        _name: T,
        _timestamp: std::time::SystemTime,
        _attributes: Vec<KeyValue>,
    ) where
        T: Into<Cow<'static, str>>,
    {
    }
    fn span_context(&self) -> &SpanContext {
        &self.0
    }
    fn is_recording(&self) -> bool {
        false
    }
    fn set_attribute(&mut self, _attribute: KeyValue) {}
    fn set_status(&mut self, _status: Status) {}
    fn update_name<T>(&mut self, _new_name: T)
    where
        T: Into<Cow<'static, str>>,
    {
    }

    fn add_link(&mut self, _span_context: SpanContext, _attributes: Vec<KeyValue>) {}
    fn end_with_timestamp(&mut self, _timestamp: std::time::SystemTime) {}
}
