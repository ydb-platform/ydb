mod activity_execution_info;
mod activity_handle;

use crate::errors::ClientError;
pub use activity_execution_info::{
    ActivityExecutionDescription, ActivityExecutionInfo, ActivityExecutionInfoLike,
    ActivityExecutionStatus, PendingActivityState,
};
pub use activity_handle::ActivityHandle;
use futures_util::{Stream, StreamExt};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use temporalio_common::{
    protos::temporal::api::{
        activity::v1::ActivityExecutionListInfo,
        workflowservice::v1::{
            CountActivityExecutionsResponse, count_activity_executions_response,
        },
    },
    search_attributes::{SearchAttributeError, SearchAttributeValue},
};

/// A stream of activity executions from a list query.
/// Internally paginates through results from the server.
pub struct ListActivitiesStream {
    inner: Pin<Box<dyn Stream<Item = Result<Vec<ActivityExecutionListInfo>, ClientError>> + Send>>,
    buffer: VecDeque<ActivityExecutionListInfo>,
}

impl ListActivitiesStream {
    pub(crate) fn new(
        stream: impl Stream<Item = Result<Vec<ActivityExecutionListInfo>, ClientError>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(stream),
            buffer: VecDeque::new(),
        }
    }
}

impl Stream for ListActivitiesStream {
    type Item = Result<ActivityExecutionInfo, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(info) = self.buffer.pop_front() {
                return Poll::Ready(Some(Ok(info.into())));
            }
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(items))) => {
                    self.buffer = items.into();
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

/// Result of an activity count operation.
///
/// If the query includes a group-by clause, `groups` will contain the aggregated
/// counts and `count` will be the sum of all group counts.
#[derive(Debug, Clone)]
pub struct ActivityExecutionCount {
    count: usize,
    groups: Vec<ActivityExecutionCountAggregationGroup>,
}

impl ActivityExecutionCount {
    pub(crate) fn from_response(resp: CountActivityExecutionsResponse) -> Self {
        Self {
            count: resp.count as usize,
            groups: resp
                .groups
                .into_iter()
                .map(ActivityExecutionCountAggregationGroup::from_proto)
                .collect(),
        }
    }

    /// The approximate number of activities matching the query.
    /// If grouping was applied, this is the sum of all group counts.
    pub fn count(&self) -> usize {
        self.count
    }

    /// The groups if the query had a group-by clause, or empty if not.
    pub fn groups(&self) -> &[ActivityExecutionCountAggregationGroup] {
        &self.groups
    }
}

/// Aggregation group from an activity count query with a group-by clause.
#[derive(Debug, Clone)]
pub struct ActivityExecutionCountAggregationGroup {
    raw: count_activity_executions_response::AggregationGroup,
}

impl ActivityExecutionCountAggregationGroup {
    fn from_proto(proto: count_activity_executions_response::AggregationGroup) -> Self {
        Self { raw: proto }
    }

    /// Retrieve a typed group value at `index`.
    ///
    ///  Returns `None` if the index is out of bounds or deserialization fails.
    ///  Use [`Self::try_get`] for explicit error handling.
    pub fn get<T: SearchAttributeValue>(&self, index: usize) -> Option<T> {
        self.try_get(index).ok().flatten()
    }

    /// Retrieve a typed group value at `index`, preserving deserialization
    /// errors.
    ///
    /// Returns `Ok(None)` if the index is out of bounds and `Err` if the
    /// payload cannot be deserialized.
    pub fn try_get<T: SearchAttributeValue>(
        &self,
        index: usize,
    ) -> Result<Option<T>, SearchAttributeError> {
        match self.raw.group_values.get(index) {
            Some(payload) => T::from_search_attribute_payload(payload).map(Some),
            None => Ok(None),
        }
    }

    /// The approximate number of workflows matching for this group.
    pub fn count(&self) -> usize {
        self.raw.count as usize
    }
}
