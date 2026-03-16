//! This module implements traits defined in the client to dispatch a
//! WFT to a worker bypassing the server.
//! This enables latency optimizations such as Eager Workflow Start.

use crate::{
    abstractions::{MeteredPermitDealer, OwnedMeteredSemPermit},
    protosext::ValidPollWFTQResponse,
    worker::workflow::wft_poller::validate_wft,
};
use temporal_client::{Slot as SlotTrait, SlotProvider as SlotProviderTrait};
use temporal_sdk_core_api::worker::WorkflowSlotKind;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use tokio::sync::mpsc::UnboundedSender;
use tonic::Status;

type WFTStreamSender = UnboundedSender<
    Result<
        (
            ValidPollWFTQResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        ),
        Status,
    >,
>;

struct Slot {
    permit: OwnedMeteredSemPermit<WorkflowSlotKind>,
    external_wft_tx: WFTStreamSender,
}

impl Slot {
    fn new(
        permit: OwnedMeteredSemPermit<WorkflowSlotKind>,
        external_wft_tx: WFTStreamSender,
    ) -> Self {
        Self {
            permit,
            external_wft_tx,
        }
    }
}

impl SlotTrait for Slot {
    fn schedule_wft(
        self: Box<Self>,
        task: PollWorkflowTaskQueueResponse,
    ) -> Result<(), anyhow::Error> {
        let wft = validate_wft(task)?;
        self.external_wft_tx.send(Ok((wft, self.permit)))?;
        Ok(())
    }
}

#[derive(derive_more::Debug)]
#[debug("SlotProvider {{ namespace:{namespace}, task_queue: {task_queue} }}")]
pub(super) struct SlotProvider {
    namespace: String,
    task_queue: String,
    wft_semaphore: MeteredPermitDealer<WorkflowSlotKind>,
    external_wft_tx: WFTStreamSender,
}

impl SlotProvider {
    pub(super) fn new(
        namespace: String,
        task_queue: String,
        wft_semaphore: MeteredPermitDealer<WorkflowSlotKind>,
        external_wft_tx: WFTStreamSender,
    ) -> Self {
        Self {
            namespace,
            task_queue,
            wft_semaphore,
            external_wft_tx,
        }
    }
}

impl SlotProviderTrait for SlotProvider {
    fn namespace(&self) -> &str {
        &self.namespace
    }
    fn task_queue(&self) -> &str {
        &self.task_queue
    }
    fn try_reserve_wft_slot(&self) -> Option<Box<dyn SlotTrait + Send>> {
        match self.wft_semaphore.try_acquire_owned().ok() {
            Some(permit) => Some(Box::new(Slot::new(permit, self.external_wft_tx.clone()))),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::abstractions::tests::fixed_size_permit_dealer;
    use temporal_sdk_core_protos::temporal::api::{
        common::v1::{WorkflowExecution, WorkflowType},
        history::v1::History,
        taskqueue::v1::TaskQueue,
    };
    use tokio::sync::mpsc::unbounded_channel;

    // make validate_wft() happy
    fn new_validatable_response() -> PollWorkflowTaskQueueResponse {
        PollWorkflowTaskQueueResponse {
            task_token: vec![1, 3, 3, 7],
            workflow_execution_task_queue: Some(TaskQueue::default()),
            workflow_execution: Some(WorkflowExecution::default()),
            workflow_type: Some(WorkflowType::default()),
            history: Some(History::default()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn slot_propagates_through_channel() {
        let wft_semaphore = fixed_size_permit_dealer(2);
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();

        let provider = SlotProvider::new(
            "my_namespace".to_string(),
            "my_queue".to_string(),
            wft_semaphore,
            external_wft_tx,
        );

        let slot = provider
            .try_reserve_wft_slot()
            .expect("failed to reserve slot");
        let p = slot.schedule_wft(new_validatable_response());
        assert!(p.is_ok());
        assert!(external_wft_rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn channel_closes_when_provider_drops() {
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();
        {
            let external_wft_tx = external_wft_tx;
            let wft_semaphore = fixed_size_permit_dealer(2);
            let provider = SlotProvider::new(
                "my_namespace".to_string(),
                "my_queue".to_string(),
                wft_semaphore,
                external_wft_tx,
            );
            assert!(provider.try_reserve_wft_slot().is_some());
        }
        assert!(external_wft_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn unused_slots_reclaimed() {
        let wft_semaphore = fixed_size_permit_dealer(2);
        {
            let wft_semaphore = wft_semaphore.clone();
            let (external_wft_tx, _) = unbounded_channel();
            let provider = SlotProvider::new(
                "my_namespace".to_string(),
                "my_queue".to_string(),
                wft_semaphore.clone(),
                external_wft_tx,
            );
            let slot = provider.try_reserve_wft_slot();
            assert!(slot.is_some());
            assert_eq!(wft_semaphore.available_permits(), Some(1));
            // drop slot without using it
        }
        assert_eq!(wft_semaphore.available_permits(), Some(2));
    }
}
