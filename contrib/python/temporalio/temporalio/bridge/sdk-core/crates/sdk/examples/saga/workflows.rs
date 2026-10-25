#![allow(unreachable_pub)]
use futures_util::future::LocalBoxFuture;
use std::time::Duration;
use temporalio_common::ActivityDefinition;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, ApplicationFailure, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

#[workflow]
#[derive(Default)]
pub struct SagaWorkflow;

#[workflow_methods]
impl SagaWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        trip_id: String,
    ) -> WorkflowResult<Vec<String>> {
        let mut saga = Saga::new(ctx, activity_opts());
        match Self::book_trip(&mut saga, trip_id).await {
            Ok(ids) => Ok(ids),
            Err(e) => {
                saga.compensate().await;
                Err(e.into())
            }
        }
    }

    async fn book_trip(
        saga: &mut Saga,
        trip_id: String,
    ) -> Result<Vec<String>, ActivityExecutionError> {
        let hotel = saga
            .step(
                BookingActivities::book_hotel,
                trip_id.clone(),
                BookingActivities::cancel_hotel,
            )
            .await?;
        let flight = saga
            .step(
                BookingActivities::book_flight,
                trip_id.clone(),
                BookingActivities::cancel_flight,
            )
            .await?;
        let car = saga
            .step(
                BookingActivities::book_car,
                trip_id.clone(),
                BookingActivities::cancel_car,
            )
            .await?;
        Ok(vec![hotel, flight, car])
    }
}

/// Records compensations and runs them in reverse on failure.
struct Saga {
    ctx: WorkflowContext<SagaWorkflow>,
    opts: ActivityOptions,
    compensations: Vec<LocalBoxFuture<'static, ()>>,
}

impl Saga {
    fn new(ctx: &WorkflowContext<SagaWorkflow>, opts: ActivityOptions) -> Self {
        Self {
            ctx: ctx.clone(),
            opts,
            compensations: Vec::new(),
        }
    }

    async fn step<Step, Compensation>(
        &mut self,
        forward: Step,
        input: impl Into<Step::Input>,
        compensate: Compensation,
    ) -> Result<Step::Output, ActivityExecutionError>
    where
        Step: ActivityDefinition,
        Step::Output: Clone + Into<Compensation::Input>,
        Compensation: ActivityDefinition<Output = ()> + 'static,
    {
        let out = self
            .ctx
            .execute_activity(forward, input, self.opts.clone())
            .await?;
        let cmp_input: Compensation::Input = out.clone().into();
        let ctx = self.ctx.clone();
        let opts = self.opts.clone();
        let compensation_name = compensate.name().to_owned();
        self.compensations.push(Box::pin(async move {
            if let Err(e) = ctx.execute_activity(compensate, cmp_input, opts).await {
                eprintln!("Compensation {compensation_name} failed: {e}");
            }
        }));
        Ok(out)
    }

    async fn compensate(self) {
        for c in self.compensations.into_iter().rev() {
            c.await;
        }
    }
}

pub struct BookingActivities;

#[activities]
impl BookingActivities {
    #[activity]
    pub async fn book_hotel(
        _ctx: ActivityContext,
        trip_id: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("hotel-{trip_id}"))
    }

    #[activity]
    pub async fn book_flight(
        _ctx: ActivityContext,
        trip_id: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("flight-{trip_id}"))
    }

    #[activity]
    pub async fn book_car(_ctx: ActivityContext, trip_id: String) -> Result<String, ActivityError> {
        if trip_id.contains("fail") {
            return Err(ActivityError::application(
                ApplicationFailure::non_retryable("Car booking failed for trip {trip_id}"),
            ));
        }
        Ok(format!("car-{trip_id}"))
    }

    #[activity]
    pub async fn cancel_hotel(
        _ctx: ActivityContext,
        booking_id: String,
    ) -> Result<(), ActivityError> {
        println!("Cancelled hotel booking: {booking_id}");
        Ok(())
    }

    #[activity]
    pub async fn cancel_flight(
        _ctx: ActivityContext,
        booking_id: String,
    ) -> Result<(), ActivityError> {
        println!("Cancelled flight booking: {booking_id}");
        Ok(())
    }

    #[activity]
    pub async fn cancel_car(
        _ctx: ActivityContext,
        booking_id: String,
    ) -> Result<(), ActivityError> {
        println!("Cancelled car booking: {booking_id}");
        Ok(())
    }
}

fn activity_opts() -> ActivityOptions {
    ActivityOptions::start_to_close_timeout(Duration::from_secs(1))
}
