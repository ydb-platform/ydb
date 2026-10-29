// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
#[path = "../tests/common/mod.rs"]
mod common;

use crate::common::{
    DONT_AUTO_INIT_INTEG_TELEM, get_integ_runtime_options, prom_metrics, replay_sdk_worker,
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
use temporalio_common::telemetry::metrics::{MetricKeyValue, MetricParameters, NewAttributes};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{
    CoreRuntime,
    replay::{DEFAULT_WORKFLOW_TYPE, HistoryForReplay, canned_histories},
};

pub fn criterion_benchmark(c: &mut Criterion) {
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _g = tokio_runtime.enter();
    DONT_AUTO_INIT_INTEG_TELEM.set(true);

    let num_timers = 10;
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let hist = HistoryForReplay::new(t.get_full_history_info().unwrap(), "whatever".to_string());

    c.bench_function("Small history replay", |b| {
        b.to_async(&tokio_runtime).iter_batched(
            || replay_sdk_worker([hist.clone()]),
            |mut worker| async move {
                worker
                    .register_workflow_with_factory(move || TimersWf { num_timers })
                    .unwrap();
                worker.run().await.unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    let num_tasks = 50;
    let t = canned_histories::lots_of_big_signals(num_tasks);
    let hist = HistoryForReplay::new(t.get_full_history_info().unwrap(), "whatever".to_string());

    c.bench_function("Large payloads history replay", |b| {
        b.to_async(&tokio_runtime).iter_batched(
            || replay_sdk_worker([hist.clone()]),
            |mut worker| async move {
                worker
                    .register_workflow_with_factory(move || BigSignalsWf {
                        num_tasks,
                        signal_count: 0,
                    })
                    .unwrap();
                worker.run().await.unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

pub fn bench_metrics(c: &mut Criterion) {
    DONT_AUTO_INIT_INTEG_TELEM.set(true);
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _tokio = tokio_runtime.enter();
    let (mut telemopts, _addr, _aborter) = prom_metrics(None);
    telemopts.logging = None;
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let meter = rt.telemetry().get_metric_meter().unwrap();

    c.bench_function("Record with new attributes on each call", move |b| {
        b.iter_batched(
            || {
                let c = meter.counter(MetricParameters::builder().name("c").build());
                let h = meter.histogram(MetricParameters::builder().name("h").build());
                let g = meter.gauge(MetricParameters::builder().name("g").build());

                let vals = [1, 2, 3, 4, 5];
                let labels = ["l1", "l2"];

                let (start_tx, start_rx) = mpsc::channel();
                let start_rx = Arc::new(std::sync::Mutex::new(start_rx));

                let mut thread_handles = Vec::new();
                for _ in 0..3 {
                    let c = c.clone();
                    let h = h.clone();
                    let g = g.clone();
                    let meter = meter.clone();
                    let start_rx = start_rx.clone();

                    let handle = thread::spawn(move || {
                        // Wait for start signal
                        let _ = start_rx.lock().unwrap().recv();

                        for _ in 1..=100 {
                            for &val in &vals {
                                for &label in &labels {
                                    let attribs = meter.new_attributes(NewAttributes::from(vec![
                                        MetricKeyValue::new("label", label),
                                    ]));
                                    c.add(val, &attribs);
                                    h.record(val, &attribs);
                                    g.record(val, &attribs);
                                }
                            }
                        }
                    });
                    thread_handles.push(handle);
                }

                (start_tx, thread_handles)
            },
            |(start_tx, thread_handles)| {
                for _ in 0..3 {
                    let _ = start_tx.send(());
                }
                for handle in thread_handles {
                    let _ = handle.join();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, criterion_benchmark, bench_metrics);
criterion_main!(benches);

#[workflow]
struct TimersWf {
    num_timers: u32,
}

#[workflow_methods(factory_only)]
impl TimersWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for _ in 1..=ctx.state(|s| s.num_timers) {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    }
}

#[workflow]
struct BigSignalsWf {
    num_tasks: usize,
    signal_count: usize,
}

#[workflow_methods(factory_only)]
impl BigSignalsWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let target_count = ctx.state(|s| s.num_tasks * 5);
        ctx.wait_condition(|s| s.signal_count >= target_count)
            .await?;
        Ok(().into())
    }

    #[signal(name = "bigsig")]
    fn handle_signal(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: Vec<u8>) {
        self.signal_count += 1;
    }
}
