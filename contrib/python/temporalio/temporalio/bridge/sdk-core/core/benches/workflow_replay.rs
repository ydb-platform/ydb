use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures_util::StreamExt;
use std::{
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};
use temporal_sdk::{WfContext, WorkflowFunction};
use temporal_sdk_core::{CoreRuntime, replay::HistoryForReplay};
use temporal_sdk_core_api::telemetry::metrics::{
    MetricKeyValue, MetricParametersBuilder, NewAttributes,
};
use temporal_sdk_core_protos::DEFAULT_WORKFLOW_TYPE;
use temporal_sdk_core_test_utils::{
    DONT_AUTO_INIT_INTEG_TELEM, canned_histories, prom_metrics, replay_sdk_worker,
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
    let hist = HistoryForReplay::new(
        t.get_full_history_info().unwrap().into(),
        "whatever".to_string(),
    );

    c.bench_function("Small history replay", |b| {
        b.to_async(&tokio_runtime).iter_batched(
            || {
                let func = timers_wf(num_timers);
                (func, replay_sdk_worker([hist.clone()]))
            },
            |(func, mut worker)| async move {
                worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
                worker.run().await.unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    let num_tasks = 50;
    let t = canned_histories::lots_of_big_signals(num_tasks);
    let hist = HistoryForReplay::new(
        t.get_full_history_info().unwrap().into(),
        "whatever".to_string(),
    );

    c.bench_function("Large payloads history replay", |b| {
        b.to_async(&tokio_runtime).iter_batched(
            || {
                let func = big_signals_wf(num_tasks);
                (func, replay_sdk_worker([hist.clone()]))
            },
            |(func, mut worker)| async move {
                worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
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
    let (mut telemopts, addr, _aborter) = prom_metrics(None);
    telemopts.logging = None;
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let meter = rt.telemetry().get_metric_meter().unwrap();

    c.bench_function("Record with new attributes on each call", move |b| {
        b.iter_batched(
            || {
                let c = meter.counter(
                    MetricParametersBuilder::default()
                        .name("c")
                        .build()
                        .unwrap(),
                );
                let h = meter.histogram(
                    MetricParametersBuilder::default()
                        .name("h")
                        .build()
                        .unwrap(),
                );
                let g = meter.gauge(
                    MetricParametersBuilder::default()
                        .name("g")
                        .build()
                        .unwrap(),
                );

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

fn timers_wf(num_timers: u32) -> WorkflowFunction {
    WorkflowFunction::new(move |ctx: WfContext| async move {
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    })
}

fn big_signals_wf(num_tasks: usize) -> WorkflowFunction {
    WorkflowFunction::new(move |ctx: WfContext| async move {
        let mut sigs = ctx.make_signal_channel("bigsig");
        for _ in 1..=num_tasks {
            for _ in 1..=5 {
                let _ = sigs.next().await.unwrap();
            }
        }

        Ok(().into())
    })
}
