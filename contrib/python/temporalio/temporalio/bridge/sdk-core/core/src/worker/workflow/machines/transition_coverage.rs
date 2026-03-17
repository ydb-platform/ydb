//! Look, I'm not happy about the code in here, and you shouldn't be either. This is all an awful,
//! dirty hack to work around the fact that there's no such thing as "run this after all unit tests"
//! in stable Rust. Don't do the things in here. They're bad. This is test only code, and should
//! never ever be removed from behind `#[cfg(test)]` compilation.

use dashmap::{DashMap, DashSet, mapref::entry::Entry};
use std::{
    path::PathBuf,
    sync::{
        LazyLock, Mutex,
        mpsc::{SyncSender, sync_channel},
    },
    thread::JoinHandle,
    time::Duration,
};

// During test we want to know about which transitions we've covered in state machines
static COVERED_TRANSITIONS: LazyLock<DashMap<String, DashSet<CoveredTransition>>> =
    LazyLock::new(DashMap::new);
static COVERAGE_SENDER: LazyLock<SyncSender<(String, CoveredTransition)>> =
    LazyLock::new(spawn_save_coverage_at_end);
static THREAD_HANDLE: LazyLock<Mutex<Option<JoinHandle<()>>>> = LazyLock::new(|| Mutex::new(None));

#[derive(Eq, PartialEq, Hash, Debug)]
struct CoveredTransition {
    from_state: String,
    to_state: String,
    event: String,
}

pub(super) fn add_coverage(
    machine_name: String,
    from_state: String,
    to_state: String,
    event: String,
) {
    let ct = CoveredTransition {
        from_state,
        to_state,
        event,
    };
    let _ = COVERAGE_SENDER.send((machine_name, ct));
}

fn spawn_save_coverage_at_end() -> SyncSender<(String, CoveredTransition)> {
    let (tx, rx) = sync_channel(1000);
    let handle = std::thread::spawn(move || {
        // Assume that, if the entire program hasn't run a state machine transition in the
        // last second that we are probably done running all the tests. This is to avoid
        // needing to instrument every single test.
        while let Ok((machine_name, ct)) = rx.recv_timeout(Duration::from_secs(1)) {
            match COVERED_TRANSITIONS.entry(machine_name) {
                Entry::Occupied(o) => {
                    o.get().insert(ct);
                }
                Entry::Vacant(v) => {
                    v.insert({
                        let ds = DashSet::new();
                        ds.insert(ct);
                        ds
                    });
                }
            }
        }
    });
    *THREAD_HANDLE.lock().unwrap() = Some(handle);
    tx
}

#[cfg(test)]
mod machine_coverage_report {
    use super::*;
    use crate::worker::workflow::machines::{
        activity_state_machine::ActivityMachine,
        cancel_external_state_machine::CancelExternalMachine,
        cancel_workflow_state_machine::CancelWorkflowMachine,
        child_workflow_state_machine::ChildWorkflowMachine,
        complete_workflow_state_machine::CompleteWorkflowMachine,
        continue_as_new_workflow_state_machine::ContinueAsNewWorkflowMachine,
        fail_workflow_state_machine::FailWorkflowMachine,
        local_activity_state_machine::LocalActivityMachine,
        modify_workflow_properties_state_machine::ModifyWorkflowPropertiesMachine,
        patch_state_machine::PatchMachine, signal_external_state_machine::SignalExternalMachine,
        timer_state_machine::TimerMachine, update_state_machine::UpdateMachine,
        upsert_search_attributes_state_machine::UpsertSearchAttributesMachine,
        workflow_task_state_machine::WorkflowTaskMachine,
    };
    use rustfsm::StateMachine;
    use std::{fs::File, io::Write};

    // This "test" needs to exist so that we have a way to join the spawned thread. Otherwise
    // it'll just get abandoned.
    #[test]
    // Use `cargo test -- --include-ignored` to run this. We don't want to bother with it by default
    // because it takes a minimum of a second.
    #[ignore]
    fn reporter() {
        // Make sure thread handle exists
        let _ = &*COVERAGE_SENDER;
        // Join it
        THREAD_HANDLE
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap();

        // Gather visualizations for all machines
        let mut activity = ActivityMachine::visualizer().to_owned();
        let mut timer = TimerMachine::visualizer().to_owned();
        let mut child_wf = ChildWorkflowMachine::visualizer().to_owned();
        let mut complete_wf = CompleteWorkflowMachine::visualizer().to_owned();
        let mut wf_task = WorkflowTaskMachine::visualizer().to_owned();
        let mut fail_wf = FailWorkflowMachine::visualizer().to_owned();
        let mut cont_as_new = ContinueAsNewWorkflowMachine::visualizer().to_owned();
        let mut cancel_wf = CancelWorkflowMachine::visualizer().to_owned();
        let mut version = PatchMachine::visualizer().to_owned();
        let mut signal_ext = SignalExternalMachine::visualizer().to_owned();
        let mut cancel_ext = CancelExternalMachine::visualizer().to_owned();
        let mut la_mach = LocalActivityMachine::visualizer().to_owned();
        let mut upsert_search_attr = UpsertSearchAttributesMachine::visualizer().to_owned();
        let mut modify_wf_props = ModifyWorkflowPropertiesMachine::visualizer().to_owned();
        let mut update = UpdateMachine::visualizer().to_owned();

        // This isn't at all efficient but doesn't need to be.
        // Replace transitions in the vizzes with green color if they are covered.
        for item in COVERED_TRANSITIONS.iter() {
            let (machine, coverage) = item.pair();
            match machine.as_ref() {
                m @ "ActivityMachine" => cover_transitions(m, &mut activity, coverage),
                m @ "TimerMachine" => cover_transitions(m, &mut timer, coverage),
                m @ "ChildWorkflowMachine" => cover_transitions(m, &mut child_wf, coverage),
                m @ "CompleteWorkflowMachine" => cover_transitions(m, &mut complete_wf, coverage),
                m @ "WorkflowTaskMachine" => cover_transitions(m, &mut wf_task, coverage),
                m @ "FailWorkflowMachine" => cover_transitions(m, &mut fail_wf, coverage),
                m @ "ContinueAsNewWorkflowMachine" => {
                    cover_transitions(m, &mut cont_as_new, coverage);
                }
                m @ "CancelWorkflowMachine" => cover_transitions(m, &mut cancel_wf, coverage),
                m @ "PatchMachine" => cover_transitions(m, &mut version, coverage),
                m @ "SignalExternalMachine" => cover_transitions(m, &mut signal_ext, coverage),
                m @ "CancelExternalMachine" => cover_transitions(m, &mut cancel_ext, coverage),
                m @ "LocalActivityMachine" => cover_transitions(m, &mut la_mach, coverage),
                m @ "UpsertSearchAttributesMachine" => {
                    cover_transitions(m, &mut upsert_search_attr, coverage)
                }
                m @ "ModifyWorkflowPropertiesMachine" => {
                    cover_transitions(m, &mut modify_wf_props, coverage)
                }
                m @ "UpdateMachine" => cover_transitions(m, &mut update, coverage),
                m => panic!("Unknown machine {m}"),
            }
        }
    }

    fn cover_transitions(machine: &str, viz: &mut String, cov: &DashSet<CoveredTransition>) {
        for trans in cov.iter() {
            let find_line = format!(
                "{} --> {}: {}",
                trans.from_state, trans.to_state, trans.event
            );
            if let Some(start) = viz.find(&find_line) {
                let new_line = format!(
                    "{} -[#blue]-> {}: {}",
                    trans.from_state, trans.to_state, trans.event
                );
                viz.replace_range(start..start + find_line.len(), &new_line);
            }
        }

        // Dump the updated viz to a file
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("machine_coverage");
        std::fs::create_dir_all(&d).unwrap();
        d.push(format!("{machine}_Coverage.puml"));
        let mut file = File::create(d).unwrap();
        file.write_all(viz.as_bytes()).unwrap();
    }
}
