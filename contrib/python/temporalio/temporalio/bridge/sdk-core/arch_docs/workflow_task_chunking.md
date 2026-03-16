# Workflow Task Chunking

One source of complexity in Core is the chunking of history into "logical" Workflow Tasks.

Workflow tasks (WFTs) always take the following form in event history:

* \[Preceding Events\] (optional)
* WFT Scheduled
* WFT Started
* WFT Completed
* \[Commands\] (optional)

In the typical case, the "logical" WFT consists of all the commands from the last workflow task,
any events generated in the interrim, and the scheduled/started preamble. So:

* WFT Completed
* \[Commands\] (optional)
* \[Events\] (optional)
* WFT Scheduled
* WFT Started

Commands and events are both "optional" in the sense that:

Workflow code, after being woken up, might not do anything, and thus generate no new commands

There may be no events for more nuanced reasons:

1. The workflow might have been running a long-running local activity. In such cases, the workflow
   must "workflow task heartbeat" in order to avoid timing out the workflow task. This means
   completing the WFT with no commands while the LA is ongoing.
2. The workflow might have received an update, which does not come as an event in history, but
   rather as a "protocol message" attached to the task.
3. Server can forcibly generate a new WFT with some obscure APIs

Core does not consider such empty WFT sequences as worthy of waking lang (on replay - as a new
task, they always will), since nothing meaningful has happened. Thus, they are grouped together
as part of a "logical" WFT with the last WFT that had any real work in it.

## Possible issues as of this writing (5/25)

The "new WFT force-issued by server" case would, currently, not cause a wakeup on replay for the
reasons discussed above. In some obscure edge cases (inspecting workflow clock) this could cause
NDE.

### Possible solutions

* Core can attach a flag on WFT completes in order to be explicit that that WFT may be skipped on
  replay. IE: During WFT heartbeating for LAs.
* We could legislate that server should never send empty WFTs. Seemingly the only case of this
  is
  the [obscure api](https://github.com/temporalio/temporal/blob/d189737aa2ed1b07c221abb9fbdd28ecf68f0492/proto/internal/temporal/server/api/adminservice/v1/service.proto#L151)
