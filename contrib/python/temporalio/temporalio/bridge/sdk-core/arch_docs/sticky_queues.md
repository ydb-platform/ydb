# Sticky (and Non-Sticky) Task Queues

A temporal worker, whether polling for workflows or activities, does so by polling a specific
"Task Queue". [Here](https://docs.temporal.io/docs/concepts/task-queues) is some basic documentation
on them. Task queues are designed to be lightweight, it's OK to have very large numbers of them.

Any time there is a new activity task or workflow task that needs to be performed by a worker, it'll
be queued in a specific task queue.

## Non-Sticky Queues
Before explaining sticky queues, let's examine how things work when they are *not* enabled.

In our example, let's say there are two workers polling the same task queue (`shared_q`):

```text
   ┌───────┬───►shared_q
 w1│       │    ┌───────┐
┌──┤       │    │  t1   │
│  │       │    ├───────┤
└──┘       │    │  t2   │
           │    ├───────┤
 w2        │    │  t3   │
┌──┬───────┘    ├───────┤
│  │            │  ..   │
└──┘            └───────┘
```

Both workers poll the same queue, and either may take the next task from it. When they do, they will
be delivered the *entire* history of that workflow. They will recreate workflow state by replaying
history from the start, and then issue the appropriate workflow task completion. The cycle continues
until the workflow executions are finished.

## Sticky Queues
Sticky queues exist to avoid needing to ship the entire workflow history to workers for every task,
and avoid the need to replay history from the start.

To accomplish this, workers maintain workflow state in a cache. When they reply with a workflow
task completion, they specify a task queue name specific to them. This queue name is unique to
the specific instance of the worker and will be different across restarts even of the same binary on
the same machine. The worker will poll on both the shared queue, and its specific task queue.

Also unlike normal task queues, sticky task queues have a schedule-to-start timeout associated with 
them. More on why this is needed later.

The interactions, at a high level, look like this:
![](https://www.planttext.com/api/plantuml/svg/jLEnRjim4Dtv5GTDSH0FNeEYI8SCtT8aG4Q3eKyI8OedyKwM_FSzKNOJkqK13rbvxxrxx-dqm6AJ36qmHhm4XE95l6iEy6gvWLy33WW_es2oJZn5BepfbE2TxsmKADueDPXWKu1b63VdmnTCUqnvLABfirZ1rE9M-lnQzHUlsp7hRJVRQPeoX7jZnWsilwi4tCCJXG0KeVYZKnWTV5khbewhPDyXuYGWwd-Uh9Mf_7kOdPQ1nYNP3KRn2Q7sB9GEgzEI37rAv91vqVYqF3CTjLr0mJiO63C4ZXd-7K8RcsqS9Nv4mBtkleFW6mGBubljh_J9n-e8v1vkRnNx61VXRCC4ekxaJB4mdlBCOvuxiS0TEbzwjpZwRs-N9fSI-ReIAOQ38iSb4w--iCJhExme4EFkx2C_xhsJZnRBH2quwseqaGGX-QgM4qml7shRTHYrw194h-OJanBCfY6XPOfdv_gCZ7ByesvyT67OeL9hx-eF0PpG3VEErTMdKlqLCviFMCxUtn0gWdVh6X1IrmXSsuIxutaOyw2bwB__6m00)]

After reviewing the diagram you may wonder what happens if the worker dies without being able
to send the `ResetSticky` request. This is where the timeout for sticky queues matters. If the
worker does not poll from the queue during the specified duration (default `5s`), then the task
in the queue will time out and will be rescheduled onto the non-sticky queue.
