"""Runner for workflow sandbox.

.. warning::
    This API for this module is considered unstable and may change in future.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence, Type

import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.common
import temporalio.converter
import temporalio.worker._workflow_instance
import temporalio.workflow

# Workflow instance has to be relative import
from .._workflow_instance import (
    UnsandboxedWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
    WorkflowRunner,
)
from ._importer import Importer
from ._restrictions import RestrictionContext, SandboxRestrictions

_fake_info = temporalio.workflow.Info(
    attempt=-1,
    continued_run_id=None,
    cron_schedule=None,
    execution_timeout=None,
    first_execution_run_id="sandbox-validate-first-run_id",
    headers={},
    namespace="sandbox-validate-namespace",
    parent=None,
    root=None,
    raw_memo={},
    retry_policy=None,
    run_id="sandbox-validate-run_id",
    run_timeout=None,
    search_attributes={},
    start_time=datetime.fromtimestamp(0, timezone.utc),
    workflow_start_time=datetime.fromtimestamp(0, timezone.utc),
    task_queue="sandbox-validate-task_queue",
    task_timeout=timedelta(),
    typed_search_attributes=temporalio.common.TypedSearchAttributes.empty,
    workflow_id="sandbox-validate-workflow_id",
    workflow_type="sandbox-validate-workflow_type",
    priority=temporalio.common.Priority.default,
)


@dataclass(frozen=True)
class SandboxedWorkflowRunner(WorkflowRunner):
    """Runner for workflows in a sandbox."""

    restrictions: SandboxRestrictions = SandboxRestrictions.default
    """Set of restrictions to apply to this sandbox"""

    runner_class: Type[WorkflowRunner] = UnsandboxedWorkflowRunner
    """The class for underlying runner the sandbox will instantiate and  use to run workflows. Note, this class is
    re-imported and instantiated for *each* workflow run."""

    _worker_level_failure_exception_types: Sequence[type[BaseException]] = field(
        default_factory=list, init=False
    )

    def prepare_workflow(self, defn: temporalio.workflow._Definition) -> None:
        """Implements :py:meth:`WorkflowRunner.prepare_workflow`."""
        # Just create with fake info which validates
        self.create_instance(
            WorkflowInstanceDetails(
                payload_converter_class=temporalio.converter.DataConverter.default.payload_converter_class,
                failure_converter_class=temporalio.converter.DataConverter.default.failure_converter_class,
                interceptor_classes=[],
                defn=defn,
                # Just use fake info during validation
                info=_fake_info,
                randomness_seed=-1,
                extern_functions={},
                disable_eager_activity_execution=False,
                worker_level_failure_exception_types=self._worker_level_failure_exception_types,
            ),
        )

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Implements :py:meth:`WorkflowRunner.create_instance`."""
        return _Instance(det, self.runner_class, self.restrictions)

    def set_worker_level_failure_exception_types(
        self, types: Sequence[type[BaseException]]
    ) -> None:
        """Implements :py:meth:`WorkflowRunner.set_worker_level_failure_exception_types`."""
        object.__setattr__(self, "_worker_level_failure_exception_types", types)


# Implements in_sandbox._ExternEnvironment. Some of these calls are called from
# within the sandbox.
class _Instance(WorkflowInstance):
    def __init__(
        self,
        instance_details: WorkflowInstanceDetails,
        runner_class: Type[WorkflowRunner],
        restrictions: SandboxRestrictions,
    ) -> None:
        self.instance_details = instance_details
        self.runner_class = runner_class
        self.importer = Importer(restrictions, RestrictionContext())

        self._current_thread_id: Optional[int] = None

        # Create the instance
        self.globals_and_locals = {
            "__file__": "workflow_sandbox.py",
        }
        self._create_instance()

    def _create_instance(self) -> None:
        module_name = self.instance_details.defn.cls.__module__
        # If the module name is __main__ then we change to __temporal_main__ so
        # we don't trigger top-level execution that happens in __main__. We do
        # not support importing __main__.
        if module_name == "__main__":
            module_name = "__temporal_main__"
        try:
            # Import user code
            self._run_code(
                "with __temporal_importer.applied():\n"
                # Import the workflow code
                f"  from {module_name} import {self.instance_details.defn.cls.__name__} as __temporal_workflow_class\n"
                f"  from {self.runner_class.__module__} import {self.runner_class.__name__} as __temporal_runner_class\n",
                __temporal_importer=self.importer,
            )

            # Set context as in runtime
            self.importer.restriction_context.is_runtime = True

            # Create the sandbox instance
            self._run_code(
                "with __temporal_importer.applied():\n"
                "  from temporalio.worker.workflow_sandbox._in_sandbox import InSandbox\n"
                "  __temporal_in_sandbox = InSandbox(__temporal_instance_details, __temporal_runner_class, __temporal_workflow_class)\n",
                __temporal_importer=self.importer,
                __temporal_instance_details=self.instance_details,
            )
        finally:
            self.importer.restriction_context.is_runtime = False

    def activate(
        self, act: temporalio.bridge.proto.workflow_activation.WorkflowActivation
    ) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
        self.importer.restriction_context.is_runtime = True
        try:
            self._run_code(
                "with __temporal_importer.applied():\n"
                "  __temporal_completion = __temporal_in_sandbox.activate(__temporal_activation)\n",
                __temporal_importer=self.importer,
                __temporal_activation=act,
            )
            return self.globals_and_locals.pop("__temporal_completion")  # type: ignore
        finally:
            self.importer.restriction_context.is_runtime = False

    def _run_code(self, code: str, **extra_globals: Any) -> None:
        for k, v in extra_globals.items():
            self.globals_and_locals[k] = v
        try:
            temporalio.workflow.unsafe._set_in_sandbox(True)
            self._current_thread_id = threading.get_ident()
            exec(code, self.globals_and_locals, self.globals_and_locals)
        finally:
            temporalio.workflow.unsafe._set_in_sandbox(False)
            self._current_thread_id = None
            for k, v in extra_globals.items():
                self.globals_and_locals.pop(k, None)

    def get_thread_id(self) -> Optional[int]:
        return self._current_thread_id
