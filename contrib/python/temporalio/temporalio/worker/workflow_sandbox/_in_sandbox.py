"""Code that runs inside the workflow sandbox.

.. warning::
    This API for this module is considered unstable and may change in future.
"""

import dataclasses
import logging
from typing import Any, Type

import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.worker._workflow_instance
import temporalio.workflow

logger = logging.getLogger(__name__)

# Set to true to log lots of sandbox details
LOG_TRACE = False


def _trace(message: object, *args: object) -> None:
    if LOG_TRACE:
        logger.debug(message, *args)


class InSandbox:
    """Instance that is expected to run inside a sandbox."""

    def __init__(
        self,
        instance_details: temporalio.worker._workflow_instance.WorkflowInstanceDetails,
        runner_class: Type[temporalio.worker._workflow_instance.WorkflowRunner],
        workflow_class: Type,
    ) -> None:
        """Create in-sandbox instance."""
        _trace("Initializing workflow %s in sandbox", workflow_class)
        # We expect to be able to get the workflow definition back off the
        # class. We can't use the definition that was given to us because it has
        # type hints and references to outside-of-sandbox types.
        new_defn = temporalio.workflow._Definition.must_from_class(workflow_class)

        # Also, we have to re-import the worker-level exception types, because
        # some exceptions are not passthrough and therefore our issubclass fails
        # because it'll be comparing out-of-sandbox types with in-sandbox types
        exception_types = instance_details.worker_level_failure_exception_types
        if exception_types:
            # Copy first, then add in-sandbox types appended
            exception_types = list(exception_types)
            # Try to re-import each
            for typ in instance_details.worker_level_failure_exception_types:
                try:
                    class_hier = typ.__qualname__.split(".")
                    module = __import__(typ.__module__, fromlist=[class_hier[0]])
                    reimported_type: Any = module
                    for name in class_hier:
                        reimported_type = getattr(reimported_type, name)
                    if not issubclass(reimported_type, BaseException):
                        raise TypeError(
                            f"Final imported type of {reimported_type} does not extend BaseException"
                        )
                    exception_types.append(reimported_type)
                except Exception as err:
                    raise TypeError(
                        f"Failed to re-import workflow exception failure type {typ} in sandbox"
                    ) from err

        new_instance_details = dataclasses.replace(
            instance_details,
            defn=new_defn,
            worker_level_failure_exception_types=exception_types,
        )

        # Instantiate the runner and the instance
        self.instance = runner_class().create_instance(new_instance_details)

    def activate(
        self, act: temporalio.bridge.proto.workflow_activation.WorkflowActivation
    ) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
        """Send activation to this instance."""
        return self.instance.activate(act)
