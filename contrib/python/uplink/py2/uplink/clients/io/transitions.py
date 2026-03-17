__all__ = ["sleep", "send", "finish", "fail", "prepare"]


def sleep(seconds):
    """
    Transitions the execution to pause for the allotted duration.

    Args:
        seconds: The number of seconds to delay execution.
    """

    def action(state):
        return state.sleep(seconds)

    return action


def send(request):
    """
    Transitions the execution to send the given request.

    Args:
        request: The intended request data to be sent.
    """

    def action(state):
        return state.send(request)

    return action


def finish(response):
    """
    Transitions the execution to completion.

    Args:
        response: The object to return to the execution's invoker.
    """

    def action(state):
        return state.finish(response)

    return action


def fail(exc_type, exc_val, exc_tb):
    """
    Transitions the execution to fail with a specific error.

    This will prompt the execution of any RequestTemplate.after_exception hooks.

    Args:
        exc_type: The exception class.
        exc_val: The exception object.
        exc_tb: The exception's stacktrace.
    """

    def action(state):
        return state.fail(exc_type, exc_val, exc_tb)

    return action


def prepare(request):
    """
    Transitions the execution to prepare the given request.

    This will prompt the execution of any RequestTemplate.before_request.

    Args:
        request: The intended request data to be sent.
    """

    def action(state):
        return state.prepare(request)

    return action
