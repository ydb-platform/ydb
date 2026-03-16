from typing import Any

def instrument_celery(**kwargs: Any) -> None:
    """Instrument the `celery` module so that spans are automatically created for each task.

    See the `Logfire.instrument_celery` method for details.
    """
