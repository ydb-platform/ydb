import logging
import typing
import warnings
from abc import ABC, abstractmethod


class WidgetUtils(ABC):

    def get(self, name: str):
        return self._get(name)

    @abstractmethod
    def _get(self, name: str) -> str:
        pass

    def getArgument(self, name: str, defaultValue: typing.Optional[str] = None):
        try:
            return self.get(name)
        except Exception:
            return defaultValue

    def remove(self, name: str):
        self._remove(name)

    @abstractmethod
    def _remove(self, name: str):
        pass

    def removeAll(self):
        self._remove_all()

    @abstractmethod
    def _remove_all(self):
        pass


try:
    # We only use ipywidgets if we are in a notebook interactive shell otherwise we raise error,
    # to fallback to using default_widgets. Also, users WILL have IPython in their notebooks (jupyter),
    # because we DO NOT SUPPORT any other notebook backends, and hence fallback to default_widgets.
    from IPython.core.getipython import get_ipython

    # Detect if we are in an interactive notebook by iterating over the mro of the current ipython instance,
    # to find ZMQInteractiveShell (jupyter). When used from REPL or file, this check will fail, since the
    # mro only contains TerminalInteractiveShell.
    if (
        len(
            list(
                filter(
                    lambda i: i.__name__ == "ZMQInteractiveShell",
                    get_ipython().__class__.__mro__,
                )
            )
        )
        == 0
    ):
        logging.debug("Not in an interactive notebook. Skipping ipywidgets implementation for dbutils.")
        raise EnvironmentError("Not in an interactive notebook.")

    # For import errors in IPyWidgetUtil, we provide a warning message, prompting users to install the
    # correct installation group of the sdk.
    try:
        from .ipywidgets_utils import IPyWidgetUtil

        widget_impl = IPyWidgetUtil
        logging.debug("Using ipywidgets implementation for dbutils.")

    except ImportError as e:
        # Since we are certain that we are in an interactive notebook, we can make assumptions about
        # formatting and make the warning nicer for the user.
        warnings.warn(
            "\nTo use databricks widgets interactively in your notebook, please install databricks sdk using:\n"
            "\tpip install 'databricks-sdk[notebook]'\n"
            "Falling back to default_value_only implementation for databricks widgets."
        )
        logging.debug(f"{e.msg}. Skipping ipywidgets implementation for dbutils.")
        raise e

except:
    from .default_widgets_utils import DefaultValueOnlyWidgetUtils

    widget_impl = DefaultValueOnlyWidgetUtils
    logging.debug("Using default_value_only implementation for dbutils.")
