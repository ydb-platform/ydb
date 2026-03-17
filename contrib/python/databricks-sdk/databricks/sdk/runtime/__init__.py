from __future__ import annotations

import logging
from typing import Dict, Optional, Union, cast

logger = logging.getLogger("databricks.sdk")
is_local_implementation = True

# All objects that are injected into the Notebook's user namespace should also be made
# available to be imported from databricks.sdk.runtime.globals. This import can be used
# in Python modules so users can access these objects from Files more easily.
dbruntime_objects = [
    "display",
    "displayHTML",
    "dbutils",
    "table",
    "sql",
    "udf",
    "getArgument",
    "sc",
    "sqlContext",
    "spark",
]

# DO NOT MOVE THE TRY-CATCH BLOCK BELOW AND DO NOT ADD THINGS BEFORE IT! WILL MAKE TEST FAIL.
try:
    # We don't want to expose additional entity to user namespace, so
    # a workaround here for exposing required information in notebook environment
    from dbruntime.sdk_credential_provider import init_runtime_native_auth

    logger.debug("runtime SDK credential provider available")
    dbruntime_objects.append("init_runtime_native_auth")
except ImportError:
    init_runtime_native_auth = None

globals()["init_runtime_native_auth"] = init_runtime_native_auth


def init_runtime_repl_auth():
    try:
        from dbruntime.databricks_repl_context import get_context

        ctx = get_context()
        if ctx is None:
            logger.debug("Empty REPL context returned, skipping runtime auth")
            return None, None
        if ctx.workspaceUrl is None:
            logger.debug("Workspace URL is not available, skipping runtime auth")
            return None, None
        host = f"https://{ctx.workspaceUrl}"

        def inner() -> Dict[str, str]:
            ctx = get_context()
            return {"Authorization": f"Bearer {ctx.apiToken}"}

        return host, inner
    except ImportError:
        return None, None


def init_runtime_legacy_auth():
    try:
        import IPython

        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            return None, None
        global_ns = ip_shell.ns_table["user_global"]
        if "dbutils" not in global_ns:
            return None, None
        dbutils = global_ns["dbutils"].notebook.entry_point.getDbutils()
        if dbutils is None:
            return None, None
        ctx = dbutils.notebook().getContext()
        if ctx is None:
            return None, None
        host = getattr(ctx, "apiUrl")().get()

        def inner() -> Dict[str, str]:
            ctx = dbutils.notebook().getContext()
            return {"Authorization": f'Bearer {getattr(ctx, "apiToken")().get()}'}

        return host, inner
    except ImportError:
        return None, None


try:
    # Internal implementation
    # Separated from above for backward compatibility
    from dbruntime import UserNamespaceInitializer

    userNamespaceGlobals = UserNamespaceInitializer.getOrCreate().get_namespace_globals()
    _globals = globals()
    for var in dbruntime_objects:
        if var not in userNamespaceGlobals:
            continue
        _globals[var] = userNamespaceGlobals[var]
    is_local_implementation = False
except ImportError:
    # OSS implementation
    is_local_implementation = True

    for var in dbruntime_objects:
        globals()[var] = None

    # The next few try-except blocks are for initialising globals in a best effort
    # mannaer. We separate them to try to get as many of them working as possible
    try:
        # We expect this to fail and only do this for providing types
        from pyspark.sql.context import SQLContext

        sqlContext: SQLContext = None  # type: ignore
        table = sqlContext.table
    except Exception as e:
        logging.debug(f"Failed to initialize globals 'sqlContext' and 'table', continuing. Cause: {e}")

    try:
        from pyspark.sql.functions import udf  # type: ignore
    except ImportError as e:
        logging.debug(f"Failed to initialise udf global: {e}")

    try:
        from databricks.connect import DatabricksSession  # type: ignore

        spark = DatabricksSession.builder.getOrCreate()
        sql = spark.sql  # type: ignore
    except Exception as e:
        # We are ignoring all failures here because user might want to initialize
        # spark session themselves and we don't want to interfere with that
        logging.debug(f"Failed to initialize globals 'spark' and 'sql', continuing. Cause: {e}")

    try:
        # We expect this to fail locally since dbconnect does not support sparkcontext. This is just for typing
        sc = spark.sparkContext  # type: ignore
    except Exception as e:
        logging.debug(f"Failed to initialize global 'sc', continuing. Cause: {e}")

    def display(input=None, *args, **kwargs) -> None:  # type: ignore
        """
        Display plots or data.
        Display plot:
                        - display() # no-op
                        - display(matplotlib.figure.Figure)
        Display dataset:
                        - display(spark.DataFrame)
                        - display(list) # if list can be converted to DataFrame, e.g., list of named tuples
                        - display(pandas.DataFrame)
                        - display(koalas.DataFrame)
                        - display(pyspark.pandas.DataFrame)
        Display any other value that has a _repr_html_() method
        For Spark 2.0 and 2.1:
                        - display(DataFrame, streamName='optional', trigger=optional pyspark.sql.streaming.Trigger,
                                                        checkpointLocation='optional')
        For Spark 2.2+:
                        - display(DataFrame, streamName='optional', trigger=optional interval like '1 second',
                                                        checkpointLocation='optional')
        """
        # Import inside the function so that imports are only triggered on usage.
        from IPython import display as IPDisplay

        return IPDisplay.display(input, *args, **kwargs)  # type: ignore

    def displayHTML(html) -> None:  # type: ignore
        """
        Display HTML data.
        Parameters
        ----------
        data : URL or HTML string
                        If data is a URL, display the resource at that URL, the resource is loaded dynamically by the browser.
                        Otherwise data should be the HTML to be displayed.
        See also:
        IPython.display.HTML
        IPython.display.display_html
        """
        # Import inside the function so that imports are only triggered on usage.
        from IPython import display as IPDisplay

        return IPDisplay.display_html(html, raw=True)  # type: ignore

    # We want to propagate the error in initialising dbutils because this is a core
    # functionality of the sdk
    from databricks.sdk.dbutils import RemoteDbUtils

    from . import dbutils_stub

    dbutils_type = Union[dbutils_stub.dbutils, RemoteDbUtils]

    dbutils = RemoteDbUtils()
    dbutils = cast(dbutils_type, dbutils)

    # We do this to prevent importing widgets implementation prematurely
    # The widget import should prompt users to use the implementation
    # which has ipywidget support.
    def getArgument(name: str, defaultValue: Optional[str] = None):
        return dbutils.widgets.getArgument(name, defaultValue)


__all__ = dbruntime_objects
