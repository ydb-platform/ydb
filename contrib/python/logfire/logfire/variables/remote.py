from __future__ import annotations as _annotations

import json
import os
import re
import threading
import warnings
import weakref
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from opentelemetry.util._once import Once
from pydantic import ValidationError
from requests import RequestException, Session

from logfire._internal.client import UA_HEADER
from logfire._internal.config import VariablesOptions
from logfire._internal.utils import UnexpectedResponse
from logfire.variables.abstract import (
    ResolvedVariable,
    VariableAlreadyExistsError,
    VariableNotFoundError,
    VariableProvider,
    VariableWriteError,
)
from logfire.variables.config import (
    KeyIsNotPresent,
    KeyIsPresent,
    ValueDoesNotEqual,
    ValueDoesNotMatchRegex,
    ValueEquals,
    ValueIsIn,
    ValueIsNotIn,
    ValueMatchesRegex,
    VariableConfig,
    VariablesConfig,
)

if TYPE_CHECKING:
    import logfire
    from logfire.variables.config import VariableTypeConfig


__all__ = ('LogfireRemoteVariableProvider',)


class LogfireRemoteVariableProvider(VariableProvider):
    """Variable provider that fetches configuration from a remote Logfire API.

    The threading implementation draws heavily from opentelemetry.sdk._shared_internal.BatchProcessor.
    """

    def __init__(self, base_url: str, token: str, options: VariablesOptions):
        """Create a new remote variable provider.

        Args:
            base_url: The base URL of the Logfire API.
            token: Authentication token for the Logfire API.
            options: Options for retrieving remote variables.
        """
        block_before_first_resolve = options.block_before_first_resolve
        polling_interval = options.polling_interval

        self._base_url = base_url
        self._token = token
        self._session = Session()
        self._session.headers.update({'Authorization': f'bearer {token}', 'User-Agent': UA_HEADER})
        self._timeout = options.timeout
        self._block_before_first_fetch = block_before_first_resolve
        self._polling_interval: timedelta = (
            timedelta(seconds=polling_interval) if isinstance(polling_interval, (float, int)) else polling_interval
        )

        self._session_lock = threading.Lock()
        self._reset_once = Once()
        self._has_attempted_fetch: bool = False
        self._last_fetched_at: datetime | None = None

        self._config: VariablesConfig | None = None

        self._shutdown = False
        self._shutdown_timeout_exceeded = False
        self._refresh_lock = threading.Lock()
        self._worker_awaken = threading.Event()
        self._force_refresh_event = threading.Event()  # Set by SSE listener to force immediate refresh

        # SSE listener for real-time updates
        self._sse_connected = False
        self._sse_thread: threading.Thread | None = None

        # Logfire instance for error logging, set via start()
        # If None, errors are reported via warnings instead.
        self._logfire: logfire.Logfire | None = None
        self._started = False

        # Worker thread is created but not started until start() is called
        self._worker_thread: threading.Thread | None = None
        self._pid = os.getpid()

    def _at_fork_reinit(self):  # pragma: no cover
        # Recreate all things threading related
        self._refresh_lock = threading.Lock()
        self._session_lock = threading.Lock()
        self._worker_awaken = threading.Event()
        self._force_refresh_event = threading.Event()
        # Only restart threads if we were started before the fork
        if self._started:
            self._worker_thread = threading.Thread(
                name='LogfireRemoteProvider',
                target=self._worker,
                daemon=True,
            )
            self._worker_thread.start()
            # Restart SSE listener
            self._sse_connected = False
            self._start_sse_listener()
        self._pid = os.getpid()

    def start(self, logfire_instance: logfire.Logfire | None) -> None:
        """Start background polling with the given logfire instance for error logging.

        Args:
            logfire_instance: The Logfire instance to use for error logging, or None if
                variable instrumentation is disabled (errors will be reported via warnings).
        """
        if logfire_instance is not None:
            self._logfire = logfire_instance.with_settings(custom_scope_suffix='variables.provider')
        if self._started:
            return
        self._started = True

        # Start the worker thread
        self._worker_thread = threading.Thread(
            name='LogfireRemoteProvider',
            target=self._worker,
            daemon=True,
        )
        self._worker_thread.start()

        # Start the SSE listener
        self._start_sse_listener()

        # Register at_fork handler
        if hasattr(os, 'register_at_fork'):  # pragma: no branch
            weak_reinit = weakref.WeakMethod(self._at_fork_reinit)

            def _after_in_child() -> None:  # pragma: no cover
                method = weak_reinit()
                if method is not None:
                    method()

            os.register_at_fork(after_in_child=_after_in_child)

    def _log_error(self, message: str, exc: Exception) -> None:
        """Log an error using logfire if available, otherwise warnings.

        Args:
            message: The error message.
            exc: The exception that occurred.
        """
        if self._logfire is not None:
            self._logfire.error('{message}: {error}', message=message, error=str(exc), _exc_info=exc)
        else:
            warnings.warn(f'{message}: {exc}', category=RuntimeWarning)

    def _start_sse_listener(self):  # pragma: no cover
        """Start the SSE listener thread for real-time updates."""
        if self._sse_thread is not None and self._sse_thread.is_alive():
            return  # Already running

        self._sse_thread = threading.Thread(
            name='LogfireRemoteProviderSSE',
            target=self._sse_listener,
            daemon=True,
        )
        self._sse_thread.start()

    def _sse_listener(self):  # pragma: no cover
        """Listen for SSE updates from the server and trigger refresh on events."""
        sse_url = urljoin(self._base_url, '/v1/variable-updates/')
        reconnect_delay = 1.0  # Start with 1 second delay
        max_reconnect_delay = 60.0  # Max 60 seconds between reconnects

        while not self._shutdown:
            try:
                # Use a separate session for SSE to avoid conflicts with polling
                with Session() as sse_session:
                    sse_session.headers.update(
                        {
                            'Authorization': f'bearer {self._token}',
                            'User-Agent': UA_HEADER,
                            'Accept': 'text/event-stream',
                            'Cache-Control': 'no-cache',
                        }
                    )

                    # Open streaming connection
                    response = sse_session.get(sse_url, stream=True, timeout=(10, None))
                    if response.status_code != 200:
                        # Server doesn't support SSE or auth failed, back off
                        self._sse_connected = False
                        self._wait_for_reconnect(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                        continue

                    # Connected successfully, reset delay
                    self._sse_connected = True
                    reconnect_delay = 1.0

                    # Process SSE events
                    for line in response.iter_lines(decode_unicode=True):
                        if self._shutdown:
                            break

                        if line is None:
                            continue

                        line = line.strip()
                        if not line:
                            continue

                        # SSE format: "data: {...json...}"
                        if line.startswith('data:'):
                            data_str = line[5:].strip()
                            try:
                                event_data = json.loads(data_str)
                                event_type = event_data.get('event')
                                # On any variable event, trigger a forced refresh
                                if event_type in ('created', 'updated', 'deleted'):
                                    # Set flag to force refresh and wake up the worker
                                    self._force_refresh_event.set()
                                    self._worker_awaken.set()
                            except (json.JSONDecodeError, TypeError):
                                # Invalid JSON, ignore
                                pass

            except Exception:
                # Connection error, will retry
                self._sse_connected = False
                if not self._shutdown:
                    self._wait_for_reconnect(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

    def _wait_for_reconnect(self, delay: float):  # pragma: no cover
        """Wait for a delay before reconnecting, checking for shutdown."""
        # Wait in small increments to allow quick shutdown
        elapsed = 0.0
        while elapsed < delay and not self._shutdown:
            wait_time = min(0.5, delay - elapsed)
            threading.Event().wait(wait_time)
            elapsed += wait_time

    def _worker(self):
        while not self._shutdown:  # pragma: no branch
            # Note: Ideally we'd be able to terminate while the following request was going even if it takes a while,
            # it's far more reasonable to terminate this worker thread "gracelessly" than an OTel exporter's.
            # But given this is pretty unlikely to cause issues, Alex and I decided are okay leaving this as-is.
            # We can change this if we run into issues, but it doesn't seem to be causing any now.

            # Check if SSE event requested a forced refresh
            force = self._force_refresh_event.is_set()
            if force:
                self._force_refresh_event.clear()

            self.refresh(force=force)

            # Use wait(timeout) then clear() to avoid lost wakeups:
            # If SSE sets the event during refresh(), wait() returns immediately
            # and we loop again without sleeping for the full polling interval.
            awakened = self._worker_awaken.wait(self._polling_interval.total_seconds())
            if awakened:  # pragma: no branch
                self._worker_awaken.clear()
            if self._shutdown:  # pragma: no branch
                break

    def refresh(self, force: bool = False):
        """Fetch the latest variable configuration from the remote API.

        Args:
            force: If True, fetch configuration even if the polling interval hasn't elapsed.
        """
        if self._refresh_lock.locked():  # pragma: no cover
            # If we're already fetching, we'll get a new value, so no need to force
            force = False

        # Note: Eventually we may want to rework the client and server implementations to use a NotModifiedResponse
        #  to reduce the amount of overhead from polling. We could also use a websocket/SSE to get real time updates
        #  when the user makes changes.
        with self._refresh_lock:  # Make at most one request at a time
            if (
                not force
                and self._last_fetched_at is not None
                and self._last_fetched_at > datetime.now(tz=timezone.utc) - self._polling_interval
            ):
                return  # nothing to do

            try:
                with self._session_lock:
                    variables_response = self._session.get(
                        urljoin(self._base_url, '/v1/variables/'), timeout=self._timeout
                    )
                    UnexpectedResponse.raise_for_status(variables_response)
                    variables_config_data = variables_response.json()
            except Exception as e:
                # Catch all request/HTTP/JSON exceptions (ConnectionError, Timeout, UnexpectedResponse,
                # JSONDecodeError, etc.) to prevent crashing the user's application on failures.
                self._log_error('Error retrieving variables', e)
                return

            try:
                new_config = VariablesConfig.model_validate(variables_config_data)
                self._config = new_config
                self._last_fetched_at = datetime.now(tz=timezone.utc)
            except ValidationError as e:
                self._log_error('Failed to parse variables configuration from Logfire API', e)
            finally:
                self._has_attempted_fetch = True

    def get_serialized_value(
        self,
        variable_name: str,
        targeting_key: str | None = None,
        attributes: Mapping[str, Any] | None = None,
    ) -> ResolvedVariable[str | None]:
        """Resolve a variable's serialized value from the remote configuration.

        Args:
            variable_name: The name of the variable to resolve.
            targeting_key: Optional key for deterministic label selection (e.g., user ID).
            attributes: Optional attributes for condition-based targeting rules.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
        if self._pid != os.getpid():  # pragma: no cover
            self._reset_once.do_once(self._at_fork_reinit)

        if not self._has_attempted_fetch and self._block_before_first_fetch:
            # Block while waiting for the request to be sent
            self.refresh()

        if self._config is None:
            return ResolvedVariable(name=variable_name, value=None, _reason='missing_config')

        return self._config.resolve_serialized_value(variable_name, targeting_key, attributes)

    def get_serialized_value_for_label(
        self,
        variable_name: str,
        label: str,
    ) -> ResolvedVariable[str | None]:
        """Resolve a variable's serialized value for a specific label from the remote configuration.

        Args:
            variable_name: The name of the variable to resolve.
            label: The name of the label to select.

        Returns:
            A ResolvedVariable containing the serialized value (or None if not found).
        """
        if self._pid != os.getpid():  # pragma: no cover
            self._reset_once.do_once(self._at_fork_reinit)

        if not self._has_attempted_fetch and self._block_before_first_fetch:
            self.refresh()

        return super().get_serialized_value_for_label(variable_name, label)

    def shutdown(self, timeout_millis: float = 5000):
        """Stop the background polling thread and clean up resources.

        Args:
            timeout_millis: The timeout budget in milliseconds for shutdown operations.
        """
        if self._shutdown:
            return
        self._shutdown = True
        self._worker_awaken.set()

        # Join the threads so that resources get cleaned up in tests
        # It might be reasonable to modify this so this _only_ happens in tests, but for now it seems fine.
        # Split the budget: 70% for worker (does HTTP), 30% for SSE.
        worker_timeout = max(timeout_millis * 0.7, 0) / 1000
        sse_timeout = max(timeout_millis * 0.3, 0) / 1000
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=worker_timeout)
        if self._sse_thread is not None:
            self._sse_thread.join(timeout=sse_timeout)

        with self._session_lock:
            self._session.close()

    def get_variable_config(self, name: str) -> VariableConfig | None:
        """Retrieve the full configuration for a variable from the cached config.

        This method supports alias-based lookup, so you can pass either the
        variable's current name or any of its configured aliases.

        Args:
            name: The name (or alias) of the variable.

        Returns:
            The VariableConfig if found, or None if the variable doesn't exist.
        """
        if self._config is None:
            return None
        return self._config._get_variable_config(name)  # pyright: ignore[reportPrivateUsage]

    def get_all_variables_config(self) -> VariablesConfig:
        """Retrieve all variable configurations from the cached config.

        Returns:
            A VariablesConfig containing all variable configurations.
            Returns an empty VariablesConfig if no config has been fetched yet.
        """
        if self._config is None:
            return VariablesConfig(variables={})
        return self._config

    def create_variable(self, config: VariableConfig) -> VariableConfig:
        """Create a new variable configuration via the remote API.

        Args:
            config: The configuration for the new variable.

        Returns:
            The created VariableConfig.

        Raises:
            VariableAlreadyExistsError: If a variable with this name already exists.
            VariableWriteError: If the API request fails.
        """
        body = self._config_to_api_body(config)
        try:
            with self._session_lock:
                response = self._session.post(
                    urljoin(self._base_url, '/v1/variables/'), json=body, timeout=self._timeout
                )
                if response.status_code == 409:
                    raise VariableAlreadyExistsError(f"Variable '{config.name}' already exists")
                UnexpectedResponse.raise_for_status(response)
        except (UnexpectedResponse, RequestException) as e:
            raise VariableWriteError(f'Failed to create variable: {e}') from e

        # Refresh cache after successful write
        self.refresh(force=True)
        return config

    def update_variable(self, name: str, config: VariableConfig) -> VariableConfig:
        """Update an existing variable configuration via the remote API.

        This is a metadata-only update (name, description, schema, example).
        Labels and versions are managed through the UI.

        Args:
            name: The name of the variable to update.
            config: The new configuration for the variable.

        Returns:
            The updated VariableConfig.

        Raises:
            VariableNotFoundError: If the variable does not exist.
            VariableWriteError: If the API request fails.
        """
        body = self._config_to_api_body(config)
        try:
            with self._session_lock:
                response = self._session.put(
                    urljoin(self._base_url, f'/v1/variables/{name}/'), json=body, timeout=self._timeout
                )
                if response.status_code == 404:
                    raise VariableNotFoundError(f"Variable '{name}' not found")
                UnexpectedResponse.raise_for_status(response)
        except (UnexpectedResponse, RequestException) as e:
            raise VariableWriteError(f'Failed to update variable: {e}') from e

        # Refresh cache after successful write
        self.refresh(force=True)
        return config

    def delete_variable(self, name: str) -> None:
        """Delete a variable configuration via the remote API.

        Args:
            name: The name of the variable to delete.

        Raises:
            VariableNotFoundError: If the variable does not exist.
            VariableWriteError: If the API request fails.
        """
        try:
            with self._session_lock:
                response = self._session.delete(
                    urljoin(self._base_url, f'/v1/variables/{name}/'), timeout=self._timeout
                )
                if response.status_code == 404:
                    raise VariableNotFoundError(f"Variable '{name}' not found")
                UnexpectedResponse.raise_for_status(response)
        except (UnexpectedResponse, RequestException) as e:
            raise VariableWriteError(f'Failed to delete variable: {e}') from e

        # Refresh cache after successful write
        self.refresh(force=True)

    def _config_to_api_body(self, config: VariableConfig) -> dict[str, Any]:
        """Convert a VariableConfig to the API request body format.

        This sends metadata only (name, description, schema, example, aliases).
        Labels and versions are managed through the UI.

        Args:
            config: The VariableConfig to convert.

        Returns:
            A dictionary suitable for the API request body.
        """
        body: dict[str, Any] = {'name': config.name}

        # description is always required by the API
        body['description'] = config.description

        if config.json_schema is not None:
            body['json_schema'] = config.json_schema

        body['rollout'] = {'labels': config.rollout.labels}

        body['overrides'] = [
            {
                'conditions': [
                    {'kind': cond.kind, 'attribute': cond.attribute, **self._condition_extra_fields(cond)}
                    for cond in override.conditions
                ],
                'rollout': {'labels': override.rollout.labels},
            }
            for override in config.overrides
        ]

        # Include aliases if present
        if config.aliases is not None:
            body['aliases'] = config.aliases

        # Include example value if present
        if config.example is not None:
            body['example'] = config.example

        return body

    def _condition_extra_fields(self, condition: Any) -> dict[str, Any]:
        """Extract extra fields from a condition based on its type.

        Args:
            condition: The condition object.

        Returns:
            A dictionary of extra fields for the condition.
        """
        if isinstance(condition, (ValueEquals, ValueDoesNotEqual)):
            return {'value': condition.value}
        elif isinstance(condition, (ValueIsIn, ValueIsNotIn)):
            return {'values': list(condition.values)}
        elif isinstance(condition, (ValueMatchesRegex, ValueDoesNotMatchRegex)):
            pattern = condition.pattern
            return {'pattern': pattern.pattern if isinstance(pattern, re.Pattern) else pattern}
        elif isinstance(condition, (KeyIsPresent, KeyIsNotPresent)):
            return {}
        return {}  # pragma: no cover

    # --- Variable Types API ---

    def list_variable_types(self) -> dict[str, VariableTypeConfig]:
        """List all variable types from the remote API.

        Returns:
            A dictionary mapping type names to their configurations.
        """
        from logfire.variables.config import VariableTypeConfig

        try:
            with self._session_lock:
                response = self._session.get(urljoin(self._base_url, '/v1/variable-types/'), timeout=self._timeout)
                UnexpectedResponse.raise_for_status(response)
                types_data = response.json()
        except UnexpectedResponse as e:
            raise VariableWriteError(f'Failed to list variable types: {e}') from e
        result: dict[str, VariableTypeConfig] = {}
        for type_data in types_data:
            config = VariableTypeConfig(
                name=type_data['name'],
                json_schema=type_data.get('json_schema', {}),
                description=type_data.get('description'),
                source_hint=type_data.get('source_hint'),
            )
            result[config.name] = config
        return result

    def upsert_variable_type(self, config: VariableTypeConfig) -> VariableTypeConfig:
        """Create or update a variable type via the remote API.

        If a type with the given name exists, it will be updated.
        Otherwise, a new type will be created.

        Args:
            config: The type configuration to upsert.

        Returns:
            The created or updated VariableTypeConfig.

        Raises:
            VariableWriteError: If the API request fails.
        """
        body: dict[str, Any] = {
            'name': config.name,
            'json_schema': config.json_schema,
            'description': config.description,
        }
        if config.source_hint is not None:
            body['source_hint'] = config.source_hint

        try:
            with self._session_lock:
                # POST endpoint is an upsert (create or update by name)
                response = self._session.post(
                    urljoin(self._base_url, '/v1/variable-types/'), json=body, timeout=self._timeout
                )
                UnexpectedResponse.raise_for_status(response)
        except UnexpectedResponse as e:
            raise VariableWriteError(f'Failed to upsert variable type: {e}') from e

        return config
