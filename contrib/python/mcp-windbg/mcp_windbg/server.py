import os
import traceback
import glob
import winreg
import logging
from typing import Dict, Optional
from contextlib import asynccontextmanager

from .cdb_session import CDBSession, CDBError
from .prompts import load_prompt

from mcp.shared.exceptions import McpError
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import (
    ErrorData,
    TextContent,
    Tool,
    Prompt,
    PromptArgument,
    PromptMessage,
    GetPromptResult,
    INVALID_PARAMS,
    INTERNAL_ERROR,
)
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

# Dictionary to store CDB sessions keyed by dump file path
active_sessions: Dict[str, CDBSession] = {}

def get_local_dumps_path() -> Optional[str]:
    """Get the local dumps path from the Windows registry."""
    try:
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps"
        ) as key:
            dump_folder, _ = winreg.QueryValueEx(key, "DumpFolder")
            if os.path.exists(dump_folder) and os.path.isdir(dump_folder):
                return dump_folder
    except (OSError, WindowsError):
        # Registry key might not exist or other issues
        pass

    # Default Windows dump location
    default_path = os.path.join(os.environ.get("LOCALAPPDATA", ""), "CrashDumps")
    if os.path.exists(default_path) and os.path.isdir(default_path):
        return default_path

    return None

class OpenWindbgDump(BaseModel):
    """Parameters for analyzing a crash dump."""
    dump_path: str = Field(description="Path to the Windows crash dump file")
    include_stack_trace: bool = Field(description="Whether to include stack traces in the analysis")
    include_modules: bool = Field(description="Whether to include loaded module information")
    include_threads: bool = Field(description="Whether to include thread information")


class OpenWindbgRemote(BaseModel):
    """Parameters for connecting to a remote debug session."""
    connection_string: str = Field(description="Remote connection string (e.g., 'tcp:Port=5005,Server=192.168.0.100')")
    include_stack_trace: bool = Field(default=False, description="Whether to include stack traces in the analysis")
    include_modules: bool = Field(default=False, description="Whether to include loaded module information")
    include_threads: bool = Field(default=False, description="Whether to include thread information")


class RunWindbgCmdParams(BaseModel):
    """Parameters for executing a WinDbg command."""
    dump_path: Optional[str] = Field(default=None, description="Path to the Windows crash dump file")
    connection_string: Optional[str] = Field(default=None, description="Remote connection string (e.g., 'tcp:Port=5005,Server=192.168.0.100')")
    command: str = Field(description="WinDbg command to execute")

    @model_validator(mode='after')
    def validate_connection_params(self):
        """Validate that exactly one of dump_path or connection_string is provided."""
        if not self.dump_path and not self.connection_string:
            raise ValueError("Either dump_path or connection_string must be provided")
        if self.dump_path and self.connection_string:
            raise ValueError("dump_path and connection_string are mutually exclusive")
        return self


class CloseWindbgDumpParams(BaseModel):
    """Parameters for unloading a crash dump."""
    dump_path: str = Field(description="Path to the Windows crash dump file to unload")


class CloseWindbgRemoteParams(BaseModel):
    """Parameters for closing a remote debugging connection."""
    connection_string: str = Field(description="Remote connection string to close")


class ListWindbgDumpsParams(BaseModel):
    """Parameters for listing crash dumps in a directory."""
    directory_path: Optional[str] = Field(
        default=None,
        description="Directory path to search for dump files. If not specified, will use the configured dump path from registry."
    )
    recursive: bool = Field(
        default=False,
        description="Whether to search recursively in subdirectories"
    )


def get_or_create_session(
    dump_path: Optional[str] = None,
    connection_string: Optional[str] = None,
    cdb_path: Optional[str] = None,
    symbols_path: Optional[str] = None,
    timeout: int = 30,
    verbose: bool = False
) -> CDBSession:
    """Get an existing CDB session or create a new one."""
    if not dump_path and not connection_string:
        raise ValueError("Either dump_path or connection_string must be provided")
    if dump_path and connection_string:
        raise ValueError("dump_path and connection_string are mutually exclusive")

    # Create session identifier
    if dump_path:
        session_id = os.path.abspath(dump_path)
    else:
        session_id = f"remote:{connection_string}"

    if session_id not in active_sessions or active_sessions[session_id] is None:
        try:
            session = CDBSession(
                dump_path=dump_path,
                remote_connection=connection_string,
                cdb_path=cdb_path,
                symbols_path=symbols_path,
                timeout=timeout,
                verbose=verbose
            )
            active_sessions[session_id] = session
            return session
        except Exception as e:
            raise McpError(ErrorData(
                code=INTERNAL_ERROR,
                message=f"Failed to create CDB session: {str(e)}"
            ))

    return active_sessions[session_id]


def unload_session(dump_path: Optional[str] = None, connection_string: Optional[str] = None) -> bool:
    """Unload and clean up a CDB session."""
    if not dump_path and not connection_string:
        return False
    if dump_path and connection_string:
        return False

    # Create session identifier
    if dump_path:
        session_id = os.path.abspath(dump_path)
    else:
        session_id = f"remote:{connection_string}"

    if session_id in active_sessions and active_sessions[session_id] is not None:
        try:
            active_sessions[session_id].shutdown()
        except Exception:
            pass
        finally:
            del active_sessions[session_id]
        return True

    return False


def execute_common_analysis_commands(session: CDBSession) -> dict:
    """
    Execute common analysis commands and return the results.

    Returns a dictionary with the results of various analysis commands.
    """
    results = {}

    try:
        results["info"] = session.send_command(".lastevent")
        results["exception"] = session.send_command("!analyze -v")
        results["modules"] = session.send_command("lm")
        results["threads"] = session.send_command("~")
    except CDBError as e:
        results["error"] = str(e)

    return results


async def serve(
    cdb_path: Optional[str] = None,
    symbols_path: Optional[str] = None,
    timeout: int = 30,
    verbose: bool = False,
) -> None:
    """Run the WinDbg MCP server with stdio transport.

    Args:
        cdb_path: Optional custom path to cdb.exe
        symbols_path: Optional custom symbols path
        timeout: Command timeout in seconds
        verbose: Whether to enable verbose output
    """
    server = _create_server(cdb_path, symbols_path, timeout, verbose)

    options = server.create_initialization_options()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, options, raise_exceptions=True)


async def serve_http(
    host: str = "127.0.0.1",
    port: int = 8000,
    cdb_path: Optional[str] = None,
    symbols_path: Optional[str] = None,
    timeout: int = 30,
    verbose: bool = False,
) -> None:
    """Run the WinDbg MCP server with Streamable HTTP transport.

    Args:
        host: Host to bind the HTTP server to
        port: Port to bind the HTTP server to
        cdb_path: Optional custom path to cdb.exe
        symbols_path: Optional custom symbols path
        timeout: Command timeout in seconds
        verbose: Whether to enable verbose output
    """
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.types import Receive, Scope, Send
    import uvicorn

    server = _create_server(cdb_path, symbols_path, timeout, verbose)

    # Create the session manager
    session_manager = StreamableHTTPSessionManager(
        app=server,
        json_response=True,
    )

    # ASGI handler for streamable HTTP connections
    async def handle_streamable_http(scope: Scope, receive: Receive, send: Send) -> None:
        await session_manager.handle_request(scope, receive, send)

    @asynccontextmanager
    async def lifespan(app: Starlette):
        async with session_manager.run():
            yield

    app = Starlette(
        debug=verbose,
        routes=[
            Mount("/mcp", app=handle_streamable_http),
        ],
        lifespan=lifespan,
    )

    logger.info(f"Starting MCP WinDbg server with streamable-http transport on {host}:{port}")
    print(f"MCP WinDbg server running on http://{host}:{port}")
    print(f"  MCP endpoint: http://{host}:{port}/mcp")

    config = uvicorn.Config(app, host=host, port=port, log_level="info" if verbose else "warning")
    server_instance = uvicorn.Server(config)
    await server_instance.serve()


def _create_server(
    cdb_path: Optional[str] = None,
    symbols_path: Optional[str] = None,
    timeout: int = 30,
    verbose: bool = False,
) -> Server:
    """Create and configure the MCP server with all tools and prompts.

    Args:
        cdb_path: Optional custom path to cdb.exe
        symbols_path: Optional custom symbols path
        timeout: Command timeout in seconds
        verbose: Whether to enable verbose output

    Returns:
        Configured Server instance
    """
    server = Server("mcp-windbg")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="open_windbg_dump",
                description="""
                Analyze a Windows crash dump file using WinDbg/CDB.
                This tool executes common WinDbg commands to analyze the crash dump and returns the results.
                """,
                inputSchema=OpenWindbgDump.model_json_schema(),
            ),
            Tool(
                name="open_windbg_remote",
                description="""
                Connect to a remote debugging session using WinDbg/CDB.
                This tool establishes a remote debugging connection and allows you to analyze the target process.
                """,
                inputSchema=OpenWindbgRemote.model_json_schema(),
            ),
            Tool(
                name="run_windbg_cmd",
                description="""
                Execute a specific WinDbg command on a loaded crash dump or remote session.
                This tool allows you to run any WinDbg command and get the output.
                """,
                inputSchema=RunWindbgCmdParams.model_json_schema(),
            ),
            Tool(
                name="close_windbg_dump",
                description="""
                Unload a crash dump and release resources.
                Use this tool when you're done analyzing a crash dump to free up resources.
                """,
                inputSchema=CloseWindbgDumpParams.model_json_schema(),
            ),
            Tool(
                name="close_windbg_remote",
                description="""
                Close a remote debugging connection and release resources.
                Use this tool when you're done with a remote debugging session to free up resources.
                """,
                inputSchema=CloseWindbgRemoteParams.model_json_schema(),
            ),
            Tool(
                name="list_windbg_dumps",
                description="""
                List Windows crash dump files in the specified directory.
                This tool helps you discover available crash dumps that can be analyzed.
                """,
                inputSchema=ListWindbgDumpsParams.model_json_schema(),
            )
        ]

    @server.call_tool()
    async def call_tool(name, arguments: dict) -> list[TextContent]:
        try:
            if name == "open_windbg_dump":
                # Check if dump_path is missing or empty
                if "dump_path" not in arguments or not arguments.get("dump_path"):
                    local_dumps_path = get_local_dumps_path()
                    dumps_found_text = ""

                    if local_dumps_path:
                        # Find dump files in the local dumps directory
                        search_pattern = os.path.join(local_dumps_path, "*.*dmp")
                        dump_files = glob.glob(search_pattern)

                        if dump_files:
                            dumps_found_text = f"\n\nI found {len(dump_files)} crash dump(s) in {local_dumps_path}:\n\n"
                            for i, dump_file in enumerate(dump_files[:10]):  # Limit to 10 dumps to avoid clutter
                                try:
                                    size_mb = round(os.path.getsize(dump_file) / (1024 * 1024), 2)
                                except (OSError, IOError):
                                    size_mb = "unknown"

                                dumps_found_text += f"{i+1}. {dump_file} ({size_mb} MB)\n"

                            if len(dump_files) > 10:
                                dumps_found_text += f"\n... and {len(dump_files) - 10} more dump files.\n"

                            dumps_found_text += "\nYou can analyze one of these dumps by specifying its path."

                    return [TextContent(
                        type="text",
                        text=f"Please provide a path to a crash dump file to analyze.{dumps_found_text}\n\n"
                              f"You can use the 'list_windbg_dumps' tool to discover available crash dumps."
                    )]

                args = OpenWindbgDump(**arguments)
                session = get_or_create_session(
                    dump_path=args.dump_path, cdb_path=cdb_path, symbols_path=symbols_path, timeout=timeout, verbose=verbose
                )

                results = []

                crash_info = session.send_command(".lastevent")
                results.append("### Crash Information\n```\n" + "\n".join(crash_info) + "\n```\n\n")

                # Run !analyze -v
                analysis = session.send_command("!analyze -v")
                results.append("### Crash Analysis\n```\n" + "\n".join(analysis) + "\n```\n\n")

                # Optional
                if args.include_stack_trace:
                    stack = session.send_command("kb")
                    results.append("### Stack Trace\n```\n" + "\n".join(stack) + "\n```\n\n")

                if args.include_modules:
                    modules = session.send_command("lm")
                    results.append("### Loaded Modules\n```\n" + "\n".join(modules) + "\n```\n\n")

                if args.include_threads:
                    threads = session.send_command("~")
                    results.append("### Threads\n```\n" + "\n".join(threads) + "\n```\n\n")

                return [TextContent(type="text", text="".join(results))]

            elif name == "open_windbg_remote":
                args = OpenWindbgRemote(**arguments)
                session = get_or_create_session(
                    connection_string=args.connection_string, cdb_path=cdb_path, symbols_path=symbols_path, timeout=timeout, verbose=verbose
                )

                results = []

                # Get target information for remote debugging
                target_info = session.send_command("!peb")
                results.append("### Target Process Information\n```\n" + "\n".join(target_info) + "\n```\n\n")

                # Get current state
                current_state = session.send_command("r")
                results.append("### Current Registers\n```\n" + "\n".join(current_state) + "\n```\n\n")

                # Optional
                if args.include_stack_trace:
                    stack = session.send_command("kb")
                    results.append("### Stack Trace\n```\n" + "\n".join(stack) + "\n```\n\n")

                if args.include_modules:
                    modules = session.send_command("lm")
                    results.append("### Loaded Modules\n```\n" + "\n".join(modules) + "\n```\n\n")

                if args.include_threads:
                    threads = session.send_command("~")
                    results.append("### Threads\n```\n" + "\n".join(threads) + "\n```\n\n")

                return [TextContent(
                    type="text",
                    text="".join(results)
                )]

            elif name == "run_windbg_cmd":
                args = RunWindbgCmdParams(**arguments)
                session = get_or_create_session(
                    dump_path=args.dump_path, connection_string=args.connection_string,
                    cdb_path=cdb_path, symbols_path=symbols_path, timeout=timeout, verbose=verbose
                )
                output = session.send_command(args.command)

                return [TextContent(
                    type="text",
                    text=f"Command: {args.command}\n\nOutput:\n```\n" + "\n".join(output) + "\n```"
                )]

            elif name == "close_windbg_dump":
                args = CloseWindbgDumpParams(**arguments)
                success = unload_session(dump_path=args.dump_path)
                if success:
                    return [TextContent(
                        type="text",
                        text=f"Successfully unloaded crash dump: {args.dump_path}"
                    )]
                else:
                    return [TextContent(
                        type="text",
                        text=f"No active session found for crash dump: {args.dump_path}"
                    )]

            elif name == "close_windbg_remote":
                args = CloseWindbgRemoteParams(**arguments)
                success = unload_session(connection_string=args.connection_string)
                if success:
                    return [TextContent(
                        type="text",
                        text=f"Successfully closed remote connection: {args.connection_string}"
                    )]
                else:
                    return [TextContent(
                        type="text",
                        text=f"No active session found for remote connection: {args.connection_string}"
                    )]

            elif name == "list_windbg_dumps":
                args = ListWindbgDumpsParams(**arguments)

                if args.directory_path is None:
                    args.directory_path = get_local_dumps_path()
                    if args.directory_path is None:
                        raise McpError(ErrorData(
                            code=INVALID_PARAMS,
                            message="No directory path specified and no default dump path found in registry."
                        ))

                if not os.path.exists(args.directory_path) or not os.path.isdir(args.directory_path):
                    raise McpError(ErrorData(
                        code=INVALID_PARAMS,
                        message=f"Directory not found: {args.directory_path}"
                    ))

                # Determine search pattern based on recursion flag
                search_pattern = os.path.join(args.directory_path, "**", "*.*dmp") if args.recursive else os.path.join(args.directory_path, "*.*dmp")

                # Find all dump files
                dump_files = glob.glob(search_pattern, recursive=args.recursive)

                # Sort alphabetically for consistent results
                dump_files.sort()

                if not dump_files:
                    return [TextContent(
                        type="text",
                        text=f"No crash dump files (*.*dmp) found in {args.directory_path}"
                    )]

                # Format the results
                result_text = f"Found {len(dump_files)} crash dump file(s) in {args.directory_path}:\n\n"
                for i, dump_file in enumerate(dump_files):
                    # Get file size in MB
                    try:
                        size_mb = round(os.path.getsize(dump_file) / (1024 * 1024), 2)
                    except (OSError, IOError):
                        size_mb = "unknown"

                    result_text += f"{i+1}. {dump_file} ({size_mb} MB)\n"

                return [TextContent(
                    type="text",
                    text=result_text
                )]

            raise McpError(ErrorData(
                code=INVALID_PARAMS,
                message=f"Unknown tool: {name}"
            ))

        except McpError:
            raise
        except Exception as e:
            traceback_str = traceback.format_exc()
            raise McpError(ErrorData(
                code=INTERNAL_ERROR,
                message=f"Error executing tool {name}: {str(e)}\n{traceback_str}"
            ))

    # Prompt constants
    DUMP_TRIAGE_PROMPT_NAME = "dump-triage"
    DUMP_TRIAGE_PROMPT_TITLE = "Crash Dump Triage Analysis"
    DUMP_TRIAGE_PROMPT_DESCRIPTION = "Comprehensive single crash dump analysis with detailed metadata extraction and structured reporting"

    # Define available prompts for triage analysis
    @server.list_prompts()
    async def list_prompts() -> list[Prompt]:
        return [
            Prompt(
                name=DUMP_TRIAGE_PROMPT_NAME,
                title=DUMP_TRIAGE_PROMPT_TITLE,
                description=DUMP_TRIAGE_PROMPT_DESCRIPTION,
                arguments=[
                    PromptArgument(
                        name="dump_path",
                        description="Path to the Windows crash dump file to analyze (optional - will prompt if not provided)",
                        required=False,
                    ),
                ],
            ),
        ]

    @server.get_prompt()
    async def get_prompt(name: str, arguments: dict | None) -> GetPromptResult:
        if arguments is None:
            arguments = {}

        if name == DUMP_TRIAGE_PROMPT_NAME:
            dump_path = arguments.get("dump_path", "")
            try:
                prompt_content = load_prompt("dump-triage")
            except FileNotFoundError as e:
                raise McpError(ErrorData(
                    code=INTERNAL_ERROR,
                    message=f"Prompt file not found: {e}"
                ))

            # If dump_path is provided, prepend it to the prompt
            if dump_path:
                prompt_text = f"**Dump file to analyze:** {dump_path}\n\n{prompt_content}"
            else:
                prompt_text = prompt_content

            return GetPromptResult(
                description=DUMP_TRIAGE_PROMPT_DESCRIPTION,
                messages=[
                    PromptMessage(
                        role="user",
                        content=TextContent(
                            type="text",
                            text=prompt_text
                        ),
                    ),
                ],
            )

        else:
            raise McpError(ErrorData(
                code=INVALID_PARAMS,
                message=f"Unknown prompt: {name}"
            ))

    return server


# Clean up function to ensure all sessions are closed when the server exits
def cleanup_sessions():
    """Close all active CDB sessions."""
    for dump_path, session in active_sessions.items():
        try:
            if session is not None:
                session.shutdown()
        except Exception:
            pass
    active_sessions.clear()


# Register cleanup on module exit
import atexit
atexit.register(cleanup_sessions)
