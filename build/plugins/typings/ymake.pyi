from typing import Literal


class Unit:
    MessageType = Literal["INFO", "WARN", "ERROR"]
    PluginArgs = str | list[str] | tuple[str]

    def message(self, args: list[MessageType | str]) -> None:
        """
        Print message to the log
        """
        ...

    def get(self, var_name: str) -> str | None:
        """
        Get variable value
        """
        ...

    def set(self, args: PluginArgs) -> None:
        """
        Set variable value
        """
        ...

    def enabled(self, var_name: str) -> None:
        """
        Set variable value to "yes"
        """
        ...

    def disabled(self, var_name: str) -> None:
        """
        Set variable value to "no"
        """
        ...

    def set_property(self, args: PluginArgs) -> None:
        """
        TODO (set vs set_property?)
        """
        ...

    def resolve(self, path: str) -> str:
        """
        Resolve path TODO?
        """
        ...

    def resolve_arc_path(self, path: str) -> str:
        """
        Resolve path TODO?
        """
        ...

    def path(self) -> str:
        """
        Get the project path
        """
        ...

    def ondepends(self, deps: PluginArgs) -> None:
        """
        Run DEPENDS(...)
        """
        ...

    def onpeerdir(self, args: str | list[str]) -> None:
        """
        Run PEERDIR(...)
        """
        ...

    # User provided macro invocation type hints
    def on_ts_configure(self):
        """
        Run base configuration for TS module
        """
        ...

    def on_node_modules_configure(self):
        """
        Calculates inputs and outputs of node_modules, fills `_NODE_MODULES_INOUTS` variable
        """
        ...

    def on_peerdir_ts_resource(self, *resources: str):
        """
        Ensure dependency installed on the project

        Also check its version (is it supported by erm)
        """
        ...

    def on_do_ts_yndexing(self) -> None:
        """
        Turn on code navigation indexing
        """
        ...

    def on_setup_install_node_modules_recipe(self, args: Unit.PluginArgs) -> None:
        """
        Setup test recipe to install node_modules before running tests
        """
        ...

    def on_setup_extract_node_modules_recipe(self, args: Unit.PluginArgs) -> None:
        """
        Setup test recipe to extract workspace-node_modules.tar before running tests
        """
        ...

    def on_setup_extract_node_modules_layer_recipe(self, args: Unit.PluginArgs) -> None:
        """
        Setup test recipe to extract the internal node_modules layer before running tests
        """
        ...

    def on_setup_extract_output_tars_recipe(self, args: Unit.PluginArgs) -> None:
        """
        Setup test recipe to extract peer's output before running tests
        """
        ...

    def on_ts_proto_auto_configure(self) -> None:
        """
        Configure auto TS_PROTO
        """
        ...

    def on_ts_proto_auto_prepare_deps_configure(self) -> None:
        """
        Configure prepare deps for auto TS_PROTO
        """
        ...


def report_configure_error(msg: str) -> None: ...
