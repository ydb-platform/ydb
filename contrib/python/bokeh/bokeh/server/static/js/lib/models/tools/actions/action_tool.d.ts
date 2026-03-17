import { Tool, ToolView } from "../tool";
import type { ToolButton } from "../tool_button";
import type { MenuItem } from "../../ui/menus";
import type { LayoutDOMView } from "../../layouts/layout_dom";
import { Signal } from "../../../core/signaling";
import type * as p from "../../../core/properties";
export declare abstract class ActionToolView extends ToolView {
    model: ActionTool;
    readonly parent: LayoutDOMView;
    connect_signals(): void;
    abstract doit(arg?: unknown): void;
}
export declare namespace ActionTool {
    type Attrs = p.AttrsOf<Props>;
    type Props = Tool.Props;
}
export interface ActionTool extends ActionTool.Attrs {
}
export declare abstract class ActionTool extends Tool {
    properties: ActionTool.Props;
    __view_type__: ActionToolView;
    constructor(attrs?: Partial<ActionTool.Attrs>);
    readonly do: Signal<string | undefined, this>;
    tool_button(): ToolButton;
    menu_item(): MenuItem;
}
//# sourceMappingURL=action_tool.d.ts.map