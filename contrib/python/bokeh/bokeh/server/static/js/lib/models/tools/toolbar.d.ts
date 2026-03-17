import type { StyleSheetLike } from "../../core/dom";
import type { ViewStorage, IterViews, ViewOf } from "../../core/build_views";
import type * as p from "../../core/properties";
import { UIElement, UIElementView } from "../ui/ui_element";
import { Logo, Location, ToolName } from "../../core/enums";
import { Tool } from "./tool";
import type { ToolLike } from "./tool_proxy";
import { ToolProxy } from "./tool_proxy";
import { ToolButton } from "./tool_button";
import { GestureTool } from "./gestures/gesture_tool";
import { InspectTool } from "./inspectors/inspect_tool";
import { ActionTool } from "./actions/action_tool";
import { HelpTool } from "./actions/help_tool";
import { Menu } from "../ui/menus";
import type { At } from "../../core/util/menus";
import { ContextMenu } from "../../core/util/menus";
import { Signal0 } from "../../core/signaling";
export declare class ToolbarView extends UIElementView {
    model: Toolbar;
    protected readonly _tool_button_views: ViewStorage<ToolButton>;
    protected _tool_buttons: ToolButton[][];
    protected _items: HTMLElement[];
    get tool_buttons(): ToolButton[];
    get tool_button_views(): ViewOf<ToolButton>[];
    protected _overflow_menu: ContextMenu;
    protected _overflow_el: HTMLElement;
    get overflow_el(): HTMLElement;
    private _visible;
    get visible(): boolean;
    children(): IterViews;
    has_finished(): boolean;
    initialize(): void;
    lazy_initialize(): Promise<void>;
    connect_signals(): void;
    stylesheets(): StyleSheetLike[];
    remove(): void;
    private readonly _our_proxies;
    private _destroy_proxies;
    /**
     * Group similar tools into tool proxies.
     */
    private _group_tools;
    protected _build_tool_button_views(): Promise<void>;
    set_visibility(visible: boolean): void;
    protected _on_visible_change(): void;
    _after_resize(): void;
    protected _menu_at(): At;
    toggle_menu(): void;
    render(): void;
    _after_render(): void;
    toggle_auto_scroll(force?: boolean): void;
}
declare const GestureToolLike: import("../../core/kinds").Kinds.Or<[GestureTool, ToolProxy<any>]>;
type GestureToolLike = GestureTool | ToolProxy<GestureTool>;
declare const GesturesMap: import("../../core/kinds").Kinds.Struct<{
    pan: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    scroll: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    pinch: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    rotate: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    move: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    tap: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    doubletap: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    press: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    pressup: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
    multi: {
        tools: /*elided*/ any;
        active: /*elided*/ any;
    };
}>;
type GesturesMap = typeof GesturesMap["__type__"];
export type Inspection = Tool;
export declare const Inspection: typeof Tool;
type ActiveGestureToolsProps = {
    active_drag: p.Property<GestureToolLike | "auto" | null>;
    active_scroll: p.Property<GestureToolLike | "auto" | null>;
    active_tap: p.Property<GestureToolLike | "auto" | null>;
    active_multi: p.Property<GestureToolLike | "auto" | null>;
};
export declare namespace Toolbar {
    type Attrs = p.AttrsOf<Props>;
    type Props = UIElement.Props & {
        tools: p.Property<(Tool | ToolProxy<Tool>)[]>;
        logo: p.Property<Logo | null>;
        autohide: p.Property<boolean>;
        group: p.Property<boolean>;
        group_types: p.Property<ToolName[]>;
        buttons: p.Property<(ToolButton | null)[] | "auto">;
        location: p.Property<Location>;
        inner: p.Property<boolean>;
        gestures: p.Property<GesturesMap>;
        actions: p.Property<ToolLike<ActionTool>[]>;
        inspectors: p.Property<ToolLike<InspectTool>[]>;
        help: p.Property<ToolLike<HelpTool>[]>;
        auxiliaries: p.Property<ToolLike<Tool>[]>;
    } & ActiveGestureToolsProps & {
        active_inspect: p.Property<ToolLike<Inspection> | ToolLike<Inspection>[] | "auto" | null>;
    };
}
export interface Toolbar extends Toolbar.Attrs {
}
export declare class Toolbar extends UIElement {
    properties: Toolbar.Props;
    __view_type__: ToolbarView;
    constructor(attrs?: Partial<Toolbar.Attrs>);
    active_changed: Signal0<this>;
    get horizontal(): boolean;
    get vertical(): boolean;
    connect_signals(): void;
    initialize(): void;
    protected _init_tools(): void;
    protected _activate_tools(): void;
    _active_change(tool: ToolLike<GestureTool>): void;
    to_menu(): Menu;
}
export {};
//# sourceMappingURL=toolbar.d.ts.map