import { Pane, PaneView } from "../ui/pane";
import type { View } from "../../core/view";
import type { StyleSheetLike } from "../../core/dom";
import { InlineStyleSheet } from "../../core/dom";
import { Location } from "../../core/enums";
import type { PanEvent } from "../../core/ui_gestures";
import { UIGestures } from "../../core/ui_gestures";
import type * as p from "../../core/properties";
declare const CSSSize: import("../../core/kinds").Kinds.Or<[number, string]>;
type CSSSize = typeof CSSSize["__type__"];
export declare class DrawerView extends PaneView {
    model: Drawer;
    constructor(options: View.Options);
    protected readonly sizing: InlineStyleSheet;
    stylesheets(): StyleSheetLike[];
    protected readonly handle_el: HTMLDivElement;
    protected readonly contents_el: HTMLDivElement;
    protected toggle_el: HTMLElement;
    protected ui_gestures: UIGestures;
    connect_signals(): void;
    get self_target(): HTMLElement | ShadowRoot;
    protected title(open: boolean): string;
    render(): void;
    toggle(open?: boolean): void;
    protected state: {
        width: number;
        height: number;
    } | null;
    on_pan_start(_event: PanEvent): void;
    on_pan(event: PanEvent): void;
    on_pan_end(_event: PanEvent): void;
}
export declare namespace Drawer {
    type Attrs = p.AttrsOf<Props>;
    type Props = Pane.Props & {
        location: p.Property<Location>;
        open: p.Property<boolean>;
        size: p.Property<CSSSize>;
        resizable: p.Property<boolean>;
    };
}
export interface Drawer extends Drawer.Attrs {
}
export declare class Drawer extends Pane {
    properties: Drawer.Props;
    __view_type__: DrawerView;
    constructor(attrs?: Partial<Drawer.Attrs>);
}
export {};
//# sourceMappingURL=drawer.d.ts.map