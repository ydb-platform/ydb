import { Annotation, AnnotationView } from "./annotation";
import type { ToolbarView } from "../tools/toolbar";
import { Toolbar } from "../tools/toolbar";
import type { IterViews } from "../../core/build_views";
import type { Size, Layoutable } from "../../core/layout";
import type * as p from "../../core/properties";
export declare class ToolbarPanelView extends AnnotationView {
    model: ToolbarPanel;
    layout: Layoutable;
    update_layout(): void;
    after_layout(): void;
    has_finished(): boolean;
    children(): IterViews;
    toolbar_view: ToolbarView;
    lazy_initialize(): Promise<void>;
    connect_signals(): void;
    remove(): void;
    render(): void;
    private get is_horizontal();
    protected _paint(): void;
    protected _get_size(): Size;
}
export declare namespace ToolbarPanel {
    type Attrs = p.AttrsOf<Props>;
    type Props = Annotation.Props & {
        toolbar: p.Property<Toolbar>;
    };
}
export interface ToolbarPanel extends ToolbarPanel.Attrs {
}
export declare class ToolbarPanel extends Annotation {
    properties: ToolbarPanel.Props;
    __view_type__: ToolbarPanelView;
    constructor(attrs?: Partial<ToolbarPanel.Attrs>);
}
//# sourceMappingURL=toolbar_panel.d.ts.map