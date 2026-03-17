import type * as p from "../../core/properties";
import { OutputBackend } from "../../core/enums";
import { UIEventBus } from "../../core/ui_events";
import type { Context2d } from "../../core/util/canvas";
import { CanvasLayer } from "../../core/util/canvas";
import type { BBox } from "../../core/util/bbox";
import { UIElement, UIElementView } from "../ui/ui_element";
import type { PlotView } from "../plots/plot";
import type { ReglWrapper } from "../glyphs/webgl/regl_wrap";
import type { StyleSheetLike } from "../../core/dom";
import { InlineStyleSheet } from "../../core/dom";
export type WebGLState = {
    readonly canvas: HTMLCanvasElement;
    readonly regl_wrapper: ReglWrapper;
};
export declare class CanvasView extends UIElementView {
    model: Canvas;
    webgl: WebGLState | null;
    underlays_el: HTMLElement;
    primary: CanvasLayer;
    overlays: CanvasLayer;
    overlays_el: HTMLElement;
    events_el: HTMLElement;
    ui_event_bus: UIEventBus;
    protected readonly _size: InlineStyleSheet;
    readonly touch_action: InlineStyleSheet;
    initialize(): void;
    get layers(): (HTMLElement | CanvasLayer)[];
    lazy_initialize(): Promise<void>;
    remove(): void;
    stylesheets(): StyleSheetLike[];
    render(): void;
    get pixel_ratio(): number;
    get pixel_ratio_changed(): boolean;
    _update_bbox(): boolean;
    after_resize(): void;
    _after_resize(): void;
    resize(): boolean;
    prepare_webgl(frame_box: BBox): void;
    blit_webgl(ctx: Context2d): void;
    protected _clear_webgl(): void;
    compose(): CanvasLayer;
    create_layer(): CanvasLayer;
    to_blob(): Promise<Blob>;
    plot_views: PlotView[];
}
export declare namespace Canvas {
    type Attrs = p.AttrsOf<Props>;
    type Props = UIElement.Props & {
        hidpi: p.Property<boolean>;
        output_backend: p.Property<OutputBackend>;
    };
}
export interface Canvas extends Canvas.Attrs {
}
export declare class Canvas extends UIElement {
    properties: Canvas.Props;
    __view_type__: CanvasView;
    constructor(attrs?: Partial<Canvas.Attrs>);
}
//# sourceMappingURL=canvas.d.ts.map