import { CanvasPanel, CanvasPanelView } from "./canvas_panel";
import { Scale } from "../scales/scale";
import { Range } from "../ranges/range";
import { Range1d } from "../ranges/range1d";
import type { BBox } from "../../core/util/bbox";
import type { Dict } from "../../core/types";
import type { StyleSheetLike } from "../../core/dom";
import type * as p from "../../core/properties";
type Ranges = Dict<Range>;
type Scales = Dict<Scale>;
export declare class CartesianFrameView extends CanvasPanelView {
    model: CartesianFrame;
    initialize(): void;
    remove(): void;
    stylesheets(): StyleSheetLike[];
    connect_signals(): void;
    protected _x_target: Range1d;
    protected _y_target: Range1d;
    protected _x_ranges: Map<string, Range>;
    protected _y_ranges: Map<string, Range>;
    protected _x_scales: Map<string, Scale>;
    protected _y_scales: Map<string, Scale>;
    protected _x_scale: Scale;
    protected _y_scale: Scale;
    protected _get_ranges(range: Range, extra_ranges: Ranges): Map<string, Range>;
    protected _get_scales(scale: Scale, extra_scales: Scales, ranges: Map<string, Range>, frame_range: Range): Map<string, Scale>;
    protected _configure_ranges(): void;
    protected _configure_scales(): void;
    protected _update_scales(): void;
    protected _register_frame(): void;
    protected _unregister_frame(): void;
    set_geometry(bbox: BBox): void;
    get x_range(): Range;
    get y_range(): Range;
    get x_target(): Range1d;
    get y_target(): Range1d;
    get x_ranges(): Map<string, Range>;
    get y_ranges(): Map<string, Range>;
    get ranges(): Set<Range>;
    get x_scales(): Map<string, Scale>;
    get y_scales(): Map<string, Scale>;
    get scales(): Set<Scale>;
    get x_scale(): Scale;
    get y_scale(): Scale;
}
export declare namespace CartesianFrame {
    type Attrs = p.AttrsOf<Props>;
    type Props = CanvasPanel.Props & {
        x_range: p.Property<Range>;
        y_range: p.Property<Range>;
        x_scale: p.Property<Scale>;
        y_scale: p.Property<Scale>;
        extra_x_ranges: p.Property<Dict<Range>>;
        extra_y_ranges: p.Property<Dict<Range>>;
        extra_x_scales: p.Property<Dict<Scale>>;
        extra_y_scales: p.Property<Dict<Scale>>;
        match_aspect: p.Property<boolean>;
        aspect_scale: p.Property<number>;
    };
}
export interface CartesianFrame extends CartesianFrame.Attrs {
}
export declare class CartesianFrame extends CanvasPanel {
    properties: CartesianFrame.Props;
    __view_type__: CartesianFrameView;
    constructor(attrs?: Partial<CartesianFrame.Attrs>);
}
export {};
//# sourceMappingURL=cartesian_frame.d.ts.map