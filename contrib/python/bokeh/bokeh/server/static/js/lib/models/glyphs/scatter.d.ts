import { Marker, MarkerView } from "./marker";
import type { VectorVisuals } from "./defs";
import type { Rect, KeyVal } from "../../core/types";
import * as p from "../../core/properties";
import type { Context2d } from "../../core/util/canvas";
import { CustomJS } from "../callbacks/customjs";
import type { SyncExecutableLike } from "../../core/util/callbacks";
export interface ScatterView extends Scatter.Data {
}
export declare class ScatterView extends MarkerView {
    model: Scatter;
    load_glglyph(): Promise<typeof import("./webgl/multi_marker").MultiMarkerGL>;
    protected _compute_can_use_webgl(): boolean;
    protected _update_defs(): Promise<void>;
    connect_signals(): void;
    lazy_initialize(): Promise<void>;
    protected _paint(ctx: Context2d, indices: number[], data?: Partial<Scatter.Data>): void;
    draw_legend_for_index(ctx: Context2d, { x0, x1, y0, y1 }: Rect, index: number): void;
}
type CustomMarkerDef = SyncExecutableLike<Scatter, [{
    ctx: Context2d;
    i: number;
    r: number;
    visuals: VectorVisuals;
}], void | Path2D>;
export declare namespace Scatter {
    type Attrs = p.AttrsOf<Props>;
    type Props = Marker.Props & {
        marker: p.MarkerSpec;
        defs: p.Property<KeyVal<p.ExtMarkerType, CustomMarkerDef | CustomJS>>;
    };
    type Visuals = Marker.Visuals;
    type Data = p.GlyphDataOf<Props>;
}
export interface Scatter extends Scatter.Attrs {
}
export declare class Scatter extends Marker {
    properties: Scatter.Props;
    __view_type__: ScatterView;
    constructor(attrs?: Partial<Scatter.Attrs>);
}
export {};
//# sourceMappingURL=scatter.d.ts.map