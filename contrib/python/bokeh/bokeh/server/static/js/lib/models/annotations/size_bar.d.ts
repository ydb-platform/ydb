import { BaseBar, BaseBarView } from "./base_bar";
import type { RadialGlyphView } from "../glyphs/radial_glyph";
import { RadialGlyph } from "../glyphs/radial_glyph";
import { GlyphRenderer } from "../renderers/glyph_renderer";
import type { Context2d, Exportable } from "../../core/util/canvas";
import { exportable } from "../../core/util/canvas";
import type { Range } from "../ranges/range";
import type { Scale } from "../scales";
import { LinearAxis } from "../axes/linear_axis";
import type * as p from "../../core/properties";
import type * as visuals from "../../core/visuals";
import * as mixins from "../../core/property_mixins";
import { ColumnDataSource } from "../sources/column_data_source";
import type { ElementLike } from "../renderers/composite_renderer";
import type { Align, Orientation } from "../../core/enums";
import { Plot, PlotView } from "../plots/plot";
import type { TickFormatter } from "../formatters/tick_formatter";
import type { Ticker } from "../tickers/ticker";
import { BBox } from "../../core/util/bbox";
import type { XY } from "../../core/util/bbox";
import { BorderLayout } from "../../core/layout/border";
import type { CanvasLayer } from "../../core/util/canvas";
declare class InternalBorderLayout extends BorderLayout {
    offset_position: XY;
    set_geometry(viewport: BBox): void;
}
declare class InternalPlotView extends PlotView {
    model: InternalPlot;
    layout: InternalBorderLayout;
    initialize(): void;
    protected _make_layout(): BorderLayout;
    _after_resize(): void;
}
declare namespace InternalPlot {
    type Attrs = p.AttrsOf<Props>;
    type Props = Plot.Props;
}
interface InternalPlot extends InternalPlot.Attrs {
}
declare class InternalPlot extends Plot {
    properties: InternalPlot.Props;
    __view_type__: InternalPlotView;
    constructor(attrs?: Partial<InternalPlot.Attrs>);
}
export declare class SizeBarView extends BaseBarView implements Exportable {
    model: SizeBar;
    visuals: SizeBar.Visuals;
    layout: InternalBorderLayout;
    protected _major_range: Range;
    protected _major_scale: Scale;
    protected _minor_range: Range;
    protected _minor_scale: Scale;
    protected _size_bar: InternalPlot;
    protected _size_bar_view: InternalPlotView;
    protected _data_source: ColumnDataSource;
    protected _major_axis: LinearAxis;
    protected _major_ticker: Ticker;
    protected _major_formatter: TickFormatter;
    get align(): {
        h: Align;
        v: Align;
    };
    get orientation(): Orientation;
    initialize(): void;
    get computed_elements(): ElementLike[];
    lazy_initialize(): Promise<void>;
    private _last_bbox;
    update_layout(): void;
    get renderer(): GlyphRenderer<RadialGlyph> | null;
    get glyph_view(): RadialGlyphView | null;
    protected _paint(_ctx: Context2d): void;
    [exportable]: boolean;
    export(type?: "auto" | "png" | "svg", hidpi?: boolean): CanvasLayer;
    get bbox(): BBox;
}
export declare namespace SizeBar {
    type Attrs = p.AttrsOf<Props>;
    type Props = BaseBar.Props & {
        renderer: p.Property<GlyphRenderer<RadialGlyph> | "auto">;
        bounds: p.Property<[number, number] | "auto">;
    } & Mixins;
    type Mixins = mixins.GlyphLineVector & mixins.GlyphFillVector & mixins.GlyphHatchVector;
    type Visuals = BaseBar.Visuals & {
        glyph_line: visuals.LineVector;
        glyph_fill: visuals.FillVector;
        glyph_hatch: visuals.HatchVector;
    };
}
export interface SizeBar extends SizeBar.Attrs {
}
export declare class SizeBar extends BaseBar {
    properties: SizeBar.Props;
    __view_type__: SizeBarView;
    constructor(attrs?: Partial<SizeBar.Attrs>);
}
export {};
//# sourceMappingURL=size_bar.d.ts.map