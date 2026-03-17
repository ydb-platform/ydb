import { Annotation, AnnotationView } from "./annotation";
import { LegendItem } from "./legend_item";
import type { GlyphRenderer } from "../renderers/glyph_renderer";
import { AlternationPolicy, Orientation, LegendLocation, LegendClickPolicy, Location } from "../../core/enums";
import type { VAlign, HAlign } from "../../core/enums";
import type * as visuals from "../../core/visuals";
import * as mixins from "../../core/property_mixins";
import type * as p from "../../core/properties";
import type { Size } from "../../core/layout";
import { BBox } from "../../core/util/bbox";
import type { Context2d, CanvasLayer } from "../../core/util/canvas";
import type { StyleSheetLike } from "../../core/dom";
import { Padding, BorderRadius } from "../common/kinds";
import type { XY, LRTB, Corners } from "../../core/util/bbox";
type Entry = {
    el: HTMLElement;
    glyph: CanvasLayer;
    label_el: HTMLElement;
    item: LegendItem;
    label: string;
    i: number;
    row: number;
    col: number;
};
export declare class LegendView extends AnnotationView {
    model: Legend;
    visuals: Legend.Visuals;
    get is_dual_renderer(): boolean;
    protected _get_size(): Size;
    update_layout(): void;
    protected _resize_observer: ResizeObserver;
    initialize(): void;
    remove(): void;
    connect_signals(): void;
    protected _bbox: BBox;
    get bbox(): BBox;
    protected readonly grid_el: HTMLElement;
    protected title_el: HTMLElement;
    protected entries: Entry[];
    get padding(): LRTB<number>;
    get border_radius(): Corners<number>;
    stylesheets(): StyleSheetLike[];
    protected _paint_glyphs(): void;
    get labels(): {
        item: LegendItem;
        label: string;
    }[];
    protected get _should_rerender_items(): boolean;
    protected _toggle_inactive({ el, item }: Entry): void;
    protected _render_items(): void;
    render(): void;
    after_render(): void;
    get location(): {
        x: HAlign | number;
        y: VAlign | number;
    };
    get anchor(): XY<number>;
    get css_position(): XY<string>;
    get is_visible(): boolean;
    update_position(): void;
    get is_interactive(): boolean;
    get click_policy(): (r: GlyphRenderer) => void;
    get is_active(): (item: LegendItem) => boolean;
    has_item_background(_i: number, row: number, col: number): boolean;
    protected _paint(ctx: Context2d): void;
    protected _draw_legend_box(ctx: Context2d, canvas_bbox: BBox): void;
    protected _draw_title(ctx: Context2d, canvas_bbox: BBox): void;
    protected _draw_legend_items(ctx: Context2d, canvas_bbox: BBox): void;
}
export declare namespace Legend {
    type Attrs = p.AttrsOf<Props>;
    type Props = Annotation.Props & {
        orientation: p.Property<Orientation>;
        ncols: p.Property<number | "auto">;
        nrows: p.Property<number | "auto">;
        location: p.Property<LegendLocation | [number, number]>;
        title: p.Property<string | null>;
        title_location: p.Property<Location>;
        title_standoff: p.Property<number>;
        label_standoff: p.Property<number>;
        glyph_width: p.Property<number>;
        glyph_height: p.Property<number>;
        label_width: p.Property<number>;
        label_height: p.Property<number | "auto">;
        margin: p.Property<number>;
        padding: p.Property<Padding>;
        border_radius: p.Property<BorderRadius>;
        spacing: p.Property<number>;
        items: p.Property<LegendItem[]>;
        click_policy: p.Property<LegendClickPolicy>;
        item_background_policy: p.Property<AlternationPolicy>;
    } & Mixins;
    type Mixins = mixins.LabelText & mixins.TitleText & mixins.InactiveFill & mixins.InactiveHatch & mixins.BorderLine & mixins.BackgroundFill & mixins.BackgroundHatch & mixins.ItemBackgroundFill & mixins.ItemBackgroundHatch;
    type Visuals = Annotation.Visuals & {
        label_text: visuals.Text;
        title_text: visuals.Text;
        inactive_fill: visuals.Fill;
        inactive_hatch: visuals.Hatch;
        border_line: visuals.Line;
        background_fill: visuals.Fill;
        background_hatch: visuals.Hatch;
        item_background_fill: visuals.Fill;
        item_background_hatch: visuals.Hatch;
    };
}
export interface Legend extends Legend.Attrs {
}
export declare class Legend extends Annotation {
    properties: Legend.Props;
    __view_type__: LegendView;
    constructor(attrs?: Partial<Legend.Attrs>);
}
export {};
//# sourceMappingURL=legend.d.ts.map