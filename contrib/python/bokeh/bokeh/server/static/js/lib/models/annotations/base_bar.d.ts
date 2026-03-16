import { Annotation, AnnotationView } from "./annotation";
import { LabelOverrides } from "../axes/axis";
import { FixedTicker } from "../tickers/fixed_ticker";
import { TickFormatter } from "../formatters/tick_formatter";
import { LabelingPolicy } from "../policies/labeling";
import { BaseText } from "../text/base_text";
import { Anchor, Orientation } from "../../core/enums";
import type * as visuals from "../../core/visuals";
import * as mixins from "../../core/property_mixins";
import type * as p from "../../core/properties";
export declare abstract class BaseBarView extends AnnotationView {
    model: BaseBar;
    visuals: BaseBar.Visuals;
}
export declare namespace BaseBar {
    type Attrs = p.AttrsOf<Props>;
    type Props = Annotation.Props & {
        location: p.Property<Anchor | [number, number]>;
        orientation: p.Property<Orientation | "auto">;
        title: p.Property<string | BaseText | null>;
        title_standoff: p.Property<number>;
        width: p.Property<number | "max">;
        height: p.Property<number | "max">;
        margin: p.Property<number>;
        padding: p.Property<number>;
        ticker: p.Property<FixedTicker | "auto">;
        formatter: p.Property<TickFormatter | "auto">;
        major_label_overrides: p.Property<LabelOverrides>;
        major_label_policy: p.Property<LabelingPolicy>;
        label_standoff: p.Property<number>;
        major_tick_in: p.Property<number>;
        major_tick_out: p.Property<number>;
        minor_tick_in: p.Property<number>;
        minor_tick_out: p.Property<number>;
    } & Mixins;
    type Mixins = mixins.TitleText & mixins.MajorLabelText & mixins.MajorTickLine & mixins.MinorTickLine & mixins.BackgroundFill & mixins.BackgroundHatch & mixins.BorderLine & mixins.BarLine;
    type Visuals = Annotation.Visuals & {
        title_text: visuals.Text;
        major_label_text: visuals.Text;
        major_tick_line: visuals.Line;
        minor_tick_line: visuals.Line;
        background_fill: visuals.Fill;
        background_hatch: visuals.Hatch;
        border_line: visuals.Line;
        bar_line: visuals.Line;
    };
}
export interface BaseBar extends BaseBar.Attrs {
}
export declare abstract class BaseBar extends Annotation {
    properties: BaseBar.Props;
    __view_type__: BaseBarView;
    constructor(attrs?: Partial<BaseBar.Attrs>);
}
//# sourceMappingURL=base_bar.d.ts.map