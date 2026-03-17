import { Widget, WidgetView } from "./widget";
import type * as p from "../../core/properties";
export declare abstract class IndicatorView extends WidgetView {
    model: Indicator;
}
export declare namespace Indicator {
    type Attrs = p.AttrsOf<Props>;
    type Props = Widget.Props;
}
export interface Indicator extends Indicator.Attrs {
}
export declare abstract class Indicator extends Widget {
    properties: Indicator.Props;
    __view_type__: IndicatorView;
    constructor(attrs?: Partial<Indicator.Attrs>);
}
//# sourceMappingURL=indicator.d.ts.map