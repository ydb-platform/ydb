import { ContinuousAxis, ContinuousAxisView } from "./continuous_axis";
import { TimedeltaTickFormatter } from "../formatters/timedelta_tick_formatter";
import { TimedeltaTicker } from "../tickers/timedelta_ticker";
import type * as p from "../../core/properties";
export declare class TimedeltaAxisView extends ContinuousAxisView {
    model: TimedeltaAxis;
}
export declare namespace TimedeltaAxis {
    type Attrs = p.AttrsOf<Props>;
    type Props = ContinuousAxis.Props;
}
export interface TimedeltaAxis extends TimedeltaAxis.Attrs {
}
export declare class TimedeltaAxis extends ContinuousAxis {
    properties: TimedeltaAxis.Props;
    __view_type__: TimedeltaAxisView;
    ticker: TimedeltaTicker;
    formatter: TimedeltaTickFormatter;
    constructor(attrs?: Partial<TimedeltaAxis.Attrs>);
}
//# sourceMappingURL=timedelta_axis.d.ts.map