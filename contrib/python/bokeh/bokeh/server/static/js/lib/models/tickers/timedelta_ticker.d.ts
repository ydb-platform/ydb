import type * as p from "../../core/properties";
import { CompositeTicker } from "./composite_ticker";
export declare namespace TimedeltaTicker {
    type Attrs = p.AttrsOf<Props>;
    type Props = CompositeTicker.Props;
}
export interface TimedeltaTicker extends TimedeltaTicker.Attrs {
}
export declare class TimedeltaTicker extends CompositeTicker {
    properties: TimedeltaTicker.Props;
    constructor(attrs?: Partial<TimedeltaTicker.Attrs>);
}
//# sourceMappingURL=timedelta_ticker.d.ts.map