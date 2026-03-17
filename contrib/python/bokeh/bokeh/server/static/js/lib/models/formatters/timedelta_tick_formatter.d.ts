import { ContextWhich, Location, TimedeltaResolutionType } from "../../core/enums";
import type * as p from "../../core/properties";
import type { Arrayable } from "../../core/types";
import { TickFormatter } from "./tick_formatter";
export type { TimedeltaResolutionType } from "../../core/enums";
export declare const resolution_order: TimedeltaResolutionType[];
export declare const formatting_map: {
    [template: string]: any;
};
export declare function _get_resolution(resolution_secs: number, span_secs: number): TimedeltaResolutionType;
export declare function _str_timedelta(t: number, format: string): string;
export declare function _days(t: number, factor_next: number | null): string;
export declare function _hours(t: number, factor_next: number | null): string;
export declare function _minutes(t: number, factor_next: number | null): string;
export declare function _seconds(t: number, factor_next: number | null): string;
export declare function _ms(t: number, factor_next: number | null): string;
export declare function _us(t: number, factor_next: number | null): string;
export declare function _ns(t: number, factor_next: number | null): string;
export declare namespace TimedeltaTickFormatter {
    type Attrs = p.AttrsOf<Props>;
    type Props = TickFormatter.Props & {
        nanoseconds: p.Property<string>;
        microseconds: p.Property<string>;
        milliseconds: p.Property<string>;
        seconds: p.Property<string>;
        minsec: p.Property<string>;
        minutes: p.Property<string>;
        hourmin: p.Property<string>;
        hours: p.Property<string>;
        days: p.Property<string>;
        months: p.Property<string>;
        years: p.Property<string>;
        strip_leading_zeros: p.Property<boolean | Arrayable<TimedeltaResolutionType>>;
        hide_repeats: p.Property<boolean>;
        context: p.Property<string | TimedeltaTickFormatter | null>;
        context_which: p.Property<ContextWhich>;
        context_location: p.Property<Location>;
    };
}
export interface TimedeltaTickFormatter extends TimedeltaTickFormatter.Attrs {
}
export declare class TimedeltaTickFormatter extends TickFormatter {
    properties: TimedeltaTickFormatter.Props;
    constructor(attrs?: Partial<TimedeltaTickFormatter.Attrs>);
    doFormat(ticks: number[], _opts: {
        loc: number;
    }, _resolution?: TimedeltaResolutionType): string[];
    _compute_label(t: number, resolution: TimedeltaResolutionType): string;
    _compute_context_labels(ticks: number[], resolution: TimedeltaResolutionType): string[];
    _build_full_labels(base_labels: string[], context_labels: string[]): string[];
    _hide_repeating_labels(labels: string[]): string[];
}
//# sourceMappingURL=timedelta_tick_formatter.d.ts.map