import { Indicator, IndicatorView } from "./indicator";
import { Signal0 } from "../../core/signaling";
import type { StyleSheetLike } from "../../core/dom";
import type * as p from "../../core/properties";
import { Orientation } from "../../core/enums";
declare const ProgressMode: import("../../core/kinds").Kinds.Enum<"determinate" | "indeterminate">;
type ProgressMode = typeof ProgressMode["__type__"];
declare const LabelLocation: import("../../core/kinds").Kinds.Enum<"none" | "inline">;
type LabelLocation = typeof LabelLocation["__type__"];
export declare class ProgressView extends IndicatorView {
    model: Progress;
    protected label_el: HTMLElement;
    protected value_el: HTMLElement;
    protected bar_el: HTMLElement;
    connect_signals(): void;
    stylesheets(): StyleSheetLike[];
    render(): void;
    protected _update_value(): void;
    protected _update_disabled(): void;
    protected _update_reversed(): void;
    protected _update_orientation(): void;
    protected _update_label_location(): void;
}
export declare namespace Progress {
    type Attrs = p.AttrsOf<Props>;
    type Props = Indicator.Props & {
        mode: p.Property<ProgressMode>;
        value: p.Property<number>;
        min: p.Property<number>;
        max: p.Property<number>;
        reversed: p.Property<boolean>;
        orientation: p.Property<Orientation>;
        label: p.Property<string | null>;
        label_location: p.Property<LabelLocation>;
        description: p.Property<string | null>;
    };
}
export interface Progress extends Progress.Attrs {
}
export declare class Progress extends Indicator {
    properties: Progress.Props;
    __view_type__: ProgressView;
    readonly finished: Signal0<this>;
    constructor(attrs?: Partial<Progress.Attrs>);
    get indeterminate(): boolean;
    get has_finished(): boolean;
    update(n: number): boolean;
    increment(n?: number): boolean;
    decrement(n?: number): void;
}
export {};
//# sourceMappingURL=progress.d.ts.map