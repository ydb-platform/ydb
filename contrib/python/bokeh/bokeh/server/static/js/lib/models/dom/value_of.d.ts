import { DOMElement, DOMElementView } from "./dom_element";
import { HasProps } from "../../core/has_props";
import { CustomJS } from "../callbacks/customjs";
import type * as p from "../../core/properties";
declare const Formatter: import("../../core/kinds").Kinds.Or<["raw" | "basic" | "numeral" | "printf" | "datetime", CustomJS]>;
type Formatter = typeof Formatter["__type__"];
export declare class ValueOfView extends DOMElementView {
    model: ValueOf;
    connect_signals(): void;
    protected _render_value(value: unknown): void;
    render(): void;
}
export declare namespace ValueOf {
    type Attrs = p.AttrsOf<Props>;
    type Props = DOMElement.Props & {
        obj: p.Property<HasProps>;
        attr: p.Property<string>;
        format: p.Property<string | null>;
        formatter: p.Property<Formatter>;
    };
}
export interface ValueOf extends ValueOf.Attrs {
}
export declare class ValueOf extends DOMElement {
    properties: ValueOf.Props;
    __view_type__: ValueOfView;
    constructor(attrs?: Partial<ValueOf.Attrs>);
}
export {};
//# sourceMappingURL=value_of.d.ts.map