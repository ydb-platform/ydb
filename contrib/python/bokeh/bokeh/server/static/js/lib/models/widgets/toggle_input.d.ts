import { Widget, WidgetView } from "./widget";
import type { StyleSheetLike } from "../../core/dom";
import type * as p from "../../core/properties";
export declare abstract class ToggleInputView extends WidgetView {
    model: ToggleInput;
    protected label_el: HTMLElement;
    stylesheets(): StyleSheetLike[];
    connect_signals(): void;
    protected abstract _update_active(): void;
    protected abstract _update_disabled(): void;
    protected _toggle_active(): void;
    render(): void;
    protected _update_label(): void;
}
export declare namespace ToggleInput {
    type Attrs = p.AttrsOf<Props>;
    type Props = Widget.Props & {
        active: p.Property<boolean>;
        label: p.Property<string>;
    };
}
export interface ToggleInput extends ToggleInput.Attrs {
}
export declare abstract class ToggleInput extends Widget {
    properties: ToggleInput.Props;
    __view_type__: ToggleInputView;
    constructor(attrs?: Partial<ToggleInput.Attrs>);
}
//# sourceMappingURL=toggle_input.d.ts.map