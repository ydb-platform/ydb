import { ToggleInput, ToggleInputView } from "./toggle_input";
import { IconLike } from "../common/kinds";
import type { FullDisplay } from "../layouts/layout_dom";
import type { StyleSheetLike } from "../../core/dom";
import type * as p from "../../core/properties";
export declare class SwitchView extends ToggleInputView {
    model: Switch;
    protected icon_el: HTMLElement;
    protected body_el: HTMLElement;
    protected bar_el: HTMLElement;
    protected knob_el: HTMLElement;
    stylesheets(): StyleSheetLike[];
    protected _intrinsic_display(): FullDisplay;
    render(): void;
    protected _apply_icon(icon: IconLike | null): void;
    protected _update_active(): void;
    protected _update_disabled(): void;
}
export declare namespace Switch {
    type Attrs = p.AttrsOf<Props>;
    type Props = ToggleInput.Props & {
        on_icon: p.Property<IconLike | null>;
        off_icon: p.Property<IconLike | null>;
    };
}
export interface Switch extends Switch.Attrs {
}
export declare class Switch extends ToggleInput {
    properties: Switch.Props;
    __view_type__: SwitchView;
    constructor(attrs?: Partial<Switch.Attrs>);
}
//# sourceMappingURL=switch.d.ts.map