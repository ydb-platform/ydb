import { OnOffButton, OnOffButtonView } from "./on_off_button";
import type { ActionTool } from "./actions/action_tool";
import type * as p from "../../core/properties";
export declare class ClickButtonView extends OnOffButtonView {
    model: ClickButton;
    tap(): void;
}
export declare namespace ClickButton {
    type Attrs = p.AttrsOf<Props>;
    type Props = OnOffButton.Props & {
        tool: p.Property<ActionTool>;
    };
}
export interface ClickButton extends ClickButton.Attrs {
    tool: ActionTool;
}
export declare class ClickButton extends OnOffButton {
    properties: ClickButton.Props;
    __view_type__: ClickButtonView;
    constructor(attrs?: Partial<ClickButton.Attrs>);
}
//# sourceMappingURL=click_button.d.ts.map