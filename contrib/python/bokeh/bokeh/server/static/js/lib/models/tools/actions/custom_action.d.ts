import { ActionTool, ActionToolView } from "./action_tool";
import { CustomJS } from "../../callbacks/customjs";
import type { CallbackLike0 } from "../../../core/util/callbacks";
import type * as p from "../../../core/properties";
export declare class CustomActionView extends ActionToolView {
    model: CustomAction;
    protected _update_active(): Promise<void>;
    lazy_initialize(): Promise<void>;
    _execute(): Promise<void>;
    doit(): void;
}
export declare namespace CustomAction {
    type Attrs = p.AttrsOf<Props>;
    type Props = ActionTool.Props & {
        callback: p.Property<CustomJS | CallbackLike0<CustomAction, unknown> | null>;
        active_callback: p.Property<CustomJS | CallbackLike0<CustomAction, unknown> | "auto" | null>;
    };
}
export interface CustomAction extends CustomAction.Attrs {
}
export declare class CustomAction extends ActionTool {
    properties: CustomAction.Props;
    __view_type__: CustomActionView;
    constructor(attrs?: Partial<CustomAction.Attrs>);
    tool_name: string;
    tool_icon: string;
}
//# sourceMappingURL=custom_action.d.ts.map