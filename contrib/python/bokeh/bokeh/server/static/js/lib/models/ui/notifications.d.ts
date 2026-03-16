import { UIElement, UIElementView } from "./ui_element";
import type * as p from "../../core/properties";
import { Signal } from "../../core/signaling";
export declare const notifications_el: HTMLElement;
export declare class NotificationsView extends UIElementView {
    model: Notifications;
    protected _connection_el: HTMLElement | null;
    protected _connection_timer: number | null;
    initialize(): void;
}
export declare namespace Notifications {
    type Attrs = p.AttrsOf<Props>;
    type Props = UIElement.Props & {};
}
export interface Notifications extends Notifications.Attrs {
}
export declare class Notifications extends UIElement {
    properties: Notifications.Props;
    __view_type__: NotificationsView;
    constructor(attrs?: Partial<Notifications.Attrs>);
    readonly push: Signal<Message, this>;
}
type Message = {
    type: "error" | "success";
    text: string;
    timeout?: number;
};
export {};
//# sourceMappingURL=notifications.d.ts.map