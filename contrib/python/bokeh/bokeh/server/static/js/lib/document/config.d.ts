import { Model } from "../model";
import { Notifications } from "../models/ui/notifications";
import type * as p from "../core/properties";
export declare namespace DocumentConfig {
    type Attrs = p.AttrsOf<Props>;
    type Props = Model.Props & {
        reconnect_session: p.Property<boolean>;
        notify_connection_status: p.Property<boolean>;
        notifications: p.Property<Notifications | null>;
    };
}
export interface DocumentConfig extends DocumentConfig.Attrs {
}
export declare class DocumentConfig extends Model {
    properties: DocumentConfig.Props;
    constructor(attrs?: Partial<DocumentConfig.Attrs>);
}
//# sourceMappingURL=config.d.ts.map