from ydb.tools.mnc.viewer.styles.common import STATUS_CSS


HOSTS_CSS = """
    AgentsPane {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #agents-summary {
        height: auto;
        min-height: 4;
        padding: 0 2;
        background: $surface;
    }

    #agents-summary:dark {
        background: $panel-darken-1;
    }

    #agents-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    #agents-status {
        height: 2;
        content-align: left middle;
    }

    #agents-hosts {
        height: auto;
        min-height: 3;
        margin-top: 1;
        background: transparent;
    }

    .hosts-empty-message {
        height: auto;
        min-height: 3;
        padding: 1 2;
        background: $surface;
        color: $text-muted;
    }

    .hosts-empty-message:dark {
        background: $panel-darken-1;
    }

    AgentsPane:focus #agents-summary {
        background: $primary-background;
    }

    HostCard {
        height: auto;
        margin-bottom: 1;
        padding: 1 2;
        background: $surface;
        border: solid $primary;
    }

    HostCard:dark {
        background: $panel-darken-1;
    }

    .host-card-header {
        height: 2;
        width: 100%;
    }

    .host-title {
        width: 1fr;
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .host-status {
        width: auto;
        height: 2;
        content-align: right middle;
        text-style: bold;
    }

    .host-section {
        height: auto;
        margin-top: 1;
    }

    .host-section-title {
        height: 1;
        text-style: bold;
        color: $text;
    }

    .host-field-row {
        height: auto;
        min-height: 1;
        width: 100%;
    }

    .host-field-name {
        width: 18;
        color: $text-muted;
    }

    .host-field-value {
        width: 1fr;
    }

    .host-message {
        color: $text-muted;
    }

    HostTasksTable {
        height: 7;
        width: 100%;
        margin-top: 1;
    }

    .host-empty-line {
        height: 1;
        margin-top: 1;
        color: $text-muted;
    }

    .disk-row {
        height: auto;
        min-height: 1;
        width: 100%;
        margin-top: 1;
    }

    .disk-name {
        width: 24;
        text-style: bold;
    }

    .disk-detail {
        width: 1fr;
        color: $text-muted;
    }
    """ + STATUS_CSS
