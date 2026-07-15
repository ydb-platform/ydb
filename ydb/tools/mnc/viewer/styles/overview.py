from ydb.tools.mnc.viewer.styles.common import STATUS_CSS


OVERVIEW_CSS = """
    OverviewPane {
        height: 1fr;
    }

    #overview-status-row {
        height: 7;
        width: 100%;
        padding: 1 2;
        layout: horizontal;
        background: transparent;
    }

    .overview-status-card {
        width: 32;
        height: 5;
        margin-right: 2;
    }

    .overview-status-card-content {
        height: 5;
        width: 100%;
        padding: 0 2;
        background: $surface;
    }

    .overview-status-card-content:dark {
        background: $panel-darken-1;
    }

    #overview-status-row > .overview-status-card.-highlight {
        background: transparent;
    }

    #overview-status-row > .overview-status-card.-highlight .overview-status-card-content {
        background: $block-cursor-blurred-background;
    }

    #overview-status-row > .overview-status-card.-highlight .overview-status-title {
        color: $block-cursor-blurred-foreground;
    }

    .overview-status-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .overview-status-value {
        height: 2;
        content-align: left middle;
    }

    .status-ok {
        color: $success;
    }

    .status-error {
        color: $error;
    }

    .status-fail {
        color: $error;
    }

    .status-not-selected {
        color: $text-muted;
    }

    .status-checking {
        color: $warning;
    }

    .status-running {
        color: $success;
    }

    .status-stopped,
    .status-not-installed {
        color: $error;
    }

    #overview-agents-list {
        width: 1fr;
        height: 1fr;
        min-height: 5;
        margin: 0 2;
        background: transparent;
    }

    #overview-agents-card {
        height: auto;
        min-height: 5;
    }

    .overview-agents-card-content {
        height: auto;
        min-height: 5;
        padding: 0 2;
        background: $surface;
    }

    .overview-agents-card-content:dark {
        background: $panel-darken-1;
    }

    #overview-agents-list > #overview-agents-card.-highlight {
        background: transparent;
    }

    #overview-agents-list > #overview-agents-card.-highlight .overview-agents-card-content {
        background: $block-cursor-blurred-background;
    }

    #overview-agents-list > #overview-agents-card.-highlight #overview-agents-title {
        color: $block-cursor-blurred-foreground;
    }

    #overview-agents-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    #overview-agents-status {
        height: 2;
        content-align: left middle;
    }

    #overview-agents-hosts {
        height: auto;
        color: $text-muted;
    }
    """ + STATUS_CSS
