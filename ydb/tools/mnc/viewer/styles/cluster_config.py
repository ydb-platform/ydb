CLUSTER_CONFIG_CSS = """
    ClusterConfigPane {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #cluster-config-left {
        width: 2fr;
        height: 1fr;
        background: $surface;
    }

    #cluster-config-left:dark {
        background: $panel-darken-1;
    }

    #cluster-configs {
        height: 1fr;
        background: transparent;
    }

    #cluster-configs > .config-candidate-item {
        height: 2;
    }

    .config-candidate-content {
        height: 2;
        padding-left: 1;
    }

    .config-candidate-name {
        height: 1;
        text-style: bold;
        content-align: left middle;
    }

    .config-candidate-path {
        height: 1;
        color: $text-muted;
        content-align: left middle;
    }

    #cluster-configs > .config-candidate-item.-highlight {
        background: $block-cursor-blurred-background;
    }

    #cluster-configs > .config-candidate-item.-highlight .config-candidate-name {
        color: $block-cursor-blurred-foreground;
        text-style: bold;
    }

    #cluster-configs > .config-candidate-item.-highlight .config-candidate-path {
        color: $text;
    }

    #cluster-config-details {
        width: 3fr;
        height: 1fr;
        padding: 1 2;
        background: $surface;
        margin-left: 1;
    }

    #cluster-config-details:dark {
        background: $panel-darken-1;
    }

    #cluster-config-details:focus {
        background: $primary-background;
    }

    #cluster-config-details-header {
        height: 1;
        width: 1fr;
        margin-bottom: 1;
    }

    #cluster-config-details-name {
        width: auto;
        margin-right: 1;
        text-style: bold;
    }

    #cluster-config-details-path {
        height: 1;
        color: $text-muted;
        margin-bottom: 1;
    }

    .cluster-config-badge {
        height: 1;
        width: auto;
        padding: 0 1;
        margin-right: 1;
        text-style: bold;
    }

    #cluster-config-selected-badge {
        background: $secondary-muted;
        color: $text-secondary;
    }

    #cluster-config-ok-badge {
        background: $success-muted;
        color: $text-success;
    }

    #cluster-config-fail-badge {
        background: $error-muted;
        color: $text-error;
    }

    #cluster-config-validation-errors {
        height: auto;
        color: $text-error;
        margin-bottom: 1;
    }

    #cluster-config-details-content {
        height: auto;
        width: 1fr;
    }
    """
