OPERATIONS_CSS = """
    OperationsPane {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #operations-form,
    #operations-result {
        height: auto;
        padding: 1 2;
        background: $surface;
    }

    #operations-form:dark,
    #operations-result:dark {
        background: $panel-darken-1;
    }

    #operations-result {
        margin-top: 1;
    }

    .operations-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .operations-row {
        height: auto;
        min-height: 1;
        width: 100%;
        margin-bottom: 1;
    }

    .operations-label {
        width: 20;
        color: $text-muted;
    }

    .operations-value {
        width: 1fr;
    }

    #operations-install-arguments {
        height: auto;
    }

    .operations-input {
        height: auto;
        width: 1fr;
        margin-bottom: 1;
    }

    .operations-checkbox {
        height: auto;
        margin-bottom: 1;
        background: transparent;
    }

    #operations-error {
        height: auto;
        color: $text-error;
        margin-bottom: 1;
    }

    #operations-actions {
        height: auto;
        align-horizontal: right;
    }

    #operations-output {
        height: auto;
        min-height: 3;
        color: $text-muted;
    }

    #operations-live {
        height: auto;
        margin-top: 1;
    }

    #operations-steps-pane {
        width: 2fr;
        height: auto;
        padding-right: 1;
    }

    #operations-details-pane {
        width: 3fr;
        height: auto;
        padding-left: 1;
    }

    .operations-live-title {
        height: 2;
        content-align: left middle;
        color: $text-muted;
        text-style: bold;
    }

    #operations-steps,
    #operations-details {
        height: auto;
        min-height: 3;
    }
    """
