SETTINGS_CSS = """
    MncConfigForm {
        height: 1fr;
        padding: 0 1 1 1;
    }

    #mnc-config-fields {
        height: auto;
        margin: 1 2 0 2;
        background: transparent;
    }

    #mnc-config-fields > ConfigFieldItem {
        height: 5;
        background: transparent;
    }

    #mnc-config-fields > ConfigFieldItem.-highlight {
        background: transparent;
        color: $block-cursor-blurred-foreground;
    }

    #mnc-config-fields > ConfigFieldItem.-highlight .config-field-row {
        background: $surface;
        border: tall blank;
    }

    #mnc-config-fields:focus > ConfigFieldItem.-highlight .config-field-row {
        background: $block-cursor-blurred-background;
        border: tall $primary;
    }

    #mnc-config-fields > ConfigFieldItem.-highlight .config-field-title,
    #mnc-config-fields > ConfigFieldItem.-highlight Input {
        color: $text;
    }

    #mnc-config-fields:focus > ConfigFieldItem.-highlight .config-field-title,
    #mnc-config-fields:focus > ConfigFieldItem.-highlight Input {
        color: $block-cursor-foreground;
        text-style: bold;
    }

    .config-field-row {
        height: 5;
        width: 100%;
        padding: 0 2;
        background: $surface;
        border: tall blank;
    }

    .config-field-row:dark {
        background: $panel-darken-1;
    }

    .config-field-title {
        width: 22;
        height: 3;
        margin-right: 1;
        background: transparent;
        content-align: left middle;
        text-style: bold;
    }

    .config-field-value {
        width: 1fr;
        height: 3;
        padding: 0 1;
        background: transparent;
    }

    .config-field-row Input {
        width: 1fr;
        height: 1;
        margin-top: 1;
        border: none;
        background: transparent;
        padding: 0;
    }

    .config-field-row Input:focus {
        border: none;
        background-tint: 0%;
    }

    #mnc-deploy-flags {
        height: auto;
        margin: 1 2 0 2;
        padding: 0 2 1 2;
        background: $surface;
    }

    #mnc-deploy-flags:dark {
        background: $panel-darken-1;
    }

    .config-group-title {
        height: 2;
        content-align: left middle;
        text-style: bold;
    }

    .config-checkbox {
        height: 3;
        width: 1fr;
        padding: 0 1;
        background: transparent;
        color: $text;
    }

    .config-checkbox:focus {
        background: $block-cursor-blurred-background;
        color: $block-cursor-blurred-foreground;
        text-style: bold;
    }

    .config-checkbox > .toggle--label {
        color: $text;
    }

    .config-checkbox:focus > .toggle--label {
        color: $block-cursor-blurred-foreground;
        text-style: bold;
    }

    .config-checkbox-row {
        height: 3;
        width: 100%;
        margin-bottom: 1;
    }
    """
