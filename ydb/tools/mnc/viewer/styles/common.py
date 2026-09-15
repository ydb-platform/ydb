def status_class(status: str) -> str:
    return "status-" + status.lower().replace(" ", "-")


STATUS_CSS = """
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
"""
