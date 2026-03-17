def get_field(model, field_name):
    return model._meta.get_field(field_name)


def get_apps_from_state(migration_state):
    return migration_state.apps


def get_rel(f):
    return f.remote_field
