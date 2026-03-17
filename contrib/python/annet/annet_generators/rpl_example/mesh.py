from annet.mesh import GlobalOptions, MeshRulesRegistry


registry = MeshRulesRegistry()


@registry.device("{name:.*}")
def device_handler(global_opts: GlobalOptions):
    global_opts.groups["GROUP1"].import_policy = "example1"
    global_opts.groups["GROUP1"].export_policy = "example2"
