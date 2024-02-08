import os
import sys
import jinja2
import library.python.resource as rs

from ydb.tools.ydbd_slice.kube import cms, dynconfig


def render_resource(name, params):
    env = jinja2.Environment(
        loader=jinja2.FunctionLoader(lambda x: rs.find(x).decode()), undefined=jinja2.StrictUndefined
    )

    template = env.get_template(name)
    return template.render(**params)


def generate_file(project_path, filename, template, template_kwargs):
    try:
        manifest = render_resource(template, template_kwargs)
        with open(os.path.join(project_path, filename), 'w') as file:
            try:
                file.write(manifest)
            except Exception:
                print(file)
                print(manifest)
                raise
    except Exception as e:
        sys.exit(f'Failed to render manifest: {filename}: {str(e)}')


def generate_legacy_configs(project_path, preferred_pool_kind='ssd'):
    cms_configs_dir = os.path.join(project_path, cms.LEGACY_CMS_CONFIG_ITEMS_DIR)
    if not os.path.exists(cms_configs_dir):
        os.mkdir(cms_configs_dir)
    generate_file(
        project_path=project_path,
        filename=os.path.join(cms_configs_dir, 'table-profile.txt'),
        template='/ydbd_slice/templates/legacy-cms-config-items/table-profile.txt',
        template_kwargs=dict(
            preferred_pool_kind=preferred_pool_kind,
        ),
    )
    generate_file(
        project_path=project_path,
        filename=os.path.join(cms_configs_dir, 'unified-agent.txt'),
        template='/ydbd_slice/templates/legacy-cms-config-items/unified-agent.txt',
        template_kwargs=dict(
            preferred_pool_kind=preferred_pool_kind,
        ),
    )


def generate_dynconfigs(project_path, namespace_name, cluster_uuid, preferred_pool_kind='ssd'):
    generate_file(
        project_path=project_path,
        filename=dynconfig.get_config_path(project_path),
        template='/ydbd_slice/templates/common/dynconfig.yaml',
        template_kwargs=dict(
            preferred_pool_kind=preferred_pool_kind,
            cluster_uuid=cluster_uuid,
        )
    )


def generate_8_node_block_4_2(project_path, user, namespace_name, nodeclaim_name, node_flavor,
                              storage_name, database_name, cluster_uuid=''):
    generate_file(
        project_path=project_path,
        filename=f'namespace-{namespace_name}.yaml',
        template='/ydbd_slice/templates/common/namespace.yaml',
        template_kwargs=dict(
            namespace_name=namespace_name,
        )
    )
    generate_file(
        project_path=project_path,
        filename=f'nodeclaim-{nodeclaim_name}.yaml',
        template='/ydbd_slice/templates/8-node-block-4-2/nodeclaim.yaml',
        template_kwargs=dict(
            nodeclaim_namespace=namespace_name,
            nodeclaim_name=nodeclaim_name,
            nodeclaim_owner=user,
            nodeclaim_flavor_name=node_flavor,
        )
    )
    generate_file(
        project_path=project_path,
        filename=f'storage-{storage_name}.yaml',
        template='/ydbd_slice/templates/8-node-block-4-2/storage.yaml',
        template_kwargs=dict(
            storage_name=storage_name,
            storage_namespace=namespace_name,
            nodeclaim_name=nodeclaim_name,
            nodeclaim_namespace=namespace_name,
        )
    )
    generate_file(
        project_path=project_path,
        filename=f'database-{database_name}.yaml',
        template='/ydbd_slice/templates/common/database.yaml',
        template_kwargs=dict(
            database_name=database_name,
            database_namespace=namespace_name,
            storage_name=storage_name,
            nodeclaim_name=nodeclaim_name,
            nodeclaim_namespace=namespace_name,
        )
    )

    generate_dynconfigs(project_path, namespace_name, cluster_uuid)

    generate_legacy_configs(project_path)
