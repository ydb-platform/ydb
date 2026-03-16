def serialize_id(pk):
    if isinstance(pk, (int, str)):
        return pk
    else:
        # Nb. special case for uuid field
        return str(pk)


def get_tree_from_queryset(
    queryset, on_create_node=None, max_level=None, item_label_field_name=None
):
    """
    Return tree data that is suitable for jqTree.
    The queryset must be sorted by 'tree_id' and 'left' fields.
    """
    pk_attname = queryset.model._meta.pk.attname

    # Result tree
    tree = []

    # Dict of all nodes; used for building the tree
    # - key is node id
    # - value is node info (label, id)
    node_dict = dict()

    # The lowest level of the tree; used for building the tree
    # - Initial value is None; set later
    # - For the whole tree this is 0, for a subtree this is higher
    min_level = None

    for instance in queryset:
        if min_level is None or instance.level < min_level:
            min_level = instance.level

        pk = getattr(instance, pk_attname)

        if item_label_field_name:
            label = getattr(instance, item_label_field_name)
        else:
            label = str(instance)

        node_info = dict(name=label, id=serialize_id(pk))
        if on_create_node:
            on_create_node(instance, node_info)

        if max_level is not None and not instance.is_leaf_node():
            # If there is a maximum level and this node has children, then initially set property 'load_on_demand' to true.
            node_info["load_on_demand"] = True

        if instance.level == min_level:
            # This is the lowest level. Skip finding a parent.
            # Add node to the tree
            tree.append(node_info)
        else:
            # NB: use instance's local value for parent attribute - consistent values for uuid
            parent_field = instance._meta.get_field(instance._mptt_meta.parent_attr)
            parent_attname = parent_field.get_attname()
            parent_id = getattr(instance, parent_attname)

            # Get parent from node dict
            parent_info = node_dict.get(parent_id)

            # Check for corner case: parent is deleted.
            if parent_info:
                if "children" not in parent_info:
                    parent_info["children"] = []

                # Add node to the tree
                parent_info["children"].append(node_info)

                # If there is a maximum level, then reset property 'load_on_demand' for parent
                if max_level is not None:
                    parent_info["load_on_demand"] = False

        # Update node dict
        node_dict[pk] = node_info

    return tree


def get_tree_queryset(model, node_id=None, max_level=None, include_root=True):
    if node_id:
        node = model.objects.get(pk=node_id)

        if max_level is None:
            max_level = node.level + 1

        qs = node.get_descendants().filter(level__lte=max_level)
    else:
        qs = model._default_manager.all()

        if max_level is True:
            max_level = 1

        if isinstance(max_level, int) and max_level is not False:
            qs = qs.filter(level__lte=max_level)

        if not include_root:
            qs = qs.exclude(level=0)

    return qs.order_by("tree_id", "lft")


def get_model_name(model):
    """
    Get the name of a Django model

    >>> get_model_name(Country)
    country
    """
    return model._meta.model_name
