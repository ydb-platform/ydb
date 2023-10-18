import copy
from google.protobuf.descriptor import FieldDescriptor as fdescriptor

"""Recursive tree traversals for protobuf. Each message
   is node, each field is leaf. Function walks through
   proto and in each node do smth."""


def search(proto, fname=None, ftype=None):
    for desc, obj in proto.ListFields():
        if desc.name == fname and (ftype is None or ftype == desc.type):
            return (obj, desc, proto)
        if desc.type == fdescriptor.TYPE_MESSAGE:
            objs = obj if desc.label == fdescriptor.LABEL_REPEATED else [obj]
            for one_obj in objs:
                return search(one_obj, fname, ftype)
    return None


def search_and_process(proto, return_func=lambda params, child_values=None: params,
                       recalc_params_func=lambda proto, obj, desc, params: params,
                       params=None):
    """Search and process each node. Recalc params on each step. Pass it down
    the tree. On each leaf calcs return value from param, and pass it up. Nodes
    calc return value with current param and childs return values.

    Args:
      * proto -- current node. to run through some proto, put its object here
      * return_func -- function that return value. takes current (recalced for current
      *                node) param and list of return values for current node children.
      *                for leafs second parametr is None
      * recalc_params_func -- function to recalc params in node. takes root proto,
      *                       current object (or objects for repeated fields), current
      *                       proto descriptor and param. return new param value
      * params -- initial values for params"""
    if proto is None:
        return None

    return_values = []
    for desc, obj in proto.ListFields():
        params = copy.deepcopy(params)
        if desc.type == fdescriptor.TYPE_MESSAGE:
            objs = obj if desc.label == fdescriptor.LABEL_REPEATED else [obj]
            params = recalc_params_func(proto, obj, desc, params)
            for one_obj in objs:
                return_values.append(search_and_process(one_obj, return_func,
                                                        recalc_params_func, params))
        else:
            return_values.append(return_func(recalc_params_func(proto, obj, desc, params), None))
    return return_func(params, return_values)


def search_and_process_descriptors(proto_desc,
                                   return_func=lambda params, child_values=None: params,
                                   recalc_params_func=lambda desc, params: params,
                                   params=None):
    """Same as search and process(except we run recalc_params in root_proto too),
    but process each node from PROTOBUF DESCRIPTIO, instead of each node from
    protobuf message."""
    params = copy.deepcopy(params)
    params = recalc_params_func(proto_desc, params)

    if proto_desc is None:
        return None
    elif hasattr(proto_desc, "type") and proto_desc.type != fdescriptor.TYPE_MESSAGE:
        return return_func(params, None)

    return_values = []
    for field_desc in proto_desc.fields:
        desc = field_desc if field_desc.message_type is None else field_desc.message_type
        return_values.append(search_and_process_descriptors(desc, return_func,
                                                            recalc_params_func, params))

    return return_func(params, return_values)
