from django.db.models.fields import json


def key_transform_as_clickhouse(self, compiler, connection):
    lhs, params, key_transforms = self.preprocess_lhs(compiler, connection)
    sql = lhs
    params = list(params)
    for key in key_transforms:
        if key.isdigit():
            sql = f"{sql}[%s]"
            params.append(int(key) + 1)
        else:
            sql = f"tupleElement({sql}, %s)"
            params.append(key)
    return sql, tuple(params)


def patch_jsonfield():
    json.KeyTransform.as_clickhouse = key_transform_as_clickhouse
