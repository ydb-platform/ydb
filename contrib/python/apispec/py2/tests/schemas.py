from marshmallow import Schema, fields


class PetSchema(Schema):
    description = dict(id='Pet id', name='Pet name')
    id = fields.Int(dump_only=True, description=description['id'])
    name = fields.Str(description=description['name'], required=True, deprecated=False, allowEmptyValue=False)


class SampleSchema(Schema):
    runs = fields.Nested('RunSchema', many=True, exclude=('sample',))

    count = fields.Int()


class RunSchema(Schema):
    sample = fields.Nested(SampleSchema, exclude=('runs',))


class AnalysisSchema(Schema):
    sample = fields.Nested(SampleSchema)


class AnalysisWithListSchema(Schema):
    samples = fields.List(fields.Nested(SampleSchema))


class PatternedObjectSchema(Schema):
    count = fields.Int(dump_only=True, **{'x-count': 1})
    count2 = fields.Int(dump_only=True, x_count2=2)


class SelfReferencingSchema(Schema):
    id = fields.Int()
    single = fields.Nested('self')
    single_with_ref_v2 = fields.Nested('self', ref='#/definitions/Self')
    single_with_ref_v3 = fields.Nested('self', ref='#/components/schemas/Self')
    many = fields.Nested('self', many=True)
    many_with_ref_v2 = fields.Nested('self', many=True, ref='#/definitions/Selves')
    many_with_ref_v3 = fields.Nested('self', many=True, ref='#/components/schemas/Selves')


class OrderedSchema(Schema):
    field1 = fields.Int()
    field2 = fields.Int()
    field3 = fields.Int()
    field4 = fields.Int()
    field5 = fields.Int()

    class Meta:
        ordered = True


class DefaultCallableSchema(Schema):
    numbers = fields.List(fields.Int, missing=list)
