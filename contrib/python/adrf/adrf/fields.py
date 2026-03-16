from rest_framework.serializers import SerializerMethodField as DRFSerializerMethodField


class SerializerMethodField(DRFSerializerMethodField):
    async def ato_representation(self, attribute):
        method = getattr(self.parent, self.method_name)
        return await method(attribute)
