from tortoise import Model, fields

from aerich.coder import decoder, encoder

MAX_VERSION_LENGTH = 255
MAX_APP_LENGTH = 100


class Aerich(Model):
    version = fields.CharField(max_length=MAX_VERSION_LENGTH)
    app = fields.CharField(max_length=MAX_APP_LENGTH)
    content: dict = fields.JSONField(encoder=encoder, decoder=decoder)

    class Meta:
        ordering = ["-id"]
