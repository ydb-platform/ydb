from django.db import models
try:
    from django.db.models import JSONField
except ImportError:
    try:
        from jsonfield.fields import JSONField
    except ImportError:
        raise ImportError("Can't find a JSONField implementation, please install jsonfield if django < 4.0")



class History(models.Model):
    id=models.AutoField(primary_key=True)
    name=models.CharField(max_length=255,default="")
    table=models.CharField(max_length=255)
    primary_key=models.CharField(max_length=255)
    old_state=JSONField(default=dict)
    new_state=JSONField(default=dict)
    done_by=models.CharField(max_length=255)
    done_on=models.DateTimeField(auto_now_add=True)

    def __unicode__(self):
        return self.id

