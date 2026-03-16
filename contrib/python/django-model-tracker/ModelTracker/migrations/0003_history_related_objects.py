# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations



class Migration(migrations.Migration):

    dependencies = [
        ('ModelTracker', '0002_auto_20160929_0517'),
    ]

    # state_operations = [
    #     migrations.AddField(
    #         model_name='history',
    #         name='related_objects',
    #         field=jsonfield.fields.JSONField(null=True),
    #     ),
    #     ]
    #
    # db_operations= [migrations.RunSQL("""ALTER TABLE `ModelTracker_history` ADD COLUMN `related_objects` JSON;""")]

    operations = [
#            migrations.SeparateDatabaseAndState(state_operations=state_operations,database_operations=db_operations)
        ]
