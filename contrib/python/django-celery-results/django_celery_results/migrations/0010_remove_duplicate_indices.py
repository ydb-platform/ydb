"""
Migration to amend the 0009 migration released on django_celery_results 2.1.0

That migration introduced duplicate indexes breaking Oracle support.
This migration will remove those indexes (on non-Oracle db's)
while in-place changing migration 0009
to not add the duplicates for new installs
"""

from django.db import DatabaseError, migrations


class TryRemoveIndex(migrations.RemoveIndex):
    """Operation to remove the Index
    without reintroducing it on reverting the migration
    """

    def database_forwards(self, *args, **kwargs):
        """Remove the index on the database if it exists"""
        try:
            super().database_forwards(*args, **kwargs)
        except DatabaseError:
            pass
        except Exception:
            # Not all DB engines throw DatabaseError when the
            #   index does not exist.
            pass

    def database_backwards(self, *args, **kwargs):
        """Don't re-add the index when reverting this migration"""
        pass


class Migration(migrations.Migration):

    dependencies = [
        ('django_celery_results', '0009_groupresult'),
    ]

    operations = [
        TryRemoveIndex(
            model_name='chordcounter',
            name='django_cele_group_i_299b0d_idx',
        ),
        TryRemoveIndex(
            model_name='groupresult',
            name='django_cele_group_i_3cddec_idx',
        ),
        TryRemoveIndex(
            model_name='taskresult',
            name='django_cele_task_id_7f8fca_idx',
        ),
    ]
