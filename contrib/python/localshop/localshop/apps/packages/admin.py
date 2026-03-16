from django.contrib import admin

from localshop.apps.packages import models


class ReleaseFileInline(admin.TabularInline):
    model = models.ReleaseFile


@admin.register(models.Classifier)
class ClassifierAdmin(admin.ModelAdmin):

    list_display = (
        'name',
    )


@admin.register(models.Repository)
class RepositoryAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'teams',
    )

    list_display = (
        'name',
        'slug',
    )


@admin.register(models.Package)
class PackageAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'repository',
        'owners',
    )

    readonly_fields = (
        'name',
    )

    list_display = (
        'repository',
        'name',
        'created',
        'modified',
        'is_local',
        'is_trusted',
    )

    list_filter = (
        'is_local',
        'repository',
    )

    search_fields = (
        'name',
    )


@admin.register(models.Release)
class ReleaseAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'classifiers',
        'package',
        'user',
    )

    inlines = (
        ReleaseFileInline,
    )

    list_display = (
        'version',
        'package',
        'created',
        'modified',
    )

    list_filter = (
        'package__repository',
        'package',
    )

    search_fields = (
        'version',
        'package__name',
    )

    ordering = (
        '-created',
        'version',
    )


@admin.register(models.ReleaseFile)
class ReleaseFileAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'release',
        'user',
    )

    list_filter = (
        'user',
        'release__package__repository',
    )

    list_display = (
        'filename',
        'created',
        'modified',
        'upstream_pypi_upload_time',
        'md5_digest',
        'url',
    )
