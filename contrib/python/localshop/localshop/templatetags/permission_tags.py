from django import template

register = template.Library()


@register.filter
def is_owner_of(user, repository):
    return repository.check_user_role(user, ['owner'])
