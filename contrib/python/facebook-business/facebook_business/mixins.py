# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

"""
mixins contains attributes that objects share
"""

from facebook_business.exceptions import FacebookBadObjectError

# @deprecated CanValidate is being deprecated
class CanValidate(object):
    """
    An instance of CanValidate will allow the ad objects
    to call remote_validate() to verify if its parameters are valid
    """
    def remote_validate(self, params=None):
        params = params or {}
        data_cache = dict(self._data)
        changes_cache = dict(self._changes)
        params['execution_options'] = ['validate_only']
        self.save(params=params)
        self._data = data_cache
        self._changes = changes_cache
        return self


# @deprecated CanArchive is being deprecated
class CanArchive(object):

    """
    An instance of CanArchive will allow the ad objects
    to call remote_delete() to be deleted using a POST request against
    the object's status field.
    """
    def remote_delete(
        self,
        batch=None,
        failure=None,
        success=None
    ):
        return self.remote_update(
            params={
                'status': self.Status.deleted,
            },
            batch=batch,
            failure=failure,
            success=success,
        )

    """
    An instance of CanArchive will allow the ad objects
    to call remote_archive() to be archived
    """
    def remote_archive(
        self,
        batch=None,
        failure=None,
        success=None
    ):
        return self.remote_update(
            params={
                'status': self.Status.archived,
            },
            batch=batch,
            failure=failure,
            success=success,
        )


# @deprecated CannotCreate is being deprecated
class CannotCreate(object):

    """
    An instance of CannotCreate will raise a TypeError when calling
    remote_create().
    """

    @classmethod
    def remote_create(cls, *args, **kwargs):
        raise TypeError('Cannot create object of type %s.' % cls.__name__)

# @deprecated CannotDelete is being deprecated
class CannotDelete(object):

    """
    An instance of CannotDelete will raise a TypeError when calling
    remote_delete().
    """

    @classmethod
    def remote_delete(cls, *args, **kwargs):
        raise TypeError('Cannot delete object of type %s.' % cls.__name__)

# @deprecated CannotUpdate is being deprecated
class CannotUpdate(object):

    """
    An instance of CannotUpdate will raise a TypeError when calling
    remote_update().
    """

    @classmethod
    def remote_update(cls, *args, **kwargs):
        raise TypeError('Cannot update object of type %s.' % cls.__name__)

# @deprecated HasObjective is being deprecated
class HasObjective(object):

    """
    An instance of HasObjective will have an enum attribute Objective.
    """

    class Objective(object):
        app_installs = 'APP_INSTALLS'
        brand_awareness = 'BRAND_AWARENESS'
        conversions = 'CONVERSIONS'
        event_responses = 'EVENT_RESPONSES'
        lead_generation = 'LEAD_GENERATION'
        link_clicks = 'LINK_CLICKS'
        local_awareness = 'LOCAL_AWARENESS'
        messages = 'MESSAGES'
        offer_claims = 'OFFER_CLAIMS'
        outcome_app_promotion = 'OUTCOME_APP_PROMOTION'
        outcome_awareness = 'OUTCOME_AWARENESS'
        outcome_engagement = 'OUTCOME_ENGAGEMENT'
        outcome_leads = 'OUTCOME_LEADS'
        outcome_sales = 'OUTCOME_SALES'
        outcome_traffic = 'OUTCOME_TRAFFIC'
        page_likes = 'PAGE_LIKES'
        post_engagement = 'POST_ENGAGEMENT'
        product_catalog_sales = 'PRODUCT_CATALOG_SALES'
        reach = 'REACH'
        store_visits = 'STORE_VISITS'
        video_views = 'VIDEO_VIEWS'

# @deprecated HasStatus is being deprecated
class HasStatus(object):

    """
    An instance of HasStatus will have an enum attribute Status.
    """

    class Status(object):
        active = 'ACTIVE'
        archived = 'ARCHIVED'
        deleted = 'DELETED'
        paused = 'PAUSED'

# @deprecated HasBidInfo is being deprecated
class HasBidInfo(object):

    """
    An instance of HasBidInfo will have an enum attribute BidInfo.
    """

    class BidInfo(object):
        actions = 'ACTIONS'
        clicks = 'CLICKS'
        impressions = 'IMPRESSIONS'
        reach = 'REACH'
        social = 'SOCIAL'

class HasAdLabels(object):

    def add_labels(self, labels=None):
        """Adds labels to an ad object.
        Args:
            labels: A list of ad label IDs
        Returns:
            The FacebookResponse object.
        """
        return self.get_api_assured().call(
            'POST',
            (self.get_id_assured(), 'adlabels'),
            params={'adlabels': [{'id': label} for label in labels]},
        )

    def remove_labels(self, labels=None):
        """Remove labels to an ad object.
        Args:
            labels: A list of ad label IDs
        Returns:
            The FacebookResponse object.
        """
        return self.get_api_assured().call(
            'DELETE',
            (self.get_id_assured(), 'adlabels'),
            params={'adlabels': [{'id': label} for label in labels]},
        )

class ValidatesFields(object):
    def __setitem__(self, key, value):
        if key not in self.Field.__dict__:
            raise FacebookBadObjectError(
                "\"%s\" is not a valid field of %s"
                % (key, self.__class__.__name__)
            )
        else:
            super(ValidatesFields, self).__setitem__(key, value)
