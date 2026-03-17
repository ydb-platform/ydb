from .endpoint import Endpoint, api
from .. import JobItem, BackgroundJobItem, PaginationItem
from ..request_options import RequestOptionsBase

import logging

try:
    basestring
except NameError:
    # In case we are in python 3 the string check is different
    basestring = str

logger = logging.getLogger('tableau.endpoint.jobs')


class Jobs(Endpoint):
    @property
    def baseurl(self):
        return "{0}/sites/{1}/jobs".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    @api(version='2.6')
    def get(self, job_id=None, req_options=None):
        # Backwards Compatibility fix until we rev the major version
        if job_id is not None and isinstance(job_id, basestring):
            import warnings
            warnings.warn("Jobs.get(job_id) is deprecated, update code to use Jobs.get_by_id(job_id)")
            return self.get_by_id(job_id)
        if isinstance(job_id, RequestOptionsBase):
            req_options = job_id

        self.parent_srv.assert_at_least_version('3.1')
        server_response = self.get_request(self.baseurl, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        jobs = BackgroundJobItem.from_response(server_response.content, self.parent_srv.namespace)
        return jobs, pagination_item

    @api(version='3.1')
    def cancel(self, job_id):
        id_ = getattr(job_id, 'id', job_id)
        url = '{0}/{1}'.format(self.baseurl, id_)
        return self.put_request(url)

    @api(version='2.6')
    def get_by_id(self, job_id):
        logger.info('Query for information about job ' + job_id)
        url = "{0}/{1}".format(self.baseurl, job_id)
        server_response = self.get_request(url)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job
