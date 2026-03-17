#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from base64 import b64decode
from urllib.parse import quote_plus
from gerrit.changes.drafts import GerritChangeRevisionDrafts
from gerrit.changes.comments import GerritChangeRevisionComments
from gerrit.changes.files import GerritChangeRevisionFiles


class GerritChangeRevision:
    def __init__(self, gerrit, change, revision="current"):
        self.change = change
        self.revision = revision
        self.gerrit = gerrit
        self.endpoint = f"/changes/{self.change}/revisions/{self.revision}"

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__} {str(self)}>"

    def __str__(self):
        return str(self.revision)

    def get_commit(self):
        """
        Retrieves a parsed commit of a revision.

        :return:
        """
        result = self.gerrit.get(self.endpoint + "/commit")
        return result

    def get_description(self):
        """
        Retrieves the description of a patch set.
        If the patch set does not have a description an empty string is returned.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/description")

    def set_description(self, input_):
        """
        Sets the description of a patch set.

        .. code-block:: python

            input_ = {
                "description": "Added Documentation"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            revision = change.get_revision('3848807f587dbd3a7e61723bbfbf1ad13ad5a00a')
            result = revision.set_description(input_)

        :param input_: the DescriptionInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#description-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/description",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def get_merge_list(self):
        """
        Returns the list of commits that are being integrated into a target branch by a merge
        commit. By default, the first parent is assumed to be uninteresting. By using the parent
        option another parent can be set as uninteresting (parents are 1-based).

        :return:
        """
        result = self.gerrit.get(self.endpoint + "/mergelist")
        return result

    def get_revision_actions(self):
        """
        Retrieves revision actions of the revision of a change.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/actions")

    def get_review(self):
        """
        Retrieves a review of a revision.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/review")

    def get_related_changes(self):
        """
        Retrieves related changes of a revision. Related changes are changes that either depend on,
        or are dependencies of the revision.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/related")

    def set_review(self, input_):
        """
        Sets a review on a revision, optionally also publishing draft comments, setting labels,
        adding reviewers or CCs, and modifying the work in progress property.
        A review cannot be set on a change edit. Trying to post a review for a change edit fails
        with 409 Conflict.

        .. code-block:: python

            input_ = {
                "tag": "jenkins",
                "message": "Some nits need to be fixed.",
                "labels": {
                    "Code-Review": -1
                },
                "comments": {
                      "sonarqube/cloud/project_badges.py": [
                            {
                                "line": 23,
                                "message": "[nit] trailing whitespace"
                            },
                            {
                                "line": 49,
                                "message": "[nit] s/conrtol/control"
                            },
                            {
                                "range": {
                                    "start_line": 50,
                                    "start_character": 0,
                                    "end_line": 55,
                                    "end_character": 20
                                },
                                "message": "Incorrect indentation"
                            }
                      ]
                }
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            revision = change.get_revision('3848807f587dbd3a7e61723bbfbf1ad13ad5a00a')
            result = revision.set_review(input_)

        :param input_: the ReviewInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#review-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/review", json=input_, headers=self.gerrit.default_headers
        )

    def rebase(self, input_):
        """
        Rebases a revision.
        Optionally, the parent revision can be changed to another patch set through the RebaseInput
        entity.

        .. code-block:: python

            input_ = {
                "base" : "1234"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            revision = change.get_revision('3848807f587dbd3a7e61723bbfbf1ad13ad5a00a')
            result = revision.rebase(input_)

        :param input_: the RebaseInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#rebase-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/rebase", json=input_, headers=self.gerrit.default_headers
        )

    def submit(self):
        """
        Submits a revision.
        If the revision cannot be submitted, e.g. because the submit rule doesn’t allow submitting
        the revision or the revision is not the current revision, the response is 409 Conflict and
        the error message is contained in the response body.

        :return:
        """
        return self.gerrit.post(self.endpoint + "/submit")

    def get_patch(self, zip_=False, download=False, path=None, decode=False):
        """
        Gets the formatted patch for one revision.
        The formatted patch is returned as text encoded inside base64 if decode is False.

        Adding query parameter zip (for example /changes/.../patch?zip) returns the patch as a
        single file inside of a ZIP archive. Clients can expand the ZIP to obtain the plain text
        patch, avoiding the need for a base64 decoding step. This option implies download.

        Query parameter download (e.g. /changes/.../patch?download) will suggest the browser
        save the patch as commitsha1.diff.base64, for later processing by command line tools.

        If the path parameter is set, the returned content is a diff of the single file that
        the path refers to.

        :param zip_:
        :param download:
        :param path:
        :param decode: Decode bas64 to plain text.
        :return:
        """
        endpoint = self.endpoint + "/patch"

        if zip_:
            endpoint += endpoint + "?zip"

        if download:
            endpoint += endpoint + "?download"

        if path:
            endpoint += endpoint + f"?path={quote_plus(path)}"

        result = self.gerrit.get(endpoint)
        if decode:
            return b64decode(result).decode("utf-8")
        return result

    def submit_preview(self):
        """
        need fix bug
        Gets a file containing thin bundles of all modified projects if this change was submitted.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/preview_submit")

    def is_mergeable(self):
        """
        Gets the method the server will use to submit (merge) the change and an indicator
        if the change is currently mergeable.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/mergeable")

    def get_submit_type(self):
        """
        Gets the method the server will use to submit (merge) the change.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/submit_type")

    def test_submit_type(self, input_):
        """
        Tests the submit_type Prolog rule in the project, or the one given.

        :param input_: the Prolog code
        :type: str
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/test.submit_type",
            data=input_,
            headers={"Content-Type": "plain/text"},
        )

    def test_submit_rule(self, input_):
        """
        Tests the submit_rule Prolog rule in the project, or the one given.

        :param input_: the Prolog code
        :type: str
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/test.submit_rule",
            data=input_,
            headers={"Content-Type": "plain/text"},
        )

    @property
    def drafts(self):
        return GerritChangeRevisionDrafts(
            change=self.change, revision=self.revision, gerrit=self.gerrit
        )

    @property
    def comments(self):
        return GerritChangeRevisionComments(
            change=self.change, revision=self.revision, gerrit=self.gerrit
        )

    def list_robot_comments(self):
        """
        Lists the robot comments of a revision.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/robotcomments")

    def get_robot_comment(self, commit_id):
        """
        Retrieves a robot comment of a revision.

        :param commit_id:
        :return:
        """
        return self.gerrit.get(self.endpoint + f"/robotcomments/{commit_id}")

    @property
    def files(self):
        return GerritChangeRevisionFiles(
            change=self.change, revision=self.revision, gerrit=self.gerrit
        )

    def cherry_pick(self, input_):
        """
        Cherry picks a revision to a destination branch.

        .. code-block:: python

            input_ = {
                "message" : "Implementing Feature X",
                "destination" : "release-branch"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            revision = change.get_revision('3848807f587dbd3a7e61723bbfbf1ad13ad5a00a')
            result = revision.cherry_pick(input_)

        :param input_: the CherryPickInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-projects.html#cherry-pick-commit
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/cherrypick",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def list_reviewers(self):
        """
        Lists the reviewers of a revision.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/reviewers")

    def list_votes(self, account):
        """
        Lists the votes for a specific reviewer of the revision.

        :param account: account id or username
        :return:
        """
        return self.gerrit.get(self.endpoint + f"/reviewers/{account}/votes/")

    def delete_vote(self, account, label, input_=None):
        """
        Deletes a single vote from a revision. The deletion will be possible only
        if the revision is the current revision. By using this endpoint you can prevent
        deleting the vote (with same label) from a newer patch set by mistake.
        Note, that even when the last vote of a reviewer is removed the reviewer itself is
        still listed on the change.

        .. code-block:: python

            input_ = {
                "notify": "NONE"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            revision = change.get_revision("0f4f97b5af9a965e082fb8cde082c5f1ba2fe930")
            revision.delete_vote('John', 'Code-Review', input_)
            # or
            revision.delete_vote('John', 'Code-Review')

        :param account:
        :param label:
        :param input_: the DeleteVoteInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#delete-vote-input
        :return:
        """
        endpoint = self.endpoint + f"/reviewers/{account}/votes/{label}"
        if input_ is None:
            self.gerrit.delete(endpoint)
        else:
            endpoint += "/delete"
            self.gerrit.post(endpoint, json=input_, headers=self.gerrit.default_headers)
