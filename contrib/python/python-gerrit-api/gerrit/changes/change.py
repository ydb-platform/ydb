#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from typing import Dict
from gerrit.utils.gerritbase import GerritBase
from gerrit.changes.reviewers import GerritChangeReviewers
from gerrit.changes.revision import GerritChangeRevision
from gerrit.changes.edit import GerritChangeEdit
from gerrit.changes.messages import GerritChangeMessages
from gerrit.utils.exceptions import ChangeEditNotFoundError


class GerritChange(GerritBase):
    def __init__(self, id: str, gerrit):
        self.id = id
        self.gerrit = gerrit
        self.endpoint = f"/changes/{self.id}"
        super().__init__(self)

        self.revisions: Dict[str, str] = {}
        self.current_revision_number = 0

    def __str__(self):
        return self.id

    def get_detail(self, options=None):
        """
        retrieve a change with labels, detailed labels, detailed accounts, reviewer updates, and messages.

        :param options: List of options to fetch additional data about a change
        :return:
        """
        if options:
            params = {"o": options}
        else:
            params = None
        return self.gerrit.get(self.endpoint + "/detail", params=params)

    def get_meta_diff(self, old=None, meta=None):
        """
        Retrieves the difference between two historical states of a change by
        specifying the and the parameters. old=SHA-1,meta=SHA-1.
        If the parameter is not provided, the parent of the SHA-1 is used.
        If the parameter is not provided, the current state of the change is used.
        If neither are provided, the difference between the current state of the change and
        its previous state is returned.

        :param old:
        :param meta:
        :return:
        """
        params = {}
        if old:
            params.update({"old": old})

        if meta:
            params.update({"meta": meta})

        return self.gerrit.get(self.endpoint + "/meta_diff", params=params)

    def create_merge_patch_set(self, input_):
        """
        Update an existing change by using a MergePatchSetInput entity.
        Gerrit will create a merge commit based on the information of MergePatchSetInput and add
        a new patch set to the change corresponding to the new merge commit.

        .. code-block:: python

            input_ = {
                "subject": "Merge master into stable",
                "merge": {
                  "source": "refs/heads/master"
                }
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.update(input_)

        :param input_: the MergePatchSetInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#merge-patch-set-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/merge", json=input_, headers=self.gerrit.default_headers
        )

    def set_commit_message(self, input_):
        """
        Creates a new patch set with a new commit message.

        .. code-block:: python

            input_ = {
                "message": "New Commit message \\n\\nChange-Id: I10394472cbd17dd12454f22b143a444\\n"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.set_commit_message(input_)

        :param input_: the CommitMessageInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#commit-message-input
        :return:
        """
        return self.gerrit.put(
            self.endpoint + "/message", json=input_, headers=self.gerrit.default_headers
        )

    def list_votes(self, account):
        """
        Lists the votes for a specific reviewer of the change.

        :param account: account id or username
        :return:
        """
        return self.gerrit.get(self.endpoint + f"/reviewers/{account}/votes")

    def delete_vote(self, account, label, input_=None):
        """
        Deletes a single vote from a change. Note, that even when the last vote of a reviewer is
        removed the reviewer itself is still listed on the change.
        If another user removed a user’s vote, the user with the deleted vote will be added to
        the attention set.

        .. code-block:: python

            input_ = {
                "notify": "NONE"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            change.delete_vote('John', 'Code-Review', input_)
            # or
            change.delete_vote('John', 'Code-Review')

        :param account:
        :param label:
        :param input_: the DeleteVoteInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#delete-vote-input
        :return:
        """
        endpoint = f"{self.endpoint}/reviewers/{account}/votes/{label}"
        if input_ is None:
            self.gerrit.delete(endpoint)
        else:
            endpoint += "/delete"
            self.gerrit.post(endpoint, json=input_, headers=self.gerrit.default_headers)

    def get_topic(self):
        """
        Retrieves the topic of a change.

        :getter: Retrieves the topic of a change.
        :setter: Sets the topic of a change.
        :deleter: Deletes the topic of a change.

        :return:
        """
        return self.gerrit.get(f"{self.endpoint}/topic")

    def set_topic(self, topic):
        """
        Sets the topic of a change.

        :param topic: The new topic
        :return:
        """
        input_ = {"topic": topic}
        return self.gerrit.put(
            self.endpoint + "/topic", json=input_, headers=self.gerrit.default_headers
        )

    def delete_topic(self):
        """
        Deletes the topic of a change.

        :return:
        """
        self.gerrit.delete(f"{self.endpoint}/topic")

    def get_assignee(self):
        """
        Retrieves the account of the user assigned to a change.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/assignee")

    def set_assignee(self, input_):
        """
        Sets the assignee of a change.

        .. code-block:: python

            input_ = {
                "assignee": "jhon.doe"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.set_assignee(input_)

        :param input_: the AssigneeInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#assignee-input
        :return:
        """
        result = self.gerrit.put(
            self.endpoint + "/assignee",
            json=input_,
            headers=self.gerrit.default_headers,
        )
        return result

    def get_past_assignees(self):
        """
        Returns a list of every user ever assigned to a change, in the order in which they were
        first assigned.

        :return:
        """
        result = self.gerrit.get(self.endpoint + "/past_assignees")
        return result

    def delete_assignee(self):
        """
        Deletes the assignee of a change.

        :return:
        """
        self.gerrit.delete(self.endpoint + "/assignee")

    def get_pure_revert(self, commit):
        """
        Check if the given change is a pure revert of the change it references in revertOf.

        :param commit: commit id
        :return:
        """
        return self.gerrit.get(self.endpoint + f"/pure_revert?o={commit}")

    def abandon(self):
        """
        Abandons a change.
        Abandoning a change also removes all users from the attention set.
        If the change cannot be abandoned because the change state doesn't allow abandoning of
        the change, the response is “409 Conflict” and the error message is contained in the
        response body.

        :return:
        """
        return self.gerrit.post(self.endpoint + "/abandon")

    def restore(self):
        """
        Restores a change.
        If the change cannot be restored because the change state doesn't allow restoring the
        change, the response is “409 Conflict” and the error message is contained in the
        response body.

        :return:
        """
        return self.gerrit.post(self.endpoint + "/restore")

    def rebase(self, input_):
        """
        Rebase a change.
        If the change cannot be rebased, e.g. due to conflicts, the response is '409 Conflict'
        and the error message is contained in the response body.

        .. code-block:: python

            input_ = {
                "base" : "1234",
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.rebase(input_)

        :param input_: the RebaseInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#rebase-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/rebase", json=input_, headers=self.gerrit.default_headers
        )

    def move(self, input_):
        """
        Move a change.
        If the change cannot be moved because the change state doesn't allow moving the change,
        the response is '409 Conflict' and the error message is contained in the response body.

        .. code-block:: python

            input_ = {
                "destination_branch" : "release-branch"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.move(input_)

        :param input_: the MoveInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#move-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/move", json=input_, headers=self.gerrit.default_headers
        )

    def revert(self, input_=None):
        """
        Reverts a change.
        The request body does not need to include a RevertInput entity if no review comment is
        added.

        If the user doesn't have revert permission on the change or upload permission on the
        destination branch, the response is '403 Forbidden', and the error message is contained in
        the response body.

        If the change cannot be reverted because the change state doesn't allow reverting the
        change, the response is 409 Conflict and the error message is contained in the
        response body.

        .. code-block:: python

            input_ = {
                "message" : "Message to be added as review comment to the change when reverting the
                change."
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.revert()
            # or
            result = change.revert(input_)

        :param input_: the RevertInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#revert-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/revert",
            json=input_ or {},
            headers=self.gerrit.default_headers,
        )

    def revert_submission(self):
        """
        Creates open revert changes for all of the changes of a certain submission.

        If the user doesn't have revert permission on the change or upload permission on the
        destination, the response is '403 Forbidden', and the error message is contained in the
        response body.

        If the change cannot be reverted because the change state doesn't allow reverting the change
        the response is '409 Conflict', and the error message is contained in the response body.

        :return:
        """
        return self.gerrit.post(self.endpoint + "/revert_submission")

    def submit(self, input_=None):
        """
        Submits  a change.
        Submitting a change also removes all users from the attention set.

        If the change cannot be submitted because the submit rule doesn't allow submitting
        the change, the response is 409 Conflict and the error message is contained in the
        response body.

        .. code-block:: python

            input_ = {
                "on_behalf_of": 1001439
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.submit(input_)

        :param input_: the SubmitInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#submit-input
        :return:
        """
        endpoint = self.endpoint + "/submit"
        if input_ is None:
            result = self.gerrit.post(endpoint)
        else:
            result = self.gerrit.post(
                endpoint, json=input_, headers=self.gerrit.default_headers
            )
        return result

    def list_submitted_together_changes(self):
        """
        Computes list of all changes which are submitted when Submit is called for this change,
        including the current change itself.

        """
        return self.gerrit.get(
            self.endpoint + "/submitted_together?o=NON_VISIBLE_CHANGES"
        )

    def delete(self):
        """
        Deletes a change.

        :return:
        """
        self.gerrit.delete(self.endpoint)

    def get_include_in(self):
        """
        Retrieves the branches and tags in which a change is included.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/in")

    def index(self):
        """
        Adds or updates the change in the secondary index.

        :return:
        """
        self.gerrit.post(self.endpoint + "/index")

    def list_comments(self):
        """
        Lists the published comments of all revisions of the change.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/comments")

    def list_robot_comments(self):
        """
        Lists the robot comments of all revisions of the change.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/robotcomments")

    def list_drafts(self):
        """
        Lists the draft comments of all revisions of the change that belong to the calling user.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/drafts")

    def consistency_check(self):
        """
        Performs consistency checks on the change, and returns a ChangeInfo entity with the problems
        field set to a list of ProblemInfo entities.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/check")

    def fix(self, input_=None):
        """
        Performs consistency checks on the change as with GET /check,
        and additionally fixes any problems that can be fixed automatically. The returned field
        values reflect any fixes.
        Some fixes have options controlling their behavior, which can be set in the FixInput
        entity body. Only the change owner, a project owner, or an administrator may fix changes.

        .. code-block:: python

            input_ = {
                "delete_patch_set_if_commit_missing": "true",
                "expect_merged_as": "something"
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.fix()
            # or
            result = change.fix(input_)

        :param input_: the FixInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#fix-input
        :return:
        """
        endpoint = self.endpoint + "/check"
        if input_ is None:
            result = self.gerrit.post(endpoint)
        else:
            result = self.gerrit.post(
                endpoint, json=input_, headers=self.gerrit.default_headers
            )
        return result

    def set_work_in_progress(self, input_=None):
        """
        Marks the change as not ready for review yet.
        Changes may only be marked not ready by the owner, project owners or site administrators.
        Marking a change work in progress also removes all users from the attention set.

        The request body does not need to include a WorkInProgressInput entity if no review comment
        is added.

        .. code-block:: python

            input_ = {
                "message": "Refactoring needs to be done before we can proceed here."
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.set_work_in_progress(input_)
            # or
            result = change.set_work_in_progress()

        :param input_: the WorkInProgressInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#work-in-progress-input
        :return:
        """
        self.gerrit.post(
            self.endpoint + "/wip",
            json=input_ or {},
            headers=self.gerrit.default_headers,
        )

    def set_ready_for_review(self, input_):
        """
        Marks the change as ready for review (set WIP property to false).
        Changes may only be marked ready by the owner, project owners or site administrators.
        Marking a change ready for review also adds all of the reviewers of the change to the
        attention set.

        .. code-block:: python

            input_ = {
                'message': 'Refactoring is done.'
            }

            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            change.set_ready_for_review(input_)

        :param input_: the WorkInProgressInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#work-in-progress-input
        :return:
        """
        self.gerrit.post(
            self.endpoint + "/ready", json=input_, headers=self.gerrit.default_headers
        )

    def mark_private(self, input_):
        """
        Marks the change to be private. Only open changes can be marked private.
        Changes may only be marked private by the owner or site administrators.

        .. code-block:: python

            input_ = {
                "message": "After this security fix has been released we can make it public now."
            }
            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            change.mark_private(input_)

        :param input_: the PrivateInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#private-input
        :return:
        """
        self.gerrit.post(
            self.endpoint + "/private", json=input_, headers=self.gerrit.default_headers
        )

    def unmark_private(self, input_=None):
        """
        Marks the change to be non-private. Note users can only unmark own private changes.
        If the change was already not private, the response is '409 Conflict'.

        .. code-block:: python

            input_ = {
                "message": "This is a security fix that must not be public."
            }
            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            change.unmark_private(input_)
            # or
            change.unmark_private()

        :param input_: the PrivateInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#private-input
        :return:
        """
        if input_ is None:
            self.gerrit.delete(self.endpoint + "/private")
        else:
            self.gerrit.post(
                self.endpoint + "/private.delete",
                json=input_,
                headers=self.gerrit.default_headers,
            )

    def ignore(self):
        """
        Marks a change as ignored. The change will not be shown in the incoming reviews' dashboard,
        and email notifications will be suppressed. Ignoring a change does not cause the change’s
        "updated" timestamp to be modified, and the owner is not notified.

        :return:
        """
        self.gerrit.put(self.endpoint + "/ignore")

    def unignore(self):
        """
        Un-marks a change as ignored.

        :return:
        """
        self.gerrit.put(self.endpoint + "/unignore")

    def mark_as_reviewed(self):
        """
        Marks a change as reviewed.

        :return:
        """
        self.gerrit.put(self.endpoint + "/reviewed")

    def mark_as_unreviewed(self):
        """
        Marks a change as unreviewed.

        :return:
        """
        self.gerrit.put(self.endpoint + "/unreviewed")

    def get_hashtags(self):
        """
        Gets the hashtags associated with a change.

        :return:
        """
        return self.gerrit.get(self.endpoint + "/hashtags")

    def set_hashtags(self, input_):
        """
        Adds and/or removes hashtags from a change.

        .. code-block:: python

            input_ = {
                "add" : [
                    "hashtag3"
                ],
                "remove" : [
                    "hashtag2"
                ]
            }
            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.set_hashtags(input_)

        :param input_: the HashtagsInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#hashtags-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/hashtags",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    @property
    def messages(self):
        return GerritChangeMessages(change=self.id, gerrit=self.gerrit)

    def check_submit_requirement(self, input_):
        """
        Tests a submit requirement.

        :param input_: the SubmitRequirementInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#submit-requirement-input
        :return:
        """
        return self.gerrit.post(
            self.endpoint + "/check.submit_requirement",
            json=input_,
            headers=self.gerrit.default_headers,
        )

    def get_edit(self):
        """
        Retrieves a change edit details.
        As response an EditInfo entity is returned that describes the change edit,
        or 204 No Content when change edit doesn't exist for this change.

        :return:
        """
        result = self.gerrit.get(self.endpoint + "/edit")
        if not result:
            raise ChangeEditNotFoundError("Change edit does not exist")

        return GerritChangeEdit(change=self.id, gerrit=self.gerrit)

    def create_empty_edit(self):
        """
        Creates empty change edit

        :return:
        """
        self.gerrit.post(self.endpoint + "/edit")

    @property
    def reviewers(self):
        return GerritChangeReviewers(change=self.id, gerrit=self.gerrit)

    def __get_revisions(self):
        number = self.to_dict().get("_number")

        endpoint = f"/changes/?q={number}&o=ALL_REVISIONS"
        results = self.gerrit.get(endpoint)

        result = {}
        for item in results:
            if item.get("_number") == number:
                result = item
                break

        revisions = result.get("revisions")

        if revisions is not None:
            for revision_sha, revision in revisions.items():
                if result["current_revision"] == revision_sha:
                    self.current_revision_number = revision["_number"]

                self.revisions[revision["_number"]] = revision_sha

    def __revision_number_to_sha(self, number):
        if number in self.revisions:
            return self.revisions[number]
        return None

    def get_revision(self, revision_id="current"):
        """
        Get one revision by revision SHA or integer number.

        :param revision_id: Optional ID. If not specified, the current revision will be retrieved.
                            It supports SHA IDs and integer numbers from -X to +X, where X is the
                            current (latest) revision.
                            Zero means current revision.
                            -N means the current revision number X minus N, so if the current
                            revision is 50, and -1 is given, the revision 49 will be retrieved.
        :return:
        """
        if isinstance(revision_id, int):
            if len(self.revisions.keys()) == 0:
                self.__get_revisions()
            if revision_id <= 0:
                revision_id = self.current_revision_number + revision_id
            revision_id = self.__revision_number_to_sha(revision_id)
            if revision_id is None:
                return None

        return GerritChangeRevision(
            gerrit=self.gerrit, change=self.id, revision=revision_id
        )

    def get_attention_set(self):
        """
        Returns all users that are currently in the attention set.
        support this method since v3.3.0

        :return:
        """
        return self.gerrit.get(f"{self.endpoint}/attention")

    def add_to_attention_set(self, input_):
        """
        Adds a single user to the attention set of a change.
        support this method since v3.3.0

        A user can only be added if they are not in the attention set.
        If a user is added while already in the attention set, the request is silently ignored.

        .. code-block:: python

            input_ = {
                "user": "John Doe",
                "reason": "reason"
            }
            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            result = change.add_to_attention_set(input_)

        :param input_: the AttentionSetInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#attention-set-input
        :return:
        """
        result = self.gerrit.post(
            self.endpoint + "/attention",
            json=input_,
            headers=self.gerrit.default_headers,
        )
        return result

    def remove_from_attention_set(self, id_, input_=None):
        """
        Deletes a single user from the attention set of a change.
        support this method since v3.3.0

        A user can only be removed from the attention set.
        if they are currently in the attention set. Otherwise, the request is silently ignored.

        .. code-block:: python

            input_ = {
                "reason": "reason"
            }
            change = client.changes.get('Project~stable~I10394472cbd17dd12454f229e4f6de00b143a444')
            change.remove_from_attention_set('kevin.shi', input_)
            # or
            change.remove_from_attention_set('kevin.shi')

        :param id_: account id
        :param input_: the AttentionSetInput entity,
          https://gerrit-review.googlesource.com/Documentation/rest-api-changes.html#attention-set-input
        :return:
        """
        endpoint = self.endpoint + f"/attention/{id_}"
        if input_ is None:
            self.gerrit.delete(endpoint)
        else:
            endpoint += "/delete"
            self.gerrit.post(endpoint, json=input_, headers=self.gerrit.default_headers)
