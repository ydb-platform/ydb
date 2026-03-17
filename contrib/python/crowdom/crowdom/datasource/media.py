from dataclasses import dataclass, field
import io
import logging
from multiprocessing.pool import ThreadPool
import os
from typing import List, Optional, Tuple, Any

import boto3
from botocore.config import Config
import toloka.client as toloka

from .. import base, mapping

logger = logging.getLogger(__name__)


@dataclass
class Attachment:
    id: str
    need_upload: bool
    meta: Optional[toloka.Attachment] = None


@dataclass
class S3:
    endpoint: str
    bucket: str
    path: str

    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None

    base_url: str = field(init=False)
    client: Any = field(init=False)

    def __post_init__(self):
        endpoint_url = f'https://{self.endpoint}'
        self.base_url = f'{endpoint_url}/{self.bucket}/{self.path}'
        self.client = boto3.session.Session().client(
            service_name='s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=self.access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=self.secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            config=Config(retries=dict(max_attempts=30)),
        )

    def get_url(self, attachment: Attachment) -> str:
        assert attachment.meta
        return f'{self.base_url}/{attachment.meta.name}'

    def upload(self, attachment: Attachment, body: bytes):
        assert attachment.meta
        self.client.put_object(
            Bucket=self.bucket,
            Key=f'{self.path}/{attachment.meta.name}',
            Body=body,
            ACL='public-read',
            ContentType=attachment.meta.media_type,
        )


# replace Toloka attachments with user-configured S3 urls
def substitute_media_output(
    assignments: List[mapping.AssignmentSolutions],
    s3: S3,
    toloka_client: toloka.TolokaClient,
) -> List[mapping.AssignmentSolutions]:
    attachments = _find_or_substitute_media(assignments, None, s3)[1]
    attachments = _get_media_meta(toloka_client, attachments)
    _upload_media(s3, toloka_client, attachments)
    return _find_or_substitute_media(assignments, attachments, s3)[0]


def has_media_output(task_function: base.TaskFunction) -> bool:
    return any(obj_meta.type.is_media() for obj_meta in task_function.get_outputs())


def _find_or_substitute_media(
    assignments: List[mapping.AssignmentSolutions],
    uploaded_attachments: Optional[List[Attachment]],
    s3: S3,
) -> Tuple[List[mapping.AssignmentSolutions], List[Attachment]]:
    new_assignments, found_attachments = [], []
    id_to_uploaded_attachment = (
        {attachment.id: attachment for attachment in uploaded_attachments} if uploaded_attachments else None
    )
    for assignment, solutions in assignments:
        new_solutions = []
        for input_objects, output_objects in solutions:
            new_output_objects = []
            for obj in output_objects:
                if obj and obj.is_media():
                    attachment = Attachment(id=obj.url, need_upload=assignment.status == toloka.Assignment.SUBMITTED)
                    found_attachments.append(attachment)
                    if id_to_uploaded_attachment:
                        obj = type(obj)(url=s3.get_url(id_to_uploaded_attachment[attachment.id]))
                new_output_objects.append(obj)
            new_solutions.append((input_objects, tuple(new_output_objects)))
        new_assignments.append((assignment, new_solutions))
    return new_assignments, found_attachments


def _get_media_meta(toloka_client: toloka.TolokaClient, attachments: List[Attachment]) -> List[Attachment]:
    if not attachments:
        return []
    return ThreadPool(50).map(
        lambda attachment: Attachment(
            id=attachment.id, meta=toloka_client.get_attachment(attachment.id), need_upload=attachment.need_upload
        ),
        attachments,
    )


def _upload_media(s3: S3, toloka_client: toloka.TolokaClient, attachments: List[Attachment]):
    attachments = [attachment for attachment in attachments if attachment.need_upload]
    if not attachments:
        return

    def upload(attachment: Attachment) -> int:
        with io.BytesIO() as f:
            toloka_client.download_attachment(attachment_id=attachment.id, out=f)
            f.flush()
            size = f.tell()
            f.seek(0)
            s3.upload(attachment, f)
            return size

    # TODO (TOLOKA-17294): OOM danger
    # Threads count should be determined by available RAM and biggest attachments. Unfortunately, Toloka does not
    # provide information about attachment size in its API. For now, let's assume 2Gb available RAM and 50Mb attachment
    # size.
    threads = 40
    logger.debug(f'uploading {len(attachments)} attachments with {threads} threads')
    sizes = ThreadPool(threads).map(upload, attachments)
    logger.debug(f'{sum(sizes)} bytes were uploaded')
