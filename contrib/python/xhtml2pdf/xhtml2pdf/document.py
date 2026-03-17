# Copyright 2010 Dirk Holtwick, holtwick.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import logging
from html import escape as html_escape

from reportlab.lib import pdfencrypt
from reportlab.platypus.flowables import Spacer
from reportlab.platypus.frames import Frame

from xhtml2pdf.builders.signs import PDFSignature
from xhtml2pdf.builders.watermarks import WaterMarks
from xhtml2pdf.context import pisaContext
from xhtml2pdf.default import DEFAULT_CSS
from xhtml2pdf.files import cleanFiles, pisaTempFile
from xhtml2pdf.parser import pisaParser
from xhtml2pdf.util import getBox
from xhtml2pdf.xhtml2pdf_reportlab import PmlBaseDoc, PmlPageTemplate

log = logging.getLogger(__name__)


def pisaErrorDocument(dest, c):
    out = pisaTempFile(capacity=c.capacity)
    out.write(
        "<p style='background-color:red;'><strong>%d error(s) occurred:</strong><p>"
        % c.err
    )
    for mode, line, msg, _ in c.log:
        if mode == "error":
            out.write("<pre>%s in line %d: %s</pre>" % (mode, line, html_escape(msg)))

    out.write("<p><strong>%d warning(s) occurred:</strong><p>" % c.warn)
    for mode, line, msg, _ in c.log:
        if mode == "warning":
            out.write("<p>%s in line %d: %s</p>" % (mode, line, html_escape(msg)))

    return pisaDocument(out.getvalue(), dest, raise_exception=False)


def pisaStory(
    src,
    path="",
    link_callback=None,
    debug=0,
    default_css=None,
    xhtml=False,  # noqa: FBT002
    encoding=None,
    context=None,
    xml_output=None,
    **_kwargs,
):
    # Prepare Context
    if not context:
        context = pisaContext(path, debug=debug)
        context.pathCallback = link_callback

    # Use a default set of CSS definitions to get an expected output
    if default_css is None:
        default_css = DEFAULT_CSS

    # Parse and fill the story
    pisaParser(src, context, default_css, xhtml, encoding, xml_output)

    # Avoid empty documents
    if not context.story:
        context.story = [Spacer(1, 1)]

    if context.indexing_story:
        context.story.append(context.indexing_story)

    # Remove anchors if they do not exist (because of a bug in Reportlab)
    for frag, anchor in context.anchorFrag:
        if anchor not in context.anchorName:
            frag.link = None
    return context


def get_encrypt_instance(data):
    if data is None:
        return None

    if isinstance(data, str):
        return pdfencrypt.StandardEncryption(data)

    return data


def pisaDocument(
    src,
    dest=None,
    dest_bytes=False,  # noqa: FBT002
    path="",
    link_callback=None,
    debug=0,
    default_css=None,
    xhtml=False,  # noqa: FBT002
    encoding=None,
    xml_output=None,
    raise_exception=True,  # noqa: FBT002, ARG001
    capacity=100 * 1024,
    context_meta=None,
    encrypt=None,
    signature=None,
    **_kwargs,
):
    log.debug(
        "pisaDocument options:\n  src = %r\n  dest = %r\n  path = %r\n  link_callback ="
        " %r\n  xhtml = %r\n  context_meta = %r",
        src,
        dest,
        path,
        link_callback,
        xhtml,
        context_meta,
    )

    # Prepare simple context
    context = pisaContext(path, debug=debug, capacity=capacity)

    if context_meta is not None:
        context.meta.update(context_meta)

    context.pathCallback = link_callback

    # Build story
    context = pisaStory(
        src,
        path,
        link_callback,
        debug,
        default_css,
        xhtml,
        encoding,
        context=context,
        xml_output=xml_output,
    )

    # Buffer PDF into memory
    out = io.BytesIO()

    doc = PmlBaseDoc(
        out,
        pagesize=context.pageSize,
        author=context.meta["author"].strip(),
        subject=context.meta["subject"].strip(),
        keywords=[x.strip() for x in context.meta["keywords"].strip().split(",") if x],
        title=context.meta["title"].strip(),
        showBoundary=0,
        encrypt=get_encrypt_instance(encrypt),
        allowSplitting=1,
    )

    # Prepare templates and their frames
    if "body" in context.templateList:
        body = context.templateList["body"]
        del context.templateList["body"]
    else:
        x, y, w, h = getBox("1cm 1cm -1cm -1cm", context.pageSize)
        body = PmlPageTemplate(
            id="body",
            frames=[
                Frame(
                    x,
                    y,
                    w,
                    h,
                    id="body",
                    leftPadding=0,
                    rightPadding=0,
                    bottomPadding=0,
                    topPadding=0,
                )
            ],
            pagesize=context.pageSize,
        )

    doc.addPageTemplates([body, *list(context.templateList.values())])

    # Use multibuild e.g. if a TOC has to be created
    if context.multiBuild:
        doc.multiBuild(context.story)
    else:
        doc.build(context.story)

    # Add watermarks
    output = io.BytesIO()
    output, has_bg = WaterMarks.process_doc(context, out, output)

    if not has_bg:
        output = out
    if signature:
        signoutput = io.BytesIO()
        do_ok = PDFSignature.sign(output, signoutput, signature)
        if do_ok:
            output = signoutput

    # Get the resulting PDF and write it to the file object
    # passed from the caller

    # Get the resulting PDF and write it to the file object
    # passed from the caller

    if dest is None:
        # No output file was passed - Let's use a pisaTempFile
        dest = io.BytesIO()
    context.dest = dest

    data = output.getvalue()
    context.dest.write(data)  # TODO: context.dest is a tempfile as well...
    cleanFiles()

    if dest_bytes:
        return data

    return context
