# Copyright 2019, OpenTelemetry Authors
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

"""Cloud Trace Span Exporter for OpenTelemetry."""

import logging
from typing import Sequence, Dict, Any, List

from google.cloud import trace_v2
from google.cloud.trace_v2 import TraceServiceClient
import opentelemetry.trace as trace_api
from google.cloud.trace_v2.proto.trace_pb2 import AttributeValue
from opentelemetry.context import Context
from opentelemetry.sdk.trace import Event
from opentelemetry.sdk.trace.export import Span, SpanExporter, SpanExportResult
from opentelemetry.sdk.util import ns_to_iso_str
from opentelemetry.util import types

from opentelemetry.ext.cloud_trace.version import __version__

logger = logging.getLogger(__name__)

AGENT = "opentelemetry-python [{}]".format(__version__)
# Max length is 128 bytes for a truncatable string.
MAX_LENGTH = 128


class CloudTraceSpanExporter(SpanExporter):
    """Cloud Trace span exporter for OpenTelemetry.

    Args:
        client: Cloud Trace client.
        project_id: project_id to create the Trace client.
    """

    def __init__(
            self, client=None, project_id=None,
    ):
        if client is None:
            client = TraceServiceClient()
        self.client = client
        self.project_id = project_id

    def export(self, spans: Sequence[Span]) -> SpanExportResult:
        """Export the spans to Cloud Trace.

        See: https://cloud.google.com/trace/docs/reference/v2/rest/v2/
             projects.traces/batchWrite

        Args:
            spans: Tuple of spans to export
        """
        cloud_trace_formatted_spans = self.translate_to_cloud_trace(spans)
        cloud_trace_spans = []
        for span in cloud_trace_formatted_spans:
            try:
                cloud_trace_spans.append(self.client.create_span(**span))
            except Exception as ex:
                logger.warning("Error {} when creating span {}".format(ex, span))

        try:
            self.client.batch_write_spans(
                "projects/{}".format(self.project_id),
                cloud_trace_spans,
            )
        except Exception as ex:
            logger.warning("Error while writing to Cloud Trace: %s", ex)
            return SpanExportResult.FAILED_RETRYABLE

        return SpanExportResult.SUCCESS

    def translate_to_cloud_trace(
            self, spans: Sequence[Span]
    ) -> List[Dict[str, Any]]:
        """Translate the spans to Cloud Trace format.

        Args:
            spans: Tuple of spans to convert
        """

        cloud_trace_spans = []

        for span in spans:
            ctx = span.get_context()
            trace_id = trace_api.format_trace_id(ctx.trace_id)[2:]
            span_id = trace_api.format_span_id(ctx.span_id)[2:]
            span_name = "projects/{}/traces/{}/spans/{}".format(
                self.project_id, trace_id, span_id
            )

            parent_id = None
            if isinstance(span.parent, trace_api.Span):
                parent_id = trace_api.format_span_id(span.parent.get_context().span_id)[2:]
            elif isinstance(span.parent, trace_api.SpanContext):
                parent_id = trace_api.format_span_id(span.parent.span_id)[2:]

            start_time = get_time_from_ns(span.start_time)
            end_time = get_time_from_ns(span.end_time)

            attributes = _extract_attributes(span.attributes)
            attributes['attribute_map']["g.co/agent"] = _format_attribute_value(AGENT)

            sd_span = {
                "name": span_name,
                "span_id": span_id,
                "display_name": get_truncatable_str(span.name),
                "start_time": start_time,
                "end_time": end_time,
                "parent_span_id": parent_id,
                "attributes": attributes,
                "links": _extract_links(span.links),
                "status": _extract_status(span.status),
                "time_events": _extract_events(span.events),
            }

            cloud_trace_spans.append(sd_span)

        return cloud_trace_spans

    def shutdown(self):
        pass


def get_time_from_ns(ns):
    """Given epoch nanoseconds, split into epoch milliseconds and remaining nanoseconds"""
    if not ns:
        return None
    return {'seconds': int(ns / 1e9), 'nanos': int(ns % 1e9)}


def get_truncatable_str(str_to_convert, max_length=MAX_LENGTH):
    """Truncate a string if exceed limit and record the truncated bytes
        count.
    """
    truncated, truncated_byte_count = check_str_length(
        str_to_convert, max_length
    )

    result = {
        "value": truncated,
        "truncated_byte_count": truncated_byte_count,
    }
    return result


def check_str_length(str_to_check, limit=MAX_LENGTH):
    """Check the length of a string. If exceeds limit, then truncate it.
    """
    str_bytes = str_to_check.encode("utf-8")
    str_len = len(str_bytes)
    truncated_byte_count = 0

    if str_len > limit:
        truncated_byte_count = str_len - limit
        str_bytes = str_bytes[:limit]

    result = str(str_bytes.decode("utf-8", errors="ignore"))

    return result, truncated_byte_count


def _extract_status(status: trace_api.Status):
    """Convert a Status object to dict."""
    status_json = {"details": None, "code": status.canonical_code.value}

    if status.description is not None:
        status_json["message"] = status.description

    return status_json


def _extract_links(links: Sequence[trace_api.Link]):
    """Convert span.links"""
    extracted_links = []
    for link in links:
        trace_id = trace_api.format_trace_id(link.context.trace_id)[2:]
        span_id = trace_api.format_span_id(link.context.span_id)[2:]
        extracted_links.append(
            {'trace_id': trace_id, 'span_id': span_id, 'type': "CHILD_LINKED_SPAN",
             'attributes': _extract_attributes(link.attributes)}
        )
    return {"link": extracted_links}


def _extract_events(events: Sequence[Event]):
    """Convert span.events to dict."""
    logs = []
    for event in events:
        annotation_json = {"description": get_truncatable_str(event.name, 256),
                           "attributes": _extract_attributes(
                               event.attributes
                           )}

        logs.append(
            {
                "time": get_time_from_ns(event.timestamp),
                "annotation": annotation_json,
            }
        )
    return {"time_event": logs}


def _extract_attributes(attrs: types.Attributes):
    """Convert span.attributes to dict."""
    attributes_json = {}

    for key, value in attrs.items():
        key = check_str_length(key)[0]
        value = _format_attribute_value(value)

        if value is not None:
            attributes_json[ATTRIBUTE_MAPPING.get(key, key)] = value
    # Add dropped_attributes?
    return {"attribute_map": attributes_json}


def _format_attribute_value(value: types.AttributeValue):
    if isinstance(value, bool):
        value_type = "bool_value"
    elif isinstance(value, int):
        value_type = "int_value"
    elif isinstance(value, str):
        value_type = "string_value"
        value = get_truncatable_str(value)
    elif isinstance(value, float):
        value_type = "string_value"
        value = get_truncatable_str(value)
    else:
        return None

    return AttributeValue(**{value_type: value})


ATTRIBUTE_MAPPING = {
    "component": "/component",
    "error.message": "/error/message",
    "error.name": "/error/name",
    "http.client_city": "/http/client_city",
    "http.client_country": "/http/client_country",
    "http.client_protocol": "/http/client_protocol",
    "http.client_region": "/http/client_region",
    "http.host": "/http/host",
    "http.method": "/http/method",
    "http.redirected_url": "/http/redirected_url",
    "http.request_size": "/http/request/size",
    "http.response_size": "/http/response/size",
    "http.status_code": "/http/status_code",
    "http.url": "/http/url",
    "http.user_agent": "/http/user_agent",
    "pid": "/pid",
    "stacktrace": "/stacktrace",
    "tid": "/tid",
    "grpc.host_port": "/grpc/host_port",
    "grpc.method": "/grpc/method",
}
