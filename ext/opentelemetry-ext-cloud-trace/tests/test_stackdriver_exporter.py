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

import unittest
from unittest import mock

import opentelemetry.ext.cloud_trace as sd_exporter
from opentelemetry.sdk.trace import Span
from opentelemetry.trace import SpanContext, SpanKind
from opentelemetry.version import __version__
from google.cloud.trace_v2.proto.trace_pb2 import AttributeValue


class TestStackdriverSpanExporter(unittest.TestCase):
    def setUp(self):
        self.client_patcher = mock.patch(
            "opentelemetry.ext.cloud_trace.TraceServiceClient"
        )
        self.client_patcher.start()

    def tearDown(self):
        self.client_patcher.stop()

    def test_constructor_default(self):
        exporter = sd_exporter.StackdriverSpanExporter()
        self.assertEqual(exporter.project_id, exporter.client.project)

    def test_constructor_explicit(self):
        client = mock.Mock()
        project_id = "PROJECT"
        client.project = project_id

        exporter = sd_exporter.StackdriverSpanExporter(
            client=client, project_id=project_id
        )

        self.assertIs(exporter.client, client)
        self.assertEqual(exporter.project_id, project_id)

    def test_export(self):
        trace_id = "6e0c63257de34c92bf9efcd03927272e"
        span_id = "95bb5edabd45950f"
        # start_times = 683647322 * 10 ** 9  # in ns
        # durations = 50 * 10 ** 6
        # end_times = start_times + durations
        span_datas = [
            Span(
                name="span_name",
                context=SpanContext(
                    trace_id=int(trace_id, 16), span_id=int(span_id, 16),
                    is_remote=False
                ),
                parent=None,
                kind=SpanKind.INTERNAL,
            )
        ]

        stackdriver_spans = {
            "spans": [
                {
                    "name": "projects/PROJECT/traces/{}/spans/{}".format(
                        trace_id, span_id
                    ),
                    "span_id": span_id,
                    "parent_span_id": None,
                    "display_name": {
                        "value": "span_name",
                        "truncated_byte_count": 0,
                    },
                    "attributes": {
                        "attribute_map": {
                            "g.co/agent": AttributeValue(string_value={
                                "value": "opentelemetry-python [{}]".format(
                                    __version__
                                ),
                                "truncated_byte_count": 0,
                            })
                        }
                    },
                    "links": None,
                    "status": {"details": None, "code": 0},
                    "timeEvents": None,
                    "startTime": None,
                    "endTime": None,
                }
            ]
        }

        client = mock.Mock()
        project_id = "PROJECT"
        client.project = project_id

        exporter = sd_exporter.StackdriverSpanExporter(
            client=client, project_id=project_id
        )

        exporter.export(span_datas)

        name = "projects/{}".format(project_id)

        client.create_span.assert_called_with(name, stackdriver_spans)
        self.assertTrue(client.create_span.called)
