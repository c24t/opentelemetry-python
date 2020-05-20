# Copyright OpenTelemetry Authors
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

import requests
from flask import Flask

import opentelemetry.ext.requests
from opentelemetry import trace
from opentelemetry.ext.cloud_trace import CloudTraceSpanExporter
from opentelemetry.ext.flask import FlaskInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleExportSpanProcessor

opentelemetry.ext.requests.RequestsInstrumentor().instrument()
trace.set_tracer_provider(TracerProvider())

cloud_trace_exporter = CloudTraceSpanExporter(project_id="my-gcloud-project")
trace.get_tracer_provider().add_span_processor(
    SimpleExportSpanProcessor(cloud_trace_exporter)
)
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)


@app.route("/")
def hello():
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("parent"):
        requests.get("https://www.wikipedia.org/wiki/Rabbit")
    return "hello"


if __name__ == "__main__":
    app.run(debug=True, port=7777)
