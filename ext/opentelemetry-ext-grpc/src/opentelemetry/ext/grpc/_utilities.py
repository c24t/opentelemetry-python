# Copyright 2020, OpenTelemetry Authors
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

"""Internal utilities for gRPC OpenTracing."""

import grpc_opentracing


class RpcInfo(grpc_opentracing.RpcInfo):

    def __init__(self,
                 full_method=None,
                 metadata=None,
                 timeout=None,
                 request=None,
                 response=None,
                 error=None):
        self.full_method = full_method
        self.metadata = metadata
        self.timeout = timeout
        self.request = request
        self.response = response
        self.error = error


def get_method_type(is_client_stream, is_server_stream):
    if is_client_stream and is_server_stream:
        return 'BIDI_STREAMING'
    elif is_client_stream:
        return 'CLIENT_STREAMING'
    elif is_server_stream:
        return 'SERVER_STREAMING'
    else:
        return 'UNARY'


def get_deadline_millis(timeout):
    if timeout is None:
        return 'None'
    return str(int(round(timeout * 1000)))


class _RequestLoggingIterator(object):

    def __init__(self, request_iterator, span):
        self._request_iterator = request_iterator
        self._span = span

    def __iter__(self):
        return self

    def next(self):
        request = next(self._request_iterator)
        self._span.log_kv({'request': request})
        return request

    def __next__(self):
        return self.next()


def log_or_wrap_request_or_iterator(span, is_client_stream,
                                    request_or_iterator):
    if is_client_stream:
        return _RequestLoggingIterator(request_or_iterator, span)
    else:
        span.log_kv({'request': request_or_iterator})
        return request_or_iterator
