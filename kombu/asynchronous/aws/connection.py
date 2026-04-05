"""Amazon AWS Connection."""

from __future__ import annotations

from email import message_from_bytes
from email.mime.message import MIMEMessage

from vine import promise, transform

from kombu.asynchronous.aws.ext import AWSRequest, get_cert_path, get_response
from kombu.asynchronous.http import Headers, Request, get_client


def message_from_headers(hdr):
    pass


__all__ = (
    'AsyncHTTPSConnection', 'AsyncConnection',
)


class AsyncHTTPResponse:
    """Async HTTP Response."""

    def __init__(self, response):
        self.response = response
        self._msg = None
        self.version = 10

    def read(self, *args, **kwargs):
        return self.response.body

    def getheader(self, name, default=None):
        pass

    def getheaders(self):
        pass

    @property
    def msg(self):
        pass

    @property
    def status(self):
        pass

    @property
    def reason(self):
        pass

    def __repr__(self):
        return repr(self.response)


class AsyncHTTPSConnection:
    """Async HTTP Connection."""

    Request = Request
    Response = AsyncHTTPResponse

    method = 'GET'
    path = '/'
    body = None
    default_ports = {'http': 80, 'https': 443}

    def __init__(self, strict=None, timeout=20.0, http_client=None):
        self.headers = []
        self.timeout = timeout
        self.strict = strict
        self.http_client = http_client or get_client()

    def request(self, method, path, body=None, headers=None):
        self.path = path
        self.method = method
        if body is not None:
            try:
                read = body.read
            except AttributeError:
                self.body = body
            else:
                self.body = read()
        if headers is not None:
            self.headers.extend(list(headers.items()))

    def getrequest(self):
        headers = Headers(self.headers)
        return self.Request(self.path, method=self.method, headers=headers,
                            body=self.body, connect_timeout=self.timeout,
                            request_timeout=self.timeout,
                            validate_cert=True, ca_certs=get_cert_path(True))

    def getresponse(self, callback=None):
        request = self.getrequest()
        request.then(transform(self.Response, callback))
        return self.http_client.add_request(request)

    def set_debuglevel(self, level):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    def putrequest(self, method, path):
        pass

    def putheader(self, header, value):
        pass

    def endheaders(self):
        pass

    def send(self, data):
        if self.body:
            self.body += data
        else:
            self.body = data

    def __repr__(self):
        return f'<AsyncHTTPConnection: {self.getrequest()!r}>'


class AsyncConnection:
    """Async AWS Connection."""

    def __init__(self, sqs_connection, http_client=None, **kwargs):
        self.sqs_connection = sqs_connection
        self._httpclient = http_client or get_client()

    def get_http_connection(self):
        return AsyncHTTPSConnection(http_client=self._httpclient)

    def _mexe(self, request, sender=None, callback=None):
        callback = callback or promise()
        conn = self.get_http_connection()

        if callable(sender):
            sender(conn, request.method, request.path, request.body,
                   request.headers, callback)
        else:
            conn.request(request.method, request.url,
                         request.body, request.headers)
            conn.getresponse(callback=callback)
        return callback


class AsyncAWSQueryConnection(AsyncConnection):
    """Async AWS Query Connection."""

    STATUS_CODE_OK = 200
    STATUS_CODE_REQUEST_TIMEOUT = 408
    STATUS_CODE_NETWORK_CONNECT_TIMEOUT_ERROR = 599
    STATUS_CODE_INTERNAL_ERROR = 500
    STATUS_CODE_BAD_GATEWAY = 502
    STATUS_CODE_SERVICE_UNAVAILABLE_ERROR = 503
    STATUS_CODE_GATEWAY_TIMEOUT = 504

    STATUS_CODES_SERVER_ERRORS = (
        STATUS_CODE_INTERNAL_ERROR,
        STATUS_CODE_BAD_GATEWAY,
        STATUS_CODE_SERVICE_UNAVAILABLE_ERROR
    )

    STATUS_CODES_TIMEOUT = (
        STATUS_CODE_REQUEST_TIMEOUT,
        STATUS_CODE_NETWORK_CONNECT_TIMEOUT_ERROR,
        STATUS_CODE_GATEWAY_TIMEOUT
    )

    def __init__(self, sqs_connection, http_client=None,
                 http_client_params=None, **kwargs):
        if not http_client_params:
            http_client_params = {}
        super().__init__(sqs_connection, http_client,
                         **http_client_params)

    def make_request(self, operation, params_, path, verb, callback=None, protocol_params=None):
        params = params_.copy()
        params.update((protocol_params or {}).get('query', {}))
        if operation:
            params['Action'] = operation
        signer = self.sqs_connection._request_signer

        # defaults for non-get
        signing_type = 'standard'
        param_payload = {'data': params}
        if verb.lower() == 'get':
            # query-based opts
            signing_type = 'presign-url'
            param_payload = {'params': params}

        request = AWSRequest(method=verb, url=path, **param_payload)
        signer.sign(operation, request, signing_type=signing_type)
        prepared_request = request.prepare()

        return self._mexe(prepared_request, callback=callback)

    def get_list(self, operation, params, markers, path='/', parent=None, verb='POST', callback=None,
                 protocol_params=None):
        return self.make_request(
            operation, params, path, verb,
            callback=transform(
                self._on_list_ready, callback, parent or self, markers,
                operation
            ),
            protocol_params=protocol_params,
        )

    def get_object(self, operation, params, path='/', parent=None, verb='GET', callback=None, protocol_params=None):
        return self.make_request(
            operation, params, path, verb,
            callback=transform(
                self._on_obj_ready, callback, parent or self, operation
            ),
            protocol_params=protocol_params,
        )

    def get_status(self, operation, params, path='/', parent=None, verb='GET', callback=None, protocol_params=None):
        return self.make_request(
            operation, params, path, verb,
            callback=transform(
                self._on_status_ready, callback, parent or self, operation
            ),
            protocol_params=protocol_params,
        )

    def _on_list_ready(self, parent, markers, operation, response):
        pass

    def _on_obj_ready(self, parent, operation, response):
        pass

    def _on_status_ready(self, parent, operation, response):
        pass

    def _for_status(self, response, body):
        pass
