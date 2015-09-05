from __future__ import with_statement
from select import select
from redis._compat import iteritems
import socket
import sys

from redis.connection import (
    Connection,
    HiredisParser,
    HIREDIS_SUPPORTS_CALLABLE_ERRORS,
    SERVER_CLOSED_CONNECTION_ERROR,
    SYM_CRLF,
)

from redis.exceptions import (
    RedisError,
    ConnectionError,
    TimeoutError,
    ResponseError,
)
from redis.utils import (
    SSL_AVAILABLE,
    HIREDIS_AVAILABLE,
    TORNADO_AVAILABLE,
    GREENLET_AVAILABLE,
)

if TORNADO_AVAILABLE:
    from tornado.iostream import IOStream
    from tornado.iostream import SSLIOStream
    from tornado.ioloop import IOLoop
if GREENLET_AVAILABLE:
    import greenlet
if SSL_AVAILABLE:
    import ssl


class AsyncHiredisParser(HiredisParser):
    def __init__(self, socket_read_size):
        self._iostream = None

        if not HIREDIS_AVAILABLE:
            raise("Async parser requires Hiredis")
        if not GREENLET_AVAILABLE:
            raise("Async parser requires Greenlet")
        if not TORNADO_AVAILABLE:
            raise("Async parser requires Tornado")

        super(AsyncHiredisParser, self).__init__(socket_read_size)

    def on_disconnect(self):
        self._iostream = None
        super(AsyncHiredisParser, self).on_disconnect()

    def on_connect(self, connection):
        self._iostream = connection._iostream
        super(AsyncHiredisParser, self).on_connect(connection)

    def read_response(self):
        if not self._reader:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        # _next_response might be cached from a can_read() call
        if self._next_response is not False:
            response = self._next_response
            self._next_response = False
            return response

        response = self._reader.gets()
        while response is False:
            try:
                self._iostream.read_until(SYM_CRLF,
                                          greenlet.getcurrent().switch)
                data = greenlet.getcurrent().parent.switch()
                if data is None or len(data) == 0:
                    raise socket.error(SERVER_CLOSED_CONNECTION_ERROR)
                self._reader.feed(data)
                response = self._reader.gets()
            except socket.timeout:
                raise TimeoutError("Timeout reading from socket")
            except socket.error:
                e = sys.exc_info()[1]
                raise ConnectionError("Error while reading from socket: %s" %
                                      (e.args,))
        # if an older version of hiredis is installed, we need to attempt
        # to convert ResponseErrors to their appropriate types.
        if not HIREDIS_SUPPORTS_CALLABLE_ERRORS:
            if isinstance(response, ResponseError):
                response = self.parse_error(response.args[0])
            elif isinstance(response, list) and response and \
                    isinstance(response[0], ResponseError):
                response[0] = self.parse_error(response[0].args[0])
        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif isinstance(response, list) and response and \
                isinstance(response[0], ConnectionError):
            raise response[0]
        return response


class ConnectionInvalidContext(Exception):
    pass


class AsyncConnection(Connection):
    "Manages TCP communication to and from a Redis server"
    description_format = ("AsyncConnection"
                          "<host=%(host)s,port=%(port)s,db=%(db)s>")

    def __init__(self, parser_class=AsyncHiredisParser, *args, **kwargs):
        self._iostream = None
        super(AsyncConnection, self).__init__(parser_class=parser_class,
                                              *args, **kwargs)

    def _wrap_socket(self, sock):
        return IOStream(sock)

    def _maybe_raise_no_greenlet_parent(self):
        if greenlet.getcurrent().parent is None:
            raise ConnectionInvalidContext("Greenlet parent not found, "
                                           "cannot perform async operations")

    def _connect(self):
        "Create a TCP socket connection"
        # we want to mimic what socket.create_connection does to support
        # ipv4/ipv6, but we want to set options prior to calling
        # socket.connect()
        self._maybe_raise_no_greenlet_parent()

        if self._iostream:
            return

        err = None
        for res in socket.getaddrinfo(self.host, self.port, 0,
                                      socket.SOCK_STREAM):
            family, socktype, proto, canonname, socket_address = res
            sock = None
            iostream = None
            try:
                sock = socket.socket(family, socktype, proto)
                # TCP_NODELAY
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in iteritems(self.socket_keepalive_options):
                        sock.setsockopt(socket.SOL_TCP, k, v)

                # set the socket_connect_timeout before we connect
                sock.settimeout(self.socket_connect_timeout)

                iostream = self._wrap_socket(sock)
                iostream.connect(socket_address,
                                 callback=greenlet.getcurrent().switch)
                # yield back to parent, wait for connect
                greenlet.getcurrent().parent.switch()

                # set the socket_timeout now that we're connected
                sock.settimeout(self.socket_timeout)

                self._iostream = iostream
                return sock
            except socket.error as _:
                err = _
                if sock is not None:
                    sock.close()
                if iostream is not None:
                    iostream.close()

        if err is not None:
            raise err
        raise socket.error("socket.getaddrinfo returned an empty list")

    def disconnect(self):
        "Disconnects from the Redis server"
        super(AsyncConnection, self).disconnect()

        if self._iostream is None:
            return
        try:
            self._iostream.close()
        except socket.error:
            pass
        self._iostream = None

    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"

        self._maybe_raise_no_greenlet_parent()

        if not self._iostream:
            self.connect()
        try:
            if isinstance(command, str):
                command = [command]
            for item in command:
                self._iostream.write(item,
                                     callback=greenlet.getcurrent().switch)
            greenlet.getcurrent().parent.switch()
        except socket.timeout:
            self.disconnect()
            raise TimeoutError("Timeout writing to socket")
        except socket.error:
            e = sys.exc_info()[1]
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %
                                  (_errno, errmsg))
        except:
            self.disconnect()
            raise

    def can_read(self, timeout=0):
        "Check if there's any data that can be read"
        if not self._iostream:
            self.connect()
        self._maybe_raise_no_greenlet_noparent()

        def check_for_data():
            if (self._parser.can_read() or
                    self._iostream._read_buffer_size):
                return True
            return bool(select([self._iostream.sock], [], [], 0)[0])

        if timeout is 0:
            return check_for_data()
        else:
            IOLoop.current().call_later(timeout, greenlet.getcurrent().switch)
            greenlet.getcurrent().parent.switch()
            return check_for_data()


class AsyncSSLConnection(AsyncConnection):
    description_format = ("AsyncSSLConnection"
                          "<host=%(host)s,port=%(port)s,db=%(db)s>")

    def __init__(self, ssl_keyfile=None, ssl_certfile=None, ssl_cert_reqs=None,
                 ssl_ca_certs=None, **kwargs):
        if not SSL_AVAILABLE:
            raise RedisError("Python wasn't built with SSL support")

        if ssl_cert_reqs is None:
            ssl_cert_reqs = ssl.CERT_NONE
        elif isinstance(ssl_cert_reqs, basestring):
            CERT_REQS = {
                'none': ssl.CERT_NONE,
                'optional': ssl.CERT_OPTIONAL,
                'required': ssl.CERT_REQUIRED
            }
            if ssl_cert_reqs not in CERT_REQS:
                raise RedisError(
                    "Invalid SSL Certificate Requirements Flag: %s" %
                    ssl_cert_reqs)
            ssl_cert_reqs = CERT_REQS[ssl_cert_reqs]

        self.ssl_options = {
            'keyfile': ssl_keyfile,
            'certfile': ssl_certfile,
            'ca_certs': ssl_ca_certs,
            'cert_reqs': ssl_cert_reqs,
        }

        super(AsyncSSLConnection, self).__init__(**kwargs)

    def _wrap_socket(self, sock):
        return SSLIOStream(sock, ssl_options=self.ssl_options)
