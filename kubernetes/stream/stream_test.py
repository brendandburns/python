# Copyright 2026 The Kubernetes Authors.
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
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import websocket

from kubernetes import client

from . import ws_client
from .stream import _websocket_request, stream


class StreamTest(unittest.TestCase):
    def test_generated_portforward_preserves_multiple_ports(self):
        api = client.CoreV1Api(client.ApiClient(client.Configuration(
            host='https://example.com',
            proxy='',
            no_proxy='',
        )))

        with (
            patch.object(ws_client, 'create_websocket') as create_websocket,
            patch.object(ws_client, 'PortForward') as port_forward,
        ):
            result = _websocket_request(
                ws_client.portforward_call,
                {'_preload_content': False},
                api.connect_get_namespaced_pod_portforward,
                'pod',
                'default',
                ports='80,443',
            )

        self.assertIs(port_forward.return_value, result)
        port_forward.assert_called_once_with(
            create_websocket.return_value,
            [80, 443],
        )
        websocket_url = create_websocket.call_args.args[1]
        self.assertEqual('wss', urlparse(websocket_url).scheme)
        self.assertEqual(
            ['80,443'], parse_qs(urlparse(websocket_url).query)['ports'])

    def test_generated_api_request_uses_websocket_transport(self):
        request = {}

        def websocket_call(configuration, method, url, **kwargs):
            request.update(method=method, url=url, kwargs=kwargs)
            return ws_client.WSResponse(data='output', status=200)

        api = client.CoreV1Api()
        output = _websocket_request(
            websocket_call,
            None,
            api.connect_get_namespaced_pod_exec,
            'pod',
            'default',
            command=['echo', 'hello'],
        )

        self.assertEqual('output', output)
        self.assertEqual('GET', request['method'])
        self.assertIn(
            '/api/v1/namespaces/default/pods/pod/exec?',
            request['url'],
        )
        self.assertIn('command=echo&command=hello', request['url'])

    def test_exec_tuple_timeout_is_connect_and_read_idle_timeout(self):
        api = client.CoreV1Api(client.ApiClient(client.Configuration(
            host='https://example.com', proxy='', no_proxy='')))
        frame = MagicMock(data=b'\x01hello')
        close = MagicMock(data=b'')
        with (
            patch.object(ws_client, 'WebSocket') as websocket_class,
            patch.object(ws_client.select, 'poll') as poll,
            patch.object(ws_client.WSClient, '_frames_immediately_available',
                         return_value=False),
        ):
            sock = websocket_class.return_value
            sock.connected = True
            sock.subprotocol = ws_client.V5_CHANNEL_PROTOCOL
            sock.recv_data_frame.side_effect = [
                (websocket.ABNF.OPCODE_BINARY, frame),
                (websocket.ABNF.OPCODE_BINARY, frame),
                (websocket.ABNF.OPCODE_CLOSE, close),
            ]
            poll.return_value.poll.return_value = [(1, 1)]
            result = stream(
                api.connect_get_namespaced_pod_exec, 'pod', 'default',
                command=['echo', 'hello'], _request_timeout=(2, 5))

        self.assertEqual('hellohello', result)
        self.assertEqual(2, sock.connect.call_args.kwargs['timeout'])
        sock.settimeout.assert_called_with(5)
        self.assertEqual([5000, 5000, 5000], [
            call.args[0] for call in poll.return_value.poll.call_args_list])

    def test_exec_tuple_read_timeout_fails_instead_of_returning_partial_output(self):
        api = client.CoreV1Api(client.ApiClient(client.Configuration(
            host='https://example.com', proxy='', no_proxy='')))
        with (
            patch.object(ws_client, 'WebSocket') as websocket_class,
            patch.object(ws_client.select, 'poll') as poll,
            patch.object(ws_client.WSClient, '_frames_immediately_available',
                         return_value=False),
        ):
            sock = websocket_class.return_value
            sock.connected = True
            sock.subprotocol = ws_client.V5_CHANNEL_PROTOCOL
            sock.recv_data_frame.return_value = (
                websocket.ABNF.OPCODE_BINARY, MagicMock(data=b'\x01partial'))
            poll.return_value.poll.side_effect = [[(1, 1)], []]
            with self.assertRaisesRegex(client.ApiException, 'read.*timed out'):
                stream(api.connect_get_namespaced_pod_exec, 'pod', 'default',
                       command=['echo'], _request_timeout=(2, 5))
            sock.close.assert_called_once()

    def test_exec_streaming_tuple_connect_timeout_without_read_timeout(self):
        configuration = client.Configuration(
            host='https://example.com', proxy='', no_proxy='')
        with patch.object(ws_client, 'WebSocket') as websocket_class:
            sock = websocket_class.return_value
            sock.subprotocol = ws_client.V5_CHANNEL_PROTOCOL
            result = ws_client.websocket_call(
                configuration, 'GET', 'https://example.com/exec',
                _preload_content=False, _request_timeout=(2, None))
        self.assertIsInstance(result, ws_client.WSClient)
        self.assertEqual(2, sock.connect.call_args.kwargs['timeout'])
        sock.settimeout.assert_called_with(None)

    def test_generated_exec_rejects_none_in_tuple(self):
        api = client.CoreV1Api()
        with self.assertRaises(ValueError):
            stream(api.connect_get_namespaced_pod_exec, 'pod', 'default',
                   command=['sh'], _preload_content=False,
                   _request_timeout=(2, None))

    def test_exec_streaming_finite_tuple_read_timeout_is_explicitly_rejected(self):
        api = client.CoreV1Api()
        with patch.object(ws_client, 'WebSocket') as websocket_class:
            with self.assertRaisesRegex(client.ApiException, 'read timeout.*stream'):
                stream(api.connect_get_namespaced_pod_exec, 'pod', 'default',
                       command=['sh'], _preload_content=False,
                       _request_timeout=(2, 5))
            websocket_class.assert_not_called()

    def test_exec_invalid_tuple_timeouts_are_explicitly_rejected(self):
        configuration = client.Configuration()
        for timeout in [(1,), (1, 2, 3), (0, 1), (1, -1),
                        ('1', 2), (1, float('inf'))]:
            with self.subTest(timeout=timeout):
                with patch.object(ws_client, 'WebSocket') as websocket_class:
                    with self.assertRaisesRegex(client.ApiException, 'timeout'):
                        ws_client.websocket_call(
                            configuration, 'GET', 'https://example.com/exec',
                            _request_timeout=timeout)
                    websocket_class.assert_not_called()

    def test_exec_scalar_and_default_timeouts_keep_legacy_behavior(self):
        api = client.CoreV1Api()
        for kwargs, expected in [({}, None), ({'_request_timeout': 3}, 3)]:
            with self.subTest(kwargs=kwargs):
                with patch.object(ws_client, 'WSClient') as ws:
                    ws.return_value.read_all.return_value = 'output'
                    result = stream(api.connect_get_namespaced_pod_exec,
                                    'pod', 'default', command=['echo'],
                                    **kwargs)
                    self.assertEqual('output', result)
                    ws.return_value.run_forever.assert_called_once_with(
                        timeout=expected)

    def test_exec_streaming_scalar_does_not_run_preloaded_loop(self):
        api = client.CoreV1Api()
        with patch.object(ws_client, 'WSClient') as ws:
            result = stream(api.connect_get_namespaced_pod_exec,
                            'pod', 'default', command=['sh'],
                            _preload_content=False, _request_timeout=3)
            self.assertIs(ws.return_value, result)
            ws.return_value.run_forever.assert_not_called()


if __name__ == '__main__':
    unittest.main()
