import http.client
import logging
import unittest

from kubernetes.client.configuration import Configuration


class TestConfigurationDebugLogging(unittest.TestCase):
    def setUp(self):
        self.package_logger = logging.getLogger("client")
        self.urllib3_logger = logging.getLogger("urllib3")
        self.package_level = self.package_logger.level
        self.urllib3_level = self.urllib3_logger.level
        self.http_debuglevel = http.client.HTTPConnection.debuglevel

    def tearDown(self):
        self.package_logger.setLevel(self.package_level)
        self.urllib3_logger.setLevel(self.urllib3_level)
        http.client.HTTPConnection.debuglevel = self.http_debuglevel

    def test_default_configuration_preserves_existing_logging_settings(self):
        self.package_logger.setLevel(logging.ERROR)
        self.urllib3_logger.setLevel(logging.INFO)
        http.client.HTTPConnection.debuglevel = 2

        Configuration()

        self.assertEqual(self.package_logger.level, logging.ERROR)
        self.assertEqual(self.urllib3_logger.level, logging.INFO)
        self.assertEqual(http.client.HTTPConnection.debuglevel, 2)

    def test_explicit_debug_toggles_shared_logging_state(self):
        first = Configuration(debug=True)

        self.assertEqual(self.package_logger.level, logging.DEBUG)
        self.assertEqual(self.urllib3_logger.level, logging.DEBUG)
        self.assertEqual(http.client.HTTPConnection.debuglevel, 1)

        second = Configuration()
        second.debug = False

        self.assertTrue(first.debug)
        self.assertFalse(second.debug)
        self.assertEqual(self.package_logger.level, logging.WARNING)
        self.assertEqual(self.urllib3_logger.level, logging.WARNING)
        self.assertEqual(http.client.HTTPConnection.debuglevel, 0)


if __name__ == "__main__":
    unittest.main()
