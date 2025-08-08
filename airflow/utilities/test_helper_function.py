import unittest
from unittest.mock import patch, MagicMock
import psycopg2
import os
import sys
import types

# Import the module to test
test_module_path = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, test_module_path)
import helper_function

class TestHelperFunction(unittest.TestCase):
    @patch('helper_function.requests.get')
    @patch('helper_function.os.getenv')
    @patch('helper_function.load_dotenv')
    def test_fetch_data_from_api_success(self, mock_load_dotenv, mock_getenv, mock_requests_get):
        mock_getenv.return_value = 'fake_api_key'
        mock_response = MagicMock()
        mock_response.json.return_value = {'location': {'name': 'London'}, 'current': {'temperature': 20, 'weather_descriptions': ['Clear'], 'wind_speed': 5}}
        mock_response.raise_for_status.return_value = None
        mock_requests_get.return_value = mock_response
        data = helper_function.fetch_data_from_api('London')
        self.assertEqual(data['location']['name'], 'London')
        mock_requests_get.assert_called_once()

    @patch('helper_function.psycopg2.connect')
    def test_connect_to_postgres_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        conn = helper_function.connect_to_postgres()
        self.assertEqual(conn, mock_conn)
        mock_connect.assert_called_once()

    @patch('helper_function.psycopg2.connect', side_effect=psycopg2.Error('fail'))
    def test_connect_to_postgres_fail(self, mock_connect):
        with self.assertRaises(psycopg2.Error):
            helper_function.connect_to_postgres()

    def test_create_schema_table_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        helper_function.create_schema_table(mock_conn)
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.commit.called)

    def test_create_schema_table_fail(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.Error('fail')
        with self.assertRaises(psycopg2.Error):
            helper_function.create_schema_table(mock_conn)
        self.assertTrue(mock_conn.rollback.called)

    def test_insert_data_into_table_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        data = {
            'location': {'name': 'London', 'localtime': '2025-08-08 12:00', 'utc_offset': '+1'},
            'current': {'temperature': 20, 'weather_descriptions': ['Clear'], 'wind_speed': 5}
        }
        helper_function.insert_data_into_table(mock_conn, data)
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.commit.called)

    def test_insert_data_into_table_fail(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = psycopg2.Error('fail')
        data = {
            'location': {'name': 'London', 'localtime': '2025-08-08 12:00', 'utc_offset': '+1'},
            'current': {'temperature': 20, 'weather_descriptions': ['Clear'], 'wind_speed': 5}
        }
        with self.assertRaises(psycopg2.Error):
            helper_function.insert_data_into_table(mock_conn, data)
        self.assertTrue(mock_conn.rollback.called)

if __name__ == '__main__':
    unittest.main()
