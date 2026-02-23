import datetime
from unittest.mock import MagicMock, sentinel

import pytest

from replication import Replication

FAKE_TIME = datetime.datetime(2026, 1, 22, 10, 1, 25)


@pytest.fixture(scope="function", autouse=True)
def patch_datetime_now(monkeypatch):
    class MockedDateTime(datetime.datetime):
        @classmethod
        def now(cls, **kwargs):
            return FAKE_TIME

    monkeypatch.setattr(datetime, 'datetime', MockedDateTime)
    
    
def test_create_subscription():
    primary = MagicMock()
    primary.db.db_name = "foo"
    primary.execute_dump.return_value.__enter__.side_effect = [sentinel.dump1, sentinel.dump2]
    secondary = MagicMock()
    secondary.get_subscription_name.return_value = ""
    
    replication = Replication(primary, secondary)
    replication.run("script_name", "today")
    
    primary.create_replication_user.assert_called_once()
    secondary.execute_pre_data_dump.assert_called_once_with(sentinel.dump1)
    secondary.execute_post_data_dump_only_pk.assert_called_once_with(sentinel.dump2)
    primary.create_publication.assert_called_once_with("foo_20260122_100125")
    secondary.create_subscription.assert_called_once_with("foo_20260122_100125")
    secondary.get_subscription_name.assert_called_with("foo")


def test_run_when_subscription():
    primary = MagicMock()
    primary.db.db_name = "foo"
    primary.execute_dump.return_value.__enter__.side_effect = [sentinel.dump1]
    secondary = MagicMock()
    secondary.get_subscription_name.return_value = "existing_subscription_name"
    
    replication = Replication(primary, secondary)
    replication.run("script_name", "today")
    
    secondary.wait_first_step_of_replication.assert_called_once()
    secondary.disable_subscription.assert_called_once_with("existing_subscription_name")
    secondary.execute_post_data_dump_without_pk.assert_called_once_with(sentinel.dump1)
    secondary.enable_subscription.assert_called_once_with("existing_subscription_name")

def test_run_on_error(capsys):
    primary = MagicMock()
    secondary = MagicMock()
    secondary.get_subscription_name.return_value = None

    replication = Replication(primary, secondary)
    replication.run("script_name", "today")

    assert "No replication running, exiting" in capsys.readouterr().out
