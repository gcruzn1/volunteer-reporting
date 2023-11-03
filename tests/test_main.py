import pytest
import os
import sys
import unittest
from unittest import mock
import names
import random

from dotenv import load_dotenv
import logging

try:
    from app.main import *
    from app.GoogleClient import GoogleServiceClient
except (ImportError, ModuleNotFoundError):
    logging.info("handling exc")
    # TODO: Determine why __init__.py isn't importing py files from app
    # when running this directly (aka, not in pytest), add root of proj to python path
    sys.path.append(os.path.abspath(os.getcwd()))
    from app.main import *
    from app.GoogleClient import GoogleServiceClient

print(os.getcwd())
MASTER_ALERT_NUM = os.getenv("MASTER_ALERT_NUM")
# CREDS = create_service_account_creds()
CREDS = GoogleServiceClient().CredsServiceAcct
SHEETS_SERVICE = build("sheets", "v4", credentials=CREDS)


def test_get_envs():
    assert os.getenv("ADMIN_NAME") == "George Cruz", "ADMIN_NAME not set!"


def test_create_service_acct_creds():
    # auth = create_service_account_creds()
    assert CREDS is not None


def test_get_report_month():
    progress_df = get_worksheet_data(
        SHEETS_SERVICE, MASTER_SHEET_ID, PROGRESS_SHEET_RANGE
    )

    # get last row from sheet
    # should we use a date parser to sort by date instead? - This would be more "fail safe"
    # DO NOT modify the index, it will be used to update the status
    progress_df = progress_df.iloc[len(progress_df) - 1 :]
    current_report_month = progress_df.year_month.unique().item()

    if len(progress_df[progress_df["status"] == "complete"]):
        # no need to continue, all volunteer data has been collected!
        # print(f"Exiting... All volunteer data has been collected for {current_report_month}.")
        logging.info(
            f"Exiting... All volunteer data has been collected for {current_report_month}"
        )
        return

    assert current_report_month is not None


def test_messaging():
    current_form_link = ""  # enter form link to test here
    errors_from_twilio, message_stats = send_twilio_message(
        [
            {
                "name": "Test Person",
                "number": MASTER_ALERT_NUM,
                "form_link": current_form_link,
            }
        ],
        None,
    )

    # add descriptive error
    if errors_from_twilio:
        for error in errors_from_twilio:
            print(error)
    assert (
        errors_from_twilio is None or len(errors_from_twilio) == 0
    ), "Error sending message"

    assert message_stats is not None


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.mock_names = [
            names.get_full_name() for _ in range(10)
        ]
        self.mock_volunteer_reports = {
            r:[
                random.choice(['Regular (Precursor de Tiempo Completo)','Auxiliar', 'No']),
                random.randint(1,100),
                random.randint(0,100),
                random.randint(1,100),
                random.randint(0,50),
                random.randint(0,100),
                random.choice([None,'','Some random note'])
            ]
            for r in self.mock_names
        }
        self.mock_report_df = self.create_mock_df()
        self.test = 'hi'

    def create_mock_df(self):
        return pd.DataFrame(self.mock_volunteer_reports)

    @mock.patch("app.main.")
    def test_main(self):
        for v in self.mock_names:
            self.assertIsNotNone(v)
            self.assertGreater(
                0,
                self.mock_report_df[self.mock_report_df.columns[0]][1]
            )
