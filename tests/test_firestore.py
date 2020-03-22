import time
from unittest import mock

import pytest
from google.cloud.firestore_v1 import Query

from app.firestore.location import retrieve_category, LocationMediator, \
    LocationDomainModel, RecordMediator
from tests.test_jhu import mocked_requests_get, DATETIME_STRING

import os
from flask_boiler import config, context

cert_path = os.path.curdir + \
            "/app/config/covid-19-track-firebase-adminsdk-utg4o-ec50eb5da3.json"

testing_config = config.Config(app_name="covid-19-track",
                               debug=True,
                               testing=True,
                               certificate_path=cert_path)

CTX = context.Context
CTX.read(testing_config)


@pytest.mark.parametrize("category, datetime_str, latest_value, country_name, \
                          country_code, province, latest_country_value, \
                          coordinate_lat, coordinate_long",
                         [("deaths", DATETIME_STRING, 1940, "Thailand", "TH", "",
                           114, "15", "101"),
                          ("recovered", DATETIME_STRING, 1940, "Thailand", "TH", "",
                           114, "15", "101"),
                          ("confirmed", DATETIME_STRING, 1940, "Thailand", "TH", "",
                           114, "15", "101")])
@mock.patch('app.services.location.jhu.datetime')
@mock.patch('app.services.location.jhu.requests.get', side_effect=mocked_requests_get)
def test_retrieve_category(mock_request_get, mock_datetime, category,
                           datetime_str,
                           latest_value, country_name, country_code, province,
                           latest_country_value,
                           coordinate_lat, coordinate_long):
    retrieve_category("recovered")


def test_mediator():
    location_mediator = LocationMediator(
        query=Query(parent=LocationDomainModel._get_collection()))
    location_mediator.start()

    record_mediator = RecordMediator(
        query=CTX.db.collection_group("records")
    )
    record_mediator.start()

    time.sleep(5)

    assert CTX.db.document("countries/Australia").get().get("recovered") == 134
