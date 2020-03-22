from flask_boiler import schema, fields, domain_model, factory
from google.cloud.firestore import WriteBatch


class LocationSchema(schema.Schema):
    case_conversion = False

    country = fields.String()
    country_code = fields.String()
    province = fields.String()

    coordinates = fields.Dict()

    history = fields.Raw()
    latest = fields.Raw()


LocationDomainModelBase = factory.ClsFactory.create(
    name="LocationDomainModelBase",
    schema=LocationSchema,
    base=domain_model.DomainModel
)


class LocationDomainModel(LocationDomainModelBase):
    class Meta:
        collection_name = "locations"
        schema = LocationSchema


"""
Base URL for fetching category.
"""
base_url = 'https://raw.githubusercontent.com/CSSEGISandData/2019-nCoV/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-%s.csv';


def retrieve_category(category) -> None:
    """
    Retrieves the data for the provided category. The data is stored
        in firestore permanently.
    """

    from app.services.location.jhu import get_category
    from flask_boiler.context import Context as CTX

    category_res = get_category(category)
    locations = category_res["locations"]

    # TODO: identify maximum number of documents supported in a batch
    batch: WriteBatch = CTX.db.batch()

    for d in locations:
        obj = LocationDomainModel.new(
            **d,
        )

        obj.save(transaction=batch)

    batch.commit()
