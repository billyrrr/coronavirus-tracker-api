
from flask_boiler import schema, fields, domain_model, factory, \
    view_mediator_dav, view_model
from flask_boiler.context import Context as CTX
from google.cloud.firestore import WriteBatch, Increment
from google.cloud.firestore_v1 import DocumentSnapshot, DocumentReference

import base64
import logging
logger = logging.getLogger()


def b64e(s):
    return base64.b64encode(s.encode()).decode()


def b64d(s):
    return base64.b64decode(s).decode()


class LocationSchema(schema.Schema):
    case_conversion = False

    category = fields.String()

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
        schema = LocationSchema
        collection_name = "locations"

    @classmethod
    def new(cls, *args, coordinates=None, category=None, **kwargs):
        location_id = cls.location_id(coordinates["lat"], coordinates["long"])
        doc_id = b64e(f"{location_id}|{category}")
        return super().new(
            *args,
            coordinates=coordinates,
            category=category,
            doc_id=doc_id,
            **kwargs
        )

    @staticmethod
    def location_id(lat, long):
        """
        Identifier for an unique region
        :param lat:
        :param long:
        :return:
        """
        return f'{lat},{long}'


class LocationSubsetSchema(schema.Schema):
    case_conversion = False
    coordinates = fields.Dict()
    parts = fields.Dict()

    confirmed = fields.Integer(dump_only=True)
    deaths = fields.Integer(dump_only=True)
    recovered = fields.Integer(dump_only=True)


LocationSubsetBase = factory.ClsFactory.create(
    name="LocationSubsetBase",
    schema=LocationSubsetSchema,
    base=view_model.ViewModel
)


class LocationSubset(LocationSubsetBase):

    @staticmethod
    def count(parts, category):
        res = 0
        for key, val in parts.items():
            location_id = b64d(key)
            location_key, category_key = location_id.split("|")
            if category == category_key:
                res += val
        return res

    @property
    def confirmed(self):
        return self.count(self.parts, "confirmed")

    @property
    def deaths(self):
        return self.count(self.parts, "deaths")

    @property
    def recovered(self):
        return self.count(self.parts, "recovered")


class LocationMediator(view_mediator_dav.ViewMediatorDeltaDAV):
    class Protocol(view_mediator_dav.ProtocolBase):

        @staticmethod
        def on_update(snapshot: DocumentSnapshot, mediator):
            obj: LocationSchema = LocationDomainModel.from_dict(
                snapshot.to_dict(),
                category=snapshot.get('category'),
                coordinates=snapshot.get("coordinates")
            )
            doc_ref: DocumentReference = CTX.db.document(
                f"countries/{obj.country}/records/{obj.doc_id}")
            obj.save(doc_ref=doc_ref)

        on_create = on_update


class RecordMediator(view_mediator_dav.ViewMediatorDeltaDAV):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.store = dict()
        self.change_set = list()

        from apscheduler.schedulers.background import BackgroundScheduler

        sched = BackgroundScheduler()

        # seconds can be replaced with minutes, hours, or days
        sched.add_job(self.commit, 'interval', seconds=2)
        sched.start()

    def close(self):
        self.thread.cancel()

    def commit(self):
        try:
            if len(self.change_set) == 0:
                return
            batch: WriteBatch = CTX.db.batch()
            change_set, self.change_set = self.change_set, list()
            for d in change_set:
                batch.set(
                    **d
                )
            else:
                batch.commit()
        except Exception as e:
            logger.debug(msg=f"error encountered{e.__class__.__name__}")
        finally:
            logger.info("Done executing commit")

    def notify(self, d):
        self.change_set.append(d)

    class Protocol(view_mediator_dav.ProtocolBase):

        @staticmethod
        def on_update(snapshot: DocumentSnapshot, mediator):
            existing = mediator.store.get(snapshot.id)
            delta = snapshot.get("latest") - existing.get("latest")
            logger.info(msg=f"on_update{snapshot.reference.parent.parent.id}: {delta}")
            mediator.notify(
                {
                    "reference": snapshot.reference.parent.parent,
                    "document_data": {
                        snapshot.get("category"): Increment(delta)},
                    "merge": True,
                })
            mediator.store[snapshot.id] = snapshot

        @staticmethod
        def on_create(snapshot: DocumentSnapshot, mediator):
            delta = snapshot.get("latest")
            logger.info(msg=f"on_create{snapshot.reference.parent.parent.id}: {delta}")
            mediator.notify(
                {
                    "reference": snapshot.reference.parent.parent,
                    "document_data": {
                        snapshot.get("category"): Increment(delta)},
                    "merge": True,
                })
            mediator.store[snapshot.id] = snapshot


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
            category=category
        )

        obj.save(transaction=batch)

    batch.commit()
