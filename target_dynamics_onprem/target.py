"""Dynamics-onprem target class."""
from target_hotglue.target import TargetHotglue
from singer_sdk import typing as th

from target_dynamics_onprem.sinks import (
    Vendors,
    Items,
    PurchaseDocuments,
    PurchaseInvoice
)
from singer_sdk.sinks import Sink
from typing import Type


class TargetDynamicsOnprem(TargetHotglue):
    """Sample target for Dynamics-onprem."""
    name = "target-dynamics-onprem"
    SINK_TYPES = [Vendors, Items, PurchaseDocuments, PurchaseInvoice]
    MAX_PARALLELISM = 10
    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
        ),
        th.Property(
            "password",
            th.StringType,
        ),
        th.Property(
            "company_id",
            th.StringType,
        ),
        th.Property(
            "tenant",
            th.StringType,
        ),
        th.Property(
            "url_base",
            th.StringType,
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        for sink_class in self.SINK_TYPES:
            # Search for streams with multiple names
            if stream_name in sink_class.available_names:
                # for Bills if flag usePurchaseInvoice use PurchaseInvoices else use PurchaseDocuments as default
                if stream_name == "Bills":
                    if self.config.get("usePurchaseInvoice"):
                        if sink_class.bills_default:
                            continue
                return sink_class


if __name__ == "__main__":
    TargetDynamicsOnprem.cli()
