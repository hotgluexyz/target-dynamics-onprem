"""Dynamics-onprem target class."""
from target_hotglue.target import TargetHotglue
from singer_sdk import typing as th

from target_dynamics_onprem.sinks import (
    Vendors,
    Items,
    PurchaseOrder
)


class TargetDynamicsOnprem(TargetHotglue):
    """Sample target for Dynamics-onprem."""
    name = "target-dynamics-onprem"
    SINK_TYPES = [Vendors, Items, PurchaseOrder]
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


if __name__ == "__main__":
    TargetDynamicsOnprem.cli()
